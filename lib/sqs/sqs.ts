// Public libraries
import * as AWS from 'aws-sdk';

import { Util, FSM, Context, LogAbstract } from '@dra2020/baseclient';
import * as Storage from '../storage/all';

import { Environment } from './env';
import { SQSBlob } from './sqsblob';
import { SQSMessage } from './sqsmessage';
import { FsmSend, FsmReceive, FsmDelete } from './sqsfsm';
import { SQSManagerBase } from './sqsmanager';
import { SQSOptions } from './sqsoptions';

let DefaultSQSOptions = {
  queueName: '',
  delaySeconds: 0,
  maximumMessageSize: 262144,
  messageRetentionPeriod: 60,
  receiveMessageWaitTimeSeconds: 5,
  visibilityTimeout: 30,
  autoDelete: true,
};

let UniqueState = FSM.FSM_CUSTOM1;
const FSM_CREATING = UniqueState++;
const FSM_SAVING = UniqueState++;
const FSM_LOADING = UniqueState++;
const FSM_DELETING = UniqueState++;
const FSM_SENDING = UniqueState++;

class FsmQueue extends FSM.Fsm
{
  sqsManager: SQSManager;
  tailQueue: Map<string, FsmSend>;
  options: SQSOptions;
  url: string;
  err: any;

  constructor(env: Environment, sqsManager: SQSManager, options: SQSOptions)
  {
    super(env);
    this.sqsManager = sqsManager;
    this.tailQueue = new Map<string, FsmSend>();
    this.options = Util.shallowAssignImmutable(DefaultSQSOptions, options);
  }

  get env(): Environment { return this._env as Environment }

  finish(url: string): void
  {
    this.url = url;
    this.setState(FSM.FSM_DONE);
  }

  tick(): void
  {
    if (this.ready && this.isDependentError)
      this.setState(FSM.FSM_ERROR);
    else if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          let gparams: any = {
              QueueName: this.sqsManager.toQueueName(this.options.queueName),
            };
          this.sqsManager.sqs.getQueueUrl(gparams, (err: any, data: any) => {
              if (err && err.code !== 'AWS.SimpleQueueService.NonExistentQueue')
              {
                this.err = err;
                this.sqsManager.reportError('getQueueUrl', err);
                this.setState(FSM.FSM_ERROR);
              }
              else if (data?.QueueUrl)
                this.finish(data.QueueUrl);
              else
                this.setState(FSM_CREATING);
            });
          break;

        case FSM_CREATING:
          let attributes: any = {
            DelaySeconds: String(this.options.delaySeconds),
            MaximumMessageSize: String(this.options.maximumMessageSize),
            MessageRetentionPeriod: String(this.options.messageRetentionPeriod),
            ReceiveMessageWaitTimeSeconds: String(this.options.receiveMessageWaitTimeSeconds),
            VisibilityTimeout: String(this.options.visibilityTimeout),

            FifoQueue: String(true),
            DeduplicationScope: 'messageGroup',
            ContentBasedDeduplication: String(false),
            FifoThroughputLimit: 'perMessageGroupId',
            };
          let params: any = {
            QueueName: this.sqsManager.toQueueName(this.options.queueName),
            Attributes: attributes,
            };
          this.sqsManager.sqs.createQueue(params, (err: any, data: any) => {
              if (err)
              {
                this.err = err;
                this.sqsManager.reportError('createQueue', err);
                this.setState(FSM.FSM_ERROR);
              }
              else
                this.finish(data.QueueUrl);
            });
          break;
      }
    }
  }
}

export class FsmSendSQS extends FsmSend
{
  q: FsmQueue;
  err: any;
  dataString: string;
  trace: LogAbstract.AsyncTimer;

  constructor(env: Environment, q: FsmQueue, m: SQSMessage)
  {
    super(env, m);
    this.q = q;
    this.waitOn(this.q);                          // Ensure queue initialized
    this.waitOn(this.q.tailQueue.get(m.groupId)); // Ensure ordering
    this.q.tailQueue.set(m.groupId, this);
  }

  tick(): void
  {
    if (this.ready && this.isDependentError)
      this.setState(FSM.FSM_ERROR);
    else if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.setState(FSM_SAVING);
          this.dataString = JSON.stringify(this.m.data);
          if (this.dataString.length > (this.q.options.maximumMessageSize-1000))
          {
            this.trace = new LogAbstract.AsyncTimer(this.env.log, 'sqs: blobsave');
            let blob = SQSBlob.createForSave(this.env, this.dataString);
            this.waitOn(blob.fsmSave);
            this.dataString = JSON.stringify(blob.params.id);
            this.env.log.chatter(`sqs: queuing large message (${this.dataString.length} bytes) using blob indirect to ${blob.params.id}`);
            break;
          }
          // fall through on small messages

        case FSM_SAVING:
          if (this.trace) this.trace.log();
          this.setState(FSM_SENDING);
          let params: any = {
              MessageBody: this.dataString,
              QueueUrl: this.q.url,
              MessageGroupId: this.m.groupId,
              MessageDeduplicationId: Util.createGuid(),
            };
          let trace = new LogAbstract.AsyncTimer(this.env.log, `sqs: send`);
          this.q.sqsManager.sqs.sendMessage(params, (err: any, data: any) => {
              trace.log();
              if (this.q.tailQueue.get(this.m.groupId) === this)
                this.q.tailQueue.delete(this.m.groupId);
              if (err)
              {
                this.err = err;
                this.q.sqsManager.reportError('sendMessage', err);
                this.setState(FSM.FSM_ERROR);
              }
              else
                this.setState(FSM.FSM_DONE);
            });
          break;

        case FSM_SENDING:
          // Not reached - set to done in callback
          break;
      }
    }
  }
}

export class FsmReceiveSQS extends FsmReceive
{
  err: any;
  q: FsmQueue;
  blobs: SQSBlob[];
  trace: LogAbstract.AsyncTimer;

  constructor(env: Environment, q: FsmQueue)
  {
    super(env, q.url);
    this.q = q;
    this.waitOn(q);
  }

  tick(): void
  {
    if (this.ready && this.isDependentError)
      this.setState(FSM.FSM_ERROR);
    else if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          let params: any = {
              QueueUrl: this.q.url,
              MaxNumberOfMessages: '10',
            };
          let trace = new LogAbstract.AsyncTimer(this.env.log, `sqs: receive`);
          this.q.sqsManager.sqs.receiveMessage(params, (err: any, data: any) => {
              trace.log();
              if (err)
              {
                this.err = err;
                this.q.sqsManager.reportError('sendMessage', err);
                this.setState(FSM.FSM_ERROR);
              }
              else
              {
                if (data && Array.isArray(data.Messages))
                {
                  data.Messages.forEach((d: any) => {
                      try
                      {
                        let m = {
                          messageId: d?.MessageId || '',
                          receiptHandle: d?.ReceiptHandle || '',
                          groupId: d?.Attributes?.MessageGroupId,
                          data: JSON.parse(d?.Body),
                          };
                        this.results.push(m);
                      }
                      catch (e)
                      {
                        console.log(`sqs.receiveMessage: crash on JSON parse of result`);
                      }
                    });
                }

                // Now load any blob indirects
                this.blobs = this.results.map((m: SQSMessage) => {
                    if (typeof m.data === 'string')
                    {
                      if (! this.trace) this.trace = new LogAbstract.AsyncTimer(this.env.log, 'sqs: blobload');
                      let blob = SQSBlob.createForLoad(this.env, m.data);
                      this.waitOn(blob.fsmLoad);
                      return blob;
                    }
                    else
                      return null;
                  });
                this.setState(FSM_LOADING);
              }
            });
          break;

        case FSM_LOADING:
          if (this.trace) this.trace.log();
          delete this.trace;
          this.blobs.forEach((blob: SQSBlob, i: number) => {
              if (blob)
              {
                try
                {
                  this.results[i].data = JSON.parse(blob.data);
                }
                catch (err)
                {
                  console.log(`sqs.receiveMessage: crash on JSON parse of indirect large message body`);
                }
              }
            });

          // See if we should autodelete the messages (rather than waiting for processing to finish)
          if (this.q.options.autoDelete)
          {
            this.results.forEach((m: SQSMessage) => {
                this.waitOn(this.q.sqsManager.delete(this.q.options.queueName, m));
              });
          }
          this.setState(FSM_DELETING);
          break;

        case FSM_DELETING:
          this.setState(FSM.FSM_DONE);
          break;
      }
    }
  }
}

export class FsmDeleteSQS extends FsmDelete
{
  q: FsmQueue;
  err: any;

  constructor(env: Environment, q: FsmQueue, m: SQSMessage)
  {
    super(env, m);
    this.q = q;
    this.waitOn(this.q);
  }

  tick(): void
  {
    if (this.ready && this.isDependentError)
      this.setState(FSM.FSM_ERROR);
    else if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          let params: any = {
              QueueUrl: this.q.url,
              ReceiptHandle: this.m.receiptHandle,
            };
          let trace = new LogAbstract.AsyncTimer(this.env.log, 'sqs: delete');
          this.q.sqsManager.sqs.deleteMessage(params, (err: any, data: any) => {
              trace.log();
              if (err)
              {
                this.err = err;
                this.q.sqsManager.reportError('deleteMessage', err);
                this.setState(FSM.FSM_ERROR);
              }
              else
                this.setState(FSM.FSM_DONE);
            });
          break;
      }
    }
  }
}

export class SQSManager extends SQSManagerBase
{
	env: Environment;
  sqs: any;
  nameToQueue: Map<string, FsmQueue>;

	constructor(env: Environment, options?: SQSOptions)
  {
    super(env);

    // Establish defaults for queues
    DefaultSQSOptions = Util.shallowAssignImmutable(DefaultSQSOptions, options);

    if (this.env.context.xstring('aws_access_key_id') === undefined
        || this.env.context.xstring('aws_secret_access_key') === undefined)
    {
      console.log('AWS not configured: exiting');
      process.exit(1);
    }

    this.sqs = new AWS.SQS({apiVersion: '2012-11-05', region: 'us-west-2'});
    this.nameToQueue = new Map<string, FsmQueue>();
  }

  toQueueName(n: string): string
  {
    let p = this.env.context.xflag('production');
    let mode = p ? 'prod' : 'dev';
    return `dra_${mode}_${n}.fifo`;
  }

  reportError(call: string, err: any): void
  {
    if (err)
    {
      this.env.log.chatter(`sqs: ${call}: ERROR:\n${JSON.stringify(err, null, 1)}`);
      this.env.log.error({ event: `sqs: ${call}`, detail: `error: ${JSON.stringify(err)}` });
    }
  }

  queueOf(name: string): FsmQueue
  {
    let q = this.nameToQueue.get(name);
    if (! q)
    {
      let options = Util.shallowAssignImmutable(DefaultSQSOptions, { queueName: name });
      q = new FsmQueue(this.env, this, options);
      this.nameToQueue.set(name, q);
    }
    return q;
  }

  send(name: string, m: SQSMessage): FsmSend
  {
    return new FsmSendSQS(this.env, this.queueOf(name), m);
  }

  receive(name: string): FsmReceive
  {
    return new FsmReceiveSQS(this.env, this.queueOf(name));
  }

  delete(name: string, m: SQSMessage): FsmDelete
  {
    return new FsmDeleteSQS(this.env, this.queueOf(name), m);
  }
}
