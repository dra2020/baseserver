// Public libraries
import * as AWS from 'aws-sdk';

import { Util, FSM, Context, LogAbstract } from '@dra2020/baseclient';
import * as Storage from '../storage/all';

interface Environment
{
  context: Context.IContext,
  log: LogAbstract.ILog,
  fsmManager: FSM.FsmManager,
  storageManager: Storage.StorageManager,
  sqsManager: SQSManager,
}

export interface SQSOptions
{
  queueName: string,
  delaySeconds?: number,
  maximumMessageSize?: number,
  messageRetentionPeriod?: number,
  receiveMessageWaitTimeSeconds?: number,
  visibilityTimeout?: number,
  autoDelete?: boolean,
}

const DefaultSQSOptions = {
  queueName: '',
  delaySeconds: 0,
  maximumMessageSize: 262144,
  messageRetentionPeriod: 60,
  receiveMessageWaitTimeSeconds: 5,
  visibilityTimeout: 30,
  autoDelete: true,
};

export interface SQSMessage
{
  groupId: string,
  data: any,
  messageId?: string,     // Only on receive
  receiptHandle?: string, // Only on receive
}

let UniqueState = FSM.FSM_CUSTOM1;
const FSM_CREATING = UniqueState++;
const FSM_SAVING = UniqueState++;
const FSM_LOADING = UniqueState++;
const FSM_DELETING = UniqueState++;
const FSM_SENDING = UniqueState++;

class SQSBlob extends Storage.StorageBlob
{
  constructor(env: Environment, params: Storage.BlobParams)
  {
    if (params.bucket == null)
      params.bucket = env.context.xflag('production') ? 'transfers' : 'transfers-dev';
    params.deleteAfterLoad = true;
    super(env, params);
  }

  get data(): string { return this.params.loadTo as string }

  static createForLoad(env: Environment, id: string): SQSBlob
  {
    let params: Storage.BlobParams = {
      id,
      loadToType: 'string',
      };
    let blob = new SQSBlob(env, params);
    blob.startLoad(env.storageManager);
    return blob;
  }

  static createForSave(env: Environment, data: string): SQSBlob
  {
    let params: Storage.BlobParams = {
        id: Util.createGuid(),
        saveFromType: 'string',
        saveFrom: data,
        ContentEncoding: 'gzip',
        ContentType: 'text/plain; charset=UTF-8',
      };
    let blob = new SQSBlob(env, params);
    blob.setDirty();
    blob.checkSave(env.storageManager);
    return blob;
  }
}

class FsmQueue extends FSM.Fsm
{
  tailQueue: Map<string, FsmSend>;
  options: SQSOptions;
  url: string;
  err: any;

  constructor(env: Environment, options: SQSOptions)
  {
    super(env);
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
              QueueName: this.env.sqsManager.toQueueName(this.options.queueName),
            };
          this.env.sqsManager.sqs.getQueueUrl(gparams, (err: any, data: any) => {
              if (err && err.code !== 'AWS.SimpleQueueService.NonExistentQueue')
              {
                this.err = err;
                this.env.sqsManager.reportError('getQueueUrl', err);
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
            QueueName: this.env.sqsManager.toQueueName(this.options.queueName),
            Attributes: attributes,
            };
          this.env.sqsManager.sqs.createQueue(params, (err: any, data: any) => {
              if (err)
              {
                this.err = err;
                this.env.sqsManager.reportError('createQueue', err);
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

export class FsmSend extends FSM.Fsm
{
  q: FsmQueue;
  m: SQSMessage;
  err: any;
  dataString: string;
  trace: LogAbstract.AsyncTimer;

  constructor(env: Environment, q: FsmQueue, m: SQSMessage)
  {
    super(env);
    this.q = q;
    this.m = m;
    this.waitOn(this.q);                          // Ensure queue initialized
    this.waitOn(this.q.tailQueue.get(m.groupId)); // Ensure ordering
    this.q.tailQueue.set(m.groupId, this);
  }

  get env(): Environment { return this._env as Environment }

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
          this.env.sqsManager.sqs.sendMessage(params, (err: any, data: any) => {
              trace.log();
              if (this.q.tailQueue.get(this.m.groupId) === this)
                this.q.tailQueue.delete(this.m.groupId);
              if (err)
              {
                this.err = err;
                this.env.sqsManager.reportError('sendMessage', err);
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

export class FsmReceive extends FSM.Fsm
{
  err: any;
  q: FsmQueue;
  results: SQSMessage[];
  blobs: SQSBlob[];
  trace: LogAbstract.AsyncTimer;

  constructor(env: Environment, q: FsmQueue)
  {
    super(env);
    this.results = [];
    this.q = q;
    this.waitOn(q);
  }

  get env(): Environment { return this._env as Environment }

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
          this.env.sqsManager.sqs.receiveMessage(params, (err: any, data: any) => {
              trace.log();
              if (err)
              {
                this.err = err;
                this.env.sqsManager.reportError('sendMessage', err);
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
                this.waitOn(this.env.sqsManager.delete(this.q.options.queueName, m));
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

export class FsmDelete extends FSM.Fsm
{
  q: FsmQueue;
  m: SQSMessage;
  err: any;

  constructor(env: Environment, q: FsmQueue, m: SQSMessage)
  {
    super(env);
    this.q = q;
    this.m = m;
    this.waitOn(this.q);
  }

  get env(): Environment { return this._env as Environment }

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
          this.env.sqsManager.sqs.deleteMessage(params, (err: any, data: any) => {
              trace.log();
              if (err)
              {
                this.err = err;
                this.env.sqsManager.reportError('deleteMessage', err);
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

export class SQSManager
{
	env: Environment;
  sqs: any;
  nameToQueue: Map<string, FsmQueue>;

	constructor(env: Environment)
  {
    this.env = env;

    if (this.env.context.xstring('aws_access_key_id') === undefined
        || this.env.context.xstring('aws_secret_access_key') === undefined)
    {
      console.log('AWS not configured: exiting');
      process.exit(1);
    }

    this.sqs = new AWS.SQS({apiVersion: '2012-11-05', region: 'us-west-2'});
    this.nameToQueue = new Map<string, FsmQueue>();
    this.env.sqsManager = this;
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
      q = new FsmQueue(this.env, options);
      this.nameToQueue.set(name, q);
    }
    return q;
  }

  send(name: string, m: SQSMessage): FsmSend
  {
    return new FsmSend(this.env, this.queueOf(name), m);
  }

  receive(name: string): FsmReceive
  {
    return new FsmReceive(this.env, this.queueOf(name));
  }

  delete(name: string, m: SQSMessage): FsmDelete
  {
    return new FsmDelete(this.env, this.queueOf(name), m);
  }
}
