// Public libraries
import { SQS } from '@aws-sdk/client-sqs';

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
const FSM_SAVING = UniqueState++;
const FSM_LOADING = UniqueState++;
const FSM_DELETING = UniqueState++;
const FSM_SENDING = UniqueState++;

function isNonExistentQueue(err: any): boolean
{
  const c = err ? (err.code || err.Code || err.name) : null;
  return c === 'AWS.SimpleQueueService.NonExistentQueue' || c === 'QueueDoesNotExist';
}

function queueAttributes(options: SQSOptions): any
{
  return {
    DelaySeconds: String(options.delaySeconds),
    MaximumMessageSize: String(options.maximumMessageSize),
    MessageRetentionPeriod: String(options.messageRetentionPeriod),
    ReceiveMessageWaitTimeSeconds: String(options.receiveMessageWaitTimeSeconds),
    VisibilityTimeout: String(options.visibilityTimeout),

    FifoQueue: String(true),
    DeduplicationScope: 'messageGroup',
    ContentBasedDeduplication: String(false),
    FifoThroughputLimit: 'perMessageGroupId',
    };
}

// Resolves a queue's URL. Queues are not created here (that needs sqs:CreateQueue): they are created explicitly
// with SQSManager.provisionQueue, run by the dra-cli provision tool. A missing queue is an error.
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
          this.setState(FSM.FSM_PENDING);
          let gparams: any = {
              QueueName: this.sqsManager.toQueueName(this.options.queueName),
            };
          this.sqsManager.sqs.getQueueUrl(gparams, (err: any, data: any) => {
              if (err || !data?.QueueUrl)
              {
                this.err = err;
                if (isNonExistentQueue(err) || !err)
                  this.sqsManager.reportMissing(gparams.QueueName);
                else
                  this.sqsManager.reportError('getQueueUrl', err);
                // Forget this queue so a later send or receive looks it up again (e.g. once it has been provisioned)
                this.sqsManager.forget(this.options.queueName, this);
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

export type QueueStatus = 'ok' | 'missing' | 'created' | 'mismatch' | 'error';

// Reports on (and with create, creates) one FIFO queue. Never deletes or changes an existing queue.
export class FsmProvisionQueue extends FSM.Fsm
{
  sqsManager: SQSManager;
  name: string;
  queueName: string;
  create: boolean;
  status: QueueStatus;
  url: string;
  detail: string[];

  constructor(env: Environment, sqsManager: SQSManager, name: string, create: boolean)
  {
    super(env);
    this.sqsManager = sqsManager;
    this.name = name;
    this.queueName = sqsManager.toQueueName(name);
    this.create = create;
    this.status = 'ok';
    this.detail = [];
  }

  get env(): Environment { return this._env as Environment }

  async run(): Promise<void>
  {
    const sqs = this.sqsManager.sqs;
    let url: string;
    try
    {
      url = (await sqs.getQueueUrl({ QueueName: this.queueName }))?.QueueUrl;
    }
    catch (err)
    {
      if (! isNonExistentQueue(err)) throw err;
    }
    if (! url)
    {
      if (! this.create)
      {
        this.status = 'missing';
        return;
      }
      let options = Util.shallowAssignImmutable(DefaultSQSOptions, { queueName: this.name });
      url = (await sqs.createQueue({ QueueName: this.queueName, Attributes: queueAttributes(options) })).QueueUrl;
      this.env.log.event({ event: 'provision: createQueue', detail: this.queueName });
      this.status = 'created';
    }
    this.url = url;
    let attrs = (await sqs.getQueueAttributes({ QueueUrl: url, AttributeNames: [ 'FifoQueue', 'VisibilityTimeout', 'MessageRetentionPeriod' ] }))?.Attributes || {};
    if (attrs.FifoQueue !== 'true')
    {
      this.status = 'mismatch';
      this.detail.push('queue is not a FIFO queue');
    }
  }

  tick(): void
  {
    if (this.ready && this.state == FSM.FSM_STARTING)
    {
      this.setState(FSM.FSM_PENDING);
      this.run()
        .then(() => this.setState(FSM.FSM_DONE))
        .catch((err: any) => {
            this.status = 'error';
            this.detail.push(String(err?.message || err?.name || err));
            this.sqsManager.reportError('provisionQueue', err);
            this.setState(FSM.FSM_ERROR);
          });
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
            let nBytes = this.dataString.length;
            this.dataString = JSON.stringify(blob.params.id);
            this.env.log.chatter(`sqs: queuing large message (${nBytes} bytes) using blob indirect to ${blob.params.id}`);
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
  msMissingReported: Map<string, number>;

	constructor(env: Environment, options?: SQSOptions)
  {
    super(env);

    // Establish defaults for queues
    DefaultSQSOptions = Util.shallowAssignImmutable(DefaultSQSOptions, options);

    // Credentials come from the SDK's default provider chain (environment key, else the EC2 instance role).
    this.sqs = new SQS({apiVersion: '2012-11-05', region: 'us-west-2'});
    this.nameToQueue = new Map<string, FsmQueue>();
    this.msMissingReported = new Map<string, number>();
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

  // A missing queue is reported prominently, at most every ten minutes per queue since callers retry.
  reportMissing(queueName: string): void
  {
    let msNow = Date.now();
    let msLast = this.msMissingReported.get(queueName);
    if (msLast === undefined || msNow - msLast > 10 * 60 * 1000)
    {
      this.msMissingReported.set(queueName, msNow);
      const detail = `queue ${queueName} does not exist. Queues are not created automatically: create it with the `
                     + `dra-cli provision tool (node dist/dra-provision.bundle.js queues ... --create).`;
      console.error(`sqs: MISSING QUEUE: ${detail}`);
      this.env.log.error({ event: 'sqs: missing queue', detail });
    }
  }

  forget(name: string, q: FsmQueue): void
  {
    if (this.nameToQueue.get(name) === q)
      this.nameToQueue.delete(name);
  }

  // Explicit provisioning of a queue (see FsmProvisionQueue). Needs sqs:GetQueueUrl, GetQueueAttributes and, to
  // create, CreateQueue, so it is meant for administrative tools.
  provisionQueue(name: string, create: boolean): FsmProvisionQueue
  {
    return new FsmProvisionQueue(this.env, this, name, create);
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
