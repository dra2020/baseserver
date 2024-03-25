import { Util, FSM, Context, LogAbstract } from '@dra2020/baseclient';
import * as Storage from '../storage/all';

import { Environment } from '../sqs/env';
import { SQSManagerBase } from '../sqs/sqsmanager';
import { FsmSend, FsmReceive } from '../sqs/sqsfsm';
import { SQSMessage } from '../sqs/sqsmessage';
import { SQSOptions } from '../sqs/sqsoptions';
import { SQSBlob } from '../sqs/sqsblob';

import * as Client from './client';
import * as Q from './queue';

const DefaultSQSOptions = {
  queueName: '',
  delaySeconds: 0,
  maximumMessageSize: 262144,
  messageRetentionPeriod: 60,
  receiveMessageWaitTimeSeconds: 5,
  visibilityTimeout: 30,
  autoDelete: true,
}

let UniqueState = FSM.FSM_CUSTOM1;
const FSM_CREATING = UniqueState++;
const FSM_SAVING = UniqueState++;
const FSM_LOADING = UniqueState++;
const FSM_DELETING = UniqueState++;
const FSM_SENDING = UniqueState++;
const FSM_RECEIVING = UniqueState++;

class FsmQueue extends FSM.Fsm
{
  client: Client.SimpleSQSClient;
  tailQueue: Map<string, FsmSend>;
  options: SQSOptions;
  queueName: string;
  err: any;

  constructor(env: Environment, client: Client.SimpleSQSClient, options: SQSOptions)
  {
    super(env);
    this.client = client;
    this.tailQueue = new Map<string, FsmSend>();
    this.options = Util.shallowAssignImmutable(DefaultSQSOptions, options);
    this.queueName = this.options.queueName;
    this.setState(FSM.FSM_DONE);
  }
}

export class FsmSendSimple extends FsmSend
{
  q: FsmQueue;
  err: any;
  dataString: string;
  trace: LogAbstract.AsyncTimer;
  fsmSend: Client.FsmClientQueueSend;

  constructor(env: Environment, q: FsmQueue, m: SQSMessage)
  {
    super(env, m);
    this.q = q;
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
          let nBytes = this.dataString.length;
          if (nBytes > (this.q.options.maximumMessageSize-1000))
          {
            this.trace = new LogAbstract.AsyncTimer(this.env.log, 'simplesqs: blobsave');
            let blob = SQSBlob.createForSave(this.env, this.dataString);
            this.waitOn(blob.fsmSave);
            this.dataString = JSON.stringify(blob.params.id);
            // Log excessive sizes
            if (nBytes > 2000000)
              this.env.log.chatter(`simplesqs: queuing large message (${nBytes} bytes) using blob indirect to ${blob.params.id}`);
            break;
          }
          // fall through on small messages

        case FSM_SAVING:
          if (this.trace) this.trace.log();
          this.setState(FSM_SENDING);
          let m: any = {
              queueName: this.q.queueName,
              groupId: this.m.groupId,
              data: this.dataString,
            };
          this.trace = new LogAbstract.AsyncTimer(this.env.log, `simplesqs: send`);
          this.fsmSend = this.q.client.send(this.q.queueName, m);
          this.waitOn(this.fsmSend);
          this.setState(FSM_SENDING);
          break;

        case FSM_SENDING:
          this.trace.log();
          this.setState(FSM.FSM_DONE);
          break;
      }
    }
  }
}

export class FsmReceiveSimple extends FsmReceive
{
  err: any;
  q: FsmQueue;
  blobs: SQSBlob[];
  trace: LogAbstract.AsyncTimer;
  fsmReceive: Client.FsmClientReceive;

  constructor(env: Environment, q: FsmQueue)
  {
    super(env, q.queueName);
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
          let trace = new LogAbstract.AsyncTimer(this.env.log, `simplesqs: receive`);
          this.fsmReceive = this.q.client.receive(this.q.queueName);
          this.waitOn(this.fsmReceive);
          this.setState(FSM_RECEIVING);
          break;

        case FSM_RECEIVING:
          this.fsmReceive.results.forEach((m: SQSMessage) => {
              try
              {
                m.data = JSON.parse(m.data);
                this.results.push(m);
              }
              catch (e)
              {
                console.log(`simplesqs.receive: crash on JSON parse of result`);
              }
            });

          // Now load any blob indirects
          this.blobs = this.results.map((m: SQSMessage) => {
              if (typeof m.data === 'string')
              {
                if (! this.trace) this.trace = new LogAbstract.AsyncTimer(this.env.log, 'simplesqs: blobload');
                let blob = SQSBlob.createForLoad(this.env, m.data);
                this.waitOn(blob.fsmLoad);
                return blob;
              }
              else
                return null;
            });
          this.setState(FSM_LOADING);
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
                  this.results[i].data = {};
                  console.log(`sqs.receiveMessage: crash on JSON parse of indirect large message body`);
                }
              }
            });
          this.setState(FSM.FSM_DONE);
          break;
      }
    }
  }
}

export class SimpleSQSManager extends SQSManagerBase
{
  nameToQueue: Map<string, FsmQueue>;
  client: Client.SimpleSQSClient;

	constructor(env: Environment, url?: string)
  {
    super(env);
    this.client = new Client.SimpleSQSClient(env, url);
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

  queueOf(queueName: string): FsmQueue
  {
    queueName = this.toQueueName(queueName);
    let q = this.nameToQueue.get(queueName);
    if (! q)
    {
      let options = Util.shallowAssignImmutable(DefaultSQSOptions, { queueName });
      q = new FsmQueue(this.env, this.client, options);
      this.nameToQueue.set(queueName, q);
    }
    return q;
  }

  send(queueName: string, m: SQSMessage): FsmSend
  {
    return new FsmSendSimple(this.env, this.queueOf(queueName), m);
  }

  receive(queueName: string): FsmReceive
  {
    return new FsmReceiveSimple(this.env, this.queueOf(queueName));
  }

  delete(queueName: string, m: SQSMessage): any
  {
    return null;
  }
}
