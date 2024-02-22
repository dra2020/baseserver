import { Util, FSM } from '@dra2020/baseclient';

// Base types from SQS
import { Environment } from '../sqs/env';
import { SQSManagerBase } from '../sqs/sqsmanager';
import { FsmSend, FsmReceive } from '../sqs/sqsfsm';
import { SQSMessage } from '../sqs/sqsmessage';

// Local types shared by SimpleSQS
import * as Q from './queue';
import { LongPoll } from './longpoll';

class OneRequest
{
  q: Q.Queue;
  fsmReceive: FsmReceive;

  constructor(q: Q.Queue, fsmReceive: FsmReceive)
  {
    this.q = q;
    this.fsmReceive = fsmReceive;
  }

  onFinish(): void
  {
    this.fsmReceive.setState(FSM.FSM_DONE);
  }

  checkLongPoll(): void
  {
    this.fsmReceive.results = this.q.receive();
    if (this.fsmReceive.results.length > 0)
      this.fsmReceive.setState(FSM.FSM_DONE);
  }

  isDone(): boolean
  {
    return this.fsmReceive.done;
  }
}

export class LoopbackSQSManager extends SQSManagerBase
{
  longpoll: LongPoll;
  queues: Q.Queues;

	constructor(env: Environment, url?: string)
  {
    super(env);
    this.longpoll = new LongPoll();
    this.queues = new Q.Queues();
    setInterval(() => { this.longpoll.processDeadlines() }, 2000);
  }

  send(queueName: string, m: SQSMessage): FsmSend
  {
    let q = this.queues.queueOf(queueName);
    q.send(m);
    this.longpoll.checkQueue(q);
    let fsmSend = new FsmSend(this.env, m);
    fsmSend.setState(FSM.FSM_DONE);
    return fsmSend;
  }

  receive(queueName: string): FsmReceive
  {
    let q = this.queues.queueOf(queueName);
    let fsmReceive = new FsmReceive(this.env, queueName);
    fsmReceive.results = q.receive();
    if (fsmReceive.results.length > 0)
      fsmReceive.setState(FSM.FSM_DONE);
    else
      this.longpoll.add(new OneRequest(q, fsmReceive));
    return fsmReceive;
  }

  delete(queueName: string, m: SQSMessage): any
  {
    return null;
  }
}
