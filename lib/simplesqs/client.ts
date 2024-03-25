// Node libraries
import * as http from 'http';
import * as url from 'url';

// Project libraries
import { Util, FSM } from '@dra2020/baseclient';

// Local libraries
import { Environment } from '../sqs/env';
import { SQSMessage } from '../sqs/sqsmessage';

import * as Q from './queue';
import { AgentPool } from './agentpool';
import { MessageData, MessageParams } from './messageparams';

let nPending = 0;

class FsmClientRequest extends FSM.Fsm
{
  client: SimpleSQSClient;
  httpoptions: any;
  params: MessageParams;
  request: any;
  response: any;
  statusCode: number;
  bufs: Buffer[];
  body: any;

  constructor(env: Environment, client: SimpleSQSClient, params: MessageParams)
  {
    super(env);
    this.client = client;
    this.httpoptions = client.allocOptions();
    this.params = params;
    this.bufs = [];
  }

  get env(): Environment { return this._env as Environment }

  onDone(): void
  {
    if (this.response)
    {
      try
      {
        let b = Buffer.concat(this.bufs);
        let s = b.toString('utf8');
        this.body = JSON.parse(s);
        delete this.bufs;
        delete this.response; // Only go through once
        this.setState(this.body?.statuscode === 0 ? FSM.FSM_DONE : FSM.FSM_ERROR);
        nPending--;
        this.client.checkQueue();
        this.end();
        this.client.freeOptions(this.httpoptions);
      }
      catch (err)
      {
        // Gateway error leading to plain/text rather than application/json?
        this.onError('exception parsing response');
      }
    }
  }

  onError(msg: string): void
  {
    console.log(`simplesqs error: ${msg}`);
    this.setState(FSM.FSM_ERROR);
    nPending--;
    this.client.checkQueue();
  }

  tick(): void
  {
    if (this.ready)
    {
      nPending++;
      if (nPending > 20)
        this.env.log.chatter(`simplesqs: anomalous number of pending client requests (${nPending})`);
      this.response = null;
      this.statusCode = 0;
      this.request = http.request(this.httpoptions, (res: any) => {
          this.response = res;
          this.statusCode = res.statusCode;
          if (this.statusCode !== 200)
            this.onError(`http error ${this.statusCode}`);
          else
          {
            res.on('data', (b: Buffer) => { this.bufs.push(b) });
            res.on('close', () => { this.onDone() });
            res.on('end', () => { this.onDone() });
            res.on('error', () => { this.onError('reading response') } );
          }
        });

      this.request.on('error', (err: any) => { this.onError('sending request') });
      this.request.setHeader('Content-Type', 'application/json; charset=UTF-8');
      this.request.end(JSON.stringify(this.params), 'utf8');
    }
  }
}

export class FsmClientSend extends FsmClientRequest
{
  constructor(env: Environment, client: SimpleSQSClient, batch: MessageData[])
  {
    super(env, client, { command: 'send', batch });
  }

  end(): void
  {
  }
}

export class FsmClientReceive extends FsmClientRequest
{
  results: SQSMessage[];

  constructor(env: Environment, client: SimpleSQSClient, queueName: string)
  {
    super(env, client, { command: 'receive', queueName });
  }

  end(): void
  {
    this.results = this.body?.result;
  }
}

export class FsmClientQueueSend extends FSM.Fsm
{
  md: MessageData;

  constructor(env: Environment, md: MessageData)
  {
    super(env);
    this.md = md;
  }

  sent(fsmSend: FsmClientSend): void
  {
    this.waitOn(fsmSend);
    this.setState(FSM.FSM_PENDING);
  }

  tick(): void
  {
    if (this.ready && this.state === FSM.FSM_PENDING)
      this.setState(this.isDependentError ? FSM.FSM_ERROR : FSM.FSM_DONE);
  }
}

const SEND_BATCH_SIZE = 20;

export class SimpleSQSClient
{
  env: Environment;
  targeturl: any;
  httpoptions: any;
  agentPool: AgentPool;
  fsmSendQueued: FsmClientQueueSend[];
  fsmSendOutstanding: FsmClientSend;

  constructor(env: Environment, urlstring: string = Q.DefaultServerUrl)
  {
    this.env = env;
    this.targeturl = new url.URL(urlstring);
    this.httpoptions = {
      protocol: this.targeturl.protocol,
      host: this.targeturl.hostname,
      port: this.targeturl.port ? this.targeturl.port : Q.DefaultPort,
      agent: null,
      method: 'POST',
      path: '/',
      };
    this.agentPool = new AgentPool();
    this.fsmSendQueued = [];
  }

  checkQueue(): void
  {
    if (this.fsmSendOutstanding?.done)
      delete this.fsmSendOutstanding;
    if (! this.fsmSendOutstanding && this.fsmSendQueued.length)
    {
      // Get next batch to send
      let fsmBatch = this.fsmSendQueued.splice(0, SEND_BATCH_SIZE);
      let batch: MessageData[] = fsmBatch.map(f => f.md);
      // Send it
      this.fsmSendOutstanding = new FsmClientSend(this.env, this, batch);
      // Notify queued sends
      fsmBatch.forEach(f => f.sent(this.fsmSendOutstanding));
    }
  }

  allocOptions(): any
  {
    return Util.shallowAssignImmutable(this.httpoptions, { agent: this.agentPool.alloc() });
  }

  freeOptions(options: any): void
  {
    this.agentPool.free(options.agent);
  }

  send(queueName: string, message: SQSMessage): FsmClientQueueSend
  {
    let fsm = new FsmClientQueueSend(this.env, { queueName, message });
    this.fsmSendQueued.push(fsm);
    this.checkQueue();
    return fsm;
  }

  receive(queueName: string): FsmClientReceive
  {
    return new FsmClientReceive(this.env, this, queueName);
  }
}
