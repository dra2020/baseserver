// Node libraries
import * as http from 'http';
import * as url from 'url';

// Project libraries
import { Util } from '@dra2020/baseclient';

import * as JS from '../jsonstream/all';

// Local libraries
import * as Q from './queue';

export type SQSCallback = (err: any, result: Q.QMessages) => void;

const MaxRetries = 10;

class SQSRequest
{
  httpoptions: any;
  data: any;
  cb: SQSCallback;
  jsonread: JS.JSONStreamReader;
  jsonwrite: JS.JSONStreamWriter;
  json: any;
  request: any;
  response: any;
  statusCode: number;
  nRetries: number;

  constructor(httpoptions: any, data: any, cb: SQSCallback)
  {
    this.httpoptions = httpoptions;
    this.data = data;
    this.cb = cb;
    this.nRetries = 0;
    this.doRequest();
  }

  doRequest(): void
  {
    this.json = null;
    this.jsonread = null;
    this.response = null;
    this.statusCode = 0;
    this.request = http.request(this.httpoptions, (res: any) => {
        this.response = res;
        this.statusCode = res.statusCode;
        this.jsonread = new JS.JSONStreamReader();
        this.jsonread.on('end', (json: any) => {
            this.json = json;
            if (this.statusCode == 200)
            {
              if (this.json.error)
                console.log(`memsqs:client: reporting server error ${this.json.error}`);
              this.cb(this.json.error, this.json.result);
            }
            else
            {
              console.log(`memsqs:client: reporting statusCode error ${this.statusCode}`);
              this.onError(`error status: ${this.statusCode}`);
            }
          });
        this.jsonread.on('error', (err: any) => {
            // gateway errors return HTML, not JSON
            if (this.statusCode != 502 && this.statusCode != 504)
            {
              console.log(`memsqs:client: reporting jsonread error ${JSON.stringify(err)}`);
              this.onError(err);
            }
            else
            {
              console.log(`memsqs:client: reporting statusCode error ${this.statusCode}`);
              this.onRequestError(`error status: ${this.statusCode}`);
            }
          });

        this.jsonread.start(res);

        res.on('error', (err: any) => {
            console.log(`memsqs:client: reporting response error ${JSON.stringify(err)}`);
            this.onError((err && err.message) ? err.message : err);
          });
      });

    this.request.on('error', (err: any) => {
        // Immediately fail ECONNREFUSED rather than retrying (server not available, probably restarting)
        if (err && err.code && err.code === 'ECONNREFUSED')
        {
          console.log('memsqs:client: immediately fail on ECONNREFUSED');
          this.onError((err && err.message) ? err.message : err);
        }
        else
          this.onRequestError((err && err.message) ? err.message : err);
      });

    this.request.setHeader('Content-Type', 'application/json; charset=UTF-8');
    this.jsonwrite = new JS.JSONStreamWriter({ outBufferSize: 20000 });
    this.jsonwrite.on('error', (err: any) => {
        console.log(`memsqs:client: encountered request error writing response ${JSON.stringify(err)}`);
        this.onRequestError(err);
      });
    this.jsonwrite.start(this.data, this.request);
  }

  onRequestError(err: any): void
  {
    if (this.nRetries >= MaxRetries)
    {
      console.log(`memsqs:client: giving up after ${this.nRetries} with error ${JSON.stringify(err)}`);
      this.onError(err);
    }
    else
    {
      this.nRetries++;
      this.doRequest();
    }
  }

  onError(err: any): void
  {
    if (this.cb)
    {
      console.log(`memsqs:client: reporting error to callback: ${JSON.stringify(err)}`);
      this.cb(err, null);
      this.cb = null;
    }
  }
}

interface AgentEntry
{
  agent: any;
  inUse: boolean;
}

class AgentPool
{
  agents: AgentEntry[];

  constructor()
  {
    this.agents = [];
  }

  alloc(): any
  {
    for (let i: number = 0; i < this.agents.length; i++)
      if (! this.agents[i].inUse)
      {
        this.agents[i].inUse = true;
        return this.agents[i].agent;
      }

    let ae: AgentEntry = { agent: new http.Agent( { keepAlive: true, maxSockets: 1 } ), inUse: true };
    this.agents.push(ae);
    return ae.agent;
  }

  free(agent: any): void
  {
    for (let i: number = 0; i < this.agents.length; i++)
      if (this.agents[i].agent === agent)
      {
        if (! this.agents[i].inUse)
          throw 'Duplicate free of agent';
        this.agents[i].inUse = false;
        return;
      }

    throw 'Freeing unallocated agent';
  }
}

export class SQSClient
{
  private targeturl?: any;
  private httpoptions?: any;
  private agentPool?: AgentPool;

  constructor(urlstring: string = Q.DefaultServerUrl)
    {
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
    }

  setOptions(queueid: string, options: Q.QQueueOptions, cb: SQSCallback): void
    {
      // Use separate agent for each outstanding call
      let httpoptions: any = Util.shallowAssignImmutable(this.httpoptions, { agent: this.agentPool.alloc() });

      new SQSRequest(httpoptions,
        {
          queueid: queueid,
          api: 'setoptions',
          data: options
        },
        (err: any, result: Q.QMessages) => { this.agentPool.free(httpoptions.agent); cb(err, result); });

    }

  claim(queueid: string, owner: string, groupid: string, cb: SQSCallback): void
    {
      // Use separate agent for each outstanding call
      let httpoptions: any = Util.shallowAssignImmutable(this.httpoptions, { agent: this.agentPool.alloc() });

      new SQSRequest(httpoptions,
        {
          queueid: queueid,
          api: 'claim',
          data: { owner: owner, groupid: groupid }
        },
        (err: any, result: Q.QMessages) => { this.agentPool.free(httpoptions.agent); cb(err, result); });
    }

  send(queueid: string, m: Q.QMessage, cb: SQSCallback): void
    {
      // Use separate agent for each outstanding call
      let httpoptions: any = Util.shallowAssignImmutable(this.httpoptions, { agent: this.agentPool.alloc() });

      new SQSRequest(httpoptions,
        {
          queueid: queueid,
          api: 'send',
          data: m
        },
        (err: any, result: Q.QMessages) => {
            this.agentPool.free(httpoptions.agent);
            if (err)
              result = [ m ];
            cb(err, result);
          });
    }

  remove(queueid: string, m: Q.QMessage, cb: SQSCallback): void
    {
      // Use separate agent for each outstanding call
      let httpoptions: any = Util.shallowAssignImmutable(this.httpoptions, { agent: this.agentPool.alloc() });

      new SQSRequest(httpoptions,
        {
          queueid: queueid,
          api: 'remove',
          data: m
        },
        (err: any, result: Q.QMessages) => { this.agentPool.free(httpoptions.agent); cb(err, result); });
    }

  receive(queueid: string, owner: string, cb: SQSCallback): void
    {
      // Use separate agent for each outstanding call
      let httpoptions: any = Util.shallowAssignImmutable(this.httpoptions, { agent: this.agentPool.alloc() });

      new SQSRequest(httpoptions,
        {
          queueid: queueid,
          api: 'receive',
          owner: owner,
        },
        (err: any, result: Q.QMessages) => { this.agentPool.free(httpoptions.agent); cb(err, result); });
    }
}
