// Node libraries
import * as http from 'http';
import * as url from 'url';

import { Util } from '@dra2020/baseclient';

import * as Q from './queue';
import { MessageData, MessageParams } from './messageparams';
import { LongPoll } from './longpoll';
import { SQSMessage } from '../sqs/sqsmessage';

export class SimpleSQSServer
{
  port: number;
  server: any;
  longpoll: LongPoll;
  queues: Q.Queues;
  nReq: number;
  nFailReq: number;
  nEmptyReq: number;
  nFailRes: number;

  constructor(port: number = Q.DefaultPort)
  {
    this.queues = new Q.Queues();
    this.longpoll = new LongPoll();
    this.port = port;
    this.nReq = 0;
    this.nFailReq = 0;
    this.nEmptyReq = 0;
    this.nFailRes = 0;

    this.server = http.createServer();
    this.server.keepAliveTimeout = 61 * 1000; // Don't interfere with longpoll timeout
    this.server.headersTimeout = 65 * 1000;   // Longer than keepAliveTimeout

    this.server.on('request', (req: any, res: any) => {
        this.nReq++;
        new OneRequest(this, req, res);
      });

    this.server.on('close', () => {
        this.server = null;
      });

    this.server.on('error', (err: any) => {
        if (err && err.message) err = err.message;
        console.log(`simplesqs: server: unexpected error ${err}`);
        this.server = null;
      });

    try
    {
      this.server.listen(this.port);
    }
    catch (err)
    {
      console.log(`simplesqs: server: unexpected exception on listen: ${JSON.stringify(err)}`);
      this.server = null;
    }

    setInterval(() => { this.report() }, 60000);
    setInterval(() => { this.longpoll.processDeadlines() }, 2000);
  }

  get running(): boolean
  {
    return this.server != null;
  }

  close(): void
  {
    if (this.server)
      this.server.close();
  }

  report(): void
  {
    console.log(`simplesqs: ${this.nReq} total requests; ${this.nEmptyReq} empty, ${this.nFailReq} errors in request format, ${this.nFailRes} errors during response`);
    this.queues.report();
  }
}

class OneRequest
{
  server: SimpleSQSServer;
  req: any;
  res: any;
  bufs: Buffer[];
  body: any;
  q: Q.Queue;

  constructor(server: SimpleSQSServer, req: any, res: any)
  {
    this.server = server;
    this.req = req;
    this.res = res;
    this.bufs = [];
    this.body = { statuscode: 0, result: null };
    if (res.statusCode !== 200)
    {
      // Non-JSON error - don't try to parse
      this.res = null;
      this.server.nFailReq++;
      this.onError(`statusCode ${res.statusCode} on request; ignoring`);
    }
    else
    {
      this.req.on('end', () => { this.onDone() });
      this.req.on('close', () => { this.onDone() });
      this.req.on('error', () => { this.server.nFailReq++; this.res = null; this.onError('reading request') });
      this.req.on('data', (b: Buffer) => { this.bufs.push(b) });
      this.res.on('error', (err: any) => {
          this.res = null;
          this.server.nFailRes++;
          this.onError(`writing response`);
        });
    }
  }

  isDone(): boolean
  {
    return this.res == null;
  }

  checkLongPoll(): void
  {
    this.body.result = this.q.receive();
    if (this.body.result.length > 0)
      this.onFinish();
  }

  onDone(): void
  {
    if (this.res)
    {
      try
      {
        let buf = Buffer.concat(this.bufs);
        if (buf && buf.length > 0)
        {
          let s = buf.toString('utf8');
          let p = JSON.parse(s) as MessageParams;
          if (p && p.command)
          {
            switch (p.command)
            {
              case 'send':
                if (p.batch)
                {
                  let qs = new Map<string, Q.Queue>();
                  p.batch.forEach((md: MessageData) => {
                      let q = this.server.queues.queueOf(md.queueName);
                      q.send(md.message);
                      qs.set(md.queueName, q);
                    });
                  qs.forEach(q => this.server.longpoll.checkQueue(q));
                }
                else
                {
                  let q = this.server.queues.queueOf(p.queueName);
                  q.send(p.message);
                  this.server.longpoll.checkQueue(q);
                }
                break;

              case 'receive':
                this.q = this.server.queues.queueOf(p.queueName);
                this.body.result = this.q.receive();
                if (this.body.result.length == 0)
                {
                  this.server.longpoll.add(this);
                  return; // don't finish request
                }
                break;
            }
          }
          else
            this.onRequestError();
          this.onFinish();
        }
        else
          this.onEmptyRequest();
      }
      catch (err)
      {
        this.onRequestError();
      }
    }
  }

  onRequestError(): void
  {
    if (this.res)
    {
      this.server.nFailReq++;
      this.res.writeHead(400, { 'Content-Type': 'application/json; charset=UTF-8' });
      this.body = { statuscode: 1, error: 'invalid request' };
      this.res.end(JSON.stringify(this.body));
      this.res = null;
    }
  }

  onEmptyRequest(): void
  {
    this.server.nEmptyReq++;
    this.onFinish();
  }

  onError(s: string): void
  {
    console.log(`simplesqs error: ${s}`);
    this.body.statuscode = 1;
    this.body.error = 'failure';
    this.onFinish();
  }

  onFinish(): void
  {
    if (this.res)
    {
      this.res.writeHead(200, { 'Content-Type': 'application/json; charset=UTF-8' });
      this.res.end(JSON.stringify(this.body));
      this.res = null;
    }
  }
}
