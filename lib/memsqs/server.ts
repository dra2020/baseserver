// Node libraries
import * as http from 'http';
import * as url from 'url';

// Shared libraries
import * as JS from '../jsonstream/all';

// Local libraries
import * as Q from './queue';

const LongPollTimeout: number = 5000;

export class SQSServer
{
  private port: number;
  private server: any;
  queuemanager: Q.QQueueManager;
  longpoll: LongPoll;

  constructor(port: number = Q.DefaultPort)
    {
      this.queuemanager = new Q.QQueueManager();
      this.longpoll = new LongPoll();
      this.port = port;

      this.server = http.createServer();
      this.server.keepAliveTimeout = 61 * 1000; // Don't interfere with longpoll timeout
      this.server.headersTimeout = 65 * 1000;   // Longer than keepAliveTimeout

      this.server.on('request', (req: any, res: any) => {
          new OneRequest(this, req, res);
        });

      this.server.on('close', () => {
          this.server = null;
        });

      this.server.on('error', (err: any) => {
          if (err && err.message) err = err.message;
          console.log(`memsqs: server: unexpected error ${err}`);
          this.server = null;
        });

      try
      {
        this.server.listen(this.port);
      }
      catch (err)
      {
        console.log(`memsqs: server: unexpected exception on listen: ${JSON.stringify(err)}`);
        this.server = null;
      }
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
}

interface LongPollRequest
{
  deadline: number;
  request: OneRequest;
}

class LongPoll
{
  requests: LongPollRequest[];

  constructor()
    {
      this.requests = [];
      this.tick = this.tick.bind(this);
      setTimeout(this.tick, 1000);
    }

  check(): void
    {
      let deadline: number = (new Date()).getTime();
      for (let i: number = 0; i < this.requests.length; )
      {
        let r = this.requests[i];
        if (! r.request.onRetry())
          if (r.deadline < deadline)
            r.request.onFinish();
        if (r.request.isDone())
          this.requests.splice(i, 1);
        else
          i++;
      }
    }

  tick(): void
    {
      this.check();
      setTimeout(this.tick, 1000);
    }

  add(one: OneRequest): void
    {
      //console.log('memsqs: queuing longpoll');
      this.requests.push({ deadline: (new Date()).getTime() + LongPollTimeout, request: one });
    }
}

class OneRequest
{
  server: SQSServer;
  req: any;
  res: any;
  jsonstream: JS.JSONStreamReader;
  json: any;
  body: any;

  constructor(server: SQSServer, req: any, res: any)
    {
      this.server = server;
      this.req = req;
      this.res = res;
      this.jsonstream = new JS.JSONStreamReader();
      this.json = null;
      this.body = { statuscode: 0 };
      this.jsonstream.on('end', (json: any) => { this.json = json; this.onDone(); });
      this.jsonstream.on('error', (err: any) => {
          this.onError(`memsqs: error parsing request body: ${err}`);
        });
      this.req.on('error', (err: any) => {
          this.onError(`memsqs: error reading request: ${err}`);
        });
      this.res.on('error', (err: any) => {
          this.onError(`memsqs: error writing response: ${err}`);
        });
      this.jsonstream.start(req);
    }

  isDone(): boolean
    {
      return this.res == null;
    }

  onRetry(): boolean
    {
      let result: Q.QMessages = [];
      let qm = this.server.queuemanager;
      qm.receive(this.json.queueid, this.json.owner, result);
      this.body.result = result;
      if (result.length != 0)
      {
        this.onFinish();
        return true;
      }
      else
        return false;
    }

  onDone(): void
    {
      try
      {
        if (this.json.queueid === undefined || this.json.queueid == '' || this.json.api === undefined)
        {
          this.onError(`memsqs: badly formed request: ${JSON.stringify(this.json)}`);
          return;
        }

        let qm = this.server.queuemanager;

        switch (this.json.api)
        {
          case 'setoptions':
            if (this.json.data === undefined)
              this.onError('memsqs: setoptions: payload is empty');
            else
              qm.setOptions(this.json.queueid, this.json.data as Q.QQueueOptions);
            break;

          case 'claim':
            if (this.json.data === undefined || this.json.data.owner === undefined || this.json.data.groupid === undefined)
              this.onError('memsqs: claim: badly formed request');
            else
            {
              let err = qm.claim(this.json.queueid, this.json.data.owner, this.json.data.groupid);
              if (err)
                this.onError(err);
            }
            break;

          case 'send':
            if (this.json.data === undefined)
              this.onError('memsqs: send: payload is empty');
            else
            {
              let err = qm.send(this.json.queueid, this.json.data as Q.QMessage);
              if (err)
                this.onError(err);
              else
                this.server.longpoll.check();
            }
            break;

          case 'receive':
            if (this.json.owner === undefined)
              this.onError('memsqs: receive: no owner specified');
            else
            {
              let result: Q.QMessages = [];
              qm.receive(this.json.queueid, this.json.owner, result);
              this.body.result = result;
              if (result.length == 0 && qm.isLongpoll(this.json.queueid))
              {
                this.server.longpoll.add(this);
                return;
              }
            }
            break;

          case 'remove':
            if (this.json.data === undefined)
              this.onError('memsqs: remove: payload is empty');
            else
            {
              let err = qm.remove(this.json.queueid, this.json.data as Q.QMessage);
              if (err)
                this.onError(err);
            }
            break;
        }

        this.onFinish();
      }
      catch (err)
      {
        console.log(`memsqs: server: unexpected exception in onDone: ${JSON.stringify(err)}`);
        this.onError((err && err.message) ? err.message : err);
      }
    }

  onError(s: string): void
    {
      console.log(`${s}: reporting failure`);
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
