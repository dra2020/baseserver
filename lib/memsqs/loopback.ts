// Local libraries
import * as Q from './queue';
import * as C from './client';

type MessageList = { [queueid: string]: Q.QMessage[] };
type ReceiveList = { [queueid: string]: C.SQSCallback };

let messageList: MessageList = {};
let receiveList: ReceiveList = {};

export class SQSClientLoopback
{
  constructor(urlstring: string = Q.DefaultServerUrl)
  {
    console.log('memsqs: running in loopback mode');
  }

  setOptions(queueid: string, options: Q.QQueueOptions, cb: C.SQSCallback): void
  {
    cb(null, null);
  }

  claim(queueid: string, owner: string, groupid: string, cb: C.SQSCallback): void
  {
    cb(null, null);
  }

  send(queueid: string, m: Q.QMessage, cb: C.SQSCallback): void
  {
    if (m.groupid === '*') return;  // eat broadcast messages
    let q = messageList[queueid];
    if (q === undefined)
      q = [], messageList[queueid] = q;
    q.push(m);
    cb(null, null);
    this.doReceive();
  }

  remove(queueid: string, m: Q.QMessage, cb: C.SQSCallback): void
  {
    // We just remove during receive so this is no-op
    cb(null, null);
  }

  receive(queueid: string, owner: string, cb: C.SQSCallback): void
  {
    receiveList[queueid] = cb;
    this.doReceive();
  }

  doReceive(): void
  {
    for (let queueid in receiveList) if (messageList.hasOwnProperty(queueid))
    {
      let q = messageList[queueid];
      let cb = receiveList[queueid];
      if (cb !== undefined)
      {
        delete messageList[queueid];
        delete receiveList[queueid];
        cb(null, q);
      }
    }
  }
}
