// Project libraries
import { Util } from '@dra2020/baseclient';

// Local libraries
import * as OL from './orderedlist';

export const DefaultPort: number = (process.env['MEMSQS_PORT'] ? Number(process.env['MEMSQS_PORT']) : 0) || 80;
export const DefaultServerUrl: string = `http://localhost:${DefaultPort}`;

export interface QMessage
{
  messageid: string;
  groupid: string;
  seqno?: number;
  contents?: any;
  blobid?: string;
}
export type QMessages = QMessage[];

interface QMessageHolder
{
  m: QMessage;
  pending: boolean;
  deadline: Util.Deadline;
}

type QMessageList = OL.OrderedList<QMessageHolder>;
type QMessageIndex = { [key: string]: boolean };

interface QVisibility
{
  owner: string;
  deadline: Util.Deadline;
  index: QMessageIndex;
}

const DefaultVisibility: QVisibility = { owner: '', deadline: null, index: null };

interface QGroup
{
  id: string;
  messages: QMessageList;
  visibility: QVisibility;
}

type QGroupIndex = { [id: string]: QGroup };

export interface QQueueOptions
{
  timeoutVisibility?: number,
  timeoutDead?: number,
  timeoutQueueDead?: number,
  receiveLimit?: number,
  longpoll?: boolean,
  ownerOnly?: boolean,
}

const DefaultQueueOptions = {
  timeoutVisibility: 1000 * 30,     // 30 seconds - if server goes down, another server will grab req in 30 sec
  timeoutDead: 1000 * 60 * 30,      // 30 minutes - just cleaning up dead server queue
  timeoutQueueDead: 1000 * 60 * 30, // 30 minutes - just cleaning up dead server queue
  receiveLimit: 10,                 // don't get too greedy, but some batching for efficiency
  longpoll: true,
  ownerOnly: false,                 // receive behavior
};

class QQueue
{
  id: string;
  groups: QGroupIndex;
  seqno: number;
  options: QQueueOptions;
  nHeld: number;
  lastActive: Util.Elapsed;

  constructor(id: string, options?: QQueueOptions)
    {
      this.id = id;
      this.options = Util.shallowAssignImmutable(DefaultQueueOptions, options);
      this.groups = {};
      this.nHeld = 0;
      this.seqno = 0;
      this.lastActive = new Util.Elapsed();
    }

  dumpLog(): void
    {
      console.log(`memsqs: queue: ${this.id}: ${this.nHeld} messages held`);
      let owners: { [owner: string]: number } = {};
      this.forEachGroup((g: QGroup) => {
          if (! g.messages.isempty())
            console.log(`memsqs: queue ${this.id}: group: ${g.id}: non-empty`);
          if (g.visibility.owner != '')
          {
            console.log(`memsqs: queue ${this.id}: group: ${g.id}: owned by ${g.visibility.owner}`);
            owners[g.visibility.owner] = (owners[g.visibility.owner] || 0) + 1;
          }
          return true;
        });
      console.log(`memsqs: queue owner counts: ${Object.values(owners).join(', ')}`);
    }

  setOptions(options: QQueueOptions)
    {
      this.options = Util.shallowAssignImmutable(this.options, options);
    }

  _group(id: string): QGroup
    {
      let g = this.groups[id];
      if (g === undefined)
      {
        g = { id: id, messages: new OL.OrderedList<QMessageHolder>(), visibility: Util.shallowCopy(DefaultVisibility) };
        this.groups[g.id] = g;
      }
      return g;
    }

  forEachGroup(cb: (g: QGroup) => boolean): void
    {
      for (let id in this.groups) if (this.groups.hasOwnProperty(id))
        if (! cb(this.groups[id]))
          break;
    }

  forEachMessageHolder(g: QGroup, cb: (h: QMessageHolder) => boolean): void
    {
      g.messages.forEach(cb);
    }

  get isActive(): boolean
    {
      if (this.nHeld > 0) return true;

      this.lastActive.end();
      return this.lastActive.ms() < this.options.timeoutQueueDead;
    }

  checkTimeout(): void
    {
      // Clear visibility and cull empty groups
      this.forEachGroup((g: QGroup) => {
          // Clear deadline if passed
          if (g.visibility.deadline && g.visibility.deadline.done())
          {
            g.messages.forEach((h: QMessageHolder) => {
                h.pending = false;
                return true;  // continue
              });
            g.visibility = Util.shallowCopy(DefaultVisibility);
          }

          // Cull group if no messages and no deadline.
          // Note that not culling group means that we can build up list of groups (during deadline)
          // if many groups are used (like one-per-message which is standard if no grouping is needed).
          if (g.visibility.owner === '' && g.messages.isempty())
            delete this.groups[g.id];
          return true;  // continue
        });

      // Cull messages past deadline
      this.forEachGroup((g: QGroup) => {
          g.messages.forEach((h: QMessageHolder) => {
              if (h.deadline.done())
                this.removeFromGroup(g, h.m);
              return true;  // continue
              });

          // Cull group if no messages and no deadline
          if (g.visibility.owner === '' && g.messages.isempty())
            delete this.groups[g.id];
          return true;  // continue
        });

    }

  send(m: QMessage): string
    {
      this.lastActive.start();
      if (m.groupid === '*')
      {
        let e: string = null;
        this.forEachGroup((g: QGroup) => {
            if (g.visibility.owner !== '')
            {
              this.nHeld++;
              let gm: QMessage = { messageid: m.messageid + g.id, groupid: g.id, contents: m.contents, seqno: this.seqno++ };
              e = g.messages.insert(gm.messageid, { m: gm, pending: false, deadline: new Util.Deadline(this.options.timeoutDead) } );
              return e == null;
            }
            else
              return true;
          });
        return e;
      }
      else
      {
        m.seqno = this.seqno++; // inject sequence number
        this.nHeld++;
        let g = this._group(m.groupid);
        return g.messages.insert(m.messageid, { m: m, pending: false, deadline: new Util.Deadline(this.options.timeoutDead) } );
      }
    }

  claim(owner: string, groupid: string): string
    {
      this.lastActive.start();
      let g = this._group(groupid);
      let deadline = new Util.Deadline(this.options.timeoutVisibility);
      if (g.visibility.owner === '')
      {
        g.visibility = { owner: owner, deadline: deadline, index: {} };
        return null;
      }
      else if (g.visibility.owner === owner)
      {
        g.visibility.deadline = deadline;
        return null;
      }
      return `memsqs: claim: group ${groupid} already owned by ${g.visibility.owner}, ${owner} cannot claim.`;
    }

  receive(owner: string, result: QMessages): void
    {
      this.lastActive.start();
      let deadline = new Util.Deadline(this.options.timeoutVisibility);

      // First prioritize returning non-pending messages for same owner
      this.forEachGroup((g: QGroup) => {
          if (g.visibility.owner === owner)
          {
            g.messages.forEach((h: QMessageHolder) => {
                if (! h.pending)
                {
                  h.pending = true;
                  g.visibility.index[h.m.messageid] = true;
                  g.visibility.deadline = deadline;
                  result.push(h.m);
                  if (result.length >= this.options.receiveLimit)
                    return false;
                }
                return true;
              });
          }
          return true;
        });

      // Now go through and add messages for any unclaimed message groups
      if (this.options.ownerOnly || result.length >= this.options.receiveLimit) return;

      this.forEachGroup((g: QGroup) => {
          if (g.visibility.owner === '')
          {
            g.messages.forEach((h: QMessageHolder) => {
              if (! h.pending)
              {
                if (g.visibility.owner === '')
                  g.visibility = { owner: owner, deadline: deadline, index: {} };
                h.pending = true;
                g.visibility.index[h.m.messageid] = true;
                result.push(h.m);
                if (result.length >= this.options.receiveLimit)
                  return false;
              }
              return true;
            });
          }
          return true;
        });
    }

  removeFromGroup(g: QGroup, m: QMessage): string
    {
      this.lastActive.start();
      let result = g.messages.remove(m.messageid);
      if (g.visibility.index)
        delete g.visibility.index[m.messageid];
      if (result === null) this.nHeld--;
      return result;
    }

  remove(m: QMessage): string
    {
      let g = this._group(m.groupid);
      if (g)
      {
        let result = this.removeFromGroup(g, m);

        // Comment these lines out to leave visiblity constrained through deadline.
        // (Not completely sure what AWS semantics for SQS is on this point.)
        // The alternative is to force client to send/receive/remove keep-alive messages.
        // if (Util.isEmpty(g.visibility.index))
          // g.visibility = Util.shallowCopy(DefaultVisibility);

        return result;
      }
      else
        return `memsqs: remove: queue ${this.id} has no existing group ${m.groupid}`;;
    }
}

type QQueueIndex = { [id: string]: QQueue };

export class QQueueManager
{
  queues: QQueueIndex;

  nSent: number;
  nReceived: number;
  nRemoved: number;
  nCulled: number;

  constructor()
    {
      this.queues = {};
      this.nSent = 0;
      this.nReceived = 0;
      this.nRemoved = 0;
      this.nCulled = 0;
      this.checkTimeout = this.checkTimeout.bind(this);
      setTimeout(this.checkTimeout, 5000);
    }

  _queue(queueid: string): QQueue
    {
      let q = this.queues[queueid];
      if (q === undefined)
      {
        q = new QQueue(queueid);
        this.queues[queueid] = q;
      }
      return q;
    }

  checkTimeout(): void
    {
      this.forEach((q: QQueue) => {
          let nHeld = q.nHeld;
          q.checkTimeout();
          this.nCulled += nHeld - q.nHeld;
          if (! q.isActive)
          {
            console.log(`memsqs:checkTimeout: deleting inactive queue ${q.id}`);
            delete this.queues[q.id];
          }
        });

      setTimeout(this.checkTimeout, 5000);
    }

  forEach(cb: (q: QQueue) => void): void
    {
      for (let qid in this.queues) if (this.queues.hasOwnProperty(qid))
        cb(this.queues[qid]);
    }

  dumpLog(): void
    {
      this.forEach((q: QQueue) => {
          q.dumpLog();
          return true;
        });
    }

  setOptions(queueid: string, options: QQueueOptions): void
    {
      this._queue(queueid).setOptions(options);
    }

  isLongpoll(queueid: string): boolean
    {
      return this._queue(queueid).options.longpoll;
    }

  claim(queueid: string, owner: string, groupid: string): string
    {
      return this._queue(queueid).claim(owner, groupid);
    }

  send(queueid: string, m: QMessage): string
    {
      let result = this._queue(queueid).send(m);
      if (! result)
        this.nSent++;
      return result;
    }

  receive(queueid: string, owner: string, result: QMessages): void
    {
      this._queue(queueid).receive(owner, result);
      this.nReceived += result.length;
    }

  remove(queueid: string, m: QMessage): string
    {
      let result = this._queue(queueid).remove(m);
      if (! result)
        this.nRemoved++;
      return result;
    }
}
