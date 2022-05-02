/*
IN PROGRESS

import { Util, FSM } from '@dra2020/baseclient';

import * as DBAbstract from '../dbabstract/all';

//
// The DBMRU class really addresses three issues:
//  1. In-memory caching of items to improve latency and reduce database load.
//  2. Serialization of reads (find) and writes (update, unset, del).
//  3. Maintain a "source of truth" for the state of some item so all code references most current value.
//
// Basic idea:
//  We assume that in the case where a single instance wants a serialized view (that is, if they
//  do an update and then a read, the read will show the results of the update) then they serialize
//  externally - this layer does not have to serialize the two requests explicitly.
//
//  So the interesting states are:
//    1. We have a find request outstanding.
//    2. We have some kind of update outstanding.
//    3. We have no request outstanding.
//
//  If we have a find request outstanding, we delay any updates until the find completes. Any new find just piggy packs
//  on the outstanding find.
//  If we have an update outstanding, the find request will resolve when the first update completes (because updates
//  return the entire new record).
//  If we have an update outstanding when a new update comes in, it gets queued until the first update finishes.
//  Updates then proceed in fifo order.
//  A find will resolve immediately if there is nothing outstanding (and something in the MRU of course).
//

const FSM_PAUSED = FSM.FSM_CUSTOM1;

export interface Options 
{
  env?: DBAbstract.DBEnvironment,
  col?: DBAbstract.DBCollection,
  latency?: number,
  limit?: number,
}

const DefaultOptions: Options = { latency: 1000 * 60, limit: 2000 };

export class Item extends FSM.Fsm
{
  mru: DBMRU;
  length: number;
  result: any,
  msExpires: number,
  writes: FSM.Fsm[];
  find: DBAbstract.DBFind;

  constructor(mru: DBMRU, iid: string)
  {
    super(mru.options.env);
    this.mru = mru;
    this.writes = [];
    this.resetExpire();
    this.result = { id: iid }; // We maintain this object shell so clients can hold on to shell and keep current
  }

  get iid(): string { return this.result.id }

  resetExpire(): void
  {
    this.msExpires = (new Date()).getTime() + this.mru.options.latency;
  }
}

export class DBMRU
{
  options: Options;
  _blocked: { [id: string]: boolean }
  _nBlocked: number;
  mru: { [id: string]: Item };

  constructor(options: Options)
  {
    this.options = Util.shallowAssignImmutable(DefaultOptions, options);
    this._blocked = {};
    this._nBlocked = 0;
    this.mru = {};
  }

  block(iid: string): void
  {
    // "blocked" is just an optimization to speed up lookups that we know will fail. So we can safely blow away if gets too big.
    if (this._nBlocked > 50000)
    {
      this._blocked = {};
      this._nBlocked = 0;
    }
    this._blocked[iid] = true;
    this._nBlocked++;
    delete this.mru[iid];
  }

  remove(id: string): void
  {
    delete this.mru[id];
  }

  cull(): void
  {
    // First delete all expired ones
    let msNow = (new Date()).getTime();
    Object.keys(this.mru).forEach((id: string) => {
        let item = this.mru[id];
        if (item.msExpires < msNow)
          delete this.mru[id];
      });

    let items: any[] = [];
    Object.keys(this.mru).forEach((id: string) => {
        items.push({ id: id, msExpires: this.mru[id].msExpires });
      });

    // Reverse sort so oldest at end of array
    items.sort((i1: any, i2: any) => { return i2.msExpires - i1.msExpires });
    // Now delete all past limit
    for (let i: number = this.options.limit; i < items.length; i++)
      delete this.mru[items[i].id];
  }

length
*/
