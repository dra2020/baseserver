import { Util, FSM } from '@dra2020/baseclient';

import * as DBAbstract from '../dbabstract/all';

//
// The DBMRU class really addresses three issues:
//  1. In-memory caching of items to improve latency and reduce database load.
//  2. Serialization of reads (find) and writes (update, unset, del).
//  3. Maintain a "source of truth" for the state of some item.
//
// Basic mechanism:
//  Simple finds if nothing else is extent will just return current data value.
//  Otherwise find executes a lookup.
//  Simple updates will execute the update, but will wait until other updates complete first.
//  A find with updates in process will wait for all updates to complete and then return the result.

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
  result: any,
  msExpires: number,
  mru: DBMRU;
  writes: FSM.Fsm[];
  find: DBAbstract.DBFind;

  constructor(mru: DBMRU)
  {
    super(mru.options.env);
    this.mru = mru;
    this.writes = [];
    this.resetExpire();
  }

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

}
