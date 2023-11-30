import * as stream from 'stream';
import * as zlib from 'zlib';

// Shared libraries
import { Util, Context, LogAbstract, FSM } from '@dra2020/baseclient';

import * as Storage from '../storage/all';

import { LogBlob } from './logblob';
import { LogKey, dateToKey, dateToMonthKey, dateToYearKey } from './logkey';

export interface Environment
{
  context: Context.IContext;
  log: LogAbstract.ILog;
  fsmManager: FSM.FsmManager;
  storageManager: Storage.StorageManager;
}

export interface AccumOptions
{
  onlyAggregateClosed?: boolean,
  dontAggregate?: boolean,
  dateFilter?: string,
}
const DefaultOptions = { onlyAggregateClosed: false };

const FSM_BLOB = FSM.FSM_CUSTOM1;
const FSM_QUERYING = FSM.FSM_CUSTOM2;
const FSM_LISTING = FSM.FSM_CUSTOM3;
const FSM_AGGREGATING = FSM.FSM_CUSTOM4;
const FSM_SAVING = FSM.FSM_CUSTOM5;


const FSM_ACCUM = FSM.FSM_CUSTOM1;

const MaxPendingDownloads = 50;
const MaxPendingUploads = 50;

function coalesceInstances(o: any): void
{
  if (o)
    if (o.instances)
    {
      let all: any = {};
      //console.log(`coalesceInstances: coalescing ${Util.countKeys(o.instances)} to 1`);
      Object.values(o.instances).forEach((v: any) => Util.deepAccum(all, v));
      o.instances = { all: all };
    }
    else if (typeof o === 'object')
      Object.values(o).forEach(coalesceInstances);
}

function collapseEvents(ia: ILogAccumulator): void
{
  // These are events that mistakenly included data-specific content so resulted
  // in explosion of number of events. This re-collapses them on aggregation since
  // log processing assumes # of unique events stays "reasonable" vs. based on size of content
  // or amount of activity.
  const collapseList = [
      { from: 'computesplits: uploaded _', to: 'computeSplits: uploaded' },
    ];
  let nCollapsed = 0;
  if (ia && ia.events && ia.events.instances)
    Object.values(ia.events.instances).forEach((id: IDayAccumulator) => {
        Object.values(id).forEach((iv: IValueAccumulatorIndex) => {
          Object.keys(iv).forEach(k => {
              collapseList.forEach(l => {
                  if (k.indexOf(l.from) == 0)
                  {
                    iv[l.to] = Util.deepAccum(iv[l.to], iv[k]);
                    delete iv[k];
                    nCollapsed++;
                  }
                });
            });
        });
      });
  //if (nCollapsed)
    //console.log(`collapseEvents: collapsed ${nCollapsed} events`);
}

class FsmAggregate extends FSM.Fsm
{
  options: AccumOptions;
  item: LogItem;

  constructor(env: Environment, item: LogItem, options?: AccumOptions)
  {
    super(env);
    this.options = Util.shallowAssignImmutable(DefaultOptions, options);
    this.item = item;
  }

  get env(): Environment { return this._env as Environment }

  fetchNext(): void
  {
    if (this.item.children)
    {
      let nPending: number = 0;
      for (let id in this.item.children) if (this.item.children.hasOwnProperty(id))
      {
        let item = this.item.children[id];
        if (item.present && item.blob == null && item.accum == null)
        {
          delete item.children; // not needed since I have this aggregate
          item.blob = LogBlob.createForDownload(this.env, item.id);
          this.waitOn(item.blob.fsmLoad);
          nPending++;
          if (nPending >= MaxPendingDownloads)
            break;
        }
      }
    }
  }

  aggregate(): void
  {
    // Already computed and aggregated completely?
    if (this.item.key.closed && this.item.accum)
      return;

    // Only aggregate closed?
    if (! this.item.key.closed && this.options.onlyAggregateClosed)
      return;

    // Otherwise aggregate
    console.log(`logaccum: aggregating ${this.item.id}`);
    let la: ILogAccumulator = LogAccumulator.create();
    this.item.accum = la;
    let bClosed = this.item.key.closed;
    for (let id in this.item.children) if (this.item.children.hasOwnProperty(id))
    {
      let item = this.item.children[id];

      if (item.key.isLog)
      {
        let instance = 'all'; // item.key.instance
        LogAccumulator.gather(la, item.id, instance, item.key.dateKey, item.blob.result);
        if (bClosed) delete item.blob;
      }
      else
      {
        if (item.accum == null)
        {
          if (item.blob)
          {
            item.accum = JSON.parse(item.blob.result);
            coalesceInstances(item.accum);
            collapseEvents(item.accum);
            delete item.blob;
          }
          else
          {
            console.log(`logaccum: expected downloaded blob for non-aggregated child ${item.id}`);
            item.accum = LogAccumulator.create();
          }
        }
        LogAccumulator.aggregate(la, item.accum);
      }
    }

    // Save memory - don't need children if I won't re-aggregate with new data (from above - test for closed)
    if (bClosed)
    {
      if (! this.item.present && ! this.options.dontAggregate)
      {
        this.env.log.chatter(`logaccum: saving ${this.item.id}`);
        let blob = LogBlob.createForUpload(this.env, this.item.id, JSON.stringify(this.item.accum));
        this.waitOn(blob.fsmSave);
        this.item.present = true;
      }
      delete this.item.children;
    }
  }

  tick(): void
  {
    if (this.ready && this.isDependentError)
      this.setState(FSM.FSM_ERROR);
    else if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.fetchNext();
          if (this.nWaitOn == 0)
            this.setState(FSM_ACCUM);
          break;

        case FSM_ACCUM:
          this.aggregate();
          this.setState(FSM_SAVING);
          break;

        case FSM_SAVING:
          this.setState(FSM.FSM_DONE);
          break;
      }
    }
  }
}

class FsmDeepAggregate extends FSM.Fsm
{
  options: AccumOptions;
  root: LogItem;
  items: LogItem[];
  cur: number;

  constructor(env: Environment, root: LogItem, options?: AccumOptions)
  {
    super(env);
    this.options = Util.shallowAssignImmutable(DefaultOptions, options);
    this.root = root;
    this.items = root.query((item: LogItem) => { return item.accum === undefined && !item.present });
    this.cur = 0;
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
          // Save memory
          if (this.cur > 100)
          {
            this.items.splice(0, this.cur);
            this.cur = 0;
          }
          if (this.cur < this.items.length)
            this.waitOn(new FsmAggregate(this.env, this.items[this.cur++], this.options));
          else
            this.setState(FSM.FSM_DONE);
          break;
      }
    }
  }
}

class FsmSaveAggregates extends FSM.Fsm
{
  options: AccumOptions;
  root: LogItem;
  items: LogItem[];
  cur: number;

  constructor(env: Environment, root: LogItem, options?: AccumOptions)
  {
    super(env);
    this.options = Util.shallowAssignImmutable(DefaultOptions, options);
    this.root = root;
    this.items = root.query((item: LogItem) => { return item.accum !== undefined && !item.present && item.key.closed });
    this.cur = 0;
    if (this.options.dontAggregate)
      this.setState(FSM.FSM_DONE);
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
          let nPending: number = 0;
          // Save memory
          if (this.cur > 100)
          {
            this.items.splice(0, this.cur);
            this.cur = 0;
          }
          while (this.cur < this.items.length && nPending < MaxPendingUploads)
          {
            let item = this.items[this.cur++];
            item.blob = LogBlob.createForUpload(this.env, item.id, JSON.stringify(item.accum));
            this.waitOn(item.blob.fsmSave);
            nPending++;
          }

          if (nPending == 0) this.setState(FSM.FSM_DONE);
          break;
      }
    }
  }
}


export class LogItem
{
  id: string;
  key?: LogKey;
  present?: boolean;
  parent?: LogItem;
  children?: { [id: string]: LogItem };
  accum?: ILogAccumulator;
  blob?: LogBlob;

  constructor(id: string)
  {
    this.id = id;
    if (id === 'root') // root
    {
      this.key = LogKey.root();
      this.children = {};
    }
    else
    {
      this.key = LogKey.create(id);
      if (this.key.kind !== 'Log')
        this.children = {};
    }
  }

  query(filter: (item: LogItem) => boolean): LogItem[]
  {
    let items: LogItem[] = [];

    for (let yearID in this.children) if (this.children.hasOwnProperty(yearID))
    {
      let year = this.children[yearID];
      for (let monthID in year.children) if (year.children.hasOwnProperty(monthID))
      {
        let month = year.children[monthID];
        for (let dateID in month.children) if (month.children.hasOwnProperty(dateID))
        {
          let date = month.children[dateID];
          for (let id in date.children) if (date.children.hasOwnProperty(id))
          {
            let log = date.children[id];
            if (filter(log)) items.push(log);
          }
          if (filter(date)) items.push(date);
        }
        if (filter(month)) items.push(month);
      }
      if (filter(year)) items.push(year);
    }
    if (filter(this)) items.push(this);

    return items;
  }

  addListing(ls: string[]): void
  {
    ls.forEach((id: string) => this.add(new LogItem(id)));
  }

  addChild(id: string, child: LogItem): void
  {
    this.children[id] = child;
    child.parent = this;
    for (let p = child.parent; p; p = p.parent)
      delete p.accum;
  }

  add(item: LogItem): void
  {
    let yearID = item.key.yearID;
    let year = this.children[yearID];
    if (item.key.isYear)
    {
      if (year === undefined)
      {
        year = item;
        this.addChild(yearID, item);
      }
      year.present = true;
    }
    else
    {
      if (year === undefined)
      {
        year = new LogItem(yearID);
        this.addChild(yearID, year);
      }
      else if (year.present)
        return;
      let monthID = item.key.monthID;
      let month = year.children[monthID];
      if (item.key.isMonth)
      {
        if (month === undefined)
        {
          month = item;
          year.addChild(monthID, item);
        }
        month.present = true;
      }
      else
      {
        if (month === undefined)
        {
          month = new LogItem(monthID);
          year.addChild(monthID, month);
        }
        else if (month.present)
          return;
        let dateID = item.key.dateID;
        let date = month.children[dateID];
        if (item.key.isDate)
        {
          if (date === undefined)
          {
            date = item;
            month.addChild(dateID, item);
          }
          date.present = true;
        }
        else
        {
          if (date === undefined)
          {
            date = new LogItem(dateID);
            month.addChild(dateID, date);
          }
          else if (date.present)
            return;
          let log = date.children[item.id];
          if (log === undefined)
          {
            date.addChild(item.id, item);
            log = item;
          }
          log.present = true;
        }
      }
    }
  }
}

// Everything we want to accumulate has a key that we use to identify and index it.
// We also want to track which instance generated the key (so bucket by instance)
// We also want to track which day some event occurred on (so bucket by day).
// Finally, we either want to just count occurrences or we want to track a value (to report min, max, avg).
// We assume NEvents >> NDays >> NInstances.
// So bucket by { [instance: string]: { [day: string]: { [event: string]: Accumulator } } }
// And then provide ways to walk that resulting data structure and aggregate and report it.
// Note that for some things (like API calls) we are interested in the total number of independent events.
// For other things (like unique users or sessions/maps) we are only interested in the total number of uniques.
// These are the type of answers:
//    NInstances: number (number of instances seen)
//    NDays: number (number of days seen)
//    NUniques: number (number of unique things tracked, e.g. total number of unique users seen)
//    NUniquesByDay: { [day: string]: number } total number of uniques each day
//    Values: { [id: string]: ValueAccumulator } total number of occurrences of each thing tracked (across all days)
//    ValuesByDay: { [id: string]: { [days: string]: ValueAccumulator } }
//

export interface IValueAccumulator
{
  count: number;
  total: number;
  min?: number;
  max?: number;
}

export class ValueAccumulator
{
  constructor()
  {
  }

  static create(): IValueAccumulator
  {
    return { count: 0, total: 0 };
  }

  static avg(va: IValueAccumulator): number
  {
    return va.count > 0 ? Math.round(va.total / va.count) : 0;
  }

  static incr(va: IValueAccumulator): void
  {
    va.count++;
  }

  static accum(va: IValueAccumulator, v: number): void
  {
    va.count++;
    va.total += v;
    if (va.min === undefined || v < va.min)
      va.min = v;
    if (va.max === undefined || v > va.max)
      va.max = v;
  }

  static reduce(va: IValueAccumulator, reduction: IValueAccumulator): void
  {
    reduction.count += va.count;
    reduction.total += va.total;
    if (reduction.min === undefined || va.min < reduction.min)
      reduction.min = va.min;
    if (reduction.max === undefined || va.max > reduction.max)
      reduction.max = va.max;
  }

  static log(va: IValueAccumulator): string
  {
    if (va.min === undefined)
      return `${va.count}`;
    else
      return `count: ${va.count} avg: ${Math.round(ValueAccumulator.avg(va))} min: ${va.min} max: ${va.max}`;
  }
}

export type IValueAccumulatorIndex = { [key: string]: IValueAccumulator };
export type IValueAccumulatorIndexIndex = { [key: string]: { [key: string]: IValueAccumulator } }; export type IEventAccumulator = IValueAccumulatorIndex; 
export class EventAccumulator
{
  constructor() { }

  static create(): IEventAccumulator
  {
    return ({});
  }

  static getAccumulator(ea: IEventAccumulator, o: any, prop: string | string[]): IValueAccumulator
  {
    let event: string;
    if (Array.isArray(prop))
    {
      let sa: string[] = [];
      for (let i: number = 0; i < prop.length; i++)
      {
        if (o[prop[i]] === undefined)
          return null;
        sa.push(o[prop[i]]);
      }
      event = sa.join('+');
    }
    else
      event = o[prop];
    if (event !== undefined)
    {
      let a = ea[event];
      if (a === undefined)
      {
        a = ValueAccumulator.create();
        ea[event] = a;
      }
      return a;
    }
    return null;
  }

  static NUnique(ea: IEventAccumulator): number
  {
    return Util.countKeys(ea);
  }

  static incr(ea: IEventAccumulator, o: any, prop: string | string[]): void
  {
    let a = EventAccumulator.getAccumulator(ea, o, prop);
    if (a) ValueAccumulator.incr(a);
  }

  static accum(ea: IEventAccumulator, o: any, name: string, value: string): void
  {
    let a = EventAccumulator.getAccumulator(ea, o, name);
    if (a && o[value] !== undefined)
      ValueAccumulator.accum(a, Number(o[value]));
  }

  static reduce(ea: IEventAccumulator, event: string, reduction: IValueAccumulator)
  {
    if (ea[event] !== undefined)
      ValueAccumulator.reduce(ea[event], reduction);
  }
}

export type IEventAccumulatorIndex = { [key: string]: IEventAccumulator };
export type IDayAccumulator = IEventAccumulatorIndex;

export class DayAccumulator
{
  constructor() { }

  static create(): IDayAccumulator { return ({}) }

  static count(da: IDayAccumulator): number
  {
    return Util.countKeys(da);
  }

  static getAccumulator(da: IDayAccumulator, date: string): IEventAccumulator
  {
    let a = da[date];
    if (a === undefined)
    {
      a = EventAccumulator.create();
      da[date] = a;
    }
    return a;
  }

  static incr(da: IDayAccumulator, date: string, o: any, prop: string | string[]): void
  {
    let a = DayAccumulator.getAccumulator(da, date);
    EventAccumulator.incr(a, o, prop);
  }

  static accum(da: IDayAccumulator, date: string, o: any, name: string, value: string): void
  {
    let a = DayAccumulator.getAccumulator(da, date);
    EventAccumulator.accum(a, o, name, value);
  }

  static reduce(da: IDayAccumulator, event: string, reduction: IValueAccumulator): void
  {
    for (let date in da) if (da.hasOwnProperty(date))
      EventAccumulator.reduce(da[date], event, reduction);
  }

  static reduceByDateKeys(da: IDayAccumulator, dateKeys: IKeyIndex, event: string, reduction: IValueAccumulator): void
  {
    if (dateKeys === undefined)
    {
      for (let date in da) if (da.hasOwnProperty(date))
        EventAccumulator.reduce(da[date], event, reduction);
    }
    else
    {
      for (let date in dateKeys) if (dateKeys.hasOwnProperty(date) && da.hasOwnProperty(date))
        EventAccumulator.reduce(da[date], event, reduction);
    }
  }

  static reduceByDate(da: IDayAccumulator, date: string, event: string, dayReduction: IValueAccumulator): void
  {
    if (da[date] !== undefined)
      EventAccumulator.reduce(da[date], event, dayReduction);
  }

  static reduceUniqueByDate(da: IDayAccumulator, date: string, days: IValueAccumulatorIndexIndex): void
  {
    if (da[date] !== undefined)
    {
      let events = da[date];
      let day = days[date];
      for (let e in events) if (events.hasOwnProperty(e))
        if (day[e] === undefined)
          day[e] = ValueAccumulator.create();
    }
  }
}

export type IDayAccumulatorIndex = { [key: string]: IDayAccumulator };
export type IKeyIndex = { [key: string]: boolean };
export interface IInstanceAccumulator
{
  dateKeys: IKeyIndex;
  uniqueKeys: IKeyIndex;
  instances: IDayAccumulatorIndex;
}

export class InstanceAccumulator
{
  constructor() { }

  static create(): IInstanceAccumulator { return { dateKeys: {}, uniqueKeys: {}, instances: {} }; }

  static getToday(ia: IInstanceAccumulator): string
  {
    let today: string = undefined;

    for (let p in ia.dateKeys) if (ia.dateKeys.hasOwnProperty(p))
      if (today === undefined || p > today)
        today = p;

    return today;
  }

  static getAccumulator(ia: IInstanceAccumulator, instance: string): IDayAccumulator
  {
    let a = ia.instances[instance];
    if (a === undefined)
    {
      a = DayAccumulator.create();
      ia.instances[instance] = a;
    }
    return a;
  }

  static incr(ia: IInstanceAccumulator, instance: string, date: string, o: any, prop: string | string[]): void
  {
    let key: string;
    if (Array.isArray(prop))
    {
      let sa: string[] = [];
      for (let i: number = 0; i < prop.length; i++)
      {
        if (o[prop[i]] === undefined)
          return;
        sa.push(o[prop[i]]);
      }
      key = sa.join('+');
    }
    else
    {
      key = o[prop];
      if (key === undefined) return;
    }

    ia.dateKeys[date] = true;
    ia.uniqueKeys[key] = true;
    let a = InstanceAccumulator.getAccumulator(ia, instance);
    DayAccumulator.incr(a, date, o, prop);
  }

  static accum(ia: IInstanceAccumulator, instance: string, date: string, o: any, name: string, value: string): void
  {
    if (o[name] !== undefined)
    {
      ia.dateKeys[date] = true;
      ia.uniqueKeys[o[name]] = true;
      let a = InstanceAccumulator.getAccumulator(ia, instance);
      DayAccumulator.accum(a, date, o, name, value);
    }
  }

  static NInstances(ia: IInstanceAccumulator): number
  {
    return Util.countKeys(ia.instances);
  }

  static NDays(ia: IInstanceAccumulator): number
  {
    return Util.countKeys(ia.dateKeys);
  }

  static NUniques(ia: IInstanceAccumulator): number
  {
    return Util.countKeys(ia.uniqueKeys);
  }

  static NUniquesByDay(ia: IInstanceAccumulator, dateKeys?: IKeyIndex): IValueAccumulatorIndex
  {
    let result: IValueAccumulatorIndex = {};
    let gather: IValueAccumulatorIndexIndex = {};
    let date: string;

    // Initialize for all the requested dates
    if (dateKeys === undefined) dateKeys = ia.dateKeys;
    for (date in dateKeys) if (dateKeys.hasOwnProperty(date))
    {
      result[date] = ValueAccumulator.create();
      gather[date] = {};
    }

    // Now gather up from each instance
    for (let instance in ia.instances) if (ia.instances.hasOwnProperty(instance))
      for (date in dateKeys) if (dateKeys.hasOwnProperty(date))
        DayAccumulator.reduceUniqueByDate(ia.instances[instance], date, gather);

    // Now record totals
    for (date in dateKeys) if (dateKeys.hasOwnProperty(date))
      result[date].count = Util.countKeys(gather[date]);

    return result;
  }

  static AllCountsByDay(ia: IInstanceAccumulator, dateKeys?: IKeyIndex, eventKeys?: IKeyIndex): IValueAccumulatorIndexIndex
  {
    let result: IValueAccumulatorIndexIndex = {};
    let date: string;
    let event: string;

    // If we don't ask for special date, just get all
    if (dateKeys === undefined) dateKeys = ia.dateKeys;

    // If we don't ask for special list, just get all
    if (eventKeys === undefined) eventKeys = ia.uniqueKeys;

    // Initialize for all the dates
    for (date in dateKeys) if (dateKeys.hasOwnProperty(date))
    {
      let day: IValueAccumulatorIndex = {};
      result[date] = day;
      for (event in eventKeys) if (eventKeys.hasOwnProperty(event))
        day[event] = ValueAccumulator.create();
    }

    // Now walk through all instances, gathering dates
    for (date in dateKeys) if (dateKeys.hasOwnProperty(date))
      for (let instance in ia.instances) if (ia.instances.hasOwnProperty(instance))
        for (event in eventKeys) if (eventKeys.hasOwnProperty(event))
          DayAccumulator.reduceByDate(ia.instances[instance], date, event, result[date][event]);

    return result;
  }

  static TotalCountsByDay(ia: IInstanceAccumulator, dateKeys?: IKeyIndex, eventKeys?: IKeyIndex): IValueAccumulatorIndex
  {
    let result: IValueAccumulatorIndex = {};
    let date: string;
    let event: string;

    // If we don't ask for special date, just get all
    if (dateKeys === undefined) dateKeys = ia.dateKeys;

    // If we don't ask for special list, just get all
    if (eventKeys === undefined) eventKeys = ia.uniqueKeys;

    // Initialize for all the dates
    for (date in dateKeys) if (dateKeys.hasOwnProperty(date))
      result[date] = ValueAccumulator.create();

    // Now walk through all instances, gathering dates
    for (date in dateKeys) if (dateKeys.hasOwnProperty(date))
      for (let instance in ia.instances) if (ia.instances.hasOwnProperty(instance))
        for (event in eventKeys) if (eventKeys.hasOwnProperty(event))
          DayAccumulator.reduceByDate(ia.instances[instance], date, event, result[date]);

    return result;
  }

  static ValuesByEvent(ia: IInstanceAccumulator, dateKeys?: IKeyIndex, eventKeys?: IKeyIndex): IValueAccumulatorIndex
  {
    let result: IValueAccumulatorIndex = {};
    let event: string;

    // If we don't ask for special list, just get all
    if (eventKeys === undefined) eventKeys = ia.uniqueKeys;

    for (event in eventKeys) if (eventKeys.hasOwnProperty(event))
      result[event] = ValueAccumulator.create();

    for (let instance in ia.instances) if (ia.instances.hasOwnProperty(instance))
      for (event in eventKeys) if (eventKeys.hasOwnProperty(event))
        DayAccumulator.reduceByDateKeys(ia.instances[instance], dateKeys, event, result[event]);

    return result;
  }

  static ValuesByDay(ia: IInstanceAccumulator, dateKeys?: IKeyIndex, eventKeys?: IKeyIndex): IValueAccumulatorIndexIndex
  {
    let result: IValueAccumulatorIndexIndex = {}
    let event: string;
    let date: string;

    // If we don't ask for special date, just get all
    if (dateKeys === undefined) dateKeys = ia.dateKeys;

    // If we don't ask for special list, just get all
    if (eventKeys === undefined) eventKeys = ia.uniqueKeys;

    // Initialize the double level index with events by day by value
    for (event in eventKeys) if (eventKeys.hasOwnProperty(event))
    {
      let days: IValueAccumulatorIndex = {};

      for (date in dateKeys) if (dateKeys.hasOwnProperty(date))
        days[date] = ValueAccumulator.create();

      result[event] = days;
    }

    // Now reduce them
    for (let instance in ia.instances) if (ia.instances.hasOwnProperty(instance))
      for (event in eventKeys) if (eventKeys.hasOwnProperty(event))
        for (date in dateKeys) if (dateKeys.hasOwnProperty(date))
          DayAccumulator.reduceByDate(ia.instances[instance], date, event, result[event][date]);

    return result;
  }

  static filterNoisyProperty(p: string): boolean
  {
    let noises: string[] = [ 'session(', 'Started server at' ];

    for (let i = 0; i < noises.length; i++)
      if (p.indexOf(noises[i]) == 0)
        return true;
    return false;
  }

  static getCountRows(ia: IInstanceAccumulator, dateKeys?: IKeyIndex, eventKeys?: IKeyIndex): any[]
  {
    let values = InstanceAccumulator.ValuesByEvent(ia, dateKeys, eventKeys);
    let rows: any[] = [];

    for (let p in values) if (values.hasOwnProperty(p))
    {
      if (this.filterNoisyProperty(p))
        continue;

      rows.push({
          id: String(rows.length),
          event: p,
          count: String(values[p].count),
        });
    }

    return rows;
  }

  static getValueRows(ia: IInstanceAccumulator, dateKeys?: IKeyIndex, eventKeys?: IKeyIndex): any[]
  {
    let values = InstanceAccumulator.ValuesByEvent(ia, dateKeys, eventKeys);
    let rows: any[] = [];

    for (let p in values) if (values.hasOwnProperty(p))
    {
      rows.push({
          id: String(rows.length),
          event: p,
          count: String(values[p].count),
          min: String(values[p].min),
          max: String(values[p].max),
          avg: String(ValueAccumulator.avg(values[p])),
        });
    }

    return rows;
  }
}


type Schema = { [pos: number]: string };
type SchemaIndex = { [index: number]: Schema };
type Template = { schemaid: number, o: { [prop: string]: any } };
type TemplateIndex = Template[];

function objectToRows(o: any): any[]
{
  let rows: any[] = [];
  for (let p in o) if (o.hasOwnProperty(p))
    rows.push({ event: p, count: o[p] });
  return rows;
}

function deltaRows(before: any[], after: any[]): any[]
{
  let index: { [event: string]: number } = {};
  let p: string;
  let i: number;

  if (before)
    for (i = 0; i < before.length; i++)
      index[before[i].event] = Number(before[i].count);
  if (after)
    for (i = 0; i < after.length; i++)
    {
      p = after[i].event;
      let n: number = Number(after[i].count) - (index[p] === undefined ? 0 : index[p]);
      if (n == 0)
        delete index[p];
      else
        index[p] = n;
    }

  let rows: any[] = [];
  for (p in index) if (index.hasOwnProperty(p))
    rows.push({ event: `${p} (delta)`, count: String(index[p]) });
  return rows;
}

export interface ILogAccumulator
{
  users: IInstanceAccumulator;
  sessions: IInstanceAccumulator;
  events: IInstanceAccumulator;
  errors: IInstanceAccumulator;
  values: IInstanceAccumulator;
  syncTimers: IInstanceAccumulator;
  asyncTimers: IInstanceAccumulator;
  logsSeen: { [id: string]: boolean };
}

const specialEventKey: IKeyIndex = {
    'Listening on port 8080': true,
    'API create  ended': true,
    'createuser': true,
    'create visitor': true,
    'anonymous visit': true,
    'socialmanager: like': true,
    'socialmanager: comment': true
  };

const specialEventNames: any = {
    'Listening on port 8080': 'restarts',
    'API create  ended': 'newmaps',
    'createuser': 'newuser',
    'create visitor': 'newvisitor',
    'anonymous visit': 'anonview',
    'socialmanager: like': 'likes',
    'socialmanager: comment': 'comments'
  };

export class LogAccumulator
{
  static create(): ILogAccumulator
  {
    let la: ILogAccumulator =
      {
        users: InstanceAccumulator.create(),
        sessions: InstanceAccumulator.create(),
        events: InstanceAccumulator.create(),
        errors: InstanceAccumulator.create(),
        values: InstanceAccumulator.create(),
        syncTimers: InstanceAccumulator.create(),
        asyncTimers: InstanceAccumulator.create(),
        logsSeen: {},
      };
    return la;
  }

  static accumulate(la: ILogAccumulator, instance: string, dateKey: string, o: any): void
  {
    switch (o.kind)
    {
      case 'error':
        InstanceAccumulator.incr(la.errors, instance, dateKey, o, 'event');
        break;

      case 'event':
        InstanceAccumulator.incr(la.sessions, instance, dateKey, o, 'sessionid');
        InstanceAccumulator.incr(la.users, instance, dateKey, o, 'userid');
        InstanceAccumulator.incr(la.events, instance, dateKey, o, 'event');
        break;

      case 'value':
        InstanceAccumulator.accum(la.values, instance, dateKey, o, 'event', 'value');
        break;

      case 'sync':
        InstanceAccumulator.accum(la.syncTimers, instance, dateKey, o, 'event', 'ms');
        break;

      case 'async':
        InstanceAccumulator.accum(la.asyncTimers, instance, dateKey, o, 'event', 'ms');
        break;

      case 'misc':
        // count misc
        break;
    }
  }

  static unquoteFields(fields: string[]): string[]
  {
    // if field begins with ' strip that character and merge with subsequent
    // fields until encountering field that ends with ' (which might be same field).
    for (let i: number = 0; i < fields.length; )
    {
      let field = fields[i];
      if (field.indexOf("'") == 0)
      {
        let accum = field.substring(1);
        let iStart = i;
        do
        {
          if (accum.lastIndexOf("'") == accum.length-1)
          {
            accum = accum.substring(0, accum.length-1);
            fields.splice(iStart, i-iStart);
            fields[iStart] = accum;
            i++;
            accum = null;
            break;
          }
          else
          {
            i++;
            if (i < fields.length)
              accum += fields[i];
          }
        }
        while (i < fields.length);

        // if never encountered closing quote...
        if (accum != null)
        {
          fields.splice(iStart, i-iStart);
          fields[iStart] = accum;
        }
      }
      else
        i++;
    }
    return fields;
  }

  static gather(la: ILogAccumulator, logid: string, instance: string, dateKey: string, csvblob: string): void
  {
    la.logsSeen[logid] = true;
    let schemas: SchemaIndex = {};
    let templates: TemplateIndex = [];
    let lines = csvblob.split('\n');

    // Process schema lines
    let i: number = 0;
    for (; i < lines.length; i++)
    {
      let l = lines[i];
      if (l.indexOf('S') != 0)
        break;
      // Line is of form S,N,field1,field2,field3,...
      let s: Schema = {};
      let fields = l.split(',');
      for (let j: number = 2; j < fields.length; j++)
        s[j] = fields[j];
      schemas[Number(fields[1])] = s;
    }

    // Process template lines
    let nMismatch: number = 0;
    for (; i < lines.length; i++)
    {
      let l = lines[i];
      if (l.indexOf('T') != 0)
        break;

      // Line is of form T,SN,v1,v2,v3
      let fields = LogAccumulator.unquoteFields(l.split(','));
      let t: Template = { schemaid: Number(fields[1]), o: { } };
      templates.push(t);
      let schema = schemas[t.schemaid];
      for (let j: number = 2; j < fields.length; j++)
      {
        let prop = schema[j];
        if (prop === undefined)
        {
          if (nMismatch == 0)
            console.error('logaccumulator: template does not match schema definition');
          nMismatch++;
        }
        else
          t.o[prop] = fields[j];
      }
    }

    // Gather log entries
    for (; i < lines.length; i++)
    {
      let l = lines[i];
      if (l == '') continue;  // Really just EOF
      let fields = LogAccumulator.unquoteFields(l.split(','));
      let templateid = Number(fields[1]);
      if (templateid < 0 || templateid >= templates.length)
      {
        console.error('logaccumulator: missing template definition in log file');
        nMismatch++;
        continue;
      }
      let t = templates[templateid];
      let schema = schemas[t.schemaid];
      if (schema === undefined)
        console.error('logaccumulator: missing schema definition in log file');
      else
      {
        let o: any = {};
        o.instance = instance;
        for (let j: number = 2; j < fields.length; j++)
        {
          let prop = schema[j];
          if (prop === undefined)
          {
            nMismatch++;
          }
          else
          {
            o[prop] = fields[j];
            if (o[prop] === '')
              o[prop] = t.o[prop];
          }
        }
        LogAccumulator.accumulate(la, instance, dateKey, o);
      }
    }
  }

  static aggregate(agg: ILogAccumulator, la: ILogAccumulator): void
  {
    Util.deepAccum(agg, la);
  }

  static datePatternToKeys(la: ILogAccumulator, pat: string): IKeyIndex
  {
    if (pat == null || pat === '' || pat === '...')
      return la.events.dateKeys;
    let apat = pat.split('.');
    if (apat.length > 3) return la.events.dateKeys;
    while (apat.length < 3) apat.push('');
    let keys: IKeyIndex = {};
    Object.keys(la.events.dateKeys).forEach((date: string) => {
        let adate = date.split('.');
        if ((apat[0] === '' || apat[0] === adate[0]) &&
            (apat[1] === '' || apat[1] === adate[1]) &&
            (apat[2] === '' || apat[2] === adate[2]))
          keys[date] = true;
      });
    return keys;
  }

  static getToday(la: ILogAccumulator): IKeyIndex
  {
    let today = InstanceAccumulator.getToday(la.events);
    return today === undefined ? {} : { [today]: true };
  }

  static getTodayRows(la: ILogAccumulator): any[]
  {
    let todayKeys = LogAccumulator.getToday(la);
    if (Util.countKeys(todayKeys) == 0)
      return [];
    let today = Util.nthKey(todayKeys, 0);
    let daysUsers = InstanceAccumulator.NUniquesByDay(la.users, todayKeys);
    let daysSessions = InstanceAccumulator.NUniquesByDay(la.sessions, todayKeys);
    let dayEvents = InstanceAccumulator.ValuesByEvent(la.events, todayKeys);
    let dayErrors = InstanceAccumulator.ValuesByEvent(la.errors, todayKeys);
    let daysErrorTotals = InstanceAccumulator.TotalCountsByDay(la.errors, todayKeys);
    let totalErrors: number = 0;
    let rows: any[] = [];
    let row: any;
    let p: string;
    let dayUsersCount: string = daysUsers[today] !== undefined ? String(daysUsers[today].count) : '0';
    let daySessionsCount: string = daysSessions[today] !== undefined ? String(daysSessions[today].count) : '0';
    let dayErrorsCount: string = daysErrorTotals[today] !== undefined ? String(daysErrorTotals[today].count) : '0';
    rows.push( { type: 'summary', event: 'users', count: dayUsersCount } );
    rows.push( { type: 'summary', event: 'sessions', count: daySessionsCount } );
    rows.push( { type: 'summary', event: 'errors', count: dayErrorsCount } );
    for (p in dayErrors) if (dayErrors.hasOwnProperty(p))
      if (dayErrors[p].count !== 0)
        rows.push( { type: 'errors', event: p, count: String(dayErrors[p].count) } );
    for (p in dayEvents) if (dayEvents.hasOwnProperty(p))
      if (dayEvents[p].count !== 0)
        rows.push( { type: 'events', event: p, count: String(dayEvents[p].count) } );
    return rows;
  }

  static getDailyRows(la: ILogAccumulator, datePattern?: string, eventKeys?: IKeyIndex): any[]
  {
    let dateKeys = LogAccumulator.datePatternToKeys(la, datePattern);
    eventKeys = eventKeys || specialEventKey;
    let daysUsers = InstanceAccumulator.NUniquesByDay(la.users, dateKeys);
    let daysSessions = InstanceAccumulator.NUniquesByDay(la.sessions, dateKeys);
    let daysEvents = InstanceAccumulator.AllCountsByDay(la.events, dateKeys, eventKeys);
    let daysErrorTotals = InstanceAccumulator.TotalCountsByDay(la.errors, dateKeys, eventKeys);
    let totalErrors: number = 0;
    let rows: any[] = [];
    let d: string;
    let row: any;
    for (d in daysUsers) if (daysUsers.hasOwnProperty(d))
    {
      row = { id: String(rows.length)+1, day: d, users: String(daysUsers[d].count) };
      let s = daysSessions[d];
      row.sessions = s === undefined ? '0' : String(s.count);
      let nErrors: number = daysErrorTotals[d] ? daysErrorTotals[d].count : 0;
      totalErrors += nErrors;
      row.errors = String(nErrors);
      let e = daysEvents[d];
      Object.keys(eventKeys).forEach((id: string) => {
          let name = (eventKeys === specialEventKey) ? specialEventNames[id] : id;
          row[name] = (e[id] === undefined) ? '0' : String(e[id].count);
        });
      rows.push(row);
    }

    // Add totals row to front of array
    row = {
            id: '0',
            day: '!total',
            users: String(InstanceAccumulator.NUniques(la.users)),
            sessions: String(InstanceAccumulator.NUniques(la.sessions)),
            errors: String(totalErrors)
          };
    Object.keys(specialEventNames).forEach((id: string) => {
        let count: number = 0;
        let name: string = (eventKeys === specialEventKey) ? specialEventNames[id] : id;
        rows.forEach((o: any) => { count += Number(o[name]); });
        row[name] = String(count);
      });
    rows.unshift(row);

    // And sort by day
    rows.sort((r1: any, r2: any) => {
        return String(r1.day) < String(r2.day) ? -1 : (String(r1.day) > String(r2.day) ? 1 : 0);
      });
    return rows;
  }

  static getLastDay(la: ILogAccumulator): any[]
  {
    let today = InstanceAccumulator.getToday(la.events);
    let rows = LogAccumulator.getDailyRows(la, today);
    if (rows.length == 0)
      return [];
    let row = rows[rows.length-1];
    delete row.day;
    return objectToRows(row);
  }

  static getTimerRows(la: ILogAccumulator, datePattern?: string, eventKeys?: IKeyIndex): any[]
  {
    let dateKeys = LogAccumulator.datePatternToKeys(la, datePattern);
    let rows1 = InstanceAccumulator.getValueRows(la.asyncTimers, dateKeys, eventKeys);
    let rows2 = InstanceAccumulator.getValueRows(la.syncTimers, dateKeys, eventKeys);
    return [...rows1, ...rows2];
  }

  static getErrorRows(la: ILogAccumulator, datePattern?: string, eventKeys?: IKeyIndex): any[]
  {
    let dateKeys = LogAccumulator.datePatternToKeys(la, datePattern);
    return InstanceAccumulator.getCountRows(la.errors, dateKeys, eventKeys);
  }

  static getEventRows(la: ILogAccumulator, datePattern?: string, eventKeys?: IKeyIndex): any[]
  {
    let dateKeys = LogAccumulator.datePatternToKeys(la, datePattern);
    return InstanceAccumulator.getCountRows(la.events, dateKeys, eventKeys);
  }

  static getValueRows(la: ILogAccumulator, datePattern?: string, eventKeys?: IKeyIndex): any[]
  {
    let dateKeys = LogAccumulator.datePatternToKeys(la, datePattern);
    return InstanceAccumulator.getValueRows(la.values, dateKeys, eventKeys);
  }
}

export class FsmLogAccum extends FSM.Fsm
{
  options: AccumOptions;
  root: LogItem;
  blobLs: LogBlob;
  lastErrorsRows: any[];
  firstScanDone: boolean;

  constructor(env: Environment, accum?: ILogAccumulator, options?: AccumOptions)
  {
    super(env);
    this.options = Util.shallowAssignImmutable(DefaultOptions, options);
    this.firstScanDone = false;
    this.lastErrorsRows = [];
    this.root = new LogItem('root');
    if (accum !== undefined)  // just using to query
    {
      this.root.accum = accum;
      this.setState(FSM.FSM_DONE);
    }
  }

  get env(): Environment { return this._env as Environment }

  resetLastErrors(): void
  {
    this.lastErrorsRows = this.getRows('errors');
  }

  getIncrementalErrors(): any[]
  {
    if (this.lastErrorsRows == null) this.lastErrorsRows = this.getRows('errors');
    return deltaRows(this.lastErrorsRows, this.getRows('errors'));
  }

  refresh(): void
  {
    if (this.done)
    {
      this.clearDependentError();
      this.setState(FSM.FSM_STARTING);
    }
  }

  getCategories(): string[]
  {
    if (this.root.accum == null)
      return [ 'Loading' ];

    return [
        'summary',
        'loading',
        'errors',
        'values',
        'events',
        'timers',
        'daily',
        'today',
      ];
  }

  getSummaryRows(): any[]
  {
    return LogAccumulator.getLastDay(this.root.accum).concat(this.getIncrementalErrors());
  }

  getRows(category: string, datePattern?: string, eventKeys?: IKeyIndex): any[]
  {
    if (this.root.accum === undefined) return [];

    switch (category)
    {
      default:
        return null;
      case 'Loading':
      case 'unknown':
        return [];

      case 'loading':
        return [
                 {
                   id: '0',
                   logs: String(Util.countKeys(this.root.accum.logsSeen))
                 }
               ];

      case 'errors':
        return LogAccumulator.getErrorRows(this.root.accum, datePattern, eventKeys);
        break;

      case 'values':
        return LogAccumulator.getValueRows(this.root.accum, datePattern, eventKeys);
        break;

      case 'events':
        return LogAccumulator.getEventRows(this.root.accum, datePattern, eventKeys);
        break;

      case 'timers':
        return LogAccumulator.getTimerRows(this.root.accum, datePattern, eventKeys);
        break;

      case 'daily':
        return LogAccumulator.getDailyRows(this.root.accum, datePattern, eventKeys);
        break;

      case 'today':
        return LogAccumulator.getTodayRows(this.root.accum);
        break;

      case 'summary':
        return this.getSummaryRows();
        break;
    }
  }

  tick(): void
  {
    if (this.ready && this.isDependentError)
      this.setState(FSM.FSM_ERROR);
    else if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.blobLs = LogBlob.createForLs(this.env, this.options);
          this.waitOn(this.blobLs.fsmArray);
          this.setState(FSM_LISTING);
          break;

        case FSM_LISTING:
          this.root.addListing(this.blobLs.keys.filter(this.blobLs.filter));
          this.blobLs.resetList();
          if (this.blobLs.fsmList.done)
          {
            delete this.blobLs;
            this.waitOn(new FsmDeepAggregate(this.env, this.root, this.options));
            this.setState(FSM_AGGREGATING);
          }
          else
            this.waitOn(this.blobLs.fsmArray);
          break;

        case FSM_AGGREGATING:
          if (! this.firstScanDone)
          {
            this.firstScanDone = true;
            this.resetLastErrors();
          }
          this.waitOn(new FsmSaveAggregates(this.env, this.root, this.options));
          this.setState(FSM_SAVING);
          break;

        case FSM_SAVING:
          this.setState(FSM.FSM_DONE);
          break;
      }
    }
  }
}
