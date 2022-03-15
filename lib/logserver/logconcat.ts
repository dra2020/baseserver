import * as fs from 'fs';
import * as stream from 'stream';
import * as zlib from 'zlib';

// Shared libraries
import { Util, Context, LogAbstract, FSM } from '@dra2020/baseclient';

import * as Storage from '../storage/all';

import { LogBlob } from './logblob';

interface Environment
{
  context: Context.IContext;
  log: LogAbstract.ILog;
  fsmManager: FSM.FsmManager;
  storageManager: Storage.StorageManager;
}

let UniqueState = FSM.FSM_CUSTOM1;
const FSM_BLOB = UniqueState++;
const FSM_QUERYING = UniqueState++;
const FSM_LISTING = UniqueState++;
const FSM_AGGREGATING = UniqueState++;
const FSM_SAVING = UniqueState++;
const FSM_LOADING = UniqueState++;
const FSM_DELETING = UniqueState++;
const FSM_FILTERING = UniqueState++;

const reLog = /^Log_([^_]*)_(\d{4}-\d{2}-\d{2}T\d{2}:)(\d{2}:\d{2}.\d{3}Z)_(\d+)_[^\.]+.csv$/;
const reLine = /^(.),(\d+),(.*)$/;

// The compaction process involves:
//    1. Listing the log files.
//    2. Grouping into sets to compact/concat.
//    3. Downloading the files and concatenating them.
//    4. Uploading the concatenated file.
//    5. Deleting the files used to construct the aggregate.
//
// The motivation for this is that further processing on the logs and log bucket will be
// much more efficient with a smaller number of larger files. In particular, the lambda
// instances result in lots of small log files.
//

function logToKey(env: Environment, log: string): string
{
  // Log is of form:
  //  {Log,Agg}_{Prod,Dev}_YYYY-MM-DDTHH:MM:SS.UUUZ_{count}_{guid}.{csv,json}
  
  let prod = env.context.xflag('production') ? 'Prod' : 'Dev';
  let a = reLog.exec(log);
  if (a && a.length == 5 && a[1] === prod)
    return `Log_${a[1]}_${a[2]}`;
  return null;
}

function specialZeroLog(log: string): boolean
{
  let a = reLog.exec(log);
  return !!a && a.length == 5 && a[3] == '00:00.000Z';
}

export class FsmCompact extends FSM.Fsm
{
  blobLs: LogBlob;
  clusters: { [key: string]: string[] };

  constructor(env: Environment)
  {
    super(env);
    this.clusters = {};
  }

  get env(): Environment { return this._env as Environment }

  gatherLogList(logs: string[]): void
  {
    logs.forEach((log: string) => {
        let key = logToKey(this.env, log);
        if (key)
        {
          let cluster = this.clusters[key];
          if (cluster === undefined)
          {
            cluster = [];
            this.clusters[key] = cluster;
          }
          cluster.push(log);
        }
      });
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
          this.blobLs = LogBlob.createForLs(this.env);
          this.waitOn(this.blobLs.fsmArray);
          this.setState(FSM_LISTING);
          break;

        case FSM_LISTING:
          this.gatherLogList(this.blobLs.keys);
          this.blobLs.resetList();
          if (this.blobLs.fsmList.done)
            this.setState(FSM_FILTERING);
          else
            this.waitOn(this.blobLs.fsmList);
          break;

        case FSM_FILTERING:
          Object.keys(this.clusters).forEach((key: string) => {
              let cluster = this.clusters[key];
              if (cluster.length == 1 && specialZeroLog(cluster[0]))
                delete this.clusters[key];
            });
          this.env.log.chatter(`logconcat: compacting ${Util.countKeys(this.clusters)} log clusters`);
          this.setState(FSM_AGGREGATING);
          break;

        case FSM_AGGREGATING:
          for (let key in this.clusters) if (this.clusters.hasOwnProperty(key))
          {
            let cluster = this.clusters[key];
            this.waitOn(new FsmCompactOne(this.env, key, cluster));
            delete this.clusters[key];
            break;  // one at a time
          }
          if (this.ready)
            this.setState(FSM.FSM_DONE);
          break;
      }
    }
  }
}

// This is a little tricky because file format is wacky. Numeric value after 'S' in schema line is just
// the schema number (redundant to implicit ordering). The numeric value after 'T' in template line is
// a reference to the schema used on that line. The template number is implicit in the ordering. And then
// the number after 'L' in a log line is a reference to this implicit template numbering order.

class Permuter
{
  tag: string;  // 'T' or 'S'
  entries: { [key: string]: { key: string, to: string, from: string } };
  mapper: { [from: string]: string };
  length: number;

  constructor(tag: string)
  {
    this.tag = tag;
    this.entries = {};
    this.mapper = {};
    this.length = 0;
  }

  reset(): void
  {
    Object.keys(this.entries).forEach((key: string) => {
        let entry = this.entries[key];
        entry.from = null;
      });
    this.mapper = {};
  }

  add(fields: string[]): void
  {
    if (fields.length != 4)
      throw 'Permuter:add: unexpected line format';
    let key = this.tag === 'S' ? fields[3] : `${fields[2]},${fields[3]}`;
    let entry = this.entries[key];
    if (entry === undefined)
    {
      entry = { key: key, to: String(this.length++), from: this.tag === 'S' ? fields[2] : String(Util.countKeys(this.mapper)) };
      this.entries[key] = entry;
    }
    else
    {
      if (entry.from != null)
        throw 'Permuter:add: unexpected duplicate field entry';
      else
        entry.from = this.tag === 'S' ? fields[2] : String(Util.countKeys(this.mapper));
    }
    this.mapper[entry.from] = entry.to;
  }

  map(from: string): string
  {
    return this.mapper[from];
  }

  asCSV(): string[]
  {
    let entryarray = Object.keys(this.entries).map((key: string) => { return this.entries[key] });
    entryarray.sort((e1: any, e2: any) => { return Number(e1.to) < Number(e2.to) ? -1 : 1 });
    return entryarray.map((e: any) => {
        if (this.tag === 'S')
          return new Array(this.tag, e.to, e.key).join(',');
        else
          return new Array(this.tag, e.key).join(',');
      });
  }
}

export class FsmCompactOne extends FSM.Fsm
{
  key: string;
  cluster: string[];
  blobs: LogBlob[];
  csv: string[];
  schemas: Permuter;
  templates: Permuter;

  constructor(env: Environment, key: string, cluster: string[])
  {
    super(env);
    this.key = key;
    this.cluster = cluster;
    this.csv =  [];
    this.schemas = new Permuter('S');
    this.templates = new Permuter('T');
    this.env.log.chatter(`logconcat: compacting ${this.key} with ${this.cluster.length} log files`);
  }

  get env(): Environment { return this._env as Environment }

  loadBlobs(): void
  {
    this.blobs.forEach((blob: LogBlob) => {
        let lines = blob.result.split('\n');
        lines.forEach((line: string) => {
            let a = reLine.exec(line);
            if (a && a.length == 4)
            {
              switch (a[1])
              {
                case 'S': this.schemas.add(a); break;
                case 'T': a[2] = this.schemas.map(a[2]); this.templates.add(a); break;
                case 'L': a[2] = this.templates.map(a[2]); this.csv.push(a.slice(1).join(',')); break;
                default:
                  throw 'FsmCompactOne: unexpected line in csv file';
              }
            }
          });
        this.schemas.reset();
        this.templates.reset();
      });
    this.blobs = [];
  }

  saveBlobs(): void
  {
    let csv = [...this.schemas.asCSV(), ...this.templates.asCSV(), ...this.csv];
    let key = `${this.key}00:00.000Z_0_${Util.createGuid()}.csv`;
    this.env.log.chatter(`logconcat: uploading ${key}`);
    let blob = LogBlob.createForUpload(this.env, key, csv.join('\n'));
    this.waitOn(blob.fsmLoad);
  }

  deleteBlobs(): void
  {
    this.cluster.forEach((log: string) => {
        let blob = LogBlob.createForDelete(this.env, log);
        this.waitOn(blob.fsmDel);
      });
  }

  tick(): void
  {
    if (this.ready && this.isDependentError)
    {
      this.env.log.error('logconcat: failure building log cluster');
      this.env.log.chatter('logconcat: failure building log cluster');
      this.setState(FSM.FSM_ERROR);
    }
    else if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.blobs = [];
          this.cluster.forEach((log: string) => {
              let blob = LogBlob.createForDownload(this.env, log);
              this.blobs.push(blob);
              this.waitOn(blob.fsmLoad);
            });
          this.setState(FSM_LOADING);
          break;

        case FSM_LOADING:
          this.loadBlobs();
          this.saveBlobs();
          this.setState(FSM_SAVING);
          break;

        case FSM_SAVING:
          this.deleteBlobs();
          this.setState(FSM_DELETING);
          break;

        case FSM_DELETING:
          this.env.log.chatter(`logconcat: compacted ${this.key}`);
          this.setState(FSM.FSM_DONE);
          break;
      }
    }
  }
}

