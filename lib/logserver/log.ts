// Node libraries
import * as fs from 'fs';
import * as Stream from 'stream';

// Shared libraries
import { Util, Context, LogAbstract, FSM } from '@dra2020/baseclient';

import * as Storage from '../storage/all';
import * as FF from '../fsmfile/all';
import * as JS from '../jsonstream/all';

export interface LogEnvironment
{
  context: Context.IContext;
  fsmManager: FSM.FsmManager;
  storageManager: Storage.StorageManager;
  log: LogAbstract.ILog;
}

// Max size of log file (approximated)
const LogContextDefaults: Context.ContextValues = {
    maxlogfilesize: 1000000,
    maxloglatency: 1000 * 60 * 60 * 4,  // every four hours
    logtimeresolution: 1000 * 30, // output log timestamp every 30 seconds
  };

const FSM_SAVING: number = FSM.FSM_CUSTOM1;
const FSM_UPLOADING: number = FSM.FSM_CUSTOM2;

// Current 'kind's of log entries in use:
//  'event': some event occurred (event property)
//  'error': some error happened
//  'value': some internal state variable has some value (name, value properties)
//  'sync': synchronous (blocking) time interval (ms property in milliseconds)
//  'async': elapsed time interval (ms property in milliseconds)

// LogManager receives logging requests (in object form) and buffers them to some internal size
// limit and then saves them out as a storage blob.
// Blobs are written out in a form that lets a separate offline process aggregate them as necessary.
// Entries are written as CSV entries with the first lines giving schema definitions for the
// entries in the file, subsequent entries defining templates with default values for log entries
// matching a specific schema and subsequent entries providing the data, matching a template.
// This allows both a flexible logging format as well as a compact representation.
//
//  Schema are of the form:
//    'S',SchemaId,P1,P2,P3
//
//  Templates are of the form:
//    'T',SchemaId,V1,V2,V3
//
//  Entries are:
//    'L',TemplateId,V1,,V3
//
//  Where "SchemaId" is a small integer and a line that starts with S defines the
//  schema and a line that starts with T references it. A line that starts with L
//  references a template and only specifies the values that differ from the template.
//

// A Schema maps property value to the index in the CSV line, starting with index 2
type Schema = { [prop: string]: number };

// A SchemaMapping indexes by a sequence ID for the Schema
type SchemaIndex = { [key: number]: Schema };

// A schema mapping maps a hash value for the schema to a small integer ID
type SchemaMapping = { [key: number]: number };

interface SchemaResult
{
  schemaid: number;
  schema: Schema;
  csv: string | null;
}

class SchemaManager
{
  index: SchemaIndex;
  map: SchemaMapping;
  nextid: number;

  constructor()
  {
    this.clear();
  }

  clear(): void
  {
    this.index = {};
    this.map = {};
    this.nextid = 0;
  }

  private extractSchema(o: any): Schema
  {
    let s: Schema = {};
    let i: number = 2;  // index in CSV line for this value (starts with 'L,schemaid')
    for (let p in o) if (o.hasOwnProperty(p))
      s[p] = i++;
    return s;
  }

  add(o: any): SchemaResult
  {
    let s = this.extractSchema(o);
    let h = Util.hashObject(s);
    let id = this.map[h];
    if (id === undefined)
    {
      id = this.nextid++;
      this.map[h] = id;
      this.index[h] = s;

      // Build schema string
      let sa: string[] = [];
      sa.push('S');
      sa.push(String(this.nextid-1));
      for (let p in s) if (s.hasOwnProperty(p))
        sa[s[p]] = p;
      return { schemaid: id, schema: s, csv: sa.join(',') };
    }
    return { schemaid: id, schema: s, csv: null };
  }
}

// A template is just an object with associated schema
interface Template
{
  sr: SchemaResult;
  o: any;
}

interface TemplateResult
{
  templateid: number;
  template: Template;
  csv: string | null;
}

function equalCount(o1: any, o2: any): number
{
  let n: number = 0;

  for (let p in o1) if (o1.hasOwnProperty(p))
    if (typeof o1[p] !== 'number' && o1[p] === o2[p]) n++;

  return n;
}

class TemplateManager
{
  templates: Template[];

  constructor()
  {
    this.clear();
  }

  clear(): void
  {
    this.templates = [];
  }

  add(sr: SchemaResult, o: any): TemplateResult
  {
    let tr: TemplateResult = { templateid: -1, template: null, csv: null };
    let bestCount: number = 0;
    let totalCount: number = 0;
    for (let p in o) if (o.hasOwnProperty(p) && typeof o[p] !== 'number')
      totalCount++;

    for (let i: number = 0; i < this.templates.length; i++)
    {
      let t = this.templates[i];
      if (t.sr.schemaid === sr.schemaid)
      {
        let n = equalCount(t.o, o);
        if (tr.template == null || n > bestCount)
        {
          tr.template = t;
          tr.templateid = i;
          bestCount = n;
        }
      }
    }

    // Either found no template or found a poor one
    if (tr.template == null || bestCount < totalCount)
    {
      o = Util.shallowCopy(o);
      this.templates.push({ sr: sr, o: o });
      tr.templateid = this.templates.length-1;
      tr.template = this.templates[tr.templateid];
      let sa: string[] = [];
      sa.push('T');
      sa.push(String(sr.schemaid));
      for (let p in o) if (o.hasOwnProperty(p))
        sa[sr.schema[p]] = csvField(o[p]);
      tr.csv = sa.join(',');
    }

    return tr;
  }
}

class LogInstance
{
  env: LogEnvironment;
  schemas: string[];
  templates: string[];
  log: string[];
  size: number;

  constructor(env: LogEnvironment)
  {
    this.env = env;
    this.schemas = [];
    this.log = [];
    this.templates = [];
    this.size = 0;
  }

  addSchema(s: string): void
  {
    this.schemas.push(s);
    this.size += s.length + 2;
  }

  addTemplate(s: string): void
  {
    this.templates.push(s);
    this.size += s.length + 2;
  }

  addLog(s: string): void
  {
    this.log.push(s);
    this.size += s.length + 2;
  }

  full(): boolean
  {
    return this.size > this.env.context.xnumber('maxlogfilesize');
  }

  empty(): boolean
  {
    return this.size == 0;
  }
}

class FsmSaveTmp extends FSM.Fsm
{
  stream: JS.BufferWritable;
  buffer: Buffer;
  lines: string[][];
  curArray: number;
  curIndex: number;

  constructor(env: LogEnvironment, log: LogInstance)
  {
    super(env);
    this.stream = new JS.BufferWritable();
    this.lines = [ log.schemas, log.templates, log.log ];
    this.curArray = 0;
    this.curIndex = 0;
    this.onError = this.onError.bind(this);
    this.onFinish = this.onFinish.bind(this);
    this.onDrain = this.onDrain.bind(this);
  }

  start(): void
  {
    this.stream.on('error', this.onError);
    this.stream.on('finish', this.onFinish);
    this.write();
  }

  write(): void
  {
    if (this.stream)
    {
      let bOK: boolean = true;

      for (; bOK && this.curArray < this.lines.length; this.curArray++, this.curIndex = 0)
      {
        let a = this.lines[this.curArray];
        for (; bOK && this.curIndex < a.length; this.curIndex++)
        {
          bOK = this.stream.write(a[this.curIndex], 'utf8');
          bOK = this.stream.write('\n', 'utf8') && bOK;
        }

        if (! bOK)
          break;
      }

      if (this.curArray == this.lines.length)
        this.stream.end();
      else 
        this.stream.once('drain', this.onDrain);
    }
  }

  onFinish(buffer?: Buffer): void
  {
    this.buffer = buffer;
    if (this.stream)
      this.stream = null;
    if (! this.done)
      this.setState(FSM.FSM_DONE);
  }

  onDrain(): void
  {
    this.write();
  }

  onError(): void
  {
    this.setState(FSM.FSM_ERROR);
    this.onFinish();
  }

  tick(): void
  {
    if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.setState(FSM.FSM_PENDING);
          this.start();
          break;
      }
    }
  }
}

class FsmUpload extends FSM.Fsm
{
  log: LogInstance;
  name: string;
  fsmTmp: FsmSaveTmp;
  fsmSave: FSM.Fsm;

  constructor(env: LogEnvironment, name: string, log: LogInstance)
  {
    super(env);
    this.name = name;
    this.log = log;
  }

  get env(): LogEnvironment { return this._env as LogEnvironment; }

  upload(buffer: Buffer): FSM.Fsm
  {
    let params: Storage.BlobParams = {
        bucket: 'logs',
        id: this.name,
        saveFromType: 'buffer',
        saveFrom: buffer,
        ContentEncoding: 'gzip'
      };
    let blob = new Storage.StorageBlob(this.env, params);
    blob.setDirty();
    blob.checkSave(this.env.storageManager);
    return blob.fsmSave;
  }

  tick(): void
  {
    if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.setState(FSM_SAVING);
          this.fsmTmp = new FsmSaveTmp(this.env, this.log);
          this.waitOn(this.fsmTmp);
          break;

        case FSM_SAVING:
          this.setState(FSM_UPLOADING);
          this.fsmSave = this.upload(this.fsmTmp.buffer);
          this.waitOn(this.fsmSave);
          break;

        case FSM_UPLOADING:
          this.setState(FSM.FSM_DONE);
          break;
      }
    }
  }
}

function csvField(a: any): string
{
  let s = String(a);
  let i = s.indexOf('"');
  let j = s.indexOf(',');
  if (i == -1 && j == -1)
    return s;
  return `'${s}'`;
}

class LogManager implements LogAbstract.ILog
{
  env: LogEnvironment;
  id: string;
  count: number;
  nextid: number;
  schemaManager: SchemaManager;
  templateManager: TemplateManager;
  curLog: LogInstance;
  msLatencyDump: number;
  msLatencyStamp: number;
  onlyUploadErrors: boolean;
  stamps: any[];
  toconsole: boolean;
  msExpireDump: number;
  msExpireStamp: number;
  _chatters: string[];

  constructor(env: LogEnvironment)
  {
    this.env = env;
    env.context.setDefaults(LogContextDefaults);
    this.id = Util.createGuid();  // guid for this server instance
    this.count = 0;
    this.nextid = 0;              // to make next log file name unique
    this.stamps = [];
    this.schemaManager = new SchemaManager();
    this.templateManager = new TemplateManager();
    this.curLog = new LogInstance(this.env);
    this.toconsole = env.context.xflag('log_to_console');
    this.msLatencyDump = env.context.xnumber('maxloglatency');
    this.msLatencyStamp = env.context.xnumber('logtimeresolution');
    this.onlyUploadErrors = env.context.xflag('onlyuploaderrors');
    let msNow = (new Date()).getTime();
    this.msExpireDump = msNow + this.msLatencyDump;
    this.msExpireStamp = msNow + this.msLatencyStamp;
    this._chatters = null;
  }

  logit(o: any): void
  {
    if (this.toconsole) console.log(JSON.stringify(o));
    // Add to current aggregate
    let sr: SchemaResult = this.schemaManager.add(o);
    if (sr.csv)
      this.curLog.addSchema(sr.csv);
    let tr: TemplateResult = this.templateManager.add(sr, o);
    if (tr.csv)
      this.curLog.addTemplate(tr.csv);
    let sa: string[] = [];
    sa.push('L');
    sa.push(String(tr.templateid));
    for (let p in o) if (o.hasOwnProperty(p))
    {
      if (o[p] !== tr.template.o[p])
        sa[sr.schema[p]] = csvField(o[p]);
      else
        sa[sr.schema[p]] = '';
    }
    this.curLog.addLog(sa.join(','));
  }

  stampit(): void
  {
    for (let i: number = 0; i < this.stamps.length; i++)
      this.logit(this.stamps[i]);
  }

  stamp(o: any): void
  {
    this.stamps.push(o);
  }

  log(o: any, verbosity: number = 0): void
  {
    // Show some restraint
    if (verbosity > this.env.context.xnumber('verbosity'))
      return;

    if (o.kind === 'error')
      this.onlyUploadErrors = false;

    let msNow = (new Date()).getTime();

    if (o.kind === undefined)
      o.kind = 'misc';

    // Keep count at top of log
    this.count++;
    if (this.curLog.log.length === 0)
    {
      this.logit({ kind: 'misc', _count: this.count });
      this.stampit(); // Ensure every log file has these entries stamped on them
    }

    // Keep timestamp every 5 seconds when it changes from the setInterval call above
    if (this.msExpireStamp < msNow)
    {
      this.logit({ kind: 'misc', _time: Util.Now() });
      this.msExpireStamp = msNow + this.msLatencyStamp;
    }

    this.logit(o);

    if (this.curLog.full() || this.msExpireDump < msNow)
      this.rotateLog();
  }

  rotateLog(): FSM.Fsm
  {
    if (! this.onlyUploadErrors)
    {
      let prod = this.env.context.xflag('production') ? 'Prod' : 'Dev';
      let fsm = new FsmUpload(this.env, `Log_${prod}_${Util.Now()}_${this.nextid++}_${this.id}.csv`, this.curLog);
      this.curLog = new LogInstance(this.env);
      this.schemaManager.clear();
      this.templateManager.clear();
      this.msExpireDump = (new Date()).getTime() + this.msLatencyDump;
      return fsm;
    }
    return null;
  }

  dump(): FSM.Fsm
  {
    return this.rotateLog();
  }

  event(o: any, verbosity: number = 0): void
  {
    if (typeof o === 'string') o = { event: o };
    o.kind = 'event';
    this.log(o, verbosity);
  }

  error(o: any): void
  {
    if (typeof o === 'string') o = { event: o };
    o.kind = 'error';
    this.log(o);
  }

  value(o: any, verbosity: number = 0): void
  {
    o.kind = 'value';
    this.log(o, verbosity);
  }

  chatter(s: string): void
  {
    console.log(s);
    if (this._chatters)
      this._chatters.push(s);
  }

  chatters(): string[]
  {
    if (this._chatters == null)
      this._chatters = [];
    return this._chatters;
  }
}

export function create(env: LogEnvironment): LogAbstract.ILog
{
  return new LogManager(env);
}
