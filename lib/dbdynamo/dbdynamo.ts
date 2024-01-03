// Node
import * as fs from 'fs';
import * as stream from 'stream';
import * as zlib from 'zlib';

// Public dynamodb API
import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBStreams } from '@aws-sdk/client-dynamodb-streams';

// Shared libraries
import { Util, LogAbstract, Context, FSM } from '@dra2020/baseclient';

import * as DB from '../dbabstract/all';
import * as Storage from '../storage/all';

export interface Environment
{
  context: Context.IContext;
  log: LogAbstract.ILog;
  fsmManager: FSM.FsmManager;
  storageManager: Storage.StorageManager;
}

export interface EnvironmentEx
{
  context: Context.IContext;
  log: LogAbstract.ILog;
  fsmManager: FSM.FsmManager;
  storageManager: Storage.StorageManager;
  dbx: DynamoClient;
}

const DBDynamoContextDefaults: Context.ContextValues =
{
  dynamo_error_frequency: 0,
}

class Deadline
{
  elapsed: Util.Elapsed;
  msDeadline: number;

  constructor(msDeadline: number)
  {
    this.elapsed = new Util.Elapsed();
    this.msDeadline = msDeadline;
  }

  start(): void
  {
    this.elapsed.start();
  }

  get msRemaining(): number
  {
    this.elapsed.end();
    let msLeft = this.msDeadline - this.elapsed.ms();
    if (msLeft < 0) msLeft = 0;
    return msLeft;
  }

  get passed(): boolean
  {
    this.elapsed.end();
    return this.elapsed.ms() >= this.msDeadline;
  }
}

function rawTypedValue(o: any): any
{
  // null is special
  if (o === undefined || o == null || (typeof o === 'string' && o === ''))
    return { NULL: true };

  // raw type
  switch (typeof o)
  {
    case 'boolean': return { BOOL: o };
    case 'number':  return { N: String(o) };
    case 'string':  return { S: o };
    default:        return { S: String(o) };
    case 'object':
      {
        if (Array.isArray(o))
        {
          let a: any[] = [];
          o.forEach((v: any) => a.push(rawTypedValue(v)));
          return { L: a };
        }
        else
        {
          let v: any = {};
          Object.keys(o).forEach((p: string) => { v[p] = rawTypedValue(o[p]) } );
          return { M: v };
        }
      }
  }
}

function rawNakedValue(o: any): any
{
  if (Util.countKeys(o) !== 1)
    throw new Error(`dynamodb: only expect one key in typed value: ${JSON.stringify(o)}`);

  for (let p in o) if (o.hasOwnProperty(p))
  {
    switch (p)
    {
      case 'S':     return String(o[p]);
      case 'B':     return o[p];
      case 'BOOL':  return Boolean(o[p]);
      case 'NULL':  return null;
      case 'N':     return Number(o[p]);
      case 'SS':    return o[p];
      case 'M':
        {
          let r: any = {};
          Object.keys(o[p]).forEach((pp: string) => { r[pp] = rawNakedValue(o[p][pp]) });
          return r;
        }
      case 'L':
        {
          let a: any[] = [];
          o[p].forEach((v: any) => a.push(rawNakedValue(v)));
          return a;
        }
      default:
        throw new Error(`dynamodb: type ${p} not recognized`);
    }
  }
}

function typedValue(key: string, value: any, attributeIndex: any): any
{
  switch (attributeIndex[key])
  {
    case 'S':
      return (value == null || value === '') ? rawTypedValue(value) : { S: String(value) };
    case 'B':
      return { B: value };
    case 'BOOL':
      return { BOOL: Boolean(!!value) };
    case 'N':
      return { N: String(Number(value)) };
    case 'SS':
      return { SS: value };
    case 'M':
    case 'L':
    case undefined:
      return rawTypedValue(value);
    default:
      throw new Error(`dynamodb: Type ${attributeIndex[key]} unsupported in schema definition`);
  }
}

function keysPresent(o: any, keyschema: any, attributeIndex: any): any
{
  let typedKey: any = {};
  let bValid: boolean = false;

  // First determine if all the projected key schema attributes are present in the object
  for (let i: number = 0; i < keyschema.length; i++)
  {
    let k = keyschema[i];
    let v = o[k.AttributeName];
    if (v === undefined)
      return bValid ? typedKey : null;
    else
    {
      typedKey[k.AttributeName] = typedValue(k.AttributeName, v, attributeIndex);
      if (k.KeyType === 'HASH') bValid = true;
    }
  }

  // Now verify that the schema attributes  is ALL that are available in the query object
  //for (let p in o) if (o.hasOwnProperty(p))
  //if (typedKey[p] === undefined)
  //return null;

  // OK, this query matches this key schema
  return typedKey;
}


class FsmAPIWatch extends FSM.Fsm
{
  constructor(env: Environment)
  {
    super(env);
  }

  get env(): Environment { return this._env as Environment }

  tick(): void
  {
    //this.env.log.value({ event: 'dynamodb: APIs outstanding', value: this.nWaitOn });
  }
}

const FSM_LISTING = FSM.FSM_CUSTOM1;
const FSM_DESCRIBING = FSM.FSM_CUSTOM2;
const FSM_DESCRIBE = FSM.FSM_CUSTOM3;

class FsmListTables extends FSM.Fsm
{
  tables: any;
  workStack: string[];

  constructor(env: Environment)
  {
    super(env);
    this.tables = {};
  }

  get env(): EnvironmentEx { return this._env as EnvironmentEx }

  tick(): void
  {
    if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.setState(FSM_LISTING);
          this.env.dbx.dynamodb.listTables({}, (err: any, result: any) => {
              //console.log(`7: AWS testing: DynamoDB.listTables called`);
              if (err)
              {
                //console.log(`dynamodb: listTables error: ${JSON.stringify(err)}`);
                this.env.log.error({ event: 'dynamodb: listTables', detail: JSON.stringify(err) });
                this.setState(FSM.FSM_ERROR);
              }
              else
              {
                if (result.TableNames)
                  result.TableNames.forEach((name: string) => this.tables[name] = {});
                this.workStack = Object.keys(this.tables);
                this.setState(FSM_DESCRIBE);
              }
            });
          break;

        case FSM_LISTING:
        case FSM_DESCRIBING:
          // Waiting for callback to move out of this state
          break;

        case FSM_DESCRIBE:
          if (this.workStack && this.workStack.length > 0)
          {
            this.setState(FSM_DESCRIBING);
            let params: any = { TableName: this.workStack.pop() };
            this.env.dbx.dynamodb.describeTable(params, (err: any, result: any) => {
                //console.log(`8: AWS testing: DynamoDB.describeTable called`);
                if (err)
                {
                  //console.log(`dynamodb: describeTable error: ${JSON.stringify(err)}`);
                  this.env.log.error({ event: 'dynamodb: describeTable', detail: JSON.stringify(err) });
                  this.setState(FSM.FSM_ERROR);
                }
                else
                {
                  this.tables[params.TableName] = result.Table;
                  this.setState(FSM_DESCRIBE);
                }
              });
          }
          else
            this.setState(FSM.FSM_DONE);
          break;
      }
    }
  }
}

export function create(env: Environment): DB.DBClient { return new DynamoClient(env) }

const FSM_CREATE = FSM.FSM_CUSTOM8;
const FSM_CREATING = FSM.FSM_CUSTOM9;

export class DynamoClient extends DB.DBClient
{
  serializerUpdate: FSM.FsmSerializer;
  fsmAPIWatch: FsmAPIWatch;
  fsmListTables: FsmListTables;
  dynamodb: DynamoDB;
  dynamostream: DynamoDBStreams;
  pendingCols: DynamoCollection[];
  existingCols: { [name: string]: DynamoCollection };
  bList: boolean;

  constructor(env: Environment)
  {
    super(env);
    env.context.setDefaults(DBDynamoContextDefaults);
    this.env.dbx = this;

    if (env.context.xstring('aws_access_key_id') === undefined || env.context.xstring('aws_secret_access_key') === undefined)
    {
      this.env.log.error('DynamoDB: not configured: exiting');
      this.env.log.dump();
      process.exit(1);
    }

    this.dynamodb = new DynamoDB({apiVersion: '2012-08-10', region: 'us-west-2'});
    this.dynamostream = new DynamoDBStreams({apiVersion: '2012-08-10', region: 'us-west-2'});

    this.pendingCols = [];
    this.existingCols = {};
    this.serializerUpdate = new FSM.FsmSerializer(env);
    this.fsmAPIWatch = new FsmAPIWatch(env);
  }

  get env(): EnvironmentEx { return this._env as EnvironmentEx; }

  get Production(): boolean { return this.env.context.xflag('production'); }
  get DBName(): string { return this.Production ? 'prod' : 'dev'; }
  get dynamoErrorFrequency(): number { return this.env.context.xnumber('dynamo_error_frequency'); }

  createCollection(name: string, options: any): DB.DBCollection
  {  
    if (this.existingCols[name]) return this.existingCols[name];
    const defaultOptions: any = { Schema: { id: 'S' }, KeySchema: { id: 'HASH' } };
    options = Util.deepCopy(Util.shallowAssignImmutable(defaultOptions, options));
    options.AttributeDefinitions = DB.fromCompactSchema(options.Schema);
    options.KeySchema = DB.fromCompactKey(options.KeySchema);
    if (options.GlobalSecondaryIndexes)
      options.GlobalSecondaryIndexes = options.GlobalSecondaryIndexes.map((v: any) => DB.fromCompactIndex(v));
    let col = new DynamoCollection(this.env, this, name, options);
    this.ensureCollection(col);
    this.existingCols[name] = col;
    this.fsmAPIWatch.waitOn(col);
    return col;
  }

  createStream(col: DynamoCollection): FSM.FsmArray
  {
    return col.createStream();
  }

  closeStream(col: DynamoCollection): void
  {
    col.closeStream();
  }

  ensureCollection(col: DynamoCollection): void
  {
    this.pendingCols.push(col);
    if (this.done) this.setState(FSM_CREATE);
  }

  createUpdate(col: DynamoCollection, query: any, values: any): DB.DBUpdate
  {
    let update = new DynamoUpdate(this.env, col, query, values);
    if (query && query.id)
      this.serializerUpdate.serialize(query.id, update);
    this.fsmAPIWatch.waitOn(update);
    return update;
  }

  createUnset(col: DynamoCollection, query: any, values: any): DB.DBUnset
  {
    let unset = new DynamoUnset(this.env, col, query, values);
    if (query && query.id)
      this.serializerUpdate.serialize(query.id, unset);
    this.fsmAPIWatch.waitOn(unset);
    return unset;
  }

  createDelete(col: DynamoCollection, query: any): DB.DBDelete
  {
    let del = new DynamoDelete(this.env, col, query);
    this.fsmAPIWatch.waitOn(del);
    return del;
  }

  createFind(col: DynamoCollection, filter: any): DB.DBFind
  {
    let find = new DynamoFind(this.env, col, filter);
    this.fsmAPIWatch.waitOn(find);
    return find;
  }

  createQuery(col: DynamoCollection, filter: any): DB.DBQuery
  {
    let query = new DynamoQuery(this.env, col, filter);
    this.fsmAPIWatch.waitOn(query);
    return query;
  }

  createIndex(col: DynamoCollection, uid: string): DB.DBIndex
  {
    let index = new DynamoIndex(this.env, col, uid);
    this.fsmAPIWatch.waitOn(index);
    return index;
  }

  createClose(): DB.DBClose
  {
    let dbclose = new DynamoClose(this.env, this);
    this.fsmAPIWatch.waitOn(dbclose);
    return dbclose;
  }

  forceError(): boolean
  {
    if (!this.Production && (Math.random() < this.dynamoErrorFrequency))
      return true;
    return false;
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
          if (this.bList || this.fsmListTables == null)
          {
            this.fsmListTables = new FsmListTables(this.env);
            this.waitOn(this.fsmListTables);
            this.setState(FSM_LISTING);
            this.bList = false;
          }
          this.setState(FSM_LISTING);
          break;
        case FSM_LISTING:
          this.setState(this.pendingCols.length > 0 ? FSM_CREATE : FSM.FSM_DONE);
          break;
        case FSM_CREATE:
          // while there are tables to create, create them, otherwise re-list the tables
          while (this.pendingCols.length)
          {
            let col = this.pendingCols.pop();
            if (this.fsmListTables.tables[col.col.TableName] === undefined)
            {
              this.waitOn(new FsmExecuteCreate(this.env, col));
              this.setState(FSM_CREATING);
              return;
            }
          }
          this.setState(FSM.FSM_STARTING);
          break;
        case FSM_CREATING:
          this.bList = true;
          this.setState(FSM_CREATE);
          break;
      }
    }
  }
}

class FsmExecuteCreate extends FSM.Fsm
{
  col: DynamoCollection;

  constructor(env: Environment, col: DynamoCollection)
  {
    super(env);
    this.col = col;
  }

  get env(): Environment { return this._env as Environment }

  create(): void
  {
    let i: number;
    let options: any = {};
    options.TableName = this.col.col.TableName;
    options.AttributeDefinitions = this.col.options.AttributeDefinitions;
    options.BillingMode = this.col.options.BillingMode || 'PAY_PER_REQUEST';
    //if (options.ProvisionedThroughput === undefined)
      //options.ProvisionedThroughput = { ReadCapacityUnits: 1, WriteCapacityUnits: 1 };
    options.StreamSpecification = this.col.options.StreamSpecification || { StreamEnabled: true, StreamViewType: 'KEYS_ONLY' };
    if (this.col.options.GlobalSecondaryIndexes)
    {
      options.GlobalSecondaryIndexes = [];
      this.col.options.GlobalSecondaryIndexes.forEach((ix: any) => {
        if (ix.Projection === undefined)
          ix.Projection = { ProjectionType: 'ALL' };
        //if (ix.ProvisionedThroughput === undefined)
          //ix.ProvisionedThroughput = { ReadCapacityUnits: '1', WriteCapacityUnits: '1' };
        options.GlobalSecondaryIndexes.push(ix);
      });
    }

    let keys: any = {};
    if (this.col.options.KeySchema)
    {
      options.KeySchema = this.col.options.KeySchema;
      options.KeySchema.forEach((p: any) => keys[p.AttributeName] = true);
    }
    if (options.GlobalSecondaryIndexes)
      options.GlobalSecondaryIndexes.forEach((ix: any) => ix.KeySchema.forEach((p: any) => keys[p.AttributeName] = true));

    // Only include attribute definitions that are used in KeySchema or SecondaryIndices
    for (i = options.AttributeDefinitions.length - 1; i >= 0; i--)
    {
      let id: any = options.AttributeDefinitions[i];
      if (keys[id.AttributeName] === undefined)
        options.AttributeDefinitions.splice(i, 1);
    }

    this.setState(FSM.FSM_PENDING);
    this.col.dynamodb.createTable(options, (err: any, data: any) => {
        //console.log(`9: AWS testing: DynamoDB.createTable called`);
        if (err)
        {
          //console.log(`dynamodb: createTable error: ${JSON.stringify(err)}`);
          this.env.log.error({ event: 'dynamodb: createTable', detail: JSON.stringify(err) });
          this.setState(FSM.FSM_ERROR);
        }
        else
        {
          this.env.log.event({ event: 'createTable', detail: options.TableName });
          this.setState(FSM.FSM_DONE);
        }
      });
  }

  tick(): void
  {
    if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.create();
          break;

        // other state transitions happen in callback
      }
    }
  }
}

const DefaultCollectionOptions = { prefix: 'dra' };

export class DynamoCollection extends DB.DBCollection
{
  attributeIndex: any;
  keyIndex: any;
  fsmStream: FsmTableStream;

  constructor(env: Environment, client: DynamoClient, name: string, options: any)
  {
    super(env, client, name, Util.shallowAssignImmutable(DefaultCollectionOptions, options));
    this.waitOn(client);
    this.col = { TableName: `${this.options.prefix}-${client.DBName}-${name}` };
    this.constructIndex();
  }

  get env(): Environment { return this._env as Environment; }

  get dynclient(): DynamoClient { return this.client as DynamoClient }
  get dynamodb(): DynamoDB { return this.dynclient.dynamodb }

  createStream(): FSM.FsmArray
  {
    if (this.fsmStream == null)
      this.fsmStream = new FsmTableStream(this.env, this);
    return this.fsmStream.fsmResult;
  }

  closeStream(): void
  {
    if (this.fsmStream)
    {
      this.fsmStream.end();
      this.fsmStream = null;
    }
  }

  constructIndex(): void
  {
    this.attributeIndex = {};
    if (this.options.AttributeDefinitions)
      this.options.AttributeDefinitions.forEach((p: any) => this.attributeIndex[p.AttributeName] = p.AttributeType);
    this.keyIndex = {};
    if (this.options.KeySchema)
      this.options.KeySchema.forEach((p: any) => this.keyIndex[p.AttributeName] = p.KeyType);
  }

  addConditionExpression(query: any, key: any): void
  {
    query.isquery = true;
    let j: number = 0;
    let ev: any = {};
    query.ExpressionAttributeValues = ev;
    let en: any = {};
    query.ExpressionAttributeNames = en;
    for (let p in key) if (key.hasOwnProperty(p))
    {
      en[`#n${j}`] = p;
      ev[`:v${j}`] = key[p];
      j++;
    }
    query.KeyConditionExpression = this.toTestExpression(query);
  }

  toInternalQuery(query: any): any
  {
    let q: any = {};
    q.TableName = this.col.TableName;
    let key = keysPresent(query, this.options.KeySchema, this.attributeIndex);
    if (key)
    {
      if (Util.countKeys(this.options.KeySchema) != Util.countKeys(key))
        this.addConditionExpression(q, key);
      else
        q.Key = key;
      return q;
    }

    if (this.options.GlobalSecondaryIndexes !== undefined)
    {
      for (let i: number = 0; i < this.options.GlobalSecondaryIndexes.length; i++)
      {
        let ix = this.options.GlobalSecondaryIndexes[i];
        let key = keysPresent(query, ix.KeySchema, this.attributeIndex);
        if (key)
        {
          q.IndexName = ix.IndexName;
          this.addConditionExpression(q, key);
          return q;
        }
      }
    }
    return q;
  }

  toInternalExpression(o: any): any
  {
    // Make sure _atomicUpdate isn't explicitly specified
    delete o._atomicUpdate;

    let expr: any = {};
    let j: number = 1;
    let en: any = {};
    expr.ExpressionAttributeNames = en;
    let ev: any = {};
    expr.ExpressionAttributeValues = ev;
    en['#n0'] = '_atomicUpdate';
    ev[':v0'] = { N: '1' };
    for (let p in o) if (o.hasOwnProperty(p))
    {
      if (this.keyIndex[p] === undefined)
      {
        let v = typedValue(p, o[p], this.attributeIndex);
        if (v && (v.SS === undefined || v.SS.length > 0))
        {
          en[`#n${j}`] = p;
          ev[`:v${j}`] = v;
          j++;
        }
      }
    }

    // Ensure we don't have an empty update expression
    if (Util.countKeys(expr.ExpressionAttributeNames) == 0)
    {
      expr.ExpressionAttributeNames['#n0'] = '__nonempty';
      expr.ExpressionAttributeValues[':v0'] = { NULL: true };
    }

    return expr;
  }

  toTestExpression(expr: any): string
  {
    let sa: string[] = [];
    let j: number = 0;
    let n: number = Util.countKeys(expr.ExpressionAttributeNames);
    for (let i: number = 0; i < n; i++)
      sa.push(`#n${i} = :v${i}`);
    return sa.join(', ');
  }

  toUpdateExpression(expr: any): string
  {
    let saSet: string[] = [];
    let saAdd: string[] = [];
    let j: number = 0;
    let n: number = Util.countKeys(expr.ExpressionAttributeNames);
    for (let i: number = 1; i < n; i++)
    {
      let sv = `:v${i}`;
      let sn = `#n${i}`;
      let v = expr.ExpressionAttributeValues[sv];
      if (v.SS !== undefined)
        saAdd.push(`${sn} ${sv}`);
      else
        saSet.push(`${sn} = ${sv}`);
    }

    let fullExpr: string = 'ADD #n0 :v0';
    if (saSet.length > 0)
      fullExpr += ` SET ${saSet.join(', ')}`;
    if (saAdd.length > 0)
      fullExpr += ` ADD ${saAdd.join(', ')}`;
    return fullExpr;
  }

  toRemoveExpression(expr: any): string
  {
    let saRemove: string[] = [];
    let saDelete: string[] = [];
    let n: number = Util.countKeys(expr.ExpressionAttributeNames);
    for (let i: number = 1; i < n; i++)
    {
      let sv = `:v${i}`;
      let sn = `#n${i}`;
      let v = expr.ExpressionAttributeValues[sv];
      if (v.SS !== undefined)
       saDelete.push(`${sn} ${sv}`);
      else
      {
        delete expr.ExpressionAttributeValues[sv];
        saRemove.push(`#n${i}`);
      }
    }

    if (Util.isEmpty(expr.ExpressionAttributeValues))
      delete expr.ExpressionAttributeValues;

    let fullExpr: string = 'ADD #n0 :v0';
    if (saRemove.length > 0)
      fullExpr += ` REMOVE ${saRemove.join(', ')}`;
    if (saDelete.length > 0)
      fullExpr += ` DELETE ${saDelete.join(', ')}`;
    return fullExpr;
  }

  toExternal(result: any): any
  {
    if (result)
    {
      let x: any = {};
      Object.keys(result).forEach((p: string) => { x[p] = rawNakedValue(result[p]) });
      delete x.__nonempty;
      return x;
    }
    return result;
  }

  forceError(): boolean
  {
    return (this.client as DynamoClient).forceError();
  }

  tick(): void
  {
    if (this.ready && this.state == FSM.FSM_STARTING)
      this.setState(FSM.FSM_DONE);
  }
}

export class DynamoUpdate extends DB.DBUpdate
{
  trace: LogAbstract.AsyncTimer;

  constructor(env: Environment, col: DynamoCollection, query: any, values: any)
  {
    super(env, col, col.toInternalQuery(query), col.toInternalExpression(values));
    if (this.query.Key === undefined)
    {
      console.log(`dynamodb: DynamoUpdate internal failure: colquery missing Key: ${JSON.stringify(query)}`);
      this.setState(FSM.FSM_ERROR);
    }
    else
    {
      this.waitOn(col);
      this.trace = new LogAbstract.AsyncTimer(env.log, `dynamodb: update(col=${col.name})`);
    }
  }

  get env(): Environment { return this._env as Environment; }

  get dyncol(): DynamoCollection { return this.col as DynamoCollection }

  forceError(): boolean
  {
    return (this.col.client as DynamoClient).forceError();
  }

  tick(): void
  {
    if (this.ready)
    {
      if (this.isDependentError)
        this.setState(FSM.FSM_ERROR);
      else if (this.forceError())
      {
        this.setState(FSM.FSM_ERROR);
        this.env.log.error('dynamodb: updateItem: forcing error');
      }
      else if (this.state == FSM.FSM_STARTING)
      {
        this.setState(FSM.FSM_PENDING);
        let params: any = Util.shallowAssignImmutable(this.query, this.values);
        params.UpdateExpression = this.dyncol.toUpdateExpression(params);
        params.ReturnValues = 'ALL_NEW';
        if (params.UpdateExpression === '')
        {
          this.setState(FSM.FSM_DONE);
        }
        this.dyncol.dynamodb.updateItem(params, (err: any, result: any) => {
            //console.log(`10: AWS testing: DynamoDB.updateItem called`);
            if (this.done)
              return;
            else if (err)
            {
              this.setState(FSM.FSM_ERROR);
              this.trace.log();
              this.env.log.error({ event: 'dynamodb: update error', detail: `error: ${JSON.stringify(err)} query: ${JSON.stringify(params)}` });
            }
            else
            {
              this.setState(FSM.FSM_DONE);
              this.result = this.dyncol.toExternal(result.Attributes);
              this.trace.log();
              if (this.env.context.xnumber('verbosity'))
                this.env.log.event({ event: 'dynamodb: updateItem', detail: JSON.stringify(result) });
            }
          });
      }
    }
  }
}

export class DynamoUnset extends DB.DBUnset
{
  trace: LogAbstract.AsyncTimer;

  constructor(env: Environment, col: DynamoCollection, query: any, values: any)
  {
    super(env, col, col.toInternalQuery(query), col.toInternalExpression(values));
    if (this.query.Key === undefined)
    {
      console.log(`dynamodb: DynamoUnset internal failure: query missing Key: ${JSON.stringify(query)}`);
      this.setState(FSM.FSM_ERROR);
    }
    else
    {
      this.waitOn(col);
      this.trace = new LogAbstract.AsyncTimer(env.log, `dynamodb: unset(col=${col.name})`);
    }
  }

  get env(): Environment { return this._env as Environment; }

  get dyncol(): DynamoCollection { return this.col as DynamoCollection }

  forceError(): boolean
  {
    return (this.col.client as DynamoClient).forceError();
  }

  tick(): void
  {
    if (this.ready)
    {
      if (this.isDependentError)
        this.setState(FSM.FSM_ERROR);
      else if (this.forceError())
      {
        this.setState(FSM.FSM_ERROR);
        this.env.log.error('dynamodb: unset: forcing error');
      }
      else if (this.state == FSM.FSM_STARTING)
      {
        this.setState(FSM.FSM_PENDING);
        let params: any = Util.shallowAssignImmutable(this.query, this.values);
        params.UpdateExpression = this.dyncol.toRemoveExpression(params);
        params.ReturnValues = 'ALL_NEW';
        this.dyncol.dynamodb.updateItem(params, (err: any, result: any) => {
            if (this.done)
              return;
            else if (err)
            {
              this.setState(FSM.FSM_ERROR);
              this.trace.log();
              this.env.log.error({ event: 'dynamodb: unset error', detail: `error: ${JSON.stringify(err)} query: ${JSON.stringify(params)}` });
            }
            else
            {
              this.setState(FSM.FSM_DONE);
              this.result = this.dyncol.toExternal(result.Attributes);
              this.trace.log();
              if (this.env.context.xnumber('verbosity'))
                this.env.log.event({ event: 'dynamodb: unset', detail: JSON.stringify(result) });
            }
          });
      }
    }
  }
}

export class DynamoDelete extends DB.DBDelete
{
  trace: LogAbstract.AsyncTimer;

  constructor(env: Environment, col: DynamoCollection, query: any)
  {
    super(env, col, col.toInternalQuery(query));
    if (this.query.Key === undefined)
    {
      console.log(`dynamodb: DynamoDelete internal failure: query missing Key: ${JSON.stringify(query)}`);
      this.setState(FSM.FSM_ERROR);
    }
    else
    {
      this.waitOn(col);
      this.trace = new LogAbstract.AsyncTimer(env.log, `dynamodb: delete(col=${col.name})`);
    }
  }

  get env(): Environment { return this._env as Environment; }

  get dyncol(): DynamoCollection { return this.col as DynamoCollection }

  forceError(): boolean
  {
    return (this.col.client as DynamoClient).forceError();
  }

  tick(): void
  {
    if (this.ready)
    {
      if (this.isDependentError)
        this.setState(FSM.FSM_ERROR);
      else if (this.forceError())
      {
        this.setState(FSM.FSM_ERROR);
        this.env.log.error('dynamodb: deleteItem: forcing error');
      }
      else if (this.state == FSM.FSM_STARTING)
      {
        this.setState(FSM.FSM_PENDING);
        this.dyncol.dynamodb.deleteItem(this.query, (err: any, result: any) => {
            //console.log(`11: AWS testing: DynamoDB.deleteItem called`);
            if (this.done)
              return;
            else if (err)
            {
              this.setState(FSM.FSM_ERROR);
              this.trace.log();
              this.env.log.error({ event: 'dynamodb: deleteItem: error', detail: JSON.stringify(err) });
            }
            else
            {
              this.setState(FSM.FSM_DONE);
              this.result = result;
              this.trace.log();
              if (this.env.context.xnumber('verbosity'))
                this.env.log.event({ event: 'dynamodb: deleteItem: succeeded', detail: JSON.stringify(result) });
            }
          });
      }
    }
  }
}

export class DynamoFind extends DB.DBFind
{
  trace: LogAbstract.AsyncTimer;

  constructor(env: Environment, col: DynamoCollection, filter: any)
  {
    super(env, col, col.toInternalQuery(filter));
    if (this.filter.Key === undefined && this.filter.IndexName === undefined)
    {
      console.log(`dynamodb: DynamoFind internal failure: (col=${col.name}) missing Key: ${JSON.stringify(filter)}`);
      this.setState(FSM.FSM_ERROR);
    }
    else
    {
      this.waitOn(col);
      this.trace = new LogAbstract.AsyncTimer(env.log, `dynamodb: find(col=${col.name})`);
    }
  }

  get env(): Environment { return this._env as Environment; }

  get dyncol(): DynamoCollection { return this.col as DynamoCollection }

  forceError(): boolean
  {
    return (this.col.client as DynamoClient).forceError();
  }

  tick(): void
  {
    if (this.ready)
    {
      if (this.isDependentError)
        this.setState(FSM.FSM_ERROR);
      else if (this.forceError())
      {
        this.setState(FSM.FSM_ERROR);
        this.env.log.error('dynamodb: find: forcing error');
      }
      else if (this.state == FSM.FSM_STARTING)
      {
        this.setState(FSM.FSM_PENDING);
        if (this.filter.isquery === undefined)
        {
          this.dyncol.dynamodb.getItem(this.filter, (err: any, result: any) => {
              //console.log(`12: AWS testing: DynamoDB.getItem called`);
              if (this.done)
                return;
              else if (err)
              {
                this.setState(FSM.FSM_ERROR);
                this.trace.log();
                this.env.log.error({ event: 'dynamodb: getItem error', detail: JSON.stringify(err) });
              }
              else
              {
                this.result = this.dyncol.toExternal(result.Item);
                this.trace.log();
                if (this.env.context.xnumber('verbosity'))
                  this.env.log.event( { event: 'dynamodb: getItem', detail: JSON.stringify(result) });
                this.setState(FSM.FSM_DONE);
              }
            });
        }
        else
        {
          delete this.filter.isquery;
          this.dyncol.dynamodb.query(this.filter, (err: any, result: any) => {
              //console.log(`13: AWS testing: DynamoDB.query called`);
              if (this.done)
                return;
              else if (err)
              {
                this.trace.log();
                this.env.log.error({ event: 'dynamodb: query error', detail: JSON.stringify(err) });
                this.setState(FSM.FSM_ERROR);
              }
              else
              {
                if (result.Items && result.Items.length > 0)
                {
                  let x = this.dyncol.toExternal(result.Items[0]);
                  this.filter = this.dyncol.toInternalQuery(x);
                  this.setState(FSM.FSM_STARTING);
                }
                else
                {
                  this.trace.log();
                  this.setState(FSM.FSM_DONE);
                }
              }
            });
        }
      }
    }
  }
}


const FSM_SCANNING = FSM.FSM_CUSTOM1;
const FSM_PAUSING = FSM.FSM_CUSTOM2;

export class DynamoQuery extends DB.DBQuery
{
  trace: LogAbstract.AsyncTimer;
  lastKey: any;

  constructor(env: Environment, col: DynamoCollection, filter: any)
  {
    super(env, col, col.toInternalQuery(filter));
    this.waitOn(col);
    this.trace = new LogAbstract.AsyncTimer(env.log, `dynamodb: query(col=${col.name})`);
    if (this.env.context.xnumber('verbosity'))
      this.env.log.event({ event: `dynamodb: query in ${col.name}`, detail: JSON.stringify(filter) });
  }

  get env(): Environment { return this._env as Environment; }

  get dyncol(): DynamoCollection { return this.col as DynamoCollection }

  forceError(): boolean
  {
    return (this.col.client as DynamoClient).forceError();
  }

  doScan(): void
  {
    let param: any = Util.deepCopy(this.filter);
    param.Select = 'ALL_ATTRIBUTES';
    if (this.lastKey)
    {
      param.ExclusiveStartKey = this.lastKey;
      this.lastKey = undefined;
    }
    this.setState(FSM_SCANNING);
    this.dyncol.dynamodb.scan(param, (err: any, result: any) => {
        //console.log(`14: AWS testing: DynamoDB.scan called`);
        if (this.done)
          return;
        else if (err)
        {
          this.setState(FSM.FSM_ERROR);
          this.trace.log();
          this.env.log.error({ event: 'dynamodb: query error', detail: JSON.stringify(err) });
        }
        else
        {
          if (result.Items)
            for (let i: number = 0; i < result.Items.length; i++)
              this.fsmResult.push(this.dyncol.toExternal(result.Items[i]));
          if (result.LastEvaluatedKey)
          {
            this.lastKey = result.LastEvaluatedKey;
            this.setState(this.options.autoContinue ? FSM.FSM_STARTING : FSM_PAUSING);
          }
          else
          {
            this.fsmResult.setState(FSM.FSM_DONE);
            this.setState(FSM.FSM_DONE);
          }
        }
      });
  }

  tick(): void
  {
    if (this.ready && this.isDependentError)
    {
      this.fsmResult.setState(FSM.FSM_ERROR);
      this.setState(FSM.FSM_ERROR);
    }
    else if (this.ready)
    {
      if (this.state == FSM.FSM_STARTING)
      {
        if (Util.countKeys(this.filter) == 1) // Only specified TableName
          this.doScan();
        else
        {
          let param: any = Util.deepCopy(this.filter);
          delete param.isquery;
          if (this.lastKey)
          {
            param.ExclusiveStartKey = this.lastKey;
            delete this.lastKey;
          }
          this.setState(FSM.FSM_PENDING);
          this.dyncol.dynamodb.query(param, (err: any, result: any) => {
              //console.log(`15: AWS testing: DynamoDB.query called`);
              if (this.done)
                return;
              else if (err)
              {
                this.setState(FSM.FSM_ERROR);
                this.trace.log();
                this.env.log.error({ event: 'dynamodb: query error', detail: JSON.stringify(err) });
              }
              else
              {
                if (result.Items && result.Items.length > 0)
                  result.Items.forEach((i: any) => this.fsmResult.push(this.dyncol.toExternal(i)));
                if (result.LastEvaluatedKey)
                {
                  this.lastKey = result.LastEvaluatedKey;
                  this.setState(this.options.autoContinue ? FSM.FSM_STARTING : FSM_PAUSING);
                }
                else
                {
                  this.trace.log();
                  this.fsmResult.setState(FSM.FSM_DONE);
                  this.setState(FSM.FSM_DONE);
                }
                if (this.env.context.xnumber('verbosity'))
                  this.env.log.event( { event: 'dynamodb: query', detail: JSON.stringify(result) });
              }
            });
        }
      }
    }
  }
}

export class DynamoIndex extends DB.DBIndex
{
  constructor(env: Environment, col: DynamoCollection, uid: string)
  {
    super(env, col, uid);
    this.waitOn(col);
    this.setState(FSM.FSM_DONE);
  }
}

export class DynamoClose extends DB.DBClose
{
  constructor(env: Environment, client: DynamoClient)
  {
    super(env, client);
    this.setState(FSM.FSM_DONE);
  }
}

interface RangeDescription
{
  StartingSequenceNumber: string;
  EndingSequenceNumber?: string;
}

interface ShardDescription
{
  ShardId: string;
  SequenceNumberRange: RangeDescription;
  ShardIterator?: string;
  ParentShardId?: string;
  LastSequenceNumber?: string;
}

function shardSort(s1: ShardDescription, s2: ShardDescription): number
{
  if (s1.ShardId === s2.ParentShardId)
    return -1;
  if (s2.ShardId === s1.ParentShardId)
    return 1;
  return 0;
}

export class FsmTableShards extends FSM.Fsm
{
  table: any;
  stream: any;
  lastKey: string;
  fsmResult: FSM.FsmArray;

  constructor(env: Environment, table: any)
  {
    super(env);
    this.table = table;
    this.fsmResult = new FSM.FsmArray(env);
  }

  get env(): EnvironmentEx { return this._env as EnvironmentEx }

  tick(): void
  {
    if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.setState(FSM_LISTING);
          this.env.dbx.dynamostream.listStreams({ TableName: this.table.TableName}, (err: any, result: any) => {
              //console.log(`16: AWS testing: DynamoStream.listStreams called`);
              if (err)
              {
                console.log(`dynamodb: listStreams error: ${JSON.stringify(err)}`);
                this.env.log.error({ event: 'dynamodb: listStreams', detail: JSON.stringify(err) });
                this.fsmResult.setState(FSM.FSM_ERROR);
                this.setState(FSM.FSM_ERROR);
              }
              else
              {
                if (result.Streams && result.Streams.length > 0)
                {
                  this.stream = result.Streams[0];
                  this.setState(FSM_DESCRIBE);
                }
                else
                {
                  this.setState(FSM.FSM_DONE);
                  this.fsmResult.setState(FSM.FSM_DONE);
                }
              }
            });
          break;

        case FSM_LISTING:
        case FSM_DESCRIBING:
          // Come out of this state in the callback
          break;

        case FSM_DESCRIBE:
          this.setState(FSM_DESCRIBING);
          let params: any = { StreamArn: this.stream.StreamArn };
          if (this.lastKey)
          {
            params.ExclusiveStartShardId = this.lastKey;
            delete this.lastKey;
          }
          this.env.dbx.dynamostream.describeStream(params, (err: any, result: any) => {
              //console.log(`17: AWS testing: DynamoStream.describeStream called`);
              if (err)
              {
                console.log(`dynamodb: describeStream failure: ${JSON.stringify(err)}`);
                this.env.log.error({ event: 'dynamodb: describeStream', detail: JSON.stringify(err) });
                this.setState(FSM.FSM_ERROR);
              }
              else
              {
                Util.shallowAssign(this.stream, result.StreamDescription);
                if (this.stream.Shards)
                {
                  this.stream.Shards.forEach((shard: ShardDescription) => {
                      this.fsmResult.push(shard);
                    });
                }
                if (this.stream.LastEvaluatedShardId)
                {
                  this.lastKey = this.stream.LastEvaluatedShardId;
                  delete this.stream.LastEvaluatedShardId;
                  this.setState(FSM_DESCRIBE);
                }
                else
                {
                  this.fsmResult.setState(FSM.FSM_DONE);
                  this.setState(FSM.FSM_DONE);
                }
              }
            });
          break;
      }
    }
  }
}

class KeySet implements FSM.ISet
{
  keyschema: any[];
  set: any;
  
  constructor(col: DynamoCollection)
  {
    this.keyschema = col.options.KeySchema;
    this.set = {};
  }

  test(o: any): boolean
  {
    let key: string = '';
    this.keyschema.forEach((p: any) => { if (o[p.AttributeName] !== undefined) key += String(o[p.AttributeName]); });
    let b = this.set[key] !== undefined;
    this.set[key] = true;
    return b;
  }

  reset(): void
  {
    this.set = {};
  }
}

const FSM_READING = FSM.FSM_CUSTOM4;
const FSM_NEXTSHARD = FSM.FSM_CUSTOM5;
const FSM_GETITERATOR = FSM.FSM_CUSTOM6;
const FSM_CALLITERATOR = FSM.FSM_CUSTOM7;
const FSM_GETRECORDS = FSM.FSM_CUSTOM8;
const FSM_LOOPING = FSM.FSM_CUSTOM9;
const MinLoopInterval = 1000 * 60 * 5;
const MaxShardInterval = 1000 * 60 * 2;
const MaxGetRecordRetries = 100;

class FsmReadOneShard extends FSM.Fsm
{
  col: DynamoCollection;
  stream: any;
  shard: ShardDescription;
  shardClosed: boolean;
  fsmResult: FSM.FsmArray;
  elapsed: Util.Elapsed;
  nTries: number;
  deadline: Deadline;

  constructor(env: Environment, col: DynamoCollection, stream: any, shard: ShardDescription, fsmResult: FSM.FsmArray)
  {
    super(env);
    this.col = col;
    this.stream = stream;
    this.shard = shard;
    this.fsmResult = fsmResult;
    this.elapsed = new Util.Elapsed();
    this.nTries = 0;
    this.shardClosed = this.shard.SequenceNumberRange.EndingSequenceNumber !== undefined;
    this.deadline = new Deadline(MaxShardInterval);
  }

  get env(): EnvironmentEx { return this._env as EnvironmentEx }

  tick(): void
  {
    if (this.ready && this.isDependentError)
      this.setState(FSM.FSM_ERROR);
    else if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.deadline.start();
          this.setState(FSM_GETITERATOR);
          let iterParams: any = {
              ShardId: this.shard.ShardId,
              ShardIteratorType: 'TRIM_HORIZON',
              StreamArn: this.stream.StreamArn
            };
          if (this.shard.LastSequenceNumber !== undefined)
          {
            iterParams.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
            iterParams.SequenceNumber = this.shard.LastSequenceNumber;
          }
          this.env.dbx.dynamostream.getShardIterator(iterParams, (err: any, result: any) => {
              //console.log(`18: AWS testing: DynamoStream.getShardIterator called`);
              // Cancel by externally setting to DONE
              if (this.done)
                return;
              if (err)
              {
                console.log(`dynamodb: getShardIterator: failure: ${JSON.stringify(err)}`);
                this.env.log.error({ event: 'dynamodb: getShardIterator', detail: JSON.stringify(err) });
                this.setState(FSM.FSM_ERROR);
              }
              else
              {
                this.shard.ShardIterator = result.ShardIterator;
                this.setState(FSM_CALLITERATOR);
              }
            });
          break;

        case FSM_CALLITERATOR:
          this.elapsed.start();
          this.setState(FSM_GETRECORDS);
          let recordParams: any = { ShardIterator: this.shard.ShardIterator };
          this.env.dbx.dynamostream.getRecords(recordParams, (err: any, result: any) => {
              //console.log(`19: AWS testing: DynamoStream.getRecords called`);
              // Cancel by externally setting to DONE
              if (this.done)
                return;
              if (err)
              {
                console.log(`dynamodb: getRecords: failure: ${JSON.stringify(err)}`);
                this.env.log.error({ event: 'dynamodb: getRecords', detail: JSON.stringify(err) });
                this.setState(FSM.FSM_ERROR);
              }
              else
              {
                this.fsmResult.concat(result.Records.map((o: any) => this.col.toExternal(o.dynamodb.Keys) ));
                if (result.Records.length > 0)
                {
                  let r: any = result.Records[result.Records.length-1];
                  this.shard.LastSequenceNumber = r.dynamodb.SequenceNumber;
                }
                if (result.NextShardIterator)
                {
                  this.nTries++;
                  this.shard.ShardIterator = result.NextShardIterator;
                  let bContinue = this.shardClosed || (this.nTries <= MaxGetRecordRetries && !this.deadline.passed);
                  this.setState(bContinue ? FSM_CALLITERATOR : FSM.FSM_DONE);
                }
                else
                  this.setState(FSM.FSM_DONE);
              }
            });
          break;
      }
    }
  }
}

export class FsmTableStream extends FSM.Fsm
{
  col: DynamoCollection;
  table: any;
  fsmShards: FsmTableShards;
  shardsDone: any;
  shardsLast: any;
  shardsQueue: FsmReadOneShard[];
  fsmResult: FSM.FsmArray;
  deadline: Deadline

  constructor(env: Environment, col: DB.DBCollection)
  {
    super(env);
    this.col = col as DynamoCollection;
    this.table = col.col;
    this.shardsDone = {};
    this.shardsLast = {};
    this.shardsQueue = [];
    this.fsmResult = new FSM.FsmArray(env, new KeySet(this.col));
    this.deadline = new Deadline(MinLoopInterval);
  }

  get env(): Environment { return this._env as Environment }

  end(): void
  {
    this.shardsDone = {};
    this.shardsLast = {};
    this.shardsQueue = [];
    this.setState(FSM.FSM_DONE);
    this.fsmResult.setState(FSM.FSM_DONE);
  }

  error(): void
  {
    this.clearDependentError();
    this.setState(FSM_LOOPING);
  }

  tick(): void
  {
    if (this.ready && this.isDependentError)
      this.error();
    else if (this.ready)
    {
      switch (this.state)
      {
        case FSM_LOOPING:
          let msLeft = this.deadline.msRemaining;
          if (msLeft > 0)
            this.waitOn(new FSM.FsmSleep(this.env, msLeft));
          this.setState(FSM.FSM_STARTING);
          break;

        case FSM.FSM_STARTING:
          this.deadline.start();
          this.setState(FSM_LISTING);
          this.shardsQueue = [];
          this.fsmShards = new FsmTableShards(this.env, this.table);
          this.waitOn(this.fsmShards.fsmResult);
          break;

        case FSM_LISTING:
          this.fsmShards.fsmResult.a.forEach((shard: ShardDescription) => {
              if (this.shardsDone[shard.ShardId] === undefined)
              {
                if (this.shardsLast[shard.ShardId])
                  shard.LastSequenceNumber = this.shardsLast[shard.ShardId];
                let fsmReadOne = new FsmReadOneShard(this.env, this.col, this.fsmShards.stream, shard, this.fsmResult);
                this.waitOn(fsmReadOne);
                this.shardsQueue.push(fsmReadOne);
              }
            });

          // Make sure child shard processed before parent shard
          // TODO: not concerned about record ordering here
          //this.shardsQueue.sort(shardSort);

          // If still listing shards, re-wait on the result set and stay in this state
          this.fsmShards.fsmResult.reset();
          if (! this.fsmShards.done)
            this.waitOn(this.fsmShards.fsmResult);
          else
            this.setState(FSM_READING);
          break;

        case FSM_READING:
          this.shardsQueue.forEach((fsmReadOne: FsmReadOneShard) => {
              if (!fsmReadOne.iserror && fsmReadOne.shardClosed)
                this.shardsDone[fsmReadOne.shard.ShardId] = true;
              this.shardsLast[fsmReadOne.shard.ShardId] = fsmReadOne.shard.LastSequenceNumber;
            });
          this.setState(FSM_LOOPING);
          break;
      }
    }
  }
}
