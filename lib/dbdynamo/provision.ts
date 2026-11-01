// Explicit DynamoDB table provisioning.
//
// Tables and their global secondary indexes are never created on demand by a running server or lambda (which
// would require every runtime role to hold dynamodb:CreateTable). Instead they are created explicitly by the
// dra-cli provision tool, which runs FsmProvisionTables with administrative credentials. Runtime code that
// touches a missing table or index logs a "missing table" error pointing back at that tool.

import { DynamoDB } from '@aws-sdk/client-dynamodb';

import { Util, LogAbstract, Context, FSM } from '@dra2020/baseclient';

import * as DB from '../dbabstract/all';

export interface ProvisionEnvironment
{
  context: Context.IContext;
  log: LogAbstract.ILog;
  fsmManager: FSM.FsmManager;
}

export const DefaultCollectionOptions = { prefix: 'dra' };

// Expand a compact collection schema (as in DT.Schemas) into the DynamoDB form used by DynamoCollection.
export function toCollectionOptions(options: any): any
{
  const defaultOptions: any = { Schema: { id: 'S' }, KeySchema: { id: 'HASH' } };
  options = Util.deepCopy(Util.shallowAssignImmutable(defaultOptions, options));
  options.AttributeDefinitions = DB.fromCompactSchema(options.Schema);
  options.KeySchema = DB.fromCompactKey(options.KeySchema);
  if (options.GlobalSecondaryIndexes)
    options.GlobalSecondaryIndexes = options.GlobalSecondaryIndexes.map((v: any) => DB.fromCompactIndex(v));
  return options;
}

export function toTableName(prefix: string, dbName: string, name: string): string
{
  return `${prefix || DefaultCollectionOptions.prefix}-${dbName}-${name}`;
}

function indexParams(ix: any): any
{
  return { IndexName: ix.IndexName, KeySchema: ix.KeySchema, Projection: ix.Projection || { ProjectionType: 'ALL' } };
}

// Only the attribute definitions used by the given key schemas (DynamoDB rejects unused definitions).
function usedAttributes(options: any, keySchemas: any[][]): any[]
{
  let keys: any = {};
  keySchemas.forEach(ks => ks.forEach((p: any) => keys[p.AttributeName] = true));
  return options.AttributeDefinitions.filter((d: any) => keys[d.AttributeName]);
}

export function createTableParams(tableName: string, options: any): any
{
  let params: any = {
    TableName: tableName,
    BillingMode: options.BillingMode || 'PAY_PER_REQUEST',
    StreamSpecification: options.StreamSpecification || { StreamEnabled: true, StreamViewType: 'KEYS_ONLY' },
    KeySchema: options.KeySchema,
  };
  let keySchemas: any[][] = [ options.KeySchema ];
  if (options.GlobalSecondaryIndexes)
  {
    params.GlobalSecondaryIndexes = options.GlobalSecondaryIndexes.map(indexParams);
    options.GlobalSecondaryIndexes.forEach((ix: any) => keySchemas.push(ix.KeySchema));
  }
  params.AttributeDefinitions = usedAttributes(options, keySchemas);
  return params;
}

function sameKeySchema(a: any[], b: any[]): boolean
{
  const s = (ks: any[]) => (ks || []).map((p: any) => `${p.AttributeName}:${p.KeyType}`).sort().join(',');
  return s(a) === s(b);
}

export type TableStatus = 'ok' | 'missing' | 'created' | 'indexmissing' | 'indexadded' | 'mismatch' | 'error';

export interface TableReport
{
  name: string;           // collection name, e.g. 'users'
  table: string;          // DynamoDB table name, e.g. 'dra-prod-users'
  status: TableStatus;
  detail: string[];
}

export interface ProvisionOptions
{
  create?: boolean;       // create missing tables and add missing indexes (default: only report)
  wait?: boolean;         // after a change, wait for the table and its indexes to become ACTIVE (default true)
  prefix?: string;        // table name prefix (default 'dra')
  msPoll?: number;        // ACTIVE polling interval (default 5s)
  msTimeout?: number;     // give up waiting for ACTIVE after this long (default 2 hours; index backfill is slow)
}

// Reports on (and with options.create, creates) the tables for a set of compact schemas, keyed by collection name.
// Never deletes or modifies existing tables except to add missing global secondary indexes.
export class FsmProvisionTables extends FSM.Fsm
{
  dynamodb: DynamoDB;
  dbName: string;
  schemas: { [name: string]: any };
  options: ProvisionOptions;
  reports: TableReport[];
  extraTables: string[];   // tables with our prefix and db name that no schema describes

  constructor(env: ProvisionEnvironment, dynamodb: DynamoDB, dbName: string, schemas: { [name: string]: any }, options?: ProvisionOptions)
  {
    super(env);
    this.dynamodb = dynamodb;
    this.dbName = dbName;
    this.schemas = schemas;
    this.options = Util.shallowAssignImmutable({ create: false, wait: true, prefix: DefaultCollectionOptions.prefix,
                                                 msPoll: 5000, msTimeout: 2 * 60 * 60 * 1000 }, options);
    this.reports = [];
    this.extraTables = [];
  }

  get env(): ProvisionEnvironment { return this._env as ProvisionEnvironment }

  get hasProblems(): boolean { return this.reports.some(r => r.status !== 'ok' && r.status !== 'created' && r.status !== 'indexadded') }

  chatter(s: string): void { this.env.log.chatter(`provision: ${s}`) }

  async listTables(): Promise<string[]>
  {
    let names: string[] = [];
    let params: any = {};
    do
    {
      let result = await this.dynamodb.listTables(params);
      if (result.TableNames) names.push(...result.TableNames);
      params.ExclusiveStartTableName = result.LastEvaluatedTableName;
    } while (params.ExclusiveStartTableName);
    return names;
  }

  async describe(tableName: string): Promise<any>
  {
    try
    {
      let result = await this.dynamodb.describeTable({ TableName: tableName });
      return result.Table;
    }
    catch (err)
    {
      if (err?.name === 'ResourceNotFoundException') return null;
      throw err;
    }
  }

  async waitActive(tableName: string): Promise<void>
  {
    let elapsed = new Util.Elapsed();
    let msLastReport = 0;
    while (true)
    {
      let t = await this.describe(tableName);
      let pending: string[] = [];
      if (!t) pending.push('table not visible yet');
      else
      {
        if (t.TableStatus !== 'ACTIVE') pending.push(`table ${t.TableStatus}`);
        (t.GlobalSecondaryIndexes || []).forEach((ix: any) => {
            if (ix.IndexStatus !== 'ACTIVE') pending.push(`index ${ix.IndexName} ${ix.IndexStatus}${ix.Backfilling ? ' (backfilling)' : ''}`);
          });
      }
      if (pending.length == 0) return;
      elapsed.end();
      if (elapsed.ms() > this.options.msTimeout)
        throw new Error(`timed out waiting for ${tableName} to become ACTIVE: ${pending.join(', ')}`);
      if (elapsed.ms() - msLastReport >= 60000 || msLastReport == 0)
      {
        msLastReport = elapsed.ms() || 1;
        this.chatter(`waiting for ${tableName}: ${pending.join(', ')}`);
      }
      await new Promise(resolve => setTimeout(resolve, this.options.msPoll));
    }
  }

  async provisionOne(name: string, schema: any, report: TableReport): Promise<void>
  {
    let options = toCollectionOptions(schema);
    let t = await this.describe(report.table);
    if (!t)
    {
      if (! this.options.create)
      {
        report.status = 'missing';
        return;
      }
      await this.dynamodb.createTable(createTableParams(report.table, options));
      this.env.log.event({ event: 'provision: createTable', detail: report.table });
      report.status = 'created';
      if (this.options.wait) await this.waitActive(report.table);
      return;
    }

    if (t.TableStatus !== 'ACTIVE')
      report.detail.push(`table status is ${t.TableStatus}`);
    if (! sameKeySchema(t.KeySchema, options.KeySchema))
    {
      report.status = 'mismatch';
      report.detail.push(`key schema is ${JSON.stringify(t.KeySchema)}, schema expects ${JSON.stringify(options.KeySchema)} (cannot be changed in place)`);
    }

    let existing: any = {};
    (t.GlobalSecondaryIndexes || []).forEach((ix: any) => existing[ix.IndexName] = ix);
    let wanted: any[] = options.GlobalSecondaryIndexes || [];
    let missing: any[] = [];
    wanted.forEach((ix: any) => {
        let have = existing[ix.IndexName];
        if (! have)
          missing.push(ix);
        else
        {
          if (! sameKeySchema(have.KeySchema, ix.KeySchema))
          {
            report.status = 'mismatch';
            report.detail.push(`index ${ix.IndexName} key schema is ${JSON.stringify(have.KeySchema)}, schema expects ${JSON.stringify(ix.KeySchema)}`);
          }
          if (have.IndexStatus !== 'ACTIVE')
            report.detail.push(`index ${ix.IndexName} status is ${have.IndexStatus}`);
          delete existing[ix.IndexName];
        }
      });
    Object.keys(existing).forEach(n => report.detail.push(`index ${n} exists but is not in the schema (left alone)`));

    if (missing.length == 0) return;
    if (! this.options.create)
    {
      if (report.status === 'ok') report.status = 'indexmissing';
      report.detail.push(`missing index(es): ${missing.map(ix => ix.IndexName).join(', ')}`);
      return;
    }

    // DynamoDB allows only one index creation per UpdateTable call (and per table at a time), so add them in turn.
    let provisioned = t.BillingModeSummary?.BillingMode === 'PROVISIONED' || (!t.BillingModeSummary && t.ProvisionedThroughput?.ReadCapacityUnits);
    for (let ix of missing)
    {
      await this.waitActive(report.table);
      let create: any = indexParams(ix);
      if (provisioned)
        create.ProvisionedThroughput = { ReadCapacityUnits: t.ProvisionedThroughput.ReadCapacityUnits,
                                         WriteCapacityUnits: t.ProvisionedThroughput.WriteCapacityUnits };
      await this.dynamodb.updateTable({
          TableName: report.table,
          AttributeDefinitions: usedAttributes(options, [ ix.KeySchema ]),
          GlobalSecondaryIndexUpdates: [ { Create: create } ],
        });
      this.env.log.event({ event: 'provision: createIndex', detail: `${report.table} ${ix.IndexName}` });
      report.detail.push(`added index ${ix.IndexName}`);
      if (report.status === 'ok') report.status = 'indexadded';
    }
    if (this.options.wait) await this.waitActive(report.table);
  }

  async run(): Promise<void>
  {
    let all = await this.listTables();
    let ours = `${this.options.prefix}-${this.dbName}-`;
    let described: any = {};
    for (let name of Object.keys(this.schemas))
    {
      let report: TableReport = { name, table: toTableName(this.options.prefix, this.dbName, name), status: 'ok', detail: [] };
      described[report.table] = true;
      this.reports.push(report);
      try
      {
        await this.provisionOne(name, this.schemas[name], report);
      }
      catch (err)
      {
        report.status = 'error';
        report.detail.push(String(err?.message || err));
        this.env.log.error({ event: 'provision: table error', detail: `${report.table}: ${String(err?.message || err)}` });
      }
    }
    this.extraTables = all.filter(n => n.startsWith(ours) && !described[n]);
  }

  tick(): void
  {
    if (this.ready && this.state == FSM.FSM_STARTING)
    {
      this.setState(FSM.FSM_PENDING);
      this.run()
        .then(() => this.setState(FSM.FSM_DONE))
        .catch((err: any) => {
            this.env.log.error({ event: 'provision: failed', detail: String(err?.message || err) });
            this.setState(FSM.FSM_ERROR);
          });
    }
  }
}
