import * as zlib from 'zlib';

// Shared libraries
import { Util, Context, LogAbstract, FSM } from '@dra2020/baseclient';

import * as Storage from '../storage/all';

import { LogKey } from './logkey';

interface Environment
{
  context: Context.IContext;
  log: LogAbstract.ILog;
  fsmManager: FSM.FsmManager;
  storageManager: Storage.StorageManager;
}

export interface Options
{
  dateFilter?: string,
}
const DefaultOptions = { dateFilter: '' };

export class LogBlob extends Storage.StorageBlob
{
  key: LogKey;
  options: Options;

  constructor(env: Environment, params: Storage.BlobParams)
  {
    if (params.bucket === undefined) params.bucket = 'logs';
    super(env, params);
    this.key = LogKey.create(this.params.id);
    this.filter = this.filter.bind(this);
  }

  get env(): Environment { return this._env as Environment; }

  get result(): string { return this.params.loadTo as string }

  static createForDownload(env: Environment, id: string): LogBlob
  {
    let blob = new LogBlob(env, { id: id, loadToType: 'string' });
    blob.startLoad(env.storageManager);
    return blob;
  }

  static createForUpload(env: Environment, id: string, data: string): LogBlob
  {
    let params: Storage.BlobParams = {
      id: id,
      ContentEncoding: 'gzip',
      ContentType: 'application/json',
      saveFromType: 'string',
      saveFrom: data
      };
    let blob = new LogBlob(env, params);
    blob.setDirty();
    blob.checkSave(env.storageManager);
    return blob;
  }

  static createForDelete(env: Environment, id: string): LogBlob
  {
    let blob = new LogBlob(env, { id: id });
    blob.startDelete(env.storageManager);
    return blob;
  }

  static createForLs(env: Environment, options?: Options): LogBlob
  {
    let blob = new LogBlob(env, { id: '' });
    blob.options = Util.shallowAssignImmutable(DefaultOptions, options);
    blob.startList(env.storageManager);
    return blob;
  }

  filter(s: string): boolean
  {
    let key = LogKey.create(s);
    if (key == null) return false;
    let mode = this.env.context.xflag('production') ? 'Prod' : 'Dev';
    return key.mode === mode && key.test(this.options.dateFilter);
  }

  asString(): string
  {
    return this.params.loadTo as string;
  }
}
