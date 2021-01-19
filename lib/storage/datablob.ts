// Shared libraries
import { Util, Context, LogAbstract, FSM } from '@dra2020/baseclient';

// App libraries
import { Environment } from './env';
import * as Storage from './storage';

export class DataBlob extends Storage.StorageBlob
{
  constructor(env: Environment, params: Storage.BlobParams)
  {
    if (params.bucket == null) params.bucket = env.context.xflag('production') ? 'data' : 'data-dev';
    super(env, params);
  }

  get env(): Environment { return this._env as Environment; }

  get json(): any { return this.params.loadTo }

  static createForJSON(env: Environment, id: string): DataBlob
  {
    let blob = new DataBlob(env, { id: id, loadToType: 'object' });
    blob.startLoad(env.storageManager);
    return blob;
  }

  static createForStream(env: Environment, id: string, bucket?: string): DataBlob
  {
    let blob = new DataBlob(env, { id: id, bucket: bucket, loadToType: 'stream' });
    blob.startLoad(env.storageManager);
    return blob;
  }
}
