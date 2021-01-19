// Node
import * as zlib from 'zlib';

// Shared libraries
import { Context, LogAbstract, FSM } from '@dra2020/baseclient';

// App libraries
import { Environment } from './env';
import * as Storage from './storage';

export class SplitsBlob extends Storage.StorageBlob
{
  ls: { [key: string]: Storage.BlobProperties };

  constructor(env: Environment, params: Storage.BlobParams)
  {
    if (params.bucket === undefined) params.bucket = env.context.xflag('production') ? 'splits' : 'splits-dev';
    super(env, params);
  }

  get env(): Environment { return this._env as Environment; }

  get collection(): any { return this.params.loadTo }

  static createForLs(env: Environment): SplitsBlob
  {
    let blob = new SplitsBlob(env, { id: '' });
    blob.startList(env.storageManager);
    new FSM.FsmOnDone(env, blob.fsmList, (f: FSM.Fsm) => {
        if (! f.iserror)
        {
          blob.ls = {};
          blob.keys.forEach((k: string, i: number) => { blob.ls[k] = blob.props[i] });
        }
      });
    return blob;
  }

  static createForUpload(env: Environment, id: string, collection: any): SplitsBlob
  {
    let params: Storage.BlobParams = {
      id: id,
      saveFromType: 'object',
      saveFrom: collection,
      ContentEncoding: 'gzip',
      ContentType: 'application/json',
      CacheControl: 'no-cache'
      };
    let blob = new SplitsBlob(env, params);
    blob.setDirty();
    blob.checkSave(env.storageManager);
    return blob;
  }

  static createForDownload(env: Environment, id: string): SplitsBlob
  {
    let blob = new SplitsBlob(env, { id: id, loadToType: 'object' });
    blob.startLoad(env.storageManager);
    return blob;
  }
}
