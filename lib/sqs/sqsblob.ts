import { Util, FSM, Context, LogAbstract } from '@dra2020/baseclient';
import * as Storage from '../storage/all';
import { Environment } from './env';

let UniqueState = FSM.FSM_CUSTOM1;
const FSM_CREATING = UniqueState++;
const FSM_SAVING = UniqueState++;
const FSM_LOADING = UniqueState++;
const FSM_DELETING = UniqueState++;
const FSM_SENDING = UniqueState++;

export class SQSBlob extends Storage.StorageBlob
{
  constructor(env: Environment, params: Storage.BlobParams)
  {
    if (params.bucket == null)
      params.bucket = env.context.xflag('production') ? 'transfers' : 'transfers-dev';
    params.deleteAfterLoad = true;
    super(env, params);
  }

  get data(): string { return this.params.loadTo as string }

  static createForLoad(env: Environment, id: string): SQSBlob
  {
    let params: Storage.BlobParams = {
      id,
      loadToType: 'string',
      };
    let blob = new SQSBlob(env, params);
    blob.startLoad(env.storageManager);
    return blob;
  }

  static createForSave(env: Environment, data: string): SQSBlob
  {
    let params: Storage.BlobParams = {
        id: Util.createGuid(),
        saveFromType: 'string',
        saveFrom: data,
        ContentEncoding: 'gzip',
        ContentType: 'text/plain; charset=UTF-8',
      };
    let blob = new SQSBlob(env, params);
    blob.setDirty();
    blob.checkSave(env.storageManager);
    return blob;
  }
}
