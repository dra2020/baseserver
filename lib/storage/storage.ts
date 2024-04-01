// Node libraries
import * as stream from 'stream';

// Shared libraries
import { Util, Context, LogAbstract, FSM } from '@dra2020/baseclient';

import { Environment } from './env';

import { FsmAPIWatch } from '../utils/all';

export const ESuccess: number = 0;
export const EFail: number = 1;
export const ENotFound: number = 2;
export const EPending: number = 3;
export const EBadFormat: number = 4;

export type StorageState = number;
export const StorageStateClean = 0;
export const StorageStateDirty = 1;
export const StorageStateSaving = 2;
export const StorageStateLoading = 4;
export const StorageStateLoadCanceled = 8;
export const StorageStateLoadFailed = 16;
export const StorageStateDeleting = 32;
export const StorageStateDeleted = 64;
export const StorageStateListing = 128;

const StorageContextDefaults: Context.ContextValues = { StorageRetryDelay: 60000 };

export type BucketMap = { [key: string]: string };
export const BucketDefault = "default";
export const BucketDev = "development";
export const BucketProd = "production";
export const BucketLogs = "logs";

export type TransferUrlOp = 'putObject' | 'getObject';

export interface TransferParams
{
  op: TransferUrlOp;
  contentType?: string;
  contentEncoding?: string;
  cacheControl?: string;
  key?: string;
  bucket?: string;
}

// Where does the source data for save come from?
export type DispositionType = 'object' | 'string' | 'buffer' | 'compressedbuffer' | 'stream' | 'compressedstream' | 'filepath';
export type LoadToFilter = (blob: StorageBlob, a: any) => any;

export interface BlobParams
{
  id: string;
  bucket?: string;
  saveFromType?: DispositionType;
  saveFrom?: any;
  loadToType?: DispositionType;
  loadTo?: any;
  loadToFilter?: LoadToFilter;
  deleteAfterLoad?: boolean;
  suppressDeleteError?: boolean;
  ContentEncoding?: string;       // 'gzip' only
  ContentType?: string;           // 'application/json' or 'application/octet-stream'
  CacheControl?: string;          // 'no-cache' or 'max-age=14400'
}

export class FsmTransferUrl extends FSM.Fsm
{
  params: TransferParams;
  url: string;

  constructor(env: Environment, params: TransferParams)
  {
    super(env);
    this.params = Util.shallowAssignImmutable({ contentType: 'text/plain; charset=UTF-8', key: Util.createGuid() }, params);
  }
}

export interface BlobProperties
{
  Key?: string;
  ContentLength?: number;
  ContentDisposition?: string;
  ContentEncoding?: string;
  ContentType?: string;
  ContentLanguage?: string;
  LastModified?: string;
  ETag?: string;
  CacheControl?: string;
}

export interface MultiBufferList
{
  buf: Buffer;
  next: MultiBufferList;
}

export class MultiBufferPassThrough extends stream.Duplex
{
  _dopush: boolean;
  _head: MultiBufferList;
  _tail: MultiBufferList;

  constructor()
  {
    super({});
    this._dopush = false;
    this._head = null;
    this._tail = null;
  }

  _read(): void
  {
    this._dopush = true;
    this._dopushing();
  }

  _dopushing(): void
  {
    while (this._dopush && this._head)
    {
      this._dopush = this.push(this._head.buf);
      this._head = this._head.next;
      if (this._head == null)
        this._tail = null;
    }
  }

  _pushChunk(chunk: any): void
  {
    let tail: MultiBufferList = { buf: chunk as Buffer, next: null };
    if (this._head == null)
      this._head = tail;
    else
      this._tail.next = tail;
    this._tail = tail;
    this._dopushing();
  }

  _write(chunk: any, encoding: string, cb: any): void
  {
    if (! Buffer.isBuffer(chunk)) throw 'MultiBufferPassThrough only supports writing Buffer type';
    this._pushChunk(chunk);
    cb(null);
  }

  _done(): void
  {
    this._pushChunk(null);
  }

  _flush(cb: any): void
  {
    this._read();
    cb();
  }

  _transform(chunk: any, encoding: string, cb: any): void
  {
    this._write(chunk, encoding, cb);
  }
}

export class StorageBlob
{
  _env: Environment;
  params: BlobParams;
  state: StorageState;
  tStarted: Date;
  fsmSave: FSM.Fsm;
  fsmLoad: FSM.Fsm;
  fsmDel: FSM.Fsm;
  fsmList: FSM.Fsm;
  _props: FSM.FsmArray;
  _keys: FSM.FsmArray;

  constructor(env: Environment, params: BlobParams)
    {
      this._env = env;
      this.params = Util.shallowCopy(params);
      this.env.context.setDefaults(StorageContextDefaults);
      if (this.params.bucket == null) this.params.bucket = 'default';
      this.state = StorageStateClean;
      this.tStarted = null;
      this.fsmSave = new FSM.Fsm(this.env);
      this.fsmSave.setState(FSM.FSM_DONE);
      this.fsmLoad = new FSM.Fsm(this.env);
      this.fsmLoad.setState(FSM.FSM_DONE);
      this.fsmDel = new FSM.Fsm(this.env);
      this.fsmDel.setState(FSM.FSM_DONE);
      this.fsmList = new FSM.Fsm(this.env);
      this.fsmList.setState(FSM.FSM_DONE);
      this._keys = new FSM.FsmArray(env);
      this._props = new FSM.FsmArray(env);
    }

  get env() { return this._env; }

  // For listings, wait on fsmArray to get partial results, call resetList to reset fsmArray for next chunk, fsmList marked done at end.
  // Or just wait for fsmList to get all results at once.
  get keys(): string[] { return this._keys.a as string[] }
  get props(): BlobProperties[] { return this._props.a as BlobProperties[] }
  resetList(): void { this._keys.reset(); this._props.reset() }
  get fsmArray(): FSM.Fsm { return this._keys }

  setSaveFrom(t: DispositionType, data: any): void
    {
      this.params.saveFromType = t;
      this.params.saveFrom = data;
    }

  setLoadTo(t: DispositionType): void
    {
      this.params.loadToType = t;
    }

  needSave(): boolean
    {
      if (this.state == StorageStateDirty) // Note this test fails if Dirty && Saving
        return true;
      if (this.state == StorageStateClean)
        return false;

      /*
       * Don't retry at this level
      if (this.isDirty() && this.isSaving())
      {
        let now = new Date();
        return now.getTime() - this.tStarted.getTime() > this.env.context.xnumber('StorageRetryDelay');
      }
      */

      // Not loaded, either not yet, or load failed
      return false;
    }

  isSaving(): boolean
    {
      return (this.state & StorageStateSaving) != 0;
    }

  isLoading(): boolean
    {
      return (this.state & StorageStateLoading) != 0;
    }

  isListing(): boolean
    {
      return (this.state & StorageStateListing) != 0;
    }

  setListing(): void
    {
      this.state |= StorageStateListing;
      this.fsmList.setState(FSM.FSM_PENDING);
      this.env.storageManager.fsmAPIWatch.setPending(this.fsmList);
    }

  setListed(): void
    {
      this.state &= ~StorageStateListing;
      // Don't set fsmList to DONE here - do in endList since we may continue listing
    }

  isDeleting(): boolean
    {
      return (this.state & StorageStateDeleting) != 0;
    }

  isDeleted(): boolean
    {
      return (this.state & StorageStateDeleted) != 0;
    }

  isLoadFailed(): boolean
    {
      return (this.state & StorageStateLoadFailed) != 0;
    }

  isValid(): boolean
    {
      return !(this.isLoading() || this.isLoadFailed() || this.isDeleting() || this.isDeleted());
    }

  isReadable(): boolean
    {
      return !(this.isLoading() || this.isDeleting() || this.isDeleted());
    }

  isDirty(): boolean
    {
      return (this.state & StorageStateDirty) != 0;
    }

  isSafeToUnload(): boolean
    {
      return !this.isLoading() && !this.isDirty() && !this.isSaving();
    }

  setDirty(): void
    {
      if ((this.state & StorageStateLoading) != 0)
      {
        this.env.log.error('storage: object being set dirty while loading');
      }
      this.state |= StorageStateDirty;
    }

  setInit(): void
    {
      // Only used to allow auto-initialization for debugging
      this.state = StorageStateClean;
    }

  setLoading(): void
    {
      if (this.isDirty())
      {
        this.env.log.error('storage: object already dirty when getting loaded');
      }
      this.state |= StorageStateLoading;
      this.tStarted = new Date();
      this.fsmLoad.setState(FSM.FSM_PENDING);
      this.env.storageManager.fsmAPIWatch.setPending(this.fsmLoad);
    }

  setLoaded(result: number): void
    {
      if (this.isDirty())
      {
        this.env.log.error('storage: object already dirty when load finished');
      }
      if (result == ESuccess)
        this.state = StorageStateClean;
      else
        this.state = StorageStateLoadFailed;
      //don't set this here so any load filter has opportunity to fail the load
      //this.fsmLoad.setState(result === ESuccess ? FSM.FSM_DONE : FSM.FSM_ERROR);
    }

  setSaving(): void
    {
      this.state = StorageStateSaving;
      this.tStarted = new Date();
      this.fsmSave.setState(FSM.FSM_PENDING);
      this.env.storageManager.fsmAPIWatch.setPending(this.fsmSave);
    }

  setSaved(result: number): void
    {
      if (result == ESuccess && this.state == StorageStateSaving)
        this.state = StorageStateClean;
      else
        this.state = StorageStateDirty;
      this.tStarted = null;
      this.fsmSave.setState(result === ESuccess ? FSM.FSM_DONE : FSM.FSM_ERROR);
    }

  setDeleting(): void
    {
      this.state |= StorageStateDeleting;
      this.fsmDel.setState(FSM.FSM_PENDING);
      this.env.storageManager.fsmAPIWatch.setPending(this.fsmDel);
    }

  setDeleted(result: number): void
    {
      if (result == ESuccess)
        this.state |= StorageStateDeleted;
      else
        this.env.log.error('storage: delete failed');
      this.state &= ~StorageStateDeleting;

      // Supports fire-and-forget
      if (this.params.suppressDeleteError)
        result = ESuccess;
      this.fsmDel.setState(result === ESuccess ? FSM.FSM_DONE : FSM.FSM_ERROR);
    }

  startLoad(sm: StorageManager): void
    {
      sm.load(this);
    }

  startHead(sm: StorageManager): void
    {
      sm.head(this);
      this.fsmList.setState(FSM.FSM_PENDING); // result fsmList for head request, results show up in props
      this.env.storageManager.fsmAPIWatch.setPending(this.fsmList);
    }

  checkSave(sm: StorageManager): void
    {
      if (this.needSave())
      {
        if (this.isSaving())
        {
          this.env.log.event('storage: save overlaps');
        }
        sm.save(this);
      }
    }

  startDelete(sm: StorageManager): void
    {
      if (this.isDeleting())
      {
        this.env.log.error('storage: attempt to delete while deleting');
      }
      else
        sm.del(this);
    }

  endSave(br: BlobRequest): void
    {
      // fsmSave triggered by setSaved
    }

  endLoad(br: BlobRequest): void
    {
      if (this.params.loadToType === undefined)
        throw 'endLoad: loadToType must be set';

      if (br.result() == ESuccess)
      {
        switch (this.params.loadToType)
        {
          case 'stream':
          case 'compressedstream':
            // No work here, processing happened in stream handlers
            break;
          case 'buffer':
          case 'compressedbuffer':
            this.params.loadTo = br.asBuffer(); // automatically uncompressed if necessary for 'buffer' type
            break;
          case 'string':
            this.params.loadTo = br.asString(); // automatically uncompressed if necessary
            break;
          case 'object':
            {
              try
              {
                this.params.loadTo = JSON.parse(br.asString());
              }
              catch(err)
              {
                this.env.log.error('storage: could not parse JSON');
                this.fsmLoad.setState(FSM.FSM_ERROR);
              }
            }
            break;
          case 'filepath':
            throw 'endLoad: filepath is not a supported value for loadTo';
            break;
        }

        if (this.params.loadToFilter)
          this.params.loadTo = this.params.loadToFilter(this, this.params.loadTo);

        // Make props available in blob
        let props = br.asProps();
        if (props.length == 1)
        {
          let p = props[0];
          if (! p.Key)
            p.Key = this.params.id;
          this._keys.push(p.Key);
          this._props.push(p);
        }
      }

      // load filter might have marked fsmLoad done (or marked load state failed)
      if (! this.fsmLoad.done)
        this.fsmLoad.setState(this.isLoadFailed() ? FSM.FSM_ERROR : FSM.FSM_DONE);

      // Support auto-delete behavior
      if (this.params.deleteAfterLoad)
        this.startDelete(this.env.storageManager);
    }

  endHead(br: BlobRequest): void
    {
      if (br.result() == ESuccess)
      {
        let a = br.asProps();
        if (a && a.length > 0)
          this.props.push(a[0]);
      }
      this.fsmList.setState(br.result() ? FSM.FSM_ERROR : FSM.FSM_DONE);
    }

  endDelete(br: BlobRequest): void
    {
      // Note that fsmDel is marked complete in setDeleted
    }

  startList(sm: StorageManager, continuationToken?: string): void
    {
      if (this.isListing())
      {
        this.env.log.error('storage: attempt to list while listing');
      }
      else
      {
        sm.ls(this, continuationToken);
      }
    }

  endList(br: BlobRequest): void
    {
      if (! br.result())
      {
        let props = br.asProps();
        this._keys.concat(props.map(p => p.Key));
        this._props.concat(props);
        if (br.continuationToken())
          this.env.storageManager.ls(this, br.continuationToken());
        else
        {
          // Set _keys and _props to done since might not have happened if no values to push
          this._keys.setState(FSM.FSM_DONE);
          this._props.setState(FSM.FSM_DONE);
          this.fsmList.setState(FSM.FSM_DONE);
        }
      }
      else
      {
        this.env.log.error(`list failed: ${br.asError()}`);
        this._keys.setState(FSM.FSM_ERROR);
        this._props.setState(FSM.FSM_ERROR);
        this.fsmList.setState(FSM.FSM_ERROR);
      }
    }

  toLoadStream(): stream.Readable
    {
      return this.params.loadTo as stream.Readable;
    }

  setLoadStream(rs: stream.Readable): void
    {
      this.params.loadTo = rs;
    }
}

export interface BlobRequest
{
  blob: StorageBlob;

  result(): number;
  asBuffer(): Buffer;
  asString(): string;
  asError(): string;
  asProps(): BlobProperties[];
  continuationToken(): string;
}

export interface BlobRequestIndex
{
  [key: string]: BlobRequest;
}

export class StorageManager
{
  _env: Environment;
  saveBlobIndex: BlobRequestIndex;
  loadBlobIndex: BlobRequestIndex;
  headBlobIndex: BlobRequestIndex;
  delBlobIndex: BlobRequestIndex;
  lsBlobIndex: BlobRequestIndex;
  onList: any;
  bucketMap: BucketMap;
  fsmAPIWatch: FsmAPIWatch;

  constructor(env: Environment, bucketMap?: BucketMap)
    {
      this._env = env;
      this.saveBlobIndex = {};
      this.loadBlobIndex = {};
      this.headBlobIndex = {};
      this.delBlobIndex = {};
      this.lsBlobIndex = {};
      this.onList = {};
      this.bucketMap = bucketMap;
      this.fsmAPIWatch = new FsmAPIWatch(env, { warningIncrement: 100, title: 'storage' });
    }

  save(blob: StorageBlob): void {} // override
  load(blob: StorageBlob): void {} // override
  del(blob: StorageBlob): void {} // override
  head(blob: StorageBlob): void {} // override
  ls(blob: StorageBlob, continuationToken?: string): void {} // override
  createTransferUrl(params: TransferParams): FsmTransferUrl { return null } // override

  on(eventName: string, cb: any): void
    {
      let aCB: any[] = this.onList[eventName];
      if (aCB === undefined)
      {
        aCB = [];
        this.onList[eventName] = aCB;
      }
      aCB.push(cb);
    }

  off(eventName: string, cb: any): void
    {
      let aCB: any[] = this.onList[eventName];
      if (aCB !== undefined)
      {
        for (let i: number = 0; i < aCB.length; i++)
          if (aCB[i] === cb)
          {
            aCB.splice(i, 1);
            break;
          }
      }
    }

  emit(eventName: string, blob: StorageBlob): void
    {
      let aCB: any[] = this.onList[eventName];
      if (aCB !== undefined)
      {
        for (let i: number = 0; i < aCB.length; i++)
          (aCB[i])(blob);
      }
    }
}
