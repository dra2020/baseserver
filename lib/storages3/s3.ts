// Node libraries
import * as fs from 'fs';
import * as stream from 'stream';
import * as zlib from 'zlib';

// Public libraries
import * as S3 from 'aws-sdk/clients/s3';

// Shared libraries
import { Context, LogAbstract, FSM } from '@dra2020/baseclient';

import * as Storage from '../storage/all';

export interface Environment
{
  context: Context.IContext;
  log: LogAbstract.ILog;
  fsmManager: FSM.FsmManager;
  storageManager: Storage.StorageManager;
}

class S3Request implements Storage.BlobRequest
{
  blob: Storage.StorageBlob;
  req: any;
  res: any;
  data: any;
  err: any;

  constructor(blob: Storage.StorageBlob)
  {
    this.blob = blob;
    this.req = null;
    this.res = null;
    this.data = null;
    this.err = null;
  }

  continuationToken(): string
  {
    if (this.data && this.data.NextContinuationToken)
      return this.data.NextContinuationToken;

    return undefined;
  }

  result(): number
  {
    if (this.data == null && this.blob.toLoadStream() == null && this.err == null)
      return Storage.EPending;
    else if (this.err != null)
    {
      if (this.err.statusCode && this.err.statusCode == 404)
        return Storage.ENotFound;
      else if (this.err.statusCode === 999)
        return Storage.EBadFormat;
      else
        return Storage.EFail;
    }
    else
      return Storage.ESuccess;
  }

  decode(): void
  {
    if (this.err == null && this.data && this.data.Body && this.data.ContentEncoding === 'gzip')
    {
      try
      {
        this.data.Body = zlib.gunzipSync(this.data.Body);
      }
      catch (err)
      {
        this.err = { statusCode: 999, message: 'Content not in gzip format.' };
      }
    }
  }

  asString(): string
  {
    if (this.err || this.res == null || this.data == null || this.data.Body == null)
      return undefined;
    let body: Buffer = this.data.Body;
    return body.toString('utf-8');
  }

  // Uncompress as necessary
  asBuffer(): Buffer
  {
    if (this.err || this.res == null || this.data == null || this.data.Body == null)
      return undefined;
    let body: Buffer = this.data.Body;
    return body;
  }

  _dataToProps(data: any): Storage.BlobProperties
  {
    let props: Storage.BlobProperties = {};
    props.ContentLength = (data.Size !== undefined) ? data.Size : 0;
    props.Key = data.Key;
    props.ETag = data.ETag;
    props.LastModified = data.LastModified;
    props.ContentEncoding = data.ContentEncoding;
    return props;
  }

  asProps(): Storage.BlobProperties[]
  {
    let a: Storage.BlobProperties[] = [];

    if (this.data && Array.isArray(this.data.Contents))
    {
      for (let i: number = 0; i < this.data.Contents.length; i++)
        a.push(this._dataToProps(this.data.Contents[i]));
    }
    else
      a.push(this._dataToProps(this.data));

    return a;
  }

  asError(): string
  {
      if (this.err)
        return this.err.message ? this.err.message : JSON.stringify(this.err);
      return undefined;
  }
}

const ChunkSize = 4000000;

export class FsmStreamLoader extends FSM.Fsm
{
  sm: StorageManager;
  blob: Storage.StorageBlob;
  param: any;
  err: any;
  contentLength: number;
  contentPos: number;
  readStream: Storage.MultiBufferPassThrough;
  passThrough: Storage.MultiBufferPassThrough;

  constructor(env: Environment, sm: StorageManager, blob: Storage.StorageBlob)
  {
    super(env);
    this.sm = sm;
    this.blob = blob;
    this.contentPos = 0;
    this.param = { Bucket: sm.blobBucket(blob), Key: blob.params.id };

    // We use passthrough stream because we want to make the load stream available
    // immediately but we don't actually know whether we are going to have to pipe
    // through gunzip or not until we get the first ContentEncoding header back.
    this.readStream = new Storage.MultiBufferPassThrough();
    this.passThrough = new Storage.MultiBufferPassThrough();
    this.blob.setLoadStream(this.passThrough);
  }

  get env(): Environment { return this._env as Environment; }

  setStreamError(): void
  {
    this.passThrough._done();
    this.setState(FSM.FSM_ERROR);
    this.err = { statusCode: 999, message: 'Content not in gzip format.' };
  }

  tick(): void
  {
    if (this.ready)
    {
      // Figure out next chunk
      if (this.contentLength === undefined)
        this.param.Range = `bytes=0-${ChunkSize-1}`;
      else
        this.param.Range = `bytes=${this.contentPos}-${Math.min(this.contentPos+ChunkSize-1, this.contentLength-1)}`;

      switch (this.state)
      {

        case FSM.FSM_STARTING:
          this.sm.s3.getObject(this.param, (err: any, data: any) => {
              if (err == null)
              {
                // On first chunk, figure out if we need to pipe through gunzip
                if (this.contentLength === undefined)
                {
                  if (data.ContentEncoding && data.ContentEncoding === 'gzip')
                  {
                    let unzip = zlib.createGunzip({});
                    unzip.on('end', () => { this.passThrough._done(); this.setState(FSM.FSM_DONE); } );
                    unzip.on('error', () => { this.setStreamError() } );
                    this.readStream.pipe(unzip).pipe(this.passThrough);
                  }
                  else
                  {
                    this.readStream.on('end', () => { this.passThrough._done(); this.setState(FSM.FSM_DONE); } );
                    this.readStream.on('error', () => { this.setStreamError() } );
                    this.readStream.pipe(this.passThrough);
                  }
                }

                // Handle this data
                if (data.Body)
                  this.readStream.write(data.Body);

                // Update content range and content length for next time through, or noticing finish
                if (data.ContentRange)
                {
                  let re = /bytes (\d+)-(\d+)\/(\d+)/
                  let s: string = data.ContentRange;  // "bytes start-end/total"
                  let matched = re.exec(s);
                  if (matched && matched.length === 4)
                  {
                    this.contentPos = Number(matched[2]) + 1;
                    this.contentLength = Number(matched[3]);
                  }
                }
              }

              // Error or done reading
              if (err || this.contentPos === this.contentLength)
              {
                this.err = err;
                this.readStream._done();
                if (err)
                {
                  this.passThrough._done();
                  this.setState(FSM.FSM_ERROR);
                }
              }
              else
                this.setState(FSM.FSM_STARTING);
            });
          break;
      }
    }
  }
}

export class FsmTransferUrl extends Storage.FsmTransferUrl
{
  storageManager: StorageManager;

  constructor(env: Environment, bucket: string, params: Storage.TransferParams)
  {
    super(env, bucket, params);
  }
}

export class StorageManager extends Storage.StorageManager
{
  s3: any;
  count: number;

  constructor(env: Environment, bucketMap?: Storage.BucketMap)
  {
    super(env, bucketMap);

    if (this.env.context.xstring('aws_access_key_id') === undefined
        || this.env.context.xstring('aws_secret_access_key') === undefined)
    {
      this.env.log.error('S3: not configured: exiting');
      this.env.log.dump();
      process.exit(1);
    }

    this.s3 = new S3({apiVersion: '2006-03-01', region: 'us-west-2'});
    this.count = 0;
  }

  get env(): Environment { return this._env as Environment; }

  lookupBucket(s: string): string
  {
    while (this.bucketMap[s] !== undefined)
      s = this.bucketMap[s];
    return s;
  }

  blobBucket(blob: Storage.StorageBlob): string
  {
    return this.lookupBucket(blob.params.bucket);
  }

  load(blob: Storage.StorageBlob): void
  {
    if (blob.params.id == '')
    {
      this.env.log.error('S3: blob load called with empty key');
      return;
    }
    let id: string = `load+${blob.params.id}+${this.count++}`;

    this.env.log.event('S3: load start', 1);
    let trace = new LogAbstract.AsyncTimer(this.env.log, 'S3: load', 1);
    let params = { Bucket: this.blobBucket(blob), Key: blob.params.id };
    let rq = new S3Request(blob);
    this.loadBlobIndex[id] = rq;
    blob.setLoading();
    if (blob.params.loadToType === 'stream')
    {
      let fsm = new FsmStreamLoader(this.env, this, blob);
      rq.req = fsm;
      new FSM.FsmOnDone(this.env, fsm, (f: FSM.Fsm) => {
          this._finishLoad(blob, id, rq, fsm.err, undefined);
          trace.log();
        });
    }
    else
    {
      rq.req = this.s3.getObject(params, (err: any, data: any) => {
          this._finishLoad(blob, id, rq, err, data);
          trace.log();
        });
    }
  }

  _finishLoad(blob: Storage.StorageBlob, id: string, rq: S3Request, err: any, data: any)
  {
    rq.res = this;
    if (err)
      rq.err = err;
    else
      rq.data = data;

    rq.decode();
    blob.setLoaded(rq.result());
    blob.endLoad(rq);
    this.emit('load', blob);

    delete this.loadBlobIndex[id];

    this.env.log.event('S3: load end', 1);
  }

  head(blob: Storage.StorageBlob): void
  {
    if (blob.params.id == '')
    {
      this.env.log.error('S3: blob head called with empty key');
      return;
    }
    let id: string = `head+${blob.params.id}+${this.count++}`;

    this.env.log.event('S3: head start', 1);
    let trace = new LogAbstract.AsyncTimer(this.env.log, 'S3: head', 1);
    let params = { Bucket: this.blobBucket(blob), Key: blob.params.id };
    let rq = new S3Request(blob);
    this.headBlobIndex[id] = rq;
    blob.setLoading();
    rq.req = this.s3.headObject(params, (err: any, data: any) => {
        rq.res = this;
        if (err)
          rq.err = err;
        else
          rq.data = data;

        blob.setLoaded(rq.result());
        blob.endHead(rq);
        this.emit('head', blob);

        delete this.headBlobIndex[id];

        this.env.log.event('S3: head end', 1);
        trace.log();
      });
  }

  safeSaveFromPath(blob: Storage.StorageBlob, rq: S3Request, id: string, trace: LogAbstract.AsyncTimer): any
  {
    try
    {
      // We can't gzip the stream, so read as buffer (more size limited than stream) if required
      if (blob.params.ContentEncoding === 'gzip')
        return fs.readFileSync(blob.params.saveFrom);
      else
        return fs.createReadStream(blob.params.saveFrom);
    }
    catch (err)
    {
      rq.err = err;
      process.nextTick(() => {
          blob.setSaved(rq.result());
          blob.endSave(rq);
          this.emit('save', blob);
          delete this.saveBlobIndex[id];
          this.env.log.error('S3: failed to open blob path file');
          trace.log();
        });
    }
    return null;
  }

  save(blob: Storage.StorageBlob): void
  {
    if (blob.params.id == '')
    {
      this.env.log.error('S3: blob save called with empty key');
      return;
    }
    let id: string = `save+${blob.params.id}+${this.count++}`;

    this.env.log.event('S3: save start', 1);

    let trace = new LogAbstract.AsyncTimer(this.env.log, 'S3: save', 1);
    let params: any = { Bucket: this.blobBucket(blob), Key: blob.params.id };
    if (blob.params.ContentEncoding)
      params['ContentEncoding'] = blob.params.ContentEncoding;
    if (blob.params.ContentType)
      params['ContentType'] = blob.params.ContentType;
    if (blob.params.CacheControl)
      params['CacheControl'] = blob.params.CacheControl;
    let rq = new S3Request(blob);
    this.saveBlobIndex[id] = rq;
    blob.setSaving();

    let body: any;
    let bodyStream: stream.Readable;
    switch (blob.params.saveFromType)
    {
      case 'object':
        body = Buffer.from(JSON.stringify(blob.params.saveFrom));
        break;
      case 'string':
        body = Buffer.from(blob.params.saveFrom);
        break;
      case 'buffer':
        body = blob.params.saveFrom;
        break;
      case 'stream':
        body = blob.params.saveFrom;
        bodyStream = body as stream.Readable;
        break;
      case 'filepath':
        body = this.safeSaveFromPath(blob, rq, id, trace);
        if (body && !Buffer.isBuffer(body)) bodyStream = body as stream.Readable;
        if (body == null) return;
        break;
    }
    if (blob.params.ContentEncoding === 'gzip' && Buffer.isBuffer(body))
      body =  zlib.gzipSync(body);

    params.Body = body;
    rq.req = this.s3.putObject(params, (err: any, data: any) => {
        if (err)
          rq.err = err;
        else
          rq.data = data;
        rq.res = this;

        blob.setSaved(rq.result());
        blob.endSave(rq);
        this.emit('save', blob);

        delete this.saveBlobIndex[id];

        this.env.log.event('S3: save done', 1);
        trace.log();

        if (bodyStream)
          bodyStream.destroy();
      });
  }

  del(blob: Storage.StorageBlob): void
  {
    if (blob.params.id == '')
    {
      this.env.log.error('S3: blob delete called with empty key');
      return;
    }
    let id: string = `delete+${blob.params.id}+${this.count++}`;

    this.env.log.event(`S3: del start`, 1);

    let trace = new LogAbstract.AsyncTimer(this.env.log, 'S3: del', 1);
    let params = { Bucket: this.blobBucket(blob), Key: blob.params.id };
    let rq = new S3Request(blob);
    this.delBlobIndex[id] = rq;
    blob.setDeleting();
    rq.req = this.s3.deleteObject(params, (err: any, data: any) => {
        if (err)
          rq.err = err;
        else
          rq.data = data;
        rq.res = this;

        blob.setDeleted(rq.result());
        blob.endDelete(rq);
        this.emit('del', blob);

        delete this.delBlobIndex[id];

        trace.log();
        this.env.log.event(`S3: del done`, 1);
      });
  }

  ls(blob: Storage.StorageBlob, continuationToken?: string): void
  {
    let b = this.blobBucket(blob);
    if (b == '')
    {
      this.env.log.error('S3: blob ls called with empty bucket');
      return;
    }
    let id: string = `ls+${b}+${this.count++}`;

    this.env.log.event(`S3: ls start`, 1);

    let trace = new LogAbstract.AsyncTimer(this.env.log, 'S3: ls', 1);
    let params: any = { Bucket: b };
    if (continuationToken)
      params.ContinuationToken = continuationToken;
    let rq = new S3Request(blob);
    this.lsBlobIndex[id] = rq;
    blob.setListing();
    rq.req = this.s3.listObjectsV2(params, (err: any, data: any) => {
        if (err)
          rq.err = err;
        else
          rq.data = data;
        rq.res = this;

        blob.setListed();
        blob.endList(rq);
        this.emit('ls', blob);

        delete this.lsBlobIndex[id];

        trace.log();
        this.env.log.event(`S3: ls done`, 1);
      });
  }

  createTransferUrl(params: Storage.TransferParams): Storage.FsmTransferUrl
  {
    let fsm = new FsmTransferUrl(this.env, this.lookupBucket('transfers'), params);
    if (fsm === null)
    {
      let params: any = { Bucket: fsm.bucket, Fields: { key: fsm.key } };
      this.s3.createPresignedPost(params, (err: any, url: string) => {
          if (err)
          {
            this.env.log.error(`S3: createPresignedPost failed: ${err}`);
            fsm.setState(FSM.FSM_ERROR);
          }
          else
          {
            fsm.url = url;
            fsm.setState(FSM.FSM_DONE);
          }
        });
    }
    else
    {
      let s3params: any = { Bucket: fsm.bucket, Key: fsm.key };
      if (params.op === 'putObject') s3params.ContentType = fsm.params.contentType;
      this.s3.getSignedUrl(params.op, s3params, (err: any, url: string) => {
          if (err)
          {
            this.env.log.error(`S3: getSignedUrl failed: ${err}`);
            fsm.setState(FSM.FSM_ERROR);
          }
          else
          {
            fsm.url = url;
            fsm.setState(FSM.FSM_DONE);
          }
        });
    }
    return fsm;
  }
}
