// Node libraries
import * as fs from 'fs';
import * as stream from 'stream';
import * as zlib from 'zlib';

// Public libraries
import * as S3 from 'aws-sdk/clients/s3';

// Shared libraries
import { Context, LogAbstract, Util, FSM } from '@dra2020/baseclient';

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
    if (this.err == null
        && this.data
        && this.data.Body
        && this.data.ContentEncoding === 'gzip'
        && this.blob.params.loadToType !== 'compressedbuffer')
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
    if (data.Size !== undefined)
      props.ContentLength = data.Size;
    else if (data.ContentLength !== undefined)
      props.ContentLength = data.ContentLength;
    props.Key = data.Key;
    props.ETag = data.ETag;
    props.LastModified = data.LastModified;
    if (data.ContentEncoding)
      props.ContentEncoding = data.ContentEncoding;
    if (data.CacheControl)
      props.CacheControl = data.CacheControl;
    if (data.ContentType)
      props.ContentType = data.ContentType;
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
    else if (this.data)
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
          this.sm.s3(this.blob).getObject(this.param, (err: any, data: any) => {
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

  constructor(env: Environment, params: Storage.TransferParams)
  {
    super(env, params);
  }
}

export class StorageManager extends Storage.StorageManager
{
  s3map: any;
  count: number;

  constructor(env: Environment, bucketMap?: Storage.BucketMap)
  {
    super(env, bucketMap);
    this.s3map = {};

    if (this.env.context.xstring('aws_access_key_id') === undefined
        || this.env.context.xstring('aws_secret_access_key') === undefined)
    {
      this.env.log.error('S3: not configured: exiting');
      this.env.log.dump();
      process.exit(1);
    }

    this.s3map[''] = new S3({apiVersion: '2006-03-01', region: 'us-west-2'});
    this.s3map['aws'] = this.s3map[''];
    if (this.env.context.xstring('b2_application_key_id') !== undefined
        && this.env.context.xstring('b2_application_key') !== undefined)
      this.s3map['b2'] = new S3({
                          apiVersion: '2006-03-01',
                          endpoint: 's3.us-west-004.backblazeb2.com',
                          accessKeyId: this.env.context.xstring('b2_application_key_id'),
                          secretAccessKey: this.env.context.xstring('b2_application_key'),
                        });
    this.count = 0;
  }

  get env(): Environment { return this._env as Environment; }

  lookupBucket(s: string): string
  {
    let re = /^([^.]+)\.(.*)$/;
    let a = re.exec(s);
    if (a && a.length == 3)
      s = a[2];

    while (this.bucketMap[s] !== undefined)
      s = this.bucketMap[s];
    return s;
  }

  s3OfBucket(bucket: string): any
  {
    let re = /^([^.]+)\.(.*)$/;

    let a = re.exec(bucket);
    return (a && a.length == 3 && this.s3map[a[1]]) ? this.s3map[a[1]] : this.s3map[''];
  }

  s3(blob: Storage.StorageBlob): any
  {
    return this.s3OfBucket(blob.params.bucket);
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
    let params = { Bucket: this.blobBucket(blob), Key: blob.params.id };
    let trace = new LogAbstract.AsyncTimer(this.env.log, `S3: load (${params.Bucket})`);
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
      rq.req = this.s3(blob).getObject(params, (err: any, data: any) => {
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
    let params = { Bucket: this.blobBucket(blob), Key: blob.params.id };
    let trace = new LogAbstract.AsyncTimer(this.env.log, `S3: head (${params.Bucket})`);
    let rq = new S3Request(blob);
    this.headBlobIndex[id] = rq;
    blob.setLoading();
    rq.req = this.s3(blob).headObject(params, (err: any, data: any) => {
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

    let params: any = { Bucket: this.blobBucket(blob), Key: blob.params.id };
    let trace = new LogAbstract.AsyncTimer(this.env.log, `S3: save (${params.Bucket})`);
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
      case 'compressedbuffer':
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
    if (blob.params.ContentEncoding === 'gzip' && Buffer.isBuffer(body) && blob.params.saveFromType !== 'compressedbuffer')
      body =  zlib.gzipSync(body);

    params.Body = body;
    rq.req = this.s3(blob).putObject(params, (err: any, data: any) => {
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

    let params = { Bucket: this.blobBucket(blob), Key: blob.params.id };
    let trace = new LogAbstract.AsyncTimer(this.env.log, `S3: del (${params.Bucket})`);
    let rq = new S3Request(blob);
    this.delBlobIndex[id] = rq;
    blob.setDeleting();
    rq.req = this.s3(blob).deleteObject(params, (err: any, data: any) => {
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

    let params: any = { Bucket: b };
    let trace = new LogAbstract.AsyncTimer(this.env.log, `S3: ls (${params.Bucket})`);
    if (continuationToken)
      params.ContinuationToken = continuationToken;
    let rq = new S3Request(blob);
    this.lsBlobIndex[id] = rq;
    blob.setListing();
    rq.req = this.s3(blob).listObjectsV2(params, (err: any, data: any) => {
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
    params = Util.shallowAssignImmutable(params, { bucket: params.bucket || 'transfers' });
    let fsm = new FsmTransferUrl(this.env, params);
    if (fsm === null)
    {
      let params: any = { Bucket: this.lookupBucket(fsm.params.bucket), Fields: { key: fsm.params.key } };
      this.s3OfBucket(params.bucket).createPresignedPost(params, (err: any, url: string) => {
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
      let s3params: any = { Bucket: this.lookupBucket(fsm.params.bucket), Key: fsm.params.key };
      if (params.op === 'putObject' && params.contentType)
        s3params['ContentType'] = params.contentType;
      if (params.op === 'putObject' && params.contentEncoding)
        s3params['ContentEncoding'] = params.contentEncoding;
      if (params.op === 'putObject' && params.cacheControl)
        s3params['CacheControl'] = params.cacheControl;
      this.s3OfBucket(params.bucket).getSignedUrl(params.op, s3params, (err: any, url: string) => {
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
