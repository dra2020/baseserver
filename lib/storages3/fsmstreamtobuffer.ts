// Node libraries
import * as stream from 'stream';
import * as zlib from 'zlib';

// Shared libraries
import { FSM } from '@dra2020/baseclient';
import * as Storage from '../storage/all';

import { Environment } from './env';

// Convert stream to Buffer, potentially unzipping along the way.
// Marks done when complete (or error).

export class FsmStreamToBuffer extends FSM.Fsm
{
  _bufs: Buffer[];
  _buf: Buffer;
  err: any;

  constructor(env: Environment, s?: stream.Readable, unzip?: boolean)
  {
    super(env);
    this._bufs = [];
    this.setStream(s);
  }

  get buffer(): Buffer { return this._buf }

  setStream(s?: stream.Readable, unzip?: boolean): void
  {
    if (s)
    {
      //console.log(`FsmStreamToBuffer: starting, unzip: ${unzip}`);
      let zs = unzip ? zlib.createGunzip({}) : null;
      let t = unzip ? zs : s;
      t.on('data', (chunk: Buffer) => {
          this._bufs.push(chunk);
          //console.log(`FsmStreamToBuffer: reading chunk of size ${chunk.length}`);
        });
      t.on('end', () => {
          this._buf = Buffer.concat(this._bufs);
          //console.log(`FsmStreamToBuffer: finished reading buffer of length ${this._buf.length}`);
          delete this._bufs;
          this.setState(FSM.FSM_DONE);
        });
      t.on('error', (err: any) => {
          //console.log(`FsmStreamToBuffer: error reading zipped buffer`);
          this.err = { statusCode: 999, message: 'Content not in gzip format.' };
          this.setState(FSM.FSM_ERROR);
        });
      if (unzip)
        s.pipe(zs);
      else
        s.on('error', (err: any) => {
            //console.log(`FsmStreamToBuffer: error reading buffer`);
            this.err = { statusCode: 999, message: 'Stream read error.' };
            this.setState(FSM.FSM_ERROR);
          });
      this.setState(FSM.FSM_STARTING);
    }
  }
}

// Convert stream to stream, potentially unzipping along the way.
// Marks done when complete. This makes the read stream available
// immediately on creation rather than waiting for actual stream
// to be set.

export class FsmStreamToStream extends FSM.Fsm
{
  passThrough: Storage.MultiBufferPassThrough;
  err: any;

  constructor(env: Environment, s?: stream.Readable, unzip?: boolean)
  {
    super(env);
    this.passThrough = new Storage.MultiBufferPassThrough();
    this.setStream(s, unzip);
  }

  get stream(): stream.Readable
  {
    return this.passThrough;
  }

  setStream(s?: stream.Readable, unzip?: boolean): void
  {
    if (s)
    {
      //console.log(`FsmStreamToStream: starting, unzip: ${unzip}`);
      if (unzip)
      {
        let t = zlib.createGunzip({});
        t.on('end', () => {
            //console.log(`FsmStreamToStream: finishing zipped stream`);
            this.passThrough._done();
            this.setState(FSM.FSM_DONE);
          });
        t.on('error', () => {
            //console.log(`FsmStreamToStream: error on zipped stream`);
            this.passThrough._done();
            this.err = { statusCode: 999, message: 'Content not in gzip format.' };
            this.setState(FSM.FSM_ERROR);
          });
        s.pipe(t).pipe(this.passThrough);
      }
      else
      {
        s.on('end', () => {
            //console.log(`FsmStreamToStream: finishing non-zipped stream`);
            this.passThrough._done();
            this.setState(FSM.FSM_DONE);
          });
        s.on('error', () => {
            //console.log(`FsmStreamToStream: error on non-zipped stream`);
            this.passThrough._done();
            this.err = { statusCode: 999, message: 'Stream read error.' };
            this.setState(FSM.FSM_ERROR);
          });
        s.pipe(this.passThrough);
      }
    }
  }
}
