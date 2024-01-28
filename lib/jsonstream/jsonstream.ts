// Node libraries
import * as Stream from 'stream';
import * as Events from 'events';

// Shared libraries
import { Util } from '@dra2020/baseclient';

// Acts like Stream.Readable, emits 'error', 'close', 'data', 'end'
export class Readable extends Events.EventEmitter
{
  constructor()
    {
      super();
    }
}

// Acts like Stream.Writable, emits 'error', 'close', 'drain', 'finish''
export class Writable extends Events.EventEmitter
{
  constructor()
    {
      super();
    }

  // override
  write(b: any, encoding?: BufferEncoding, cb?: () => void): boolean
    {
      return true;
    }

  // override
  end(): void
    {
      this.emit('finish');
    }
}

// Useful class for having JSONStreamWriter generate a Buffer
export class BufferWritable extends Writable
{
  _bufs: Buffer[];
  _buf: Buffer;

  constructor()
  {
    super();
    this._bufs = [];
    this._buf = null;
  }

  write(b: any, encoding?: BufferEncoding, cb?: () => void): boolean
  {
    if (typeof b === 'string')
      b = Buffer.from(b as string, encoding);
    this._bufs.push(b as Buffer);
    if (this._bufs.length > 100) this._bufs = [ Buffer.concat(this._bufs) ];
    return true;
  }

  end(): void
  {
    this._buf = Buffer.concat(this._bufs);
    this.emit('finish', this._buf);
  }
}

// Useful class for having JSONStreamReader read from an existing Buffer
export class BufferReadable extends Stream.Readable
{
  _b: Buffer;

  constructor(b: Buffer)
  {
    super({});
    this._b = b;
  }

  _read(): void
  {
    if (this._b)
    {
      this.push(this._b);
      this.push(null);
      this._b = null;
    }
  }
}

export class MultiBufferReadable extends Stream.Readable
{
  constructor()
  {
    super({});
  }

  _read(): void
  {
  }

  more(buf: Buffer): void
  {
    this.push(buf);
  }

  end(): void
  {
    this.push(null);
  }
}

enum ReadState {
  Start,
  ObjectPropertyStart,
  ObjectPropertyEnd,
  ValueStart,
  ValueEnd,
  InName,
  InNameBackslash,
  NeedColon,
  InNumber,
  InString,
  InStringBackslash,
  InTrue,
  InFalse,
  InNull,
  InUndefined,
  Error,
  Ended
  };

const CurlyOpen = 123;
const CurlyClose = 125;
const SquareOpen = 91;
const SquareClose = 93;
const Comma = 44;
const Colon = 58;
const Quote = 34;
const Backslash = 92;
const Forwardslash = 47
const Zero = 48;
const Nine = 57;
const Minus = 45;
const Plus = 43;
const Period = 46;
const UpperA = 65;
const LowerA = 97;
const UpperB = 66;
const LowerB = 98;
const UpperE = 69;
const LowerE = 101;
const UpperF = 70;
const LowerF = 102;
const UpperN = 78;
const LowerN = 110;
const UpperR = 82;
const LowerR = 114;
const UpperT = 84;
const LowerT = 116;
const UpperU = 85;
const LowerU = 117;
const UpperZ = 90;
const LowerZ = 122;
const Space = 32;
const Backspace = 8;
const Tab = 9;
const Newline = 10;
const CR = 13;


function isWhite(c: number): boolean
{
  return c == Space || c == Newline || c == Tab || c == CR;
}

function isEscapable(c: number): number
{
  switch (c)
  {
    case Quote:         return Quote;
    case Backslash:     return Backslash;
    case Forwardslash:  return Forwardslash;
    case UpperB:
    case LowerB:        return Backspace;        
    case UpperT:
    case LowerT:        return Tab;
    case UpperN:
    case LowerN:        return Newline;
    case UpperR:
    case LowerR:        return CR;

    // TODO: Really should support unicode \uHHHH
    default:
      return -1;
  }
}

function isNumeric(c: number): boolean
{
  return (c >= Zero && c <= Nine) || c == Minus || c == Plus || c == UpperE || c == LowerE || c == Period;
}

function isAlpha(c: number): boolean
{
  return (c >= UpperA && c <= UpperZ) || (c >= LowerA && c <= LowerZ);
}

enum TokType { TTNone, TTArray, TTObject, TTName, TTString, TTNumber, TTTrue, TTFalse, TTNull, TTUndefined };

interface Token
{
  tt: TokType;
  v: any;
  emit?: string;
}

export interface JSONStreamOptions
{
  maxTokenSize?: number;
  outBufferSize?: number;
  encoding?: BufferEncoding;
  syncChunkSize?: number;
}

const DefaultStreamOptions: JSONStreamOptions =
{
  maxTokenSize: 10000,
  outBufferSize: 10000,
  encoding: 'utf8',
  syncChunkSize: 10000,
}

export class JSONStreamReader extends Events.EventEmitter
{
  _s: Readable;
  _charCount: number;
  _lineCount: number;
  _state: ReadState;
  _tok: Buffer;
  _tokLen: number;
  _stack: Token[];
  _result: any;
  _incr: any;
  _options: JSONStreamOptions;
  _chunks: Buffer[];
  _chunkCur: number;
  _chunkIndex: number;
  _chunkScheduled: boolean;
  _ended: boolean;
  _closed: boolean;

  constructor(options?: JSONStreamOptions)
    {
      super();
      this.onData = this.onData.bind(this);
      this.onClose = this.onClose.bind(this);
      this.onEnd = this.onEnd.bind(this);
      this.onError = this.onError.bind(this);
      this.nextChunk = this.nextChunk.bind(this);
      this._state = ReadState.Start;
      this._charCount = 1;
      this._lineCount = 1;
      this._stack = [];
      this._result = undefined;
      this._incr = {};
      this._options = Util.shallowAssignImmutable(DefaultStreamOptions, options);
      this._tok = Buffer.alloc(this._options.maxTokenSize);
      this._tokLen = 0;
      this._s = undefined;
      this._chunks = [];
      this._chunkCur = 0;
      this._chunkIndex = 0;
      this._chunkScheduled = false;
      this._ended = false;
      this._closed = false;
    }

  start(s: Readable): void
    {
      this._s = s;
      this._s.on('close', this.onClose);
      this._s.on('end', this.onEnd);
      this._s.on('error', this.onError);
      this._s.on('data', this.onData);
    }

  finish(): void
    {
      this._s.off('close', this.onClose);
      this._s.off('end', this.onEnd);
      this._s.off('error', this.onError);
      this._s.off('data', this.onData);
      this._s = undefined;
    }

  emitIncremental(name: string): void
    {
      this._incr[name] = true;
    }

  pushToken(tok: Token): void
    {
      this._stack.push(tok);
      if (tok.tt == TokType.TTArray || tok.tt == TokType.TTObject)
      {
        let prevTok: Token = this._stack.length > 1 ? this._stack[this._stack.length - 2] : null;
        if (prevTok && prevTok.tt == TokType.TTName && this._incr[prevTok.v] == true)
          tok.emit = prevTok.v;
        else if (prevTok == null && this._incr[''] == true) // special-case top-level object
          tok.emit = '';
      }
    }

  private badJSON(detail?: string): void
    {
      this._state = ReadState.Error;
      if (detail !== undefined)
        this.emit('error', `Invalid JSON (${detail}) at line ${this._lineCount}, character ${this._charCount}`);
      else
        this.emit('error', `Invalid JSON at line ${this._lineCount}, character ${this._charCount}`);
      this.finish();
    }

  private startTok(s: ReadState, c?: number): void
    {
      this._state = s;
      if (c !== undefined)
        this.addToTok(c);
    }

  private addToTok(c: number): void
    {
      if (this._tokLen == this._options.maxTokenSize)
        this.badJSON('token too long');
      else
      {
        this._tok.writeUInt8(c, this._tokLen);
        this._tokLen++;
      }
    }

  private consumeTok(): string
    {
      let s: string = this._tok.toString(this._options.encoding, 0, this._tokLen);
      this._tokLen = 0;
      return s;
    }

  private produceValue(tok: Token): void
    {
      if (this._stack.length == 0)
      {
        this._state = ReadState.Ended;
        this._result = tok.v;
      }
      else
      {
        this._state = ReadState.ValueEnd;
        let topTok: Token = this._stack[this._stack.length - 1];
        if (topTok.tt == TokType.TTArray)
        {
          if (topTok.emit !== undefined)
            this.emit('array', topTok.emit, tok.v);
          else
            topTok.v.push(tok.v);
        }
        else if (topTok.tt == TokType.TTName)
        {
          let p: string = topTok.v;
          this._stack.pop();
          topTok = this._stack[this._stack.length - 1];
          if (topTok.tt != TokType.TTObject)
            this.badJSON();
          else
          {
            if (topTok.emit !== undefined)
              this.emit('object', topTok.emit, p, tok.v);
            else
              topTok.v[p] = tok.v;
          }
        }
      }
    }

  private produceValidatedValue(tok: Token): void
    {
      let s: string = tok.v;
      switch (tok.tt)
      {
        case TokType.TTNumber:
          // TODO: Constrained parsing: [-]{0 | [1-9][0-9]*}[.[0-9]+][{e|E}[+-][0-9]+]
          // Optional leading minus, either zero or set of digits not starting with zero.
          // Optional decimal point with required digit sequence
          // Optional exponent, e or E, optional sign, required digit sequence
          tok.v = parseFloat(s);
          break;

        case TokType.TTNull:
          if (s != 'null')
            this.badJSON('bad value');
          else
            tok.v = null;
          break;

        case TokType.TTUndefined:
          if (s != 'undefined')
            this.badJSON('bad value');
          else
            tok.v = undefined;
          break;

        case TokType.TTTrue:
          if (s != 'true')
            this.badJSON('bad value');
          else
            tok.v = true;
          break;

        case TokType.TTFalse:
          if (s != 'false')
            this.badJSON('bad value');
          else
            tok.v = false;
          break;
      }

      if (this._state != ReadState.Error)
        this.produceValue(tok);
    }

  private onData(chunk: Buffer): void
    {
      this._chunks.push(chunk);
      this.nextChunk();
    }

  private scheduleNext(): void
    {
      if (this._state != ReadState.Error && this._chunkCur < this._chunks.length)
      {
        if (! this._chunkScheduled)
        {
          this._chunkScheduled = true;
          setImmediate(this.nextChunk);
        }
      }
      else if (this._closed)
        this.doClose();
      else if (this._ended)
        this.doEnd();
    }

  private nextChunk(): void
    {
      this._chunkScheduled = false;
      if (this._chunkCur >= this._chunks.length)
      {
        this.scheduleNext();
        return;
      }

      let chunk = this._chunks[this._chunkCur];
      let i = this._chunkIndex;
      let e = i + this._options.syncChunkSize;
      if (e > chunk.length) e = chunk.length;

      for (; this._state != ReadState.Error && i < e; i++)
      {
        let c: number = chunk[i];
        this._charCount++;
        if (c == 10) { this._lineCount++; this._charCount = 1; }

        switch (this._state)
        {
          case ReadState.Start:
            if (! isWhite(c))
              if (c === CurlyOpen)
              {
                this.pushToken({ tt: TokType.TTObject, v: {} });
                this._state = ReadState.ObjectPropertyStart;
              }
              else
                this.badJSON(`Unexpected character: ${String.fromCharCode(c)} in chunk ${chunk.toString('utf8')}`);
            break;

          case ReadState.ObjectPropertyStart:
            if (! isWhite(c))
              switch (c)
              {
                case Quote:
                  this.startTok(ReadState.InName);
                  break;
                case CurlyClose:
                  this.produceValue(this._stack.pop());
                  break;
                default:
                  this.badJSON();
              }
            break;

          case ReadState.ValueEnd:
            if (! isWhite(c))
            {
              let topTok: Token = this._stack[this._stack.length - 1];
              if (c == Comma)
                this._state = topTok.tt == TokType.TTArray ? ReadState.ValueStart : ReadState.ObjectPropertyStart;
              else if ((c == SquareClose && topTok.tt == TokType.TTArray) || (c == CurlyClose && topTok.tt == TokType.TTObject))
                this.produceValue(this._stack.pop());
              else
                this.badJSON(`missing comma or ending ${topTok.tt == TokType.TTArray ? 'square' : 'curly'} bracket`);
            }
            break;

          case ReadState.ValueStart:
            if (! isWhite(c))
            {
              if (c == CurlyOpen)
              {
                this.pushToken({ tt: TokType.TTObject, v: {} });
                this._state = ReadState.ObjectPropertyStart;
              }
              else if (c == SquareOpen)
              {
                this.pushToken({ tt: TokType.TTArray, v: [] });
                this._state = ReadState.ValueStart;
              }
              else if (c == SquareClose)
              {
                // Could be in array (good) or expecting value of object property (bad)
                let topTok: Token = this._stack[this._stack.length - 1];
                if (topTok.tt == TokType.TTArray)
                  this.produceValue(this._stack.pop());
                else
                  this.badJSON(`Unexpected character: ${String.fromCharCode(c)} in chunk ${chunk.toString('utf8')}`);
              }
              else if (c == Quote)
                this.startTok(ReadState.InString);
              else if (c == UpperT || c == LowerT)
                this.startTok(ReadState.InTrue, c);
              else if (c == UpperF || c == LowerF)
                this.startTok(ReadState.InFalse, c);
              else if (c == UpperN || c == LowerN)
                this.startTok(ReadState.InNull, c);
              else if (c == UpperU || c == LowerU)
                this.startTok(ReadState.InUndefined, c);
              else if (isNumeric(c))
                this.startTok(ReadState.InNumber, c);
              else
                this.badJSON();
            }
            break;

          case ReadState.InName:
            if (c == Backslash)
              this._state = ReadState.InNameBackslash;
            else if (c != Quote)
              this.addToTok(c);
            else
            {
              this.pushToken({ tt: TokType.TTName, v: this.consumeTok() });
              this._state = ReadState.NeedColon;
            }
            break;

          case ReadState.InNameBackslash:
            {
              // TODO: add hex encoding support
              let escC = isEscapable(c);
              if (escC < 0)
                this.badJSON(`Illegal escaped character: ${String.fromCharCode(c)}`);
              this.addToTok(escC);
              this._state = ReadState.InName;
            }
            break;

          case ReadState.NeedColon:
            if (! isWhite(c))
              if (c !== Colon)
                this.badJSON('expected colon');
              else
                this._state = ReadState.ValueStart;
            break;

          case ReadState.InNumber:
            if (isNumeric(c))
              this.addToTok(c);
            else
            {
              this.produceValidatedValue({ tt: TokType.TTNumber, v: this.consumeTok() });
              i--;
            }
            break;

          case ReadState.InString:
            if (c == Backslash)
              this._state = ReadState.InStringBackslash;
            else if (c != Quote)
              this.addToTok(c);
            else
              this.produceValue({ tt: TokType.TTString, v: this.consumeTok() });
            break;

          case ReadState.InStringBackslash:
            {
              // TODO: add hex encoding support
              let escC = isEscapable(c);
              if (escC < 0)
                this.badJSON(`Illegal escaped character: ${String.fromCharCode(c)}`);
              this.addToTok(escC);
              this._state = ReadState.InString;
            }
            break;

          case ReadState.InTrue:
            if (isAlpha(c))
              this.addToTok(c);
            else
            {
              this.produceValidatedValue({ tt: TokType.TTTrue, v: this.consumeTok() });
              i--;
            }
            break;

          case ReadState.InFalse:
            if (isAlpha(c))
              this.addToTok(c);
            else
            {
              this.produceValidatedValue({ tt: TokType.TTFalse, v: this.consumeTok() });
              i--;
            }
            break;

          case ReadState.InNull:
            if (isAlpha(c))
              this.addToTok(c);
            else
            {
              this.produceValidatedValue({ tt: TokType.TTNull, v: this.consumeTok() });
              i--;
            }
            break;

          case ReadState.InUndefined:
            if (isAlpha(c))
              this.addToTok(c);
            else
            {
              this.produceValidatedValue({ tt: TokType.TTUndefined, v: this.consumeTok() });
              i--;
            }
            break;

          case ReadState.Ended:
            if (! isWhite(c))
              this.badJSON('unexpected character after end of object');
            break;
        }
      }

      // Potentially advance to next chunk and schedule next processing slice
      if (this._state === ReadState.Error)
      {
        this._chunks = [];
        this._chunkCur = 0;
        this._chunkIndex = 0;
      }
      else
      {
        if (e === chunk.length)
        {
          this._chunkIndex = 0;
          if (this._chunkCur === this._chunks.length - 1)
          {
            this._chunks = [];
            this._chunkCur = 0;
          }
          else
          {
            this._chunkCur++;
            if (this._chunkCur >= 100)
            {
              this._chunks.splice(0, this._chunkCur);
              this._chunkCur = 0;
            }
          }
        }
        else
          this._chunkIndex = e;
      }
      this.scheduleNext();
    }

  private onClose(): void
    {
      this._closed = true;
      this.scheduleNext();
    }

  private doClose(): void
    {
      this._closed = undefined;
      this.doEnd();
      this.emit('close');
    }

  private onError(e: Error): void
    {
      this._state = ReadState.Error;
      let s: string = 'stream error';
      if (e && typeof e.message === 'string')
        s = e.message;
      console.log(`jsonreader: emitting error: ${s}`);
      this.emit('error', s);
      this.finish();
    }

  private onEnd(): void
    {
      this._ended = true;
      this.scheduleNext();
    }

  private doEnd(): void
    {
      this._ended = undefined;
      if (this._state == ReadState.Ended)
      {
        // Only emit once
        if (this._result !== undefined)
        {
          this.emit('end', this._result);
          this._result = undefined;
          this.finish();
        }
      }
      else if (this._state !== ReadState.Error)
        this.badJSON('Unexpected end of input');
    }
}

interface ObjectWriteState
{
  o: any;
  keys: string[];
  index: number;
}

export class JSONStreamWriter extends Events.EventEmitter
{
  _s: Writable;
  _stack: ObjectWriteState[];
  _chunk: Buffer;
  _chunkLen: number;
  _options: JSONStreamOptions;
  _bDone: boolean;

  constructor(options?: JSONStreamOptions)
    {
      super();
      this._bDone = false;
      this._s = null;
      this._stack = [];
      this.onDrain = this.onDrain.bind(this);
      this.onError = this.onError.bind(this);
      this.onFinish = this.onFinish.bind(this);
      this.onClose = this.onClose.bind(this);
      this._options = Util.shallowAssignImmutable(DefaultStreamOptions, options);
      this._chunk = Buffer.alloc(this._options.outBufferSize);
      this._chunkLen = 0;
    }

  start(o: any, s: Writable): void
    {
      this._s = s;
      this._s.on('drain', this.onDrain);
      this._stack.push({ o: o, keys: Array.isArray(o) ? null : Object.keys(o), index: 0 });
      this.onDrain();
    }

  addToChunk(s: string): boolean
    {
      let b = Buffer.from(s as string, this._options.encoding);
      let bContinue: boolean = true;

      if (b.length + this._chunkLen > this._options.outBufferSize)
        bContinue = this.drainChunk();

      if (b.length > this._options.outBufferSize)
      {
        console.log(`JSONWriter error writing string of length ${s.length} at:`);
        let e = new Error();
        console.log(e.stack);
        throw 'Internal error in JSONStreamWriter: string larger than buffer size';
      }

      b.copy(this._chunk, this._chunkLen);
      this._chunkLen += b.length;
      return bContinue;
    }

  drainChunk(): boolean
    {
      if (this._chunkLen > 0)
      {
        let b: Buffer = this._chunk.slice(0, this._chunkLen);

        // false just indicates backpressure - write still succeeded.
        let bDraining: boolean = this._s.write(b);

        // Allocate new write buffer to prevent overwriting output buffer before output stream
        // gets chance to copy out the data.
        this._chunk = Buffer.alloc(this._options.outBufferSize);
        this._chunkLen = 0;

        return bDraining;
      }
      return true;
    }

  onDrain(): void
    {
      let bContinue: boolean;

      while (! this._bDone && this._stack.length > 0)
      {
        let top: ObjectWriteState = this._stack[this._stack.length - 1];

        // Object
        if (top.keys)
        {
          if (top.keys.length == 0)
            bContinue = this.addToChunk('{');
          let bRecurse = false;
          // Even index means propname not written
          while (! this._bDone && top.index < top.keys.length * 2)
          {
            let ii: number = Math.floor(top.index / 2);
            if ((top.index % 2) == 0)
            {
              let s: string = `${top.index == 0 ? '{' : ','}"${top.keys[ii]}":`;
              bContinue = this.addToChunk(s);
              top.index++;
              if (! bContinue)
                return;
            }
            let oo: any = top.o[top.keys[ii]];
            if (oo && typeof oo === 'object')
            {
              bRecurse = true;
              top.index++;
              this._stack.push({ o: oo, keys: Array.isArray(oo) ? null : Object.keys(oo), index: 0 });
              break;
            }
            else
            {
              let s: string = JSON.stringify(oo === undefined ? null : oo);
              bContinue = this.addToChunk(s);
              top.index++;
              if (! bContinue)
                return;
            }
          }

          if (! bRecurse)
          {
            bContinue = this.addToChunk('}');
            this._stack.pop();
            if (! bContinue)
              return;
          }
        }

        // Array
        else
        {
          if (top.o.length == 0)
            bContinue = this.addToChunk('[');
          let bRecurse = false;

          // Even index means separator not written
          while (! this._bDone && top.index < top.o.length * 2)
          {
            let ii: number = Math.floor(top.index / 2);
            if ((top.index % 2) == 0)
            {
              let s: string = top.index == 0 ? '[' : ',';
              bContinue = this.addToChunk(s);
              top.index++;
              if (! bContinue)
                return;
            }
            let oo: any = top.o[ii];
            if (oo && typeof oo === 'object')
            {
              bRecurse = true;
              top.index++;
              this._stack.push({ o: oo, keys: Array.isArray(oo) ? null : Object.keys(oo), index: 0 });
              break;
            }
            else
            {
              let s: string = JSON.stringify(oo === undefined ? null : oo);
              bContinue = this.addToChunk(s);
              top.index++;
              if (! bContinue)
                return;
            }
          }

          if (! bRecurse)
          {
            bContinue = this.addToChunk(']');
            this._stack.pop();
            if (! bContinue)
              return;
          }
        }
      }

      this.drainChunk();
      this._s.end();
    }

    onError(e: Error): void
      {
        this._bDone = true;
        let s: string = 'stream error';
        if (e && typeof e.message === 'string')
          s = e.message;
        console.log(`jsonwriter: emitting error: ${s}`);
        this.emit('error', s);
      }

    onClose(): void
      {
        this._bDone = true;
        this.emit('close');
      }

    onFinish(): void
      {
        this._bDone = true;
        this.emit('finish');
      }
}
