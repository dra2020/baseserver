// Node libraries
import * as fs from 'fs';

// Shared libraries
import { LogAbstract, FSM } from '@dra2020/baseclient';

export interface Environment
{
  log: LogAbstract.ILog;
  fsmManager: FSM.FsmManager;
}

const FSM_READING = FSM.FSM_CUSTOM1;
const FSM_WRITING = FSM.FSM_CUSTOM1;

export class FsmUnlink extends FSM.Fsm
{
  path: string;
  err: any;

  constructor(env: Environment, path: string)
  {
    super(env);
    this.path = path;
    this.err = null;
  }

  tick(): void
  {
    if (this.ready && this.state === FSM.FSM_STARTING)
    {
      this.setState(FSM.FSM_PENDING);
      fs.unlink(this.path, (err: any) => {
          if (err)
          {
            this.setState(FSM.FSM_ERROR);
            this.err = err;
          }
          else
            this.setState(FSM.FSM_DONE);
        });
    }
  }
}

export class FsmMkdir extends FSM.Fsm
{
  dir: string;
  bAllowExist: boolean;
  err: any;

  constructor(env: Environment, dir: string, bAllowExist: boolean = true)
  {
    super(env);
    this.dir = dir;
    this.err = null;
    this.bAllowExist = true;
  }

  tick(): void
  {
    if (this.ready && this.state == FSM.FSM_STARTING)
    {
      this.err = null;
      this.setState(FSM.FSM_PENDING);
      fs.mkdir(this.dir, 0o777, (err: any) => {
          // ignore EEXISTS errors
          if (err === null || (this.bAllowExist && err.code === 'EEXIST'))
            this.setState(FSM.FSM_DONE);
          else
          {
            this.err = err;
            this.setState(FSM.FSM_ERROR);
          }
        });
    }
  }
}

export class FsmRmdir extends FSM.Fsm
{
  dir: string;
  err: any;

  constructor(env: Environment, dir: string)
  {
    super(env);
    this.dir = dir;
  }

  tick(): void
  {
    if (this.ready && this.state == FSM.FSM_STARTING)
    {
      this.err = null;
      this.setState(FSM.FSM_PENDING);
      fs.rmdir(this.dir, (err: any) => {
          // ignore ENOENT errors
          if (err === null || err.code === 'ENOENT')
            this.setState(FSM.FSM_DONE);
          else
          {
            this.err = err;
            this.setState(FSM.FSM_ERROR);
          }
        });
    }
  }
}

export class FsmLs extends FSM.Fsm
{
  dir: string;
  err: any;
  entries: string[];

  constructor(env: Environment, dir: string)
  {
    super(env);
    this.dir = dir;
    this.err = null;
  }

  tick(): void
  {
    if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.setState(FSM.FSM_PENDING);
          fs.readdir(this.dir, (err: any, entries: string[]) => {
            if (err)
            {
              this.err = err;
              this.setState(FSM.FSM_ERROR);
            }
            else
            {
              this.entries = entries;
              this.setState(FSM.FSM_DONE);
            }
          });
        break;
      }
    }
  }
}

export class FsmReadFile extends FSM.Fsm
{
  path: string;
  result: string;
  err: any;

  constructor(env: Environment, path: string)
  {
    super(env);

    this.path = path;
    this.result = null;
    this.err = null;
  }

  tick(): void
  {
    if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.err = null;
          this.result = null;
          this.setState(FSM_READING);
          fs.readFile(this.path, 'utf8', (err: any, data: string) => {
              if (err)
              {
                this.result = null;
                this.err = err;
                this.setState(FSM.FSM_ERROR);
              }
              else
              {
                this.err = null;
                this.result = data;
                this.setState(FSM.FSM_DONE);
              }
            });
          break;
      }
    }
  }
}

export class FsmWriteFile extends FSM.Fsm
{
  path: string;
  contents: string | Buffer;
  err: any;

  constructor(env: Environment, path: string, contents: string | Buffer)
  {
    super(env);

    this.path = path;
    this.contents = contents;
    this.err = null;
  }

  tick(): void
  {
    if (this.ready)
    {
      switch (this.state)
      {
        case FSM.FSM_STARTING:
          this.err = null;
          this.setState(FSM_WRITING);
          fs.writeFile(this.path, this.contents, (err: any) => {
              if (err)
              {
                this.err = err;
                this.setState(FSM.FSM_ERROR);
              }
              else
              {
                this.err = null;
                this.setState(FSM.FSM_DONE);
              }
            });
          break;
      }
    }
  }
}
