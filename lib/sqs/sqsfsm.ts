import { FSM } from '@dra2020/baseclient';

import { Environment } from './env';
import { SQSMessage } from './sqsmessage';

export class FsmSend extends FSM.Fsm
{
  m: SQSMessage;

  constructor(env: Environment, m: SQSMessage)
  {
    super(env);
    this.m = m;
  }

  get env(): Environment { return this._env as Environment }
}

export class FsmReceive extends FSM.Fsm
{
  results: SQSMessage[];

  constructor(env: Environment, queueName: string)
  {
    super(env);
    this.results = [];
  }

  get env(): Environment { return this._env as Environment }
}

export class FsmDelete extends FSM.Fsm
{
  m: SQSMessage;

  constructor(env: Environment, m: SQSMessage)
  {
    super(env);
    this.m = m;
  }

  get env(): Environment { return this._env as Environment }
}
