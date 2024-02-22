import { Util, FSM, Context, LogAbstract } from '@dra2020/baseclient';
import * as Storage from '../storage/all';

import { Environment } from './env';
import { SQSMessage } from './sqsmessage';
import { FsmSend, FsmReceive, FsmDelete } from './sqsfsm';

export class SQSManagerBase
{
	env: Environment;

	constructor(env: Environment)
  {
    this.env = env;
  }

  send(name: string, m: SQSMessage): FsmSend
  {
    return null;
  }

  receive(name: string): FsmReceive
  {
    return null;
  }

  delete(name: string, m: SQSMessage): FsmDelete
  {
    return null;
  }
}
