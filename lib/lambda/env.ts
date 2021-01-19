import { Context, LogAbstract, FSM } from '@dra2020/baseclient';

import * as Lambda from './lambda';

export interface Environment
{
  context: Context.IContext,
  log: LogAbstract.ILog,
  fsmManager: FSM.FsmManager;
  lambdaManager: Lambda.Manager;
}
