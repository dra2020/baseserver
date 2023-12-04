import { Context, LogAbstract, FSM } from '@dra2020/baseclient';

import * as Storage from '../storage/all';

export interface Environment
{
  context: Context.IContext;
  log: LogAbstract.ILog;
  fsmManager: FSM.FsmManager;
  storageManager: Storage.StorageManager;
}
