// Public libraries
import * as AWS from 'aws-sdk';

import { FSM, Context, LogAbstract } from '@dra2020/baseclient';
import * as Storage from '../storage/all';
import { SQSManagerBase } from './sqsmanager';

export interface Environment
{
  context: Context.IContext,
  log: LogAbstract.ILog,
  fsmManager: FSM.FsmManager,
  storageManager: Storage.StorageManager,
  sqsManager: SQSManagerBase,
}
