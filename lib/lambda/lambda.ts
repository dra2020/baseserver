// Shared libraries
import { Util, Context, LogAbstract, FSM } from '@dra2020/baseclient';

import { Environment } from './env';

import * as Lambda from 'aws-sdk/clients/lambda';

class FsmList extends FSM.Fsm
{
  constructor(env: Environment)
  {
    super(env);
  }

  get env(): Environment { return this._env as Environment }

  tick(): void
  {
    if (this.ready && this.state === FSM.FSM_STARTING)
    {
      this.setState(FSM.FSM_PENDING);
      this.env.lambdaManager.awslambda.listFunctions({}, (err: any, data: any) => {
          if (err)
          {
            this.env.log.chatter(`listFunctions: failed: ${JSON.stringify(err)}`);
          }
          else
          {
            this.env.log.chatter(`listFunctions: success: ${JSON.stringify(data)}`);
          }
          this.setState(err ? FSM.FSM_ERROR : FSM.FSM_DONE);
        });
    }
  }
}

interface InvokeOptions
{
  isSync?: boolean,
}

export class FsmInvoke extends FSM.Fsm
{
  name: string;
  params: any;
  result: any;
  options: InvokeOptions;

  constructor(env: Environment, name: string, params: any)
  {
    super(env);
    this.name = name;
    this.params = Util.shallowAssignImmutable({ context: { production: env.context.xnumber('production') } }, params);
    this.options = { isSync: true };
  }

  get env(): Environment { return this._env as Environment }

  setOptions(options: InvokeOptions): FsmInvoke
  {
    Util.shallowAssign(this.options, options);
    return this;
  }

  tick(): void
  {
    if (this.ready && this.state === FSM.FSM_STARTING)
    {
      this.setState(FSM.FSM_PENDING);
      let awsparam: any = {
        FunctionName: `${this.name}:${this.env.context.xflag('production') ? 'production' : 'development'}`,
        InvocationType: this.options.isSync ? 'RequestResponse' : 'Event',
        LogType: 'None',
        Payload: JSON.stringify(this.params)
        };
      this.env.lambdaManager.awslambda.invoke(awsparam, (err: any, data: any) => {
          let payload: any = data && data.Payload ? data.Payload : null;
          try
          {
            this.result = (typeof payload === 'string') ? JSON.parse(payload) : payload;
          }
          catch (exception)
          {
            this.result = payload;
          }
          if (err)
          {
            this.env.log.chatter(`lambdamanager: invoke ${this.name} error: ${JSON.stringify(err)}`);
            this.env.log.error({ event: 'lambdamanager: invoke error', detail: JSON.stringify(this.result) });
            this.setState(FSM.FSM_ERROR);
          }
          else if (this.result && this.result.errorMessage)
          {
            this.result = { result: 1, message: this.result.errorMessage };
            this.env.log.chatter(`lambdamanager: invoke failure: ${this.name}`);
            this.env.log.event(`lambdamanager: invoke failure: ${this.name}`);
            this.setState(FSM.FSM_ERROR);
          }
          else
          {
            this.env.log.event(`lambdamanager: invoke success: ${this.name}`);
            this.setState(FSM.FSM_DONE);
          }
        });
    }
  }
}

export class Manager extends FSM.Fsm
{
  awslambda: Lambda;

  constructor(env: Environment)
  {
    super(env);
    this.awslambda = new Lambda({ apiVersion: '2015-03-31', region: 'us-west-2' });
  }

  get env(): Environment { return this._env as Environment }

  invoke(name: string, params?: any): FsmInvoke
  {
    return new FsmInvoke(this.env, name, params);
  }
}

export function create(env: Environment): Manager
{
  return new Manager(env);
}
