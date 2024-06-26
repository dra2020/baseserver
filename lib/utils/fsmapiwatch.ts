// Shared libraries
import { Util, LogAbstract, Context, FSM } from '@dra2020/baseclient';

// Baseserver
import * as Storage from '../storage/all';

interface Environment
{
  context: Context.IContext;
  log: LogAbstract.ILog;
  fsmManager: FSM.FsmManager;
  storageManager: Storage.StorageManager;
}

export interface APIWatchOptions
{
  warningIncrement?: number,
  title?: string,
  trace?: boolean,
}

const Options: APIWatchOptions = { warningIncrement: 500, title: 'APIWatch', trace: false };

export class FsmAPIWatch extends FSM.Fsm
{
  pendingMap: Map<string, Set<any>>;
  warningLevel: Map<string, number>;
  options: APIWatchOptions;

  constructor(env: Environment, options?: APIWatchOptions)
  {
    super(env);
    this.options = Util.shallowAssignImmutable(Options, options);
    this.pendingMap = new Map<string, Set<any>>();
    this.warningLevel = new Map<string, number>();
  }

  get env(): Environment { return this._env as Environment }

  setPending(fsm: FSM.Fsm): void
  {
    // We need to get notified when this completes
    if (! fsm.done) this.waitOn(fsm);

    let label = (fsm as any).__name || fsm.constructor.name;
    let pending = this.pendingMap.get(label);
    if (!pending)
    {
      pending = new Set<any>();
      this.pendingMap.set(label, pending);
      this.warningLevel.set(label, this.options.warningIncrement);
    }
    pending.add(fsm);
    if (pending.size == this.warningLevel.get(label))
    {
      // Spit all currently pending operations
      this.pendingMap.forEach((p: Set<any>, l: string) => {
        if (p.size) console.log(`${this.options.title}: ${l}: ${p.size} pending operations`)});
      // Also provide stack trace
      if (this.options.trace)
        console.trace();
      this.warningLevel.set(label, this.warningLevel.get(label)+this.options.warningIncrement);
    }
  }

  waitOnCompleted(fsm: FSM.Fsm): void
  {
    let label = (fsm as any).__name || fsm.constructor.name;
    let set = this.pendingMap.get(label);
    set.delete(fsm);
  }
}
