// Node libraries
import * as http from 'http';

export interface AgentEntry
{
  agent: any;
  inUse: boolean;
}

export class AgentPool
{
  agents: AgentEntry[];

  constructor()
  {
    this.agents = [];
  }

  alloc(): any
  {
    for (let i: number = 0; i < this.agents.length; i++)
      if (! this.agents[i].inUse)
      {
        this.agents[i].inUse = true;
        return this.agents[i].agent;
      }

    let ae: AgentEntry = { agent: new http.Agent( { keepAlive: true, maxSockets: 1 } ), inUse: true };
    this.agents.push(ae);
    return ae.agent;
  }

  free(agent: any): void
  {
    for (let i: number = 0; i < this.agents.length; i++)
      if (this.agents[i].agent === agent)
      {
        if (! this.agents[i].inUse)
          throw 'Duplicate free of agent';
        this.agents[i].inUse = false;
        return;
      }

    throw 'Freeing unallocated agent';
  }
}
