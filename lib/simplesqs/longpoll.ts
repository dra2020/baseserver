import { Util } from '@dra2020/baseclient';

import * as Q from './queue';

const LongPollTimeout: number = 20000;

interface OneRequestBase
{
  q: Q.Queue,

  onFinish      : () => void,
  checkLongPoll : () => void,
  isDone        : () => boolean,
}

interface LongPollRequest
{
  deadline: Util.Deadline;
  request: OneRequestBase;
}

export class LongPoll
{
  requests: LongPollRequest[];

  constructor()
  {
    this.requests = [];
  }

  processDeadlines(): void
  {
    this.requests = this.requests.filter((r: LongPollRequest) => {
        if (r.deadline.done())
          r.request.onFinish();
        return !r.request.isDone();
      });
  }

  add(request: OneRequestBase): void
  {
    this.requests.push({ deadline: new Util.Deadline(LongPollTimeout), request });
  }

  checkQueue(q: Q.Queue): void
  {
    let nTriggered = 0;
    this.requests.forEach((r: LongPollRequest) => {
        if (r.request.q === q)
        {
          r.request.checkLongPoll();
          nTriggered++;
        }
      });

    // If I triggered any waiting longpoll request, clear out done ones
    if (nTriggered)
      this.processDeadlines();
  }
}
