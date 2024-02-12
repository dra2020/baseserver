// Project libraries
import { Util } from '@dra2020/baseclient';
import { SQSMessage } from '../sqs/sqsmessage';

// Local libraries
export const DefaultPort: number = (process.env['SIMPLESQS_PORT'] ? Number(process.env['SIMPLESQS_PORT']) : 0) || 80;
export const DefaultServerUrl: string = `http://localhost:${DefaultPort}`;

const MessageDeadTimeout = 1000 * 30;
const QueueDeadTimeout = 1000 * 60 * 5;

export interface QMessage
{
  deadline?: Util.Deadline,
  message: SQSMessage,
}

export class Queue
{
  queueName: string;
  deadline: Util.Deadline;
  messages: QMessage[];
  nTotal: number;
  nHighwater: number;
  nStale: number;

  constructor(queueName: string)
  {
    this.queueName = queueName;
    this.deadline = new Util.Deadline(QueueDeadTimeout);
    this.messages = [];
    this.nTotal = 0;
    this.nHighwater = 0;
    this.nStale = 0;
  }

  get length(): number { return this.messages.length }

  isdead(): boolean
  {
    return this.deadline.done();
  }

  clean(): void
  {
    let n = this.length;
    this.messages = this.messages.filter(m => { return !m.deadline.done() });
    this.nStale += n - this.length;
  }

  receive(): SQSMessage[]
  {
    let messages = this.messages;
    this.messages = [];
    return messages.map(m => m.message);
  }

  send(message: SQSMessage): void
  {
    this.deadline.start();
    let deadline = new Util.Deadline(MessageDeadTimeout);
    this.messages.push({ message, deadline });
    this.nTotal++;
    this.nHighwater = Math.max(this.nHighwater, this.length);
  }

  report(): void
  {
    console.log(`queue ${this.queueName}: ${this.nTotal} total, ${this.nHighwater} highwater, ${this.nStale} stale, ${this.length} current`);
  }
}

export class Queues
{
  queues: Map<string, Queue>;

  constructor()
  {
    this.queues = new Map<string, Queue>();
    this.clean = this.clean.bind(this);
    setInterval(this.clean, 2000);
  }

  queueOf(queueName: string): Queue
  {
    let q = this.queues.get(queueName);
    if (!q)
    {
      q = new Queue(queueName);
      this.queues.set(queueName, q);
    }
    return q;
  }

  clean(): void
  {
    this.queues.forEach((q: Queue) => {
        if (q.isdead())
        {
          this.queues.delete(q.queueName);
          console.log(`simplesqs.server: cleaning stale queue ${q.queueName} with ${q.length} unreceived messages`);
        }
        else
          q.clean();
      });
  }

  report(): void
  {
    this.queues.forEach((q: Queue) => { q.report() });
  }
}
