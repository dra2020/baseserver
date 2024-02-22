import { SQSMessage } from '../sqs/sqsmessage';

export interface MessageParams
{
  queueName?: string,
  command?: string,
  message?: SQSMessage,
}

