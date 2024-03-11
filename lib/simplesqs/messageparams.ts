import { SQSMessage } from '../sqs/sqsmessage';

export interface MessageData
{
  queueName?: string,
  message?: SQSMessage,
}

export interface MessageParams extends MessageData
{
  command?: string,
  batch?: MessageData[]; // only applies to send
}

