export interface SQSOptions
{
  queueName: string,
  delaySeconds?: number,
  maximumMessageSize?: number,
  messageRetentionPeriod?: number,
  receiveMessageWaitTimeSeconds?: number,
  visibilityTimeout?: number,
  autoDelete?: boolean,
}

