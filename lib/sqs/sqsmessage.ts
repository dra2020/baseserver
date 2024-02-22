export interface SQSMessage
{
  groupId: string,
  data: any,
  messageId?: string,     // Only on receive
  receiptHandle?: string, // Only on receive
}
