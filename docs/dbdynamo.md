# dbdynamo
Dynamo abstraction layer

## Table provisioning
`createCollection` makes no AWS call and assumes the table exists. Tables and their global secondary indexes
are created explicitly with `DynamoClient.provisionTables(schemas, { create })` (see `lib/dbdynamo/provision.ts`),
which the dra-cli `provision` tool runs with administrative credentials. Without `create` it only reports; with
it, it creates missing tables, adds missing indexes and waits for them to become ACTIVE. It never deletes or
otherwise changes a table. An operation on a missing table or index logs a `dynamodb: missing table` (or
`missing index`) error naming the tool. SQS queues work the same way (`SQSManager.provisionQueue`).
