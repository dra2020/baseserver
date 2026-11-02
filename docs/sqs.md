# sqs

Inter-server messaging for the dra server (`dra/src/mysqsmanager.ts`) runs on one of three interchangeable
`SQSManagerBase` implementations, chosen by the server's `SQS_URL` setting:

| `SQS_URL` | Manager | Use |
|---|---|---|
| unset or `loopback` | `SimpleSQS.LoopbackSQSManager` | in-process; a single server (and local development) |
| a URL | `SimpleSQS.SimpleSQSManager` | the private simplesqs server; what production uses (cheaper than AWS SQS and sufficient) |
| `sqs` | `SQS.SQSManager` | AWS SQS FIFO queues; not currently used |

## Queue names

Each server set is identified by its `SQS_QUEUE_NAME` label (e.g. `dra_green`). Every live server receives on its
own queue `<label>-<n>`, where `n` is its 1-based position in the consensus live list, so a set of up to N servers
uses `<label>-1` ... `<label>-N`. With AWS SQS each becomes the FIFO queue `dra_prod_<label>-<n>.fifo` (or
`dra_dev_...` when not production).

## Turning on AWS SQS

The SQS manager never creates queues (that would require every server to hold `sqs:CreateQueue`). A missing queue
is logged as `sqs: missing queue` (at most every ten minutes per queue) and the lookup is retried on the next send.
So provision first, then switch:

1. Find the most instances the environment can run (Elastic Beanstalk auto scaling maximum), N.
2. Check, then create, the queues with the dra-cli provision tool (administrative credentials):

       node dist/dra-provision.bundle.js queues --prod -l <SQS_QUEUE_NAME> -n <N>
       node dist/dra-provision.bundle.js queues --prod -l <SQS_QUEUE_NAME> -n <N> --create

   It reports each queue as `ok`, `missing`, `created` or `mismatch` (exists but is not FIFO), and exits 1 if
   anything is missing or wrong. Use `-d` instead of `--prod` for the dev queues, and `-q <names>` for queues
   outside a label set.
3. Give the server's IAM identity `sqs:GetQueueUrl`, `sqs:SendMessage`, `sqs:ReceiveMessage`
   and `sqs:DeleteMessage` (the only calls the runtime makes) on `arn:aws:sqs:us-west-2:<account>:dra_prod_<label>-*.fifo`.
4. Set `SQS_URL=sqs` on the environment and deploy. Check the log for `mysqsmanager: starting in sqsmode sqs` and
   for no `sqs: missing queue` errors.
5. If the auto scaling maximum is raised later, provision the extra queues first; a server whose queue is missing
   cannot receive messages.

The old `dra_prod_dra_green-*`, `dra_dev_*` and `dra_prod_play` queues are leftovers from earlier use.
