// Integration-only subprocess: killed on purpose after the SQL commit, before the SQS acknowledgement.
import { ReceiveMessageCommand } from '@aws-sdk/client-sqs';
import { database } from '../src/infrastructure/database';
import { Broker, Workers } from '../src/infrastructure/sqs';
import { Processor } from '../src/application/processor';

if (process.env.NODE_ENV !== 'test') throw new Error('TEST_ONLY');

const db = await database().initialize();
const broker = new Broker();
await broker.connect();

const received = await broker.client.send(
  new ReceiveMessageCommand({
    QueueUrl: broker.urls.input,
    MaxNumberOfMessages: 1,
    VisibilityTimeout: 1,
    WaitTimeSeconds: 2,
  }),
);
if (!received.Messages?.length) throw new Error('NO_TEST_MESSAGE');

await new Workers(db, new Processor(db), broker).handle(received.Messages[0]!);

await db.destroy();
broker.client.destroy();
