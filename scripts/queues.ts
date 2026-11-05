import { Broker } from '../src/infrastructure/sqs';

const broker = new Broker();

try {
  await broker.initialize();
  console.log(JSON.stringify(broker.urls, null, 2));
} finally {
  broker.client.destroy();
}
