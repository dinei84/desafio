import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';

import { database } from './infrastructure/database';
import { Broker, Workers } from './infrastructure/sqs';
import { Processor } from './application/processor';
import { ApiController, ApiErrors, HealthController, ProviderIdentityGuard } from './http/api';
import { log } from './infrastructure/observability';

const port = Number(process.env.PORT ?? 3000);

const db = await database().initialize();
const broker = new Broker();
await broker.connect();
const processor = new Processor(db);

@Module({
  controllers: [ApiController, HealthController],
  providers: [
    ProviderIdentityGuard,
    { provide: Processor, useValue: processor },
    { provide: Broker, useValue: broker },
  ],
})
class AppModule {}

const app = await NestFactory.create(AppModule, { logger: false });
app.useGlobalFilters(new ApiErrors());
await app.listen(port, '0.0.0.0');

const workers = new Workers(db, processor, broker);
if (process.env.WORKERS_ENABLED !== 'false') workers.start();
log('application_started', { port });

let stopping = false;

async function stop() {
  if (stopping) return;
  stopping = true;
  log('shutdown_started');

  await Promise.all([app.close(), workers.stop()]);
  await db.destroy();
  broker.client.destroy();
  log('shutdown_complete');
}

process.on('SIGTERM', () => void stop());
process.on('SIGINT', () => void stop());
