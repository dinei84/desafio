import {
  Body,
  CanActivate,
  Catch,
  Controller,
  Get,
  Headers,
  HttpCode,
  HttpException,
  Inject,
  Injectable,
  Param,
  Post,
  Query,
  Res,
  UseGuards,
  type ArgumentsHost,
  type ExceptionFilter,
} from '@nestjs/common';
import type { Response } from 'express';
import { randomUUID } from 'node:crypto';

import { Processor, type Result } from '../application/processor';
import { ApplicationError } from '../application/contracts';
import { DomainError } from '../domain/money';
import { Broker } from '../infrastructure/sqs';
import { errorCode, log, registry } from '../infrastructure/observability';
import type { Status } from '../domain/model';

const MAX_CORRELATION_LENGTH = 200;

const httpStatus: Record<Status, number> = {
  PROCESSED: 200,
  PENDING: 202,
  PENDING_REFERENCE: 202,
  REJECTED: 422,
  FAILED: 500,
};

/** Extension point: replace with external OIDC provider identity verification. */
@Injectable()
export class ProviderIdentityGuard implements CanActivate {
  canActivate(): boolean {
    return true;
  }
}

@Controller()
@UseGuards(ProviderIdentityGuard)
export class ApiController {
  constructor(@Inject(Processor) private readonly processor: Processor) {}

  @Post('wallets')
  open(@Body() body: unknown) {
    return this.processor.openWallet(body, randomUUID());
  }

  @Get('wallets/:id')
  wallet(@Param('id') id: string) {
    return this.processor.wallet(id);
  }

  @Get('wallets/:id/ledger')
  ledger(
    @Param('id') id: string,
    @Query('cursor') cursor?: string,
    @Query('limit') limit?: string,
  ) {
    return this.processor.ledger(id, cursor, limit);
  }

  @Post('wallets/:id/reconciliation')
  @HttpCode(200)
  reconcile(@Param('id') id: string) {
    return this.processor.reconcile(id);
  }

  @Get('wagering/transactions/:id')
  transaction(@Param('id') id: string) {
    return this.processor.transaction(id);
  }

  @Get('providers/:provider/wagering/transactions/:external')
  external(@Param('provider') provider: string, @Param('external') external: string) {
    return this.processor.providerTransaction(provider, external);
  }

  @Post('wagering/transactions')
  async submit(
    @Body() body: unknown,
    @Headers('idempotency-key') key: string | undefined,
    @Headers('x-correlation-id') incoming: string | undefined,
    @Res({ passthrough: true }) res: Response,
  ): Promise<Result> {
    const correlationId =
      incoming && incoming.length <= MAX_CORRELATION_LENGTH ? incoming : randomUUID();
    res.setHeader('X-Correlation-Id', correlationId);

    const result = await this.processor.submit(body, key, { correlationId });
    res.status(httpStatus[result.status]);
    return result;
  }
}

@Controller()
export class HealthController {
  constructor(
    @Inject(Processor) private readonly processor: Processor,
    @Inject(Broker) private readonly broker: Broker,
  ) {}

  @Get('health/live')
  live() {
    return { status: 'ok' };
  }

  @Get('health/ready')
  async ready() {
    try {
      await Promise.all([this.processor.db.query('SELECT 1'), this.broker.ready()]);
      return { status: 'ok' };
    } catch {
      throw new ApplicationError('DEPENDENCY_UNAVAILABLE', 503);
    }
  }

  @Get('metrics')
  async metrics(@Res() res: Response) {
    res.setHeader('Content-Type', registry.contentType);
    res.send(await registry.metrics());
  }
}

@Catch()
export class ApiErrors implements ExceptionFilter {
  catch(error: unknown, host: ArgumentsHost) {
    const res = host.switchToHttp().getResponse<Response>();

    if (error instanceof ApplicationError) {
      res.status(error.httpStatus).json({ code: error.code });
      return;
    }
    if (error instanceof DomainError) {
      res.status(400).json({ code: error.code });
      return;
    }
    if (error instanceof HttpException) {
      const status = error.getStatus();
      res.status(status).json({ code: status === 404 ? 'NOT_FOUND' : 'INVALID_REQUEST' });
      return;
    }

    const code = errorCode(error);
    if (code) {
      log('request_error', { code });
      res.status(503).json({ code: 'INFRASTRUCTURE_UNAVAILABLE' });
      return;
    }

    // No driver code means our own code broke. Retrying the same request will not help.
    log('request_error', {
      code: 'UNEXPECTED_ERROR',
      error: (error as Error)?.name,
      detail: (error as Error)?.message,
    });
    res.status(500).json({ code: 'INTERNAL_ERROR' });
  }
}
