# Distributed Wagering Processor — implementação do desafio

Implementação do [desafio original](docs/CHALLENGE.md) com Bun, TypeScript estrito, NestJS, TypeORM, PostgreSQL e SQS via LocalStack. Inclui três instâncias, ledger imutável, idempotência persistente, inbox, outbox e recuperação de referências fora de ordem.

As decisões, garantias e limitações estão em [ARCHITECTURE.md](ARCHITECTURE.md). Autenticação foi deliberadamente omitida, conforme permitido pelo enunciado; os endpoints de negócio estão abertos para demonstração.

## Executar com Docker

Pré-requisitos: Docker Engine e Docker Compose v2; portas 3000–3002, 5432 e 4566 livres.

```bash
docker compose up --build -d
```

Se alguma porta HTTP estiver ocupada, use `HTTP_PORT=3100 HTTP_PORT_2=3101 HTTP_PORT_3=3102 docker compose up --build -d`.

O serviço `setup` executa migrations e cria as três filas antes de iniciar as APIs. PostgreSQL tem volume persistente. As APIs ficam disponíveis em `http://localhost:3000`, `:3001` e `:3002`; todas executam consumidores, publishers e o worker de referências.

```bash
curl http://localhost:3000/health/ready
docker compose logs -f api api2 api3
```

Aguarde `/health/ready` retornar HTTP 200 antes de executar a demonstração.

Demonstração executável: cria uma wallet com `100.00 BRL`, envia uma aposta de `80.00` **50 vezes em paralelo**, verifica um único débito e imprime ledger e reconciliação. O saldo final deve ser `20.00`, com 49 replays.

```bash
docker compose exec api bun run demo
```

Parar preservando dados:

```bash
docker compose down
```

## Desenvolvimento local

Requer Bun 1.x; validado com Bun 1.3.14.

```bash
cp .env.example .env
bun install --frozen-lockfile
docker compose up -d postgres localstack
bun run migration:run
bun run queues:init
bun run dev
```

Bun lê `.env` automaticamente. As configurações do Compose usam os nomes internos dos serviços e não dependem do arquivo local.

## Testes

```bash
bun run typecheck
bun test
```

`bun test` executa unidades e integração. Para executar separadamente:

```bash
bun run test
bun run test:integration
```

A integração exige PostgreSQL e LocalStack acessíveis nas URLs de `.env.example`. Cria um **banco temporário**, filas com prefixo exclusivo e **três processos HTTP reais**, removendo esses recursos no encerramento. O usuário de teste precisa de `CREATEDB`; os dados do banco principal não são apagados. Não usa mocks de PostgreSQL ou SQS.

Para executar tudo em containers:

```bash
docker compose --profile test run --build --rm tests
```

Cobertura dos cenários:

- dinheiro exato, validação sem arredondamento, moedas, wallet e transições terminais;
- payload dos eventos de integração, incluindo identidade e `walletVersion`;
- migrations `up → down → up`, unicidade, saldo não negativo e imutabilidade no banco;
- 50 reenvios paralelos distribuídos entre três processos, com um único débito;
- chaves idempotentes iguais em provedores diferentes, sem conflito entre eles;
- duas apostas de `80.00` sobre saldo de `100.00`, com exatamente uma rejeição;
- wallets independentes avançam mesmo enquanto outra wallet está bloqueada;
- operações, reversões, saldo insuficiente na reversão e referências inválidas;
- uma única reversão por referência, por REFUND ou ROLLBACK, também no schema;
- rollback atômico de wallet, transação, ledger, inbox e outbox mediante falha injetada;
- duplicação de inbox, publishers concorrentes, retry de publicação e recuperação;
- `SIGKILL` após commit e antes do ack, redelivery real e deduplicação persistente;
- wallet inexistente reentregue em vez de descartada na DLQ;
- retry transitório, limite de tentativas, DLQ, reinício HTTP e paginação;
- consumidor e agendador ativos resolvendo reversão recebida antes da aposta.

Todos os cenários financeiros verificam reconciliação com o ledger. O teste de crash usa um subprocesso exclusivo e uma falha habilitada apenas com `NODE_ENV=test`.

## API

Criar wallet:

```bash
curl -s http://localhost:3000/wallets \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"100.00","currency":"BRL"}}'
```

Substitua `WALLET_ID` pelo `id` retornado:

```bash
curl -i http://localhost:3000/wagering/transactions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:bet-1' \
  -d '{"providerId":"provider-a","externalTransactionId":"bet-1","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"WALLET_ID","roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

| Método | Endpoint | Finalidade |
|---|---|---|
| POST | `/wallets` | Criar wallet e lançamento de abertura |
| GET | `/wallets/:id` | Saldo e versão |
| GET | `/wallets/:id/ledger?limit=50&cursor=...` | Ledger por cursor opaco |
| POST | `/wallets/:id/reconciliation` | Comparar saldo materializado com ledger |
| POST | `/wagering/transactions` | Processar operação; exige `Idempotency-Key` |
| GET | `/wagering/transactions/:id` | Consultar resultado persistido |
| GET | `/providers/:provider/wagering/transactions/:external` | Consultar identidade externa |
| GET | `/health/live` | Processo vivo |
| GET | `/health/ready` | PostgreSQL e filas alcançáveis |
| GET | `/metrics` | Métricas Prometheus |

Valores monetários são strings decimais. `OPENING` é exclusivamente interno. Para `REFUND` e `ROLLBACK`, envie `referenceExternalTransactionId`. Consulte a transação pendente para acompanhar sua resolução; reenviar não acelera nem reinicia o prazo do worker.

A `Idempotency-Key` é única dentro do provider. Dois provedores podem usar a mesma string sem colidir, e o default recomendado continua `{providerId}:{externalTransactionId}`.

| HTTP na submissão | Significado |
|---|---|
| 200 | Processada ou replay de uma operação processada |
| 202 | Persistida, aguardando referência |
| 400 | Contrato inválido, dinheiro inválido ou referência obrigatória ausente |
| 404 | Wallet inexistente |
| 409 | Wallet duplicada, conflito de chave ou identidade externa |
| 422 | Rejeição de negócio persistida, com `failureCode` |
| 500 | Defeito do serviço; reenviar a mesma requisição não resolve |
| 503 | Dependência indisponível; reenviar com a mesma chave |

Consultas retornam 200 mesmo ao consultar um resultado rejeitado. O corpo preserva seu status de domínio e saldo histórico. `idempotentReplay` identifica a submissão repetida; rejeições também são idempotentes.

## SQS

Filas: `wager-transactions.fifo`, `wager-transactions-dlq.fifo` e `wager-events.fifo` (destino da outbox). Use `MessageGroupId=walletId` e um `MessageDeduplicationId` único por envio; deduplicação do broker não substitui o banco.

O corpo de entrada segue o envelope do [enunciado](docs/CHALLENGE.md#10-processamento-por-sqs). O campo `data.idempotencyKey` é obrigatório e cumpre a mesma função do header HTTP. `messageId` identifica a inbox; `occurredAt` deve ser uma data válida. Há um exemplo de produtor em `scripts/demo.ts` para HTTP e helpers SQS em `src/infrastructure/sqs.ts`.

Mensagens de negócio rejeitadas são confirmadas depois do commit. Contratos inválidos e conflitos vão para a DLQ; indisponibilidade e wallet ainda inexistente causam retry com backoff. A DLQ contém o corpo original e o código de falha; seu acesso deve ser restrito. Um redrive nativo do SQS pode preservar diretamente o envelope original.

Os eventos da outbox saem em lotes de até 10 por `SendMessageBatch`, com `MessageGroupId` igual ao id da wallet, de modo que o publisher acompanhe o caminho de escrita.

## Migrations e estrutura

```bash
bun run migration:run
bun run migration:revert
```

O revert remove as tabelas desta solução; use apenas em um banco descartável ou após backup. Não é executado automaticamente no encerramento.

```text
src/domain/          Money, agregados, ledger, eventos de integração, inbox e outbox
src/application/     validação de contratos e caso de uso financeiro
src/infrastructure/  TypeORM, migrations, SQS, workers, logs e métricas
src/http/            controllers, erros HTTP e extensão de autenticação
scripts/             inicialização de filas, demonstração e crash de teste
test/                unidades e integração real
```
