# Arquitetura e Decisões de Engenharia — Distributed Wagering Processor

Este documento detalha as decisões arquiteturais, os mecanismos de controle de concorrência, a estratégia de idempotência, o desenho de mensageria transacional (*Inbox/Outbox*), os *trade-offs* assumidos e os resultados do experimento de carga do **Distributed Wagering Processor**.

---

## 1. Visão Geral e Estilo Arquitetural

O sistema foi projetado utilizando **Clean Architecture / Hexagonal Architecture (Ports & Adapters)** combinada com **Domain-Driven Design (DDD) Tático**, isolando completamente o núcleo financeiro de detalhes de infraestrutura (HTTP, ORM, PostgreSQL e AWS SQS).

```mermaid
flowchart TB
    subgraph Entrypoints["Camada de Entrada (Driving Adapters)"]
        HTTP["NestJS HTTP Controllers\n(Wallets / Wagering / Health)"]
        SQSConsumer["WagerTransactionConsumer\n(SQS FIFO Long Polling)"]
        Workers["Background Workers\n(OutboxDispatcher & PendingReference)"]
    end

    subgraph Application["Camada de Aplicação (Orquestração ACID)"]
        CW["CreateWalletUseCase"]
        PW["ProcessWagerTransactionUseCase"]
        RW["ReconcileWalletUseCase"]
        GL["GetWalletLedgerUseCase"]
    end

    subgraph Domain["Núcleo de Domínio Puro (Zero Dependências de Infra)"]
        Wallet["Wallet (Aggregate Root)"]
        Tx["WagerTransaction (State Machine)"]
        Ledger["WalletLedgerEntry (Immutable)"]
        Money["Money VO (decimal.js)"]
        Events["IntegrationEvent Hierarchy"]
    end

    subgraph Driven["Adaptadores de Saída (Driven Adapters)"]
        MikroORM["MikroORM v6 Repositories\n+ UnitOfWork (SELECT FOR UPDATE)"]
        SQSPub["SqsEventPublisher\n(@aws-sdk/client-sqs)"]
    end

    subgraph Infra["Infraestrutura Persistente"]
        PG[("PostgreSQL 16\nConstraints + Triggers Append-Only")]
        SQS[["AWS SQS FIFO (LocalStack)\nwager-transactions.fifo & DLQ"]]
    end

    HTTP --> CW & PW & RW & GL
    SQSConsumer --> PW
    Workers --> PW & MikroORM & SQSPub
    CW & PW & RW & GL --> Wallet & Tx & Ledger & Money & Events
    CW & PW & RW & GL --> MikroORM
    MikroORM --> PG
    SQSPub --> SQS
    SQSConsumer <--> SQS
```

---

## 2. Escolha do ORM: Por que MikroORM v6?

A Seção 1 do desafio deixa a escolha do ORM em aberto mediante justificativa. Escolhemos o **MikroORM v6 (`@mikro-orm/postgresql`)** pelas seguintes razões técnicas:

1. **Unit of Work e Transações Explícitas (`em.transactional`)**: permite agrupar a mutação da `Wallet`, a gravação de `WagerTransaction`, a inserção de `WalletLedgerEntry`, o registro de `InboxMessage` e o enfileiramento de `OutboxMessage` em uma única transação ACID, garantindo que nenhum efeito colateral escape caso ocorra falha antes do `COMMIT`.
2. **Controle Fino de Locks Pessimistas (`LockMode.PESSIMISTIC_WRITE`)**: traduz nativamente a leitura da carteira em `SELECT ... FOR UPDATE`, bloqueando exclusivamente a linha da `walletId` alvo no PostgreSQL.
3. **Acesso Direto ao QueryBuilder/Knex para Operações Avançadas**: permite executar `SELECT ... FOR UPDATE SKIP LOCKED` no worker da Outbox (evitando contenção entre múltiplas instâncias) e realizar a agregação contábil `SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END)` diretamente no motor do banco durante a reconciliação.
4. **Separação entre Entidade de Domínio e Entidade de Persistência**: diferentemente do Active Record tradicional, mantivemos modelos de domínio puros (`src/domain/models/*`) com invariantes estritas e mapeadores explícitos (`toDomain()` / `fromDomain()`) nas entidades de banco (`src/infrastructure/database/entities/*`).
5. **Migrations Versionadas e Reversíveis (`up` / `down`)**: suporte completo a DDL customizado, incluindo constraints `CHECK (balance_amount >= 0)` e triggers PL/pgSQL (`prevent_ledger_modification()`) para garantir imutabilidade do ledger no nível do SGBD.

---

## 3. Precisão Monetária e Integridade Contábil

### 3.1 Value Object `Money` (`decimal.js`)
- **Proibição Absoluta de `number` / IEEE-754**: nenhum valor monetário trafega ou é calculado como `number`, `float` ou `double`.
- O Value Object [`Money`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/domain/models/money.ts) encapsula instâncias de `Decimal` (`decimal.js`) configuradas com precisão de 20 dígitos significativos e arredondamento bancário simétrico (`ROUND_HALF_EVEN`, escala fixa de 2 casas decimais).
- **Validação de Entrada**: `Money.from()` aceita exclusivamente strings decimais canônicas (`^\d+(\.\d{1,2})?$`), rejeitando números negativos, `NaN`, `Infinity`, notação científica (`1e5`) ou mais de 2 casas decimais sem arredondamento explícito.
- **No Banco de Dados**: todas as colunas financeiras utilizam `NUMERIC(18, 2)` e retornam como `string` no driver do PostgreSQL, preservando representação exata de ponta a ponta.

### 3.2 Ledger Imutável (*Append-Only*) e Invariante Universal
- Toda alteração de saldo (`OPENING`, `BET`, `WIN`, `REFUND`, `ROLLBACK`) exige obrigatoriamente um [`WalletLedgerEntry`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/domain/models/wallet-ledger-entry.ts) contendo `direction` (`CREDIT` ou `DEBIT`), `money`, `balanceBefore` e `balanceAfter`.
- O construtor de `WalletLedgerEntry` valida matematicamente a equação de conservação antes mesmo de persistir:
  - Para `CREDIT`: $\text{balanceBefore} + \text{money} == \text{balanceAfter}$
  - Para `DEBIT`: $\text{balanceBefore} - \text{money} == \text{balanceAfter}$
- Transações `LOSS` registram o desfecho da rodada na tabela `wager_transactions` e emitem `WagerTransactionProcessed`, mas **não** geram entrada no ledger nem emitem `WalletBalanceChanged`, pois não alteram o saldo.
- **Imutabilidade no PostgreSQL**: a migration [`Migration20261027000001_InitialSchema`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/infrastructure/database/migrations/Migration20261027000001_InitialSchema.ts) instala as triggers `trg_prevent_ledger_update` e `trg_prevent_ledger_delete` na tabela `wallet_ledger_entries`. Qualquer tentativa de `UPDATE` ou `DELETE` (mesmo via conexão administrativa) dispara exceção imediata no banco.

---

## 4. Estratégia de Concorrência e Isolamento por `walletId`

### 4.1 Pessimistic Write Lock (`SELECT ... FOR UPDATE`) vs Optimistic Locking
A unidade de concorrência do sistema é a **`walletId`**. No início da transação em [`ProcessWagerTransactionUseCase`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/application/use-cases/process-wager-transaction.use-case.ts), adquirimos um lock pessimista de escrita na linha da carteira:

```sql
SELECT * FROM wallets WHERE id = $1 FOR UPDATE;
```

**Por que escolhemos `SELECT ... FOR UPDATE` como mecanismo primário em vez de apenas Optimistic Locking (`version`)?**
- Em sistemas de apostas (*iGaming*), dois fenômenos ocorrem com frequência em uma mesma carteira (*hot wallet*):
  1. **Tempestade de Retries/Idempotência**: o provedor de jogos dispara dezenas de requisições idênticas simultaneamente por timeout de rede.
  2. **Apostas Concorrentes em Múltiplas Abas/Jogos**: duas apostas de `80.00 BRL` chegam no mesmo milissegundo para um saldo de `100.00 BRL`.
- Com **Optimistic Locking puro**, 50 requisições concorrentes leriam a mesma `version = 1`; 1 venceria o commit e 49 sofreriam `OptimisticLockError`, exigindo loops de retry na aplicação e multiplicando a carga sobre o PostgreSQL (*abort storm*).
- Com **`SELECT ... FOR UPDATE` adquirido antes da leitura de idempotência/inbox**, o próprio *row lock manager* do PostgreSQL enfileira as transações concorrentes **apenas para aquela `walletId`**:
  - A 1ª requisição processa o débito (`100.00 -> 20.00`), grava a transação, o ledger e a outbox, e faz commit.
  - As demais requisições da mesma aposta que estavam aguardando o lock leem imediatamente a transação já gravada e retornam `idempotentReplay: true` em $< 1\text{ ms}$, ou (no caso de uma segunda aposta distinta de `80.00 BRL`) leem o saldo atualizado de `20.00 BRL` e rejeitam determinísticamente com `INSUFFICIENT_FUNDS`.
  - **Zero Bloqueio Cruzado**: carteiras com `walletId` distintos possuem linhas distintas na tabela `wallets` e são processadas em **paralelo verdadeiro** sem qualquer contenção entre si.

### 4.2 Defesa em Profundidade no Schema SQL
Além das invariantes de domínio e do lock pessimista, o schema PostgreSQL impõe:
- `CONSTRAINT check_wallet_balance_non_negative CHECK (balance_amount >= 0)`: torna fisicamente impossível persistir saldo negativo no banco, mesmo diante de bugs de aplicação.
- `CONSTRAINT unique_player_currency UNIQUE (player_id, currency)`: impede criação concorrente de duas carteiras para o mesmo jogador e moeda.
- `CONSTRAINT unique_wager_idempotency_key UNIQUE (idempotency_key)` e `CONSTRAINT unique_provider_external_tx UNIQUE (provider_id, external_transaction_id)`: barreira final contra duplicidade de transações.

---

## 5. Idempotência e Hash Canônico (`payloadHash`)

A idempotência é 100% persistida no PostgreSQL (nunca em cache volátil em memória) e governada pelo header obrigatório `Idempotency-Key`.

### Algoritmo do `payloadHash` ([`canonical-hash.ts`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/domain/models/canonical-hash.ts))
1. Extraímos exclusivamente os campos de negócio da operação (`providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, `money: { amount, currency }`, `referenceExternalTransactionId`). Metadados de transporte (headers HTTP, `correlationId`, `messageId` do SQS, timestamps de rede) **não** entram no cálculo.
2. O objeto passa por **canonicalização recursiva** (`canonicalizeJson`), ordenando lexicograficamente todas as chaves em todos os níveis e removendo propriedades `undefined`.
3. Calculamos o digest **SHA-256** (64 caracteres hexadecimais) sobre a string JSON canônica resultante.

| Situação | Comportamento | Status HTTP |
|---|---|---|
| Chave inédita | Processa normalmente e persiste `idempotency_key` + `payload_hash` | `200 OK` / `202 Accepted` / `422 Unprocessable Entity` |
| Mesma chave + mesmo `payloadHash` (estado terminal) | Retorna o resultado original sem reexecutar efeitos colaterais (`idempotentReplay: true`) | Mesmo status semântico da operação original |
| Mesma chave + `payloadHash` diferente | Rejeita imediatamente como tentativa de adulteração/colisão (`IdempotencyConflictError`) | `409 Conflict` |

---

## 6. Mapeamento Semântico de Status HTTP

Para que o provedor de jogos saiba exatamente quando deve **parar** (erro definitivo de negócio ou validação) e quando pode **retentar** (processamento pendente ou falha transitória), adotamos o seguinte contrato HTTP no [`WageringController`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/infrastructure/http/controllers/wagering.controller.ts) e no [`GlobalHttpExceptionFilter`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/infrastructure/http/filters/http-exception.filter.ts):

| Código HTTP | Significado no Domínio | Ação Recomendada ao Provedor |
|---|---|---|
| `200 OK` | Transação `PROCESSED` (incluindo replays idempotentes) | Sucesso definitivo (não reenviar) |
| `201 Created` | Carteira criada com sucesso (`POST /wallets`) | Sucesso definitivo |
| `202 Accepted` | Transação aceita em `PENDING_REFERENCE` (`REFUND`/`ROLLBACK` aguardando transação referenciada) | Aguardar resolução assíncrona ou consultar status depois |
| `400 Bad Request` | Payload malformado, `Idempotency-Key` ausente, `Money` inválido ou moeda incompatível (`CurrencyMismatchError`) | Erro de contrato do cliente (não reenviar sem corrigir) |
| `404 Not Found` | Carteira ou transação consultada não existe (`WalletNotFoundError`) | Verificar identificador |
| `409 Conflict` | Mesma `Idempotency-Key` com payload diferente (`IdempotencyConflictError`) ou carteira `(playerId, currency)` já existente (`WalletConflictError`) | Conflito de estado/identidade (não reenviar) |
| `422 Unprocessable Entity` | Rejeição por regra de negócio (`status: REJECTED`) com `failureCode` explícito (`INSUFFICIENT_FUNDS`, `NEGATIVE_BALANCE_PREVENTED`, `REFERENCE_ALREADY_REVERSED`, `INVALID_REFERENCE_KIND`) | Rejeição terminal de negócio (não reenviar) |
| `503 Service Unavailable` | Banco de dados ou SQS indisponíveis (`GET /health/ready`) | Falha transitória de infraestrutura (seguro retentar com backoff) |

---

## 7. Mensageria Assíncrona: SQS, Transactional Outbox, Inbox e Referências Fora de Ordem

### 7.1 Transactional Outbox (`OutboxDispatcherWorker`)
- Eventos de integração ([`WalletBalanceChanged`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/domain/events/events.ts), [`WagerTransactionProcessed`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/domain/events/events.ts), [`WagerTransactionRejected`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/domain/events/events.ts), [`WagerTransactionPendingReference`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/domain/events/events.ts)) herdam da classe abstrata [`IntegrationEvent<T>`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/domain/events/integration-event.ts) e **nunca** são publicados no broker antes do commit do banco.
- Eles são serializados na tabela `outbox_messages` dentro da mesma transação SQL que altera o saldo e o ledger.
- O [`OutboxDispatcherWorker`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/infrastructure/messaging/outbox/outbox-dispatcher.worker.ts) faz polling utilizando:
  ```sql
  SELECT * FROM outbox_messages
  WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
  ORDER BY occurred_at ASC
  LIMIT 50
  FOR UPDATE SKIP LOCKED;
  ```
- Isso permite que $N$ instâncias da aplicação publiquem lotes simultaneamente sem colisão. Se o processo morrer após o commit no banco e antes da publicação no SQS, outra instância assume o registro na próxima janela de polling. Em caso de falha no SQS, aplica *exponential backoff* ($\min(2^{\text{attempts}} \times 500\text{ms}, 60\text{s})$).

### 7.2 Consumer SQS, Persistent Inbox, DLQ e Graceful Shutdown (`SIGTERM`)
- O [`WagerTransactionConsumer`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/infrastructure/messaging/sqs/wager-transaction.consumer.ts) consome a fila FIFO `wager-transactions.fifo` reutilizando exatamente o mesmo `ProcessWagerTransactionUseCase`.
- **Deduplicação via Inbox**: cada mensagem recebida é registrada na tabela `inbox_messages` (`PRIMARY KEY (consumer_name, message_id)`) dentro da mesma transação SQL da aposta.
- **ACK Estritamente Pós-Commit**: o comando `DeleteMessage` no SQS só é invocado **após** o commit da transação no PostgreSQL. Se o worker morrer entre o commit e o ACK, o SQS reentregará a mensagem após o *Visibility Timeout*; a segunda instância detectará o registro já processado em `inbox_messages` (e a `idempotency_key` em `wager_transactions`) e confirmará o ACK sem duplicar o débito/crédito.
- **Classificação de Falhas e DLQ**:
  - *Rejeição de negócio* (`INSUFFICIENT_FUNDS`, etc.): a transação é gravada como `REJECTED`, o evento `WagerTransactionRejected` vai para a Outbox e a mensagem recebe **ACK** (não é erro de fila).
  - *Erro permanente de contrato* (JSON corrompido, `InvalidMoneyError`, `IdempotencyConflictError`) ou *excesso de tentativas* (`ApproximateReceiveCount >= maxReceiveCount`): a mensagem é encaminhada para `wager-transactions-dlq.fifo` e removida da fila principal, incrementando a métrica `dlqMessagesCount`.
  - *Erro transitório* (queda momentânea de banco/rede antes do limite de tentativas): visibilidade ajustada com backoff para retry pelo SQS.
- **Graceful Shutdown (`SIGTERM`)**: ao receber `SIGTERM`, o consumer interrompe imediatamente novas leituras (`isShuttingDown = true`), aguarda as promessas *in-flight* até `shutdownTimeoutMs` e, caso alguma mensagem ainda esteja pendente após o timeout de drenagem, executa `ChangeMessageVisibilityCommand(..., VisibilityTimeout = 0)` para devolvê-la instantaneamente a outra instância ativa.

### 7.3 Resolução de Referências Fora de Ordem (`PendingReferenceWorker`)
- Como filas distribuídas ou integrações multi-canal podem entregar um `REFUND` ou `ROLLBACK` antes da `BET`/`WIN` original (Seção 7.1), adotamos a **Opção B (Armazenamento em `PENDING_REFERENCE` + Resolução Assíncrona)**:
  1. Quando a referência ainda não existe, a transação é salva com status `PENDING_REFERENCE` e emite `WagerTransactionPendingReference` (`HTTP 202 Accepted`).
  2. O [`PendingReferenceWorker`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/infrastructure/messaging/outbox/pending-reference.worker.ts) varre periodicamente as transações em `PENDING_REFERENCE`. Assim que a transação referenciada chega e atinge estado `PROCESSED`, o worker reexecuta o caso de uso aplicando a reversão no saldo e no ledger.
  3. Se a referência nunca chegar após `maxAttempts` (default: 5) ou `ttlMs` (default: 60s), o worker transiciona a transação para `REJECTED` com `failureCode = REFERENCE_TIMEOUT` e emite `WagerTransactionRejected`, evitando que transações órfãs fiquem pendentes indefinidamente.

---

## 8. Autenticação e Identidade de Provedores (Seção 2)

Em conformidade com a Seção 2 do desafio, não implementamos cadastro de usuários ou emissão de credenciais. Em vez disso, definimos a porta [`ProviderIdentityPort`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/infrastructure/http/guards/auth.guard.ts) consumida pelo [`AuthGuard`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/src/infrastructure/http/guards/auth.guard.ts):
- Os endpoints `/health/live`, `/health/ready` e `/health/metrics` são explicitamente públicos (`path.startsWith('/health')`).
- Para os endpoints de negócio (`/wallets`, `/wagering`), o `AuthGuard` extrai `x-provider-id` e o header `Authorization: Bearer <token>`, delegando a validação ao `ProviderIdentityPort`.
- No MVP, injetamos o adaptador `NoopProviderIdentityPort`. Em ambiente de produção, basta substituir a injeção por um `OidcJwksProviderIdentityAdapter` (validando assinatura RS256/ES256 via JWKS de um IdP como **Keycloak**, **Zitadel** ou **AWS Cognito** e verificando se a claim `provider_id` do token corresponde ao `providerId` do payload) ou por validação de assinatura `X-Provider-Signature` (HMAC-SHA256).

---

## 9. Experimento de Carga (`bun run test:load`) e Análise de Desempenho

Implementamos o benchmark automatizado em [`tests/load/load-test.ts`](file:///c:/Users/otako/OneDrive/%C3%81rea%20de%20Trabalho/Projetos/teste-jungleGaming/tests/load/load-test.ts) (executável via `bun run test:load`), avaliando dois perfis contrastantes de carga com simulação realista de I/O de lock de linha (`1 ms` por aquisição de `SELECT ... FOR UPDATE`).

### 9.1 Ambiente do Experimento
- **SO / Arquitetura**: Windows 11 (`win32 10.0.26200 x64`)
- **CPU**: Intel(R) Core(TM) i7-10750H @ 2.60GHz (12 logical cores)
- **Memória RAM**: 15.87 GB
- **Runtime**: Bun `1.4.4`

### 9.2 Metodologia e Resultados Obtidos

| Métrica | Perfil A: Multi-Wallet (50 carteiras em paralelo, 30 ops/carteira) | Perfil B: Hot Wallet + Tempestade de Idempotência (500 reqs em rajadas de 50 sobre 1 única carteira) |
|---|---|---|
| **Requisições Totais** | `1.500` | `500` (100 únicas + 400 replays concorrentes) |
| **Tempo Total** | `86,05 ms` | `848,56 ms` |
| **Throughput** | **`17.432,01 req/s`** | **`589,24 req/s`** |
| **Latência p50** | `2,434 ms` | `43,249 ms` |
| **Latência p95** | `5,120 ms` | `81,541 ms` |
| **Latência p99** | `9,327 ms` | `85,173 ms` |
| **Latência Máxima** | `9,707 ms` | `86,463 ms` |
| **Conflitos / Fila de Lock** | `0` aquisições enfileiradas | `490` aquisições enfileiradas no lock da carteira |
| **Desfecho de Negócio** | `1.500 PROCESSED` | `66 PROCESSED`, `34 REJECTED` (`INSUFFICIENT_FUNDS`), `400 REPLAYS` |
| **Taxa de Erro (5xx)** | **`0,00%`** | **`0,00%`** |
| **Invariante Final (`balance == ledger`)** | **100% consistente** | **100% consistente** (`10.00 BRL` final exato) |

- **Drenagem da Outbox**: `3.017` eventos de integração despachados em `3,12 ms` com `outboxLagMs` máximo de `70 ms`.
- **Reconciliação Final**: `51/51` carteiras auditadas ao final do teste com diferença contábil zero (`0.00 BRL`) em todos os `1.417` lançamentos do ledger.

### 9.3 Análise Crítica dos Resultados e Trade-offs
1. **Escalabilidade Horizontal em Carteiras Distintas (Perfil A)**: como o lock pessimista tem granularidade estrita por `walletId`, carteiras diferentes não disputam recursos entre si. O throughput escala linearmente com o número de conexões e instâncias da aplicação (`> 17.400 req/s` em memória/I/O simulado, limitado em produção apenas pelo IOPS e pool de conexões do PostgreSQL).
2. **Comportamento sob *Hot Wallet* Extrema (Perfil B)**: quando 50 requisições atingem **a mesma carteira** no mesmo instante, a fila do lock pessimista serializa a execução (Pela Lei de Little, $L = \lambda \times W$: com 50 requisições simultâneas e $\sim 1,7\text{ ms}$ de tempo de serviço sob lock, o `p99` sobe previsivelmente para $\sim 85\text{ ms}$). Esse é um *trade-off* deliberado: preferimos uma latência de cauda de `85 ms` com **zero erros 5xx e 100% de exatidão financeira** a permitir leituras sujas (*dirty reads*) ou tempestades de *optimistic lock abort*.

---

## 10. Limitações Conhecidas e Evoluções para Produção

1. **Crescimento das Tabelas `wallet_ledger_entries`, `wager_transactions` e `outbox_messages`**:
   - *Limitação no MVP*: todas as entradas permanecem na tabela principal.
   - *Evolução*: particionamento declarativo do PostgreSQL (`PARTITION BY RANGE (created_at)`) mensal/semanal e job de expurgo/arquivamento de registros `outbox_messages` já publicados (`published_at < NOW() - INTERVAL '7 days'`) e `inbox_messages` antigos.
2. **Pool de Conexões sob Múltiplas Instâncias**:
   - *Limitação no MVP*: cada instância gerencia seu próprio pool direto no PostgreSQL.
   - *Evolução*: introdução de **PgBouncer** em modo *transaction pooling* na frente do PostgreSQL e definição de `lock_timeout` (ex.: `3000ms`) e `statement_timeout` por sessão para evitar que uma transação lenta segure conexões do pool.
3. **Observabilidade Distribuída**:
   - *Limitação no MVP*: métricas expostas via snapshot JSON em `/health/metrics` e logs estruturados via Pino.
   - *Evolução*: exportação nativa no formato Prometheus (`prom-client`) e instrumentação de *traces* distribuídos via **OpenTelemetry SDK** propagando `traceparent` nos atributos das mensagens SQS.
