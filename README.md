# Financial Platform

Monorepo Node.js/TypeScript para transferências financeiras com ledger double-entry imutável, idempotência concorrente, saldos reconstruíveis, conciliação bancária e transactional outbox.

> 5.000 TPS é uma meta mensurável, não uma capacidade afirmada pelo código. Resultados só são válidos com artefato reproduzível, ambiente e variância registrados. Os benchmarks deste projeto não alteram `fsync`, `synchronous_commit`, Kafka `acks=all` ou qualquer garantia de durabilidade.

## Garantias em uma frase

HTTP requests are idempotent by database constraint.  
Financial effects are immutable and double-entry.  
Outbox/Kafka delivery is at-least-once.  
Consumers and reconciliation runs are idempotent.  
Exactly-once end-to-end delivery is not claimed.

## Arquitetura

```mermaid
flowchart LR
    Client --> API[Transaction API]
    API -->|single DB transaction| PG[(PostgreSQL)]
    PG --> TX[financial_transactions]
    PG --> JE[journal_entries]
    PG --> LP[ledger_postings]
    PG --> BAL[account_balances]
    PG --> OB[outbox_events]
    Relay[Outbox Relay] -->|SKIP LOCKED| OB
    Relay --> Kafka[(Kafka)]
    Kafka --> Consumer[Idempotent Consumer]
    Bank[Bank Statement/API] --> Recon[Reconciliation Worker]
    Recon --> PG
    Redis[(Redis)] -. optional cache/lab .-> API
```

PostgreSQL é a fonte de verdade. A API nunca chama Kafka, Redis ou provedor bancário dentro da transação financeira. Relay, consumidor e conciliação são processos separados e escaláveis horizontalmente.

## Ciclo de uma transação financeira

`POST /v1/transactions` valida um corpo limitado a 16 KiB e uma `Idempotency-Key`. Em uma transação curta do PostgreSQL:

1. tenta reservar a chave idempotente única e persiste a resposta determinística;
2. bloqueia as contas com `FOR SHARE` e as projeções de saldo com `FOR UPDATE`, por UUID;
3. valida conta ativa, moeda e fundos;
4. cria journal e dois postings balanceados;
5. incrementa os saldos e suas versões;
6. cria um evento outbox;
7. confirma tudo junto.

Qualquer erro desfaz todos os passos. A resposta `201` só ocorre após o commit.

Request:

```http
POST /v1/transactions
Idempotency-Key: order-123-attempt-1
Content-Type: application/json

{
  "externalReference": "order-123",
  "sourceAccountId": "uuid",
  "destinationAccountId": "uuid",
  "amountMinor": 12990,
  "currency": "BRL",
  "endToEndId": "optional-strong-id",
  "providerTransactionId": "optional-provider-id"
}
```

Dinheiro é inteiro em unidades mínimas (`12990` = BRL 129,90) e limitado ao inteiro seguro na fronteira JSON; não há aritmética financeira de ponto flutuante.

## Idempotência HTTP concorrente

A canonicalização normaliza moeda, UUIDs e espaços externos, materializa opcionais como `null`, serializa campos em ordem fixa e calcula SHA-256. O insert usa a constraint única de `financial_transactions.idempotency_key` como autoridade final:

- mesma chave + mesmo hash: aguarda o vencedor concorrente e reproduz `response_status` e `response_body` persistidos;
- mesma chave + hash diferente: `409 IDEMPOTENCY_CONFLICT`;
- chave ausente ou maior que 128 caracteres: `400`;
- falha antes do commit: a reserva some no rollback, sem resposta ou efeito parcial.

O header `Idempotency-Replayed` informa replay. Um cache Redis futuro pode acelerar leitura, mas nunca decidir a idempotência. Deadlock ou falha de serialização recebe até duas novas tentativas com jitter; falhas de domínio não são repetidas.

Idempotência HTTP não implica exactly-once global. Kafka deduplica por `event_id`; import bancário, pela identidade do provedor; conciliação, pela chave do run.

## Double-entry ledger e imutabilidade

Cada journal deve ter `sum(debits) = sum(credits)` por moeda. A aplicação valida antes do insert e uma constraint trigger diferida repete a validação no commit. Triggers rejeitam `UPDATE` e `DELETE` de `journal_entries` e `ledger_postings` durante operação normal.

Contas `asset`/`expense` são debit-normal: débito aumenta, crédito reduz. Contas `liability`/`equity`/`revenue` são credit-normal: crédito aumenta, débito reduz. A conta de um cliente é passivo; uma saída é débito e reduz seu saldo.

Uma transferência de duas pernas reduz a origem e aumenta o destino: entre ativos, crédito/débito; entre passivos, débito/crédito. Contas em lados normais diferentes exigem um journal multi-leg com clearing e recebem `422 ACCOUNT_TYPE_MISMATCH` neste endpoint, pois duas pernas não conseguiriam simultaneamente produzir o efeito econômico e permanecer balanceadas.

Erros históricos não são editados. `createCompensatingEntry` cria um novo journal com as direções opostas, atualiza a projeção no mesmo commit e emite `financial.transaction.reversed.v1`. Repetir a reversão retorna o journal existente.

## Atualização, auditoria e reconstrução de saldos

`account_balances` é uma projeção/cache para o hot path; postings imutáveis são autoritativos. As linhas de conta usam lock compartilhado para estabilizar status/moeda. Apenas os saldos usam lock exclusivo e sempre na ordem do UUID. Essa separação evita o deadlock de promoção entre foreign-key `KEY SHARE` e `FOR UPDATE`.

O saldo é incrementado no mesmo commit do ledger e a versão aumenta. Reagregar todo o histórico por request seria simples, mas inviável no hot path. Auditoria e rebuild ficam fora da ingestão:

```bash
pnpm balance:audit
pnpm balance:rebuild -- --account=<uuid>       # dry-run
pnpm balance:rebuild -- --account=<uuid> --apply
pnpm balance:rebuild -- --apply                # todas as contas
```

`--apply` registra a discrepância em `balance_audit_events` antes de substituir a projeção. Nunca modifica postings.

## Conciliação com extratos bancários

A importação é idempotente por `(provider, bank_account_id, provider_entry_id)` e preserva `raw_payload` sem sobrescrever o original. O matching automático segue esta precedência:

1. end-to-end ID;
2. ID de transação do provedor;
3. referência externa;
4. valor, direção, moeda e janela temporal somente como evidência secundária.

Valor isolado nunca gera match. Ausência de identificador forte vira `manual_review`. A execução detecta `matched`, ausências dos dois lados, valor/direção divergentes, duplicatas, liquidação tardia e ambiguidade.

O run usa SHA-256 do provedor, conta, período e saldos informados. Reexecutar um run concluído reproduz seus itens e não duplica outbox nem ajustes. A conciliação valida independentemente `abertura + créditos - débitos = fechamento` no extrato e deriva os saldos internos de abertura/fechamento dos postings. Matching é leitura sem locks no hot path.

```bash
pnpm reconciliation:import -- statement.json provider bank-account-id
pnpm reconciliation:run -- reconciliation-input.json
pnpm start:reconciliation       # API operacional, porta 3003
```

Divergências ficam persistidas. Resolução financeira usa lançamento compensatório auditável; nunca apaga história.

## Transactional outbox

O outbox é inserido no mesmo commit de transação, ledger e saldo. O relay:

1. recupera locks `processing` abandonados;
2. reivindica até 500 linhas usando `FOR UPDATE SKIP LOCKED`;
3. confirma o claim;
4. publica fora da transação, agrupado por tópico, com GZIP e concorrência limitada;
5. marca `published` somente após ACK;
6. em falha, agenda backoff exponencial com jitter, limitado a 60 s.

Réplicas usam `locked_by` distinto e claims disjuntos. Idade do item mais antigo é monitorada, porque backlog pequeno e antigo ainda representa incidente.

## Kafka e consumer idempotency

O produtor é único e long-lived por relay, usa idempotência Kafka, no máximo cinco requests in-flight e `acks=all`. A chave normalmente é a conta que exige ordem. Tópicos locais usam 12 partições e replication factor 1; produção deve normalmente usar fator 3 e `min.insync.replicas` coerente.

Há um gap inevitável: Kafka pode confirmar e o relay cair antes de atualizar PostgreSQL. A recuperação republica a mensagem. Por isso a entrega é at-least-once.

O consumidor insere `(consumer_name,event_id)` e seu efeito em `processed_financial_events` na mesma transação. Duplicata é no-op. O offset só avança quando o handler retorna: falha antes do commit reprocessa; falha depois do commit é neutralizada pela chave. Tópicos:

- `financial.transaction.accepted.v1`;
- `financial.ledger.posted.v1`;
- `financial.transaction.reversed.v1`;
- `financial.reconciliation.completed.v1`;
- `financial.reconciliation.divergence-detected.v1`;
- `financial.events.dlq` (contrato reservado para poison messages).

```bash
pnpm start:relay               # métricas em :9091/metrics
pnpm start:consumer            # métricas em :9093/metrics
```

## Por que Redis Streams é opcional

PostgreSQL não pode confirmar atomicamente junto com Redis. Escrever os dois na API criaria dual-write e permitiria saldo sem mensagem ou mensagem sem saldo. Kafka é o backbone durável de integração.

O laboratório opcional lê apenas outbox PostgreSQL já publicada e a copia para um Stream. `redis_lab_deliveries` mantém checkpoint; crash entre `XADD` e checkpoint pode duplicar, logo consumer groups ainda precisam de idempotência. O lab permite comparar throughput, latência, pending entries, reclaim e complexidade operacional, sem promover Redis a fonte financeira.

```bash
docker compose up -d redis
pnpm start:redis-lab
```

A API e sua correção continuam funcionando se Redis estiver indisponível.

## Instalação e execução

Requisitos: Node.js 22+, pnpm, Docker e Docker Compose.

```bash
cp .env.example .env
pnpm install
docker compose up -d postgres kafka
pnpm db:migrate
pnpm db:seed
pnpm start:api
```

Em terminais separados, execute relay e consumer pelos comandos acima. `db:seed` cria contas contábeis e financia clientes por journals double-entry idempotentes; ele não injeta saldos diretamente.

### Variáveis de ambiente

| Variável | Padrão | Uso |
|---|---:|---|
| `DATABASE_URL` | `postgres://financial:financial@localhost:5432/financial` | PostgreSQL |
| `KAFKA_BROKERS` | `localhost:9092` | brokers separados por vírgula |
| `PORT` | `3000` | API |
| `DB_POOL_MAX` | `20` | pool por processo |
| `LOG_LEVEL` | `info` | Pino |
| `RELAY_BATCH_SIZE` | `500` | claim por ciclo |
| `RELAY_POLL_MS` | `250` | polling sem trabalho |
| `RELAY_LOCK_TIMEOUT_MS` | `30000` | recuperação de relay morto |
| `RELAY_METRICS_PORT` | `9091` | métricas do relay |
| `CONSUMER_METRICS_PORT` | `9093` | métricas do consumidor |
| `RECONCILIATION_PORT` | `3003` | worker operacional |
| `REDIS_URL` | `redis://localhost:6379` | somente lab |

`traceparent` HTTP é copiado aos headers do outbox para correlação OpenTelemetry-compatible.

## Testes

```bash
pnpm typecheck
pnpm test
pnpm test:integration
pnpm test:all
```

Integração exige PostgreSQL migrado; o teste Kafka exige o broker local real. Concorrência, locking e delivery não usam mocks. A suíte cobre rollback total, 120 duplicatas concorrentes, conflito de hash, conta quente, rebuild, import/rerun, reversão, relays concorrentes, consumer duplicado e publicação/consumo Kafka reais.

## Autocannon por 10 segundos

Com API ativa e seed aplicado:

```bash
pnpm load:10s
```

Padrões: 10 s, 200 conexões, pipelining 1 e chave/referência únicas por operação lógica. O comando salva JSON bruto em `artifacts/load-tests`, imprime requests/s, bytes/s, p50/p95/p99, errors, timeouts e non-2xx. Configuração:

Autocannon fornece p50/p97.5/p99 no agregado; o runner também captura cada `responseTime` e calcula p95 por nearest-rank, persistindo-o no mesmo JSON.

```bash
LOAD_URL=http://localhost:3000 \
LOAD_DURATION=10 LOAD_CONNECTIONS=200 LOAD_PIPELINING=1 \
LOAD_MIN_RPS=0 LOAD_MAX_P99_MS=1000 \
pnpm load:10s
```

Testes especializados:

```bash
pnpm load:idempotency           # >=100 requests, uma única chave e prova SQL
pnpm load:balance-contention    # 1.000 contas: uniforme, Zipf, hot e mistura 80/20
pnpm load:report                # resume todos os JSON persistidos
```

O teste de idempotência prova uma transação, um journal, dois postings, um outbox, mesmo ID de resposta e `409` com outro valor. O de contenção cria 500 pares independentes (1.000 contas), financia cada origem pela API e executa, por padrão, três runs de 10 s em quatro cenários: uniforme, Zipf α=1,1, uma conta quente e 80% uniforme/20% hot. Ele registra seed, média/desvio padrão de TPS, p50/p95/p99, retries, deadlocks e auditoria final; falha se houver erro HTTP, deadlock ou discrepância ledger/projeção.

```bash
CONTENTION_ACCOUNT_COUNT=1000 CONTENTION_RUNS=3 CONTENTION_DURATION=10 \
CONTENTION_CONNECTIONS=200 CONTENTION_ZIPF_ALPHA=1.1 CONTENTION_SEED=5000 \
pnpm load:balance-contention
```

## Como apagar dados de teste

Cleanup é bloqueado por padrão e sempre recusado em `NODE_ENV=production`:

```bash
ALLOW_DESTRUCTIVE_CLEANUP=true pnpm cleanup:transactions
ALLOW_DESTRUCTIVE_CLEANUP=true pnpm cleanup:transactions -- --before=2026-08-02T12:00:00Z
ALLOW_DESTRUCTIVE_CLEANUP=true pnpm cleanup:transactions -- --include-balances
ALLOW_DESTRUCTIVE_CLEANUP=true pnpm cleanup:transactions -- --include-accounts
```

O script imprime host/database, contagens e duração; remove em ordem de dependência. Contas seed são preservadas sem `--include-accounts`, e saldos sem `--include-balances`. Para manutenção não produtiva ele desabilita os dois triggers de imutabilidade somente dentro da transação de cleanup e os reabilita antes do commit; qualquer erro faz rollback inclusive do estado dos triggers.

## Observabilidade

`/metrics` da API expõe métricas Prometheus de HTTP, transações, replay/conflito, ledger, retries, discrepâncias, backlog/idade do outbox, publicação, reconciliação, Kafka, duplicatas do consumidor, pool e métricas padrão Node.js de event loop. Logs estruturados redigem `Authorization` e `Idempotency-Key`.

Além da aplicação, monitore `pg_stat_activity`, espera de locks, deadlocks, slow queries, pool, WAL bytes, checkpoints, autovacuum, `fsync` e latência do volume. Alertas devem usar idade do outbox e lag Kafka, não apenas taxa média.

## Estratégias e gargalos rumo a 5.000 TPS

### Node.js

Fastify usa validação pré-compilável, payload limitado e middleware mínimo. Não há CPU síncrona pesada ou cliente por request. Escale processos horizontalmente, com pool pequeno e medido por processo; amostre logs de sucesso e monitore utilização/delay do event loop.

### PostgreSQL

O commit financeiro é curto, SQL parametrizado e tabelas quentes têm poucos índices. Saldos são bloqueados determinísticamente; a conta quente serializa naturalmente e pode dominar p99 mesmo quando TPS distribuído é alto. Meça antes de particionar. PgBouncer ajuda excesso de conexões, não contenção de uma mesma linha. SSD/NVMe, WAL, checkpoints e autovacuum importam. Nunca desligue `fsync` ou `synchronous_commit` para alcançar o número.

### Ledger e saldos

Postings append-only escalam melhor que recalcular histórico; projeção incremental evita agregação por request. Tráfego distribuído reduz colisão. Se a conta quente for o gargalo medido, uma evolução possível é particionar comandos por conta, mantendo a mesma atomicidade — não relaxar o ledger.

### Kafka e relay

Partições suficientes, GZIP, batches e produtor long-lived aumentam throughput. `acks=all`, idempotência e in-flight limitado permanecem ativos. Escale relays pelo backlog e idade; mais réplicas deixam de ajudar quando Kafka ou PostgreSQL saturam.

### Conciliação

Importe incrementalmente e processe por conta/período em lotes. Índices de identificadores fortes evitam scans. O worker nunca deve disputar locks de saldo com ingestão.

### Limites de interpretação

Docker em notebook é evidência local, não capacidade produtiva. Média distribuída pode ocultar conta quente; uma janela única não mostra variância, warm-up ou checkpoint. Registre várias execuções e percentis, depois investigue CPU, WAL, lock waits e pool antes de mudar arquitetura.

## Resultado local registrado em 2026-08-02

Execução válida de `pnpm load:10s`, em Docker local, Node 24.13.1, PostgreSQL 17, pool 20, 200 conexões, pipelining 1, 10 segundos e uma única dupla de contas:

| requests/s | throughput | p50 | p95 | p99 | errors | non-2xx |
|---:|---:|---:|---:|---:|---:|---:|
| 237,1 | 109.779 bytes/s | 813 ms | 939,565 ms | 1.141 ms | 0 | 0 |

Artefato: `artifacts/load-tests/autocannon-10s-2026-08-02T11-54-04.898Z.json`.

O teste de contenção, com 50 conexões e 10 segundos por cenário, mediu 535,4 requests/s distribuídos (p95 134,109 ms; p99 161 ms) contra 166,4 requests/s na mesma conta quente (p95 440,003 ms; p99 513 ms), sem erros, non-2xx ou discrepâncias de saldo. Artefato: `artifacts/load-tests/balance-contention-2026-08-02T11-54-33.097Z.json`.

### Matriz com 1.000 contas

Em 2026-08-02, o teste ampliado criou 500 pares independentes — 1.000 contas — e executou três runs de 10 s por cenário, 200 conexões, pipelining 1 e seed 5000. As origens foram financiadas pela API a partir da conta de abertura.

| Distribuição | TPS médio | Desvio TPS | p50 médio | p95 médio | p99 médio | erros/non-2xx |
|---|---:|---:|---:|---:|---:|---:|
| Uniforme | 443,67 | 2,38 | 416,67 ms | 604,28 ms | 684,67 ms | 0/0 |
| Zipf α=1,1 | 446,87 | 40,20 | 398,33 ms | 631,18 ms | 799,33 ms | 0/0 |
| 80% uniforme / 20% hot | 431,40 | 57,56 | 378,33 ms | 981,11 ms | 1.335 ms | 0/0 |
| 100% hot | 189,57 | 6,60 | 961,67 ms | 1.396,70 ms | 1.514 ms | 0/0 |

O PostgreSQL registrou zero deadlocks; a aplicação registrou zero retries; a auditoria final encontrou zero discrepâncias entre ledger e projeções. Artefato bruto com todos os 12 runs: `artifacts/load-tests/account-distribution-1000-2026-08-02T12-08-52.074Z.json`.

As 1.000 contas melhoraram o paralelismo em relação à conta totalmente quente, mas não produziram 5.000 TPS. Zipf ficou próximo da média uniforme, porém com variância e p99 maiores; apenas 20% de tráfego hot já elevou fortemente p95/p99. Neste ambiente, além dos locks, WAL/fsync, custo do commit, pool e CPU precisam ser medidos para explicar o teto distribuído.

Esses números **não demonstram 5.000 TPS** e não são capacidade produtiva. São uma linha de base reproduzível de uma única execução local. Os dois primeiros artefatos de Autocannon preservam execuções inválidas, marcadas pelo `load:report` como `INVALID`, que detectaram o body dinâmico mal configurado; eles não são resultados de throughput financeiro.

## Template do ambiente de benchmark

Cada artefato automático registra parte destes campos; complete os restantes ao publicar resultados:

```text
data/hora e git commit:
CPU/modelo/núcleos:
RAM e limites Docker:
SO/kernel:
Node/pnpm:
PostgreSQL versão/configuração/volume:
fsync/synchronous_commit:
pool por processo e número de processos:
Kafka versão/brokers/partições/replicação/min ISR/acks:
payload e distribuição de contas:
conexões/pipelining/duração/warm-up:
runs, média e variância:
requests/s, throughput, p50/p95/p99, errors/non-2xx:
WAL, CPU, memória, locks, deadlocks, retries, outbox age:
```

## Falhas e recuperação

- API cai antes do commit: nenhum efeito persiste; cliente repete a mesma chave.
- API cai após commit e antes da resposta: replay retorna a resposta persistida.
- Kafka indisponível: transações continuam; outbox acumula e relay aplica backoff.
- Relay cai antes do ACK: evento é tentado novamente.
- Relay cai após ACK: evento pode duplicar; consumidor deduplica.
- Consumer cai antes do commit: offset não avança e mensagem retorna.
- Consumer cai após commit: replay encontra `consumed_events` e vira no-op.
- Worker de conciliação cai: mesmo run pode ser retomado; run concluído é replay.
- Redis cai: nenhuma garantia financeira ou Kafka é afetada; apenas o lab pausa.
- Projeção diverge: auditoria registra; rebuild explícito repara sem tocar no ledger.

## Segurança, redaction e auditoria

Use TLS/mTLS, autenticação/autorização de serviço, roles PostgreSQL de mínimo privilégio e secrets fora do repositório. Não registre corpos bancários, PAN, tokens, `Authorization` nem chaves idempotentes. Restrinja endpoints de reconciliação/reversão e registre o ator de resolução. Proteja métricas e health checks fora da rede pública. A cleanup requer flag deliberada, recusa produção e deve usar credencial de manutenção separada em ambientes compartilhados.
