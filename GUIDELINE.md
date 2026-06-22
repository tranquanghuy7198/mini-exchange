# Running & Testing the Mini Exchange Portfolio System

This guide walks through bringing up the whole system locally and exercising
every scenario from the README via `curl`.

The system is **3 microservices** that talk **only over Kafka** (SAGA
choreography), backed by Postgres:

```
            POST /orders                Kafka topics                     REST reads
client ───────────────▶ Portfolio ──OrderCreated──▶ Market ──PriceQuoted──▶ Portfolio
                          │  ▲                          │                      │ (executes/rejects)
                          │  └──────OrderRejected───────┘                      ├─OrderExecuted─▶ Audit
                          └────────────── all saga events ───────────────────▶ Audit
```

| Service   | Port | Responsibility                                   | Database      |
| --------- | ---- | ------------------------------------------------ | ------------- |
| Market    | 8081 | symbols/prices (mocked); prices orders via Kafka | — (stateless) |
| Portfolio | 8082 | portfolio/orders REST + the order saga           | `portfolio`   |
| Audit     | 8083 | records every saga event                         | `audit`       |

---

## Prerequisites

- **Docker** (with `docker compose`)
- **Rust** toolchain (pinned to 1.95 via `rust-toolchain.toml`)
- `curl` (and optionally `jq` for prettier output)

All commands below are run from the **repo root**. Connection settings live in
`.env` (already present; copy from `.env.example` if missing):

```bash
cp .env.example .env   # only if .env is missing
```

---

## Step 1 — Start the infrastructure

Brings up Postgres + Kafka (KRaft), creates the per-service databases and Kafka
topics, and starts the inspection UIs. Waits until everything is healthy.

```bash
make infra-up
```

Optional dashboards:

- **Kafka UI** — http://localhost:8080 (browse topics/messages)
- **Adminer** — http://localhost:8085 (browse Postgres; server `postgres`, user/pass/db `exchange`)

> Ports 8081/8082/8083 are reserved for the services, 8080 for Kafka UI, so
> Adminer is on **8085** to avoid colliding with the Market Service (8081).

Check status anytime:

```bash
make ps
```

---

## Step 2 — Build

```bash
make build
```

(The first build compiles the bundled `librdkafka`, so it takes a couple of
minutes; subsequent builds are fast.)

---

## Step 3 — Run the services

The services are decoupled via Kafka, so start order doesn't strictly matter,
but the natural order is **Market → Portfolio → Audit**. Use **three terminals**
(each command runs in the foreground and logs to stdout):

```bash
# terminal 1
make market

# terminal 2
make portfolio

# terminal 3
make audit
```

Stop a service with `Ctrl-C` (it shuts down gracefully).

### Demo data (seeding)

**Seeding is automatic.** On startup, the Portfolio Service runs its migrations
and seeds a demo user — so as soon as `make portfolio` is up you have:

| Field          | Value                                  |
| -------------- | -------------------------------------- |
| `user_id`      | `11111111-1111-1111-1111-111111111111` |
| `cash_balance` | `100000.00`                            |
| holdings       | `1.0 BTC`                              |

Listed tradable symbols (from the Market Service): **BTC, ETH, SOL, ADA, DOGE**.

> Want to top up cash or add holdings for your own tests? Use Adminer, or psql:
>
> ```bash
> make psql-portfolio
> -- then, inside psql:
> UPDATE portfolios SET cash_balance = 1000000 WHERE user_id = '11111111-1111-1111-1111-111111111111';
> INSERT INTO holdings (user_id, symbol, quantity) VALUES
>   ('11111111-1111-1111-1111-111111111111','SOL','10')
>   ON CONFLICT (user_id, symbol) DO UPDATE SET quantity = EXCLUDED.quantity;
> ```

---

## Step 4 — Exercise the API with curl

> **These scenarios assume the pristine demo state** (`100000.00` cash + `1.0 BTC`).
> The `portfolio` database **persists across runs**, so once you've placed orders
> the balances change. To return to the clean starting state at any time:
>
> ```bash
> make infra-reset && make infra-up    # wipes all data; demo is reseeded when you restart `make portfolio`
> ```
>
> Alternatively, check the live balances with `GET /portfolio/$DEMO` (§4.1) and
> adjust the quantities below to what you actually hold.

Set a couple of shell variables first:

```bash
DEMO=11111111-1111-1111-1111-111111111111
MARKET=http://localhost:8081
PORT=http://localhost:8082
AUDIT=http://localhost:8083
```

Orders execute **asynchronously** (the result flows back over Kafka), so the
pattern is: `POST /orders` → get an `order_id` → poll `GET /orders/{id}` until
`status` is `EXECUTED` or `REJECTED`. This helper does that:

```bash
# place SIDE SYMBOL QTY  -> prints the final order JSON
place() {
  oid=$(curl -s -X POST "$PORT/orders" -H 'content-type: application/json' \
        -d "{\"user_id\":\"$DEMO\",\"symbol\":\"$2\",\"side\":\"$1\",\"quantity\":\"$3\"}" \
        | sed -n 's/.*"order_id":"\([^"]*\)".*/\1/p')
  for _ in $(seq 1 40); do
    s=$(curl -s "$PORT/orders/$oid")
    echo "$s" | grep -qE '"status":"(EXECUTED|REJECTED)"' && break
    sleep 0.25
  done
  echo "$s"
}
```

### 4.0 Market Service — symbols & prices

```bash
curl -s $MARKET/symbols                 # ["ADA","BTC","DOGE","ETH","SOL"]
curl -s $MARKET/prices                  # all current prices
curl -s $MARKET/prices/BTC              # {"symbol":"BTC","price":"...","as_of":"..."}
curl -s -i $MARKET/prices/NOPE          # 404 unknown symbol
```

### 4.1 Inspect the demo portfolio

```bash
curl -s $PORT/portfolio/$DEMO
# {"user_id":"...","cash_balance":"100000.0000000000","holdings":[{"symbol":"BTC","quantity":"1.0000000000"}]}
```

### 4.2 Successful BUY ✅ (README: "Successful BUY")

```bash
place BUY ETH 2
# ... "status":"EXECUTED", "price":"<quoted>" ...
curl -s $PORT/portfolio/$DEMO          # cash reduced by 2 x price, ETH holding +2
```

### 4.3 Successful SELL ✅ (README: "Successful SELL")

Sell **no more than you currently hold** — on the pristine demo state that's
`1.0 BTC`, so `0.5` works. (If you've already traded, check §4.1 first; selling
more than you hold lands in §4.5 instead.)

```bash
place SELL BTC 0.5 # replace 0.5 here with the amount which is less than your current BTC balance
# ... "status":"EXECUTED" ...
curl -s $PORT/portfolio/$DEMO          # cash increased, BTC holding -0.5
```

### 4.4 Insufficient balance ✅ (README: "Insufficient balance scenarios")

```bash
place BUY BTC 100000
# ... "status":"REJECTED", "reason":"insufficient balance: need ..., have ..." ...
curl -s $PORT/portfolio/$DEMO          # UNCHANGED (compensation: nothing deducted)
```

### 4.5 Insufficient asset (SELL more than held)

```bash
place SELL ADA 5
# ... "status":"REJECTED", "reason":"insufficient asset: need 5 ADA, have 0" ...
```

### 4.6 Market service failure handling ✅ (README: "Market service failure handling")

Two flavours:

**(a) Unknown symbol** — the Market Service rejects symbols it doesn't list:

```bash
place BUY ZZZ 1
# ... "status":"REJECTED", "reason":"unknown symbol ZZZ" ...
```

**(b) Market unavailable** — restart the Market Service with a symbol forced to
fail, then order it. Stop `make market` (Ctrl-C) and instead run:

```bash
make market-fail        # DOGE now reports "market unavailable"
```

```bash
place BUY DOGE 1
# ... "status":"REJECTED", "reason":"market unavailable for DOGE" ...
curl -s -i $MARKET/prices/DOGE         # 503 over REST too
```

(Restore normal behaviour by stopping `make market-fail` and running `make market` again.)

### 4.7 Correct portfolio updates after trades ✅ (README requirement)

Snapshot before/after to confirm balances move exactly as expected:

```bash
curl -s $PORT/portfolio/$DEMO          # BEFORE
place BUY SOL 3                        # EXECUTED at price P
curl -s $PORT/portfolio/$DEMO          # AFTER: cash -= 3*P, SOL += 3
```

### 4.8 Audit trail (full lifecycle)

Every order's events are recorded by the Audit Service:

```bash
# place an order and capture its id
oid=$(curl -s -X POST "$PORT/orders" -H 'content-type: application/json' \
      -d "{\"user_id\":\"$DEMO\",\"symbol\":\"ETH\",\"side\":\"BUY\",\"quantity\":\"1\"}" \
      | sed -n 's/.*"order_id":"\([^"]*\)".*/\1/p')
sleep 1
curl -s $AUDIT/audit/orders/$oid
# [ORDER_CREATED, PRICE_QUOTED, ORDER_EXECUTED]  (or ORDER_CREATED + ORDER_REJECTED)
```

### Health / readiness

```bash
curl -s $MARKET/health      # {"status":"ok","service":"market-service"}
curl -s $PORT/health        # liveness
curl -s $PORT/ready         # {"status":"ready"} when DB reachable
curl -s $AUDIT/ready
```

---

## Automated tests

```bash
make test       # unit tests — no infrastructure needed
make test-it    # portfolio saga integration tests (starts postgres+db-init)
make e2e        # full end-to-end choreography smoke test (boots everything)
```

`make e2e` is the quickest way to confirm the entire system works — it boots the
infra, starts all three services, and asserts every scenario above (incl. market
unavailable) plus the audit trail.

---

## Resetting & teardown

```bash
make infra-down     # stop containers, keep data
make infra-reset    # stop containers and DELETE all data (fresh demo state next run)
```

Because the demo user's balance persists across runs, use `make infra-reset`
whenever you want to start from the pristine `100000 cash + 1.0 BTC` state.

---

## Troubleshooting

| Symptom                                    | Fix                                                                                                                 |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| `address already in use` on 8081/8082/8083 | A previous service is still running: `lsof -ti tcp:8082 \| xargs kill`.                                             |
| Service can't reach Kafka/Postgres         | Ensure `make infra-up` finished healthy (`make ps`); services read `localhost:9094` / `localhost:5432` from `.env`. |
| Orders stay `CREATED` forever              | The Market Service isn't running (no one to price them) — start `make market`.                                      |
| Want to watch events flow                  | Open Kafka UI at http://localhost:8080 and inspect the `order-*` / `price-quoted` topics.                           |
| Weird balances                             | `make infra-reset` to wipe and reseed the demo user.                                                                |
