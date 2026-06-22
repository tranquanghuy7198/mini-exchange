-- Portfolio Service schema (Task 5).
-- Owns: users, portfolios, holdings, orders.
-- NUMERIC(38,10) maps to rust_decimal::Decimal (exact, no float drift).

CREATE TABLE IF NOT EXISTS users (
    id          UUID PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS portfolios (
    user_id       UUID PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
    cash_balance  NUMERIC(38, 10) NOT NULL DEFAULT 0 CHECK (cash_balance >= 0),
    -- Cash set aside for in-flight BUY orders (saga reservation, Task 9).
    reserved_cash NUMERIC(38, 10) NOT NULL DEFAULT 0 CHECK (reserved_cash >= 0),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS holdings (
    user_id   UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    symbol    TEXT NOT NULL,
    quantity  NUMERIC(38, 10) NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    PRIMARY KEY (user_id, symbol)
);

CREATE TABLE IF NOT EXISTS orders (
    id          UUID PRIMARY KEY,
    user_id     UUID NOT NULL REFERENCES users (id),
    symbol      TEXT NOT NULL,
    side        TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity    NUMERIC(38, 10) NOT NULL CHECK (quantity > 0),
    status      TEXT NOT NULL CHECK (status IN ('CREATED', 'PRICED', 'EXECUTED', 'REJECTED')),
    price       NUMERIC(38, 10),
    reason      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders (user_id);
