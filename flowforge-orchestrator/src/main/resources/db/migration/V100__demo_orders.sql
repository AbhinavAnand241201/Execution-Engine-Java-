-- Sample data for DATABASE tasks (named queries in flowforge.executors.database.queries).
-- Deterministic (no random()) so demos and tests always see the same 500 rows.
CREATE SCHEMA demo;

CREATE TABLE demo.orders (
  id         INT PRIMARY KEY,
  customer   VARCHAR(50) NOT NULL,
  amount     NUMERIC(10,2) NOT NULL,
  status     VARCHAR(20) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

INSERT INTO demo.orders (id, customer, amount, status, created_at)
SELECT g,
       'customer-' || (g % 25 + 1),
       ((g * 37) % 500 + 5) + (g % 100) / 100.0,
       (ARRAY['NEW', 'PAID', 'SHIPPED', 'CANCELLED'])[g % 4 + 1],
       TIMESTAMPTZ '2026-01-01 00:00:00+00' + (g * INTERVAL '17 hours')
FROM generate_series(1, 500) AS g;

CREATE INDEX ix_demo_orders_status ON demo.orders(status);
CREATE INDEX ix_demo_orders_created_at ON demo.orders(created_at);
