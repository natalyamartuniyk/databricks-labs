-- Lab 10 — source database setup
-- Run in Neon SQL Editor (database: neondb, branch: production), NOT in Databricks.
-- Creates the operational "shop" database used as the external source
-- for Lakehouse Federation (Part A) and CDC (Part B).

-- Schema and customers (1,000 deterministic rows)
CREATE SCHEMA IF NOT EXISTS sales;

CREATE TABLE sales.customers (
    customer_id  INT PRIMARY KEY,
    full_name    VARCHAR(100) NOT NULL,
    email        VARCHAR(150) NOT NULL UNIQUE,
    city         VARCHAR(50),
    segment      VARCHAR(20),
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO sales.customers (customer_id, full_name, email, city, segment)
SELECT
    i,
    'Customer ' || i,
    'customer' || i || '@example.com',
    (ARRAY['Kyiv', 'Lviv', 'Lutsk', 'Odesa', 'Kharkiv'])[(i % 5) + 1],
    (ARRAY['regular', 'silver', 'gold'])[(i % 3) + 1]
FROM generate_series(1, 1000) AS i;

-- Orders (200,000 random rows) + indexes for pushed-down filters
CREATE TABLE sales.orders (
    order_id     INT PRIMARY KEY,
    customer_id  INT NOT NULL REFERENCES sales.customers (customer_id),
    order_date   DATE NOT NULL,
    amount       NUMERIC(10, 2) NOT NULL,
    status       VARCHAR(20) NOT NULL
);

INSERT INTO sales.orders (order_id, customer_id, order_date, amount, status)
SELECT
    i,
    1 + floor(random() * 1000)::INT,
    DATE '2025-01-01' + floor(random() * 365)::INT,
    round((5 + random() * 495)::NUMERIC, 2),
    (ARRAY['new', 'paid', 'shipped', 'cancelled'])[1 + floor(random() * 4)::INT]
FROM generate_series(1, 200000) AS i;

CREATE INDEX idx_orders_customer_id ON sales.orders (customer_id);
CREATE INDEX idx_orders_order_date  ON sales.orders (order_date);

-- Verification: expected 1000 and 200000
SELECT 'customers' AS table_name, count(*) FROM sales.customers
UNION ALL
SELECT 'orders', count(*) FROM sales.orders;