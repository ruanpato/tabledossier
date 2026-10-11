-- TableDossier synthetic demo tables for PostgreSQL (the twin of examples/demo/create_demo_tables.sql).
--
-- Every value is generated from integer sequences: no real people, companies or production data.
-- The planted cases are the same as in the Spark demo (NaN and infinite scores, all-null and
-- constant columns, special column names, five orphan orders, one amount outlier, duplicated event
-- ids, broken and truncated JSON payloads, empty arrays and null elements). Values that the Spark
-- demo derives from Spark's hash() come from deterministic arithmetic instead (h1, h2, ...), so
-- the two demos hold different rows with the same shape. Order ids are md5-based in both.
--
-- Nested values (structs, maps, arrays of structs) are jsonb here; tags is a text[] array.
-- PostgreSQL enforces keys: the foreign key from orders to customers is added NOT VALID because
-- the five planted orphan orders already exist.
--
-- Run as a role that may create the schema, for example: psql -f create_demo_tables.sql
-- The script DROPS AND RECREATES the schema analytics.

DROP SCHEMA IF EXISTS analytics CASCADE;
CREATE SCHEMA analytics;

CREATE TABLE analytics.customers (
  customer_id bigint NOT NULL,
  email text,
  segment text,
  signup_date date,
  is_active boolean,
  currency text,
  lifetime_value numeric(12, 2),
  score double precision,
  middle_name text,
  "display name" text,
  "a.b" text,
  referrer_id integer,
  CONSTRAINT customers_pk PRIMARY KEY (customer_id)
);

-- Planted for the relationship hypothesis demo: referrer_id holds customer ids of other customers
-- for one customer in ten.
INSERT INTO analytics.customers
SELECT
  id + 1,
  CASE WHEN h1 % 10 = 0 THEN NULL ELSE 'user' || (id + 1) || '@example.com' END,
  CASE h2 % 3 WHEN 0 THEN 'retail' WHEN 1 THEN 'smb' ELSE 'enterprise' END,
  DATE '2024-01-01' + (h3 % 700)::integer,
  h4 % 4 <> 0,
  'EUR',
  ((h5 % 1000000) / 100.0)::numeric(12, 2),
  CASE
    WHEN id % 97 = 0 THEN 'NaN'::double precision
    WHEN id % 89 = 0 THEN 'Infinity'::double precision
    WHEN id % 7 = 0 THEN NULL
    ELSE ((h6 % 2000) - 1000) / 10.0
  END,
  NULL,
  'Customer ' || (id + 1),
  CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END,
  CASE WHEN id % 10 = 3 THEN (h8 % 500) + 1 ELSE NULL END
FROM generate_series(0::bigint, 499) AS g(id),
LATERAL (
  SELECT
    (id * 7919 + 101) % 1000003 AS h1,
    (id * 104729 + 7) % 1000003 AS h2,
    (id * 15485863 + 31) % 1000003 AS h3,
    (id * 32452843 + 3) % 1000003 AS h4,
    (id * 49979687 + 17) % 1000003 AS h5,
    (id * 67867967 + 23) % 1000003 AS h6,
    (id * 86028121 + 5) % 1000003 AS h8
) AS h;

COMMENT ON TABLE analytics.customers IS
  'Synthetic customers (simple types, special column names, all-null and constant columns)';
COMMENT ON COLUMN analytics.customers.customer_id IS 'Synthetic surrogate key';

CREATE TABLE analytics.orders (
  order_id text NOT NULL,
  customer_id integer,
  order_ts timestamp with time zone,
  status text,
  amount numeric(12, 2),
  shipping jsonb,
  items jsonb,
  attributes jsonb,
  tags text[],
  discount double precision,
  CONSTRAINT orders_pk PRIMARY KEY (order_id)
);

-- Planted for the referential validation demo: orders 199, 399, 599, 799 and 999 reference
-- customer ids 501-505, which do not exist in customers (five orphan rows).
INSERT INTO analytics.orders
SELECT
  concat_ws('-', substr(md5(id::text), 1, 8), substr(md5(id::text), 9, 4),
            substr(md5(id::text), 13, 4), substr(md5(id::text), 17, 4),
            substr(md5(id::text), 21, 12)),
  CASE WHEN id % 200 = 199 THEN 501 + id / 200 ELSE (h10 % 500) + 1 END,
  to_timestamp(1735689600 + id * 3600),
  CASE h11 % 5 WHEN 0 THEN 'created' WHEN 1 THEN 'paid' WHEN 2 THEN 'shipped'
       WHEN 3 THEN 'delivered' ELSE 'cancelled' END,
  CASE WHEN id = 999 THEN 95000.00 ELSE ((h12 % 50000) / 100.0)::numeric(12, 2) END,
  CASE WHEN id % 25 = 0 THEN NULL ELSE jsonb_build_object(
    'address', CASE WHEN id % 10 = 0 THEN NULL ELSE jsonb_build_object(
      'city', CASE h13 % 3 WHEN 0 THEN 'Springfield' WHEN 1 THEN 'Riverside' ELSE 'Lakeside' END,
      'zip', CASE WHEN id % 3 = 0 THEN NULL ELSE lpad((h14 % 99999)::text, 5, '0') END) END,
    'method', CASE WHEN id % 2 = 0 THEN 'standard' ELSE 'express' END) END,
  CASE WHEN id = 500 THEN (
         SELECT jsonb_agg(jsonb_build_object('sku', 'SKU-' || i, 'qty', 1, 'price', 1.00) ORDER BY i)
         FROM generate_series(1, 1500) AS s(i))
       ELSE (
         SELECT coalesce(jsonb_agg(item ORDER BY position), '[]'::jsonb)
         FROM (VALUES
           (1, jsonb_build_object('sku', 'SKU-' || (h15 % 90), 'qty', (h16 % 5) + 1,
                                  'price', ((h17 % 10000) / 100.0)::numeric(10, 2))),
           (2, jsonb_build_object('sku', 'SKU-' || (h18 % 90), 'qty', NULL,
                                  'price', ((h19 % 10000) / 100.0)::numeric(10, 2))),
           (3, jsonb_build_object('sku', 'SKU-' || (h20 % 90), 'qty', 2,
                                  'price', ((h21 % 10000) / 100.0)::numeric(10, 2)))
         ) AS v(position, item)
         WHERE position <= id % 4) END,
  CASE WHEN id % 8 = 0 THEN NULL
       ELSE jsonb_build_object('channel', CASE WHEN id % 3 = 0 THEN 'web' ELSE 'app' END,
                               'coupon', CASE WHEN id % 5 = 0 THEN 'C' || id ELSE NULL END) END,
  CASE WHEN id % 6 = 0 THEN ARRAY[]::text[]
       ELSE ARRAY['gift', CASE WHEN id % 4 = 0 THEN NULL ELSE 'priority' END] END,
  CASE WHEN id % 12 = 0 THEN (h22 % 30)::double precision ELSE NULL END
FROM generate_series(0::bigint, 999) AS g(id),
LATERAL (
  SELECT
    (id * 7919 + 211) % 1000003 AS h10,
    (id * 104729 + 3) % 1000003 AS h11,
    (id * 15485863 + 41) % 1000003 AS h12,
    (id * 32452843 + 19) % 1000003 AS h13,
    (id * 49979687 + 2) % 1000003 AS h14,
    (id * 67867967 + 11) % 1000003 AS h15,
    (id * 86028121 + 29) % 1000003 AS h16,
    (id * 122949829 + 37) % 1000003 AS h17,
    (id * 141650939 + 43) % 1000003 AS h18,
    (id * 160481183 + 47) % 1000003 AS h19,
    (id * 179424673 + 53) % 1000003 AS h20,
    (id * 198491317 + 59) % 1000003 AS h21,
    (id * 217645177 + 61) % 1000003 AS h22
) AS h;

COMMENT ON TABLE analytics.orders IS
  'Synthetic orders with nested values (jsonb), a text array and a planted amount outlier';
COMMENT ON COLUMN analytics.orders.customer_id IS 'Customer who placed the order (synthetic)';

ALTER TABLE analytics.orders
  ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id)
  REFERENCES analytics.customers (customer_id) NOT VALID;

CREATE TABLE analytics.order_events (
  event_id text,
  order_id text,
  event_type text,
  payload text,
  notes text,
  legacy_flag text,
  amount_text text,
  event_date_text text,
  tracking_url text,
  created_at_local timestamp without time zone
);

-- Planted for the exact uniqueness demo: rows 1997-1999 reuse the event_id of rows 0-2
-- (three duplicated ids, six rows).
INSERT INTO analytics.order_events
SELECT
  'evt-' || lpad((CASE WHEN id >= 1997 THEN id - 1997 ELSE id END)::text, 6, '0'),
  concat_ws('-', substr(md5((id % 1000)::text), 1, 8), substr(md5((id % 1000)::text), 9, 4),
            substr(md5((id % 1000)::text), 13, 4), substr(md5((id % 1000)::text), 17, 4),
            substr(md5((id % 1000)::text), 21, 12)),
  CASE id % 4 WHEN 0 THEN 'created' WHEN 1 THEN 'payment' WHEN 2 THEN 'shipment' ELSE 'delivery' END,
  CASE
    WHEN id % 50 = 0 THEN '{"status": "broken'
    WHEN id % 45 = 0 THEN 'null'
    WHEN id % 40 = 0 THEN '[1, 2, 3]'
    WHEN id % 333 = 0 THEN jsonb_build_object('status', 'bulk', 'blob', repeat('x', 10000))::text
    WHEN id % 30 = 0 THEN NULL
    ELSE jsonb_build_object(
      'status', CASE id % 3 WHEN 0 THEN 'ok' WHEN 1 THEN 'retry' ELSE 'ok' END,
      'amount', ((h30 % 10000) / 100.0)::numeric(10, 2),
      'channel', CASE WHEN id % 2 = 0 THEN 'web' ELSE 'app' END)::text
  END,
  CASE WHEN id % 20 = 0 THEN 'manual note ' || id ELSE NULL END,
  CASE WHEN id % 2 = 0 THEN 'Y' ELSE 'N' END,
  ((h31 % 100000) / 100.0)::numeric(12, 2)::text,
  (DATE '2025-01-01' + (id % 365)::integer)::text,
  'https://shop.example.com/orders/' || id,
  to_timestamp(1735689600 + id * 60) AT TIME ZONE 'UTC'
FROM generate_series(0::bigint, 1999) AS g(id),
LATERAL (
  SELECT
    (id * 7919 + 307) % 1000003 AS h30,
    (id * 104729 + 311) % 1000003 AS h31
) AS h;

COMMENT ON TABLE analytics.order_events IS 'Synthetic order events with a JSON payload stored as text';

CREATE TABLE analytics.returns (
  return_id bigint,
  order_id text,
  reason text,
  created_on date
);

COMMENT ON TABLE analytics.returns IS 'Synthetic returns: intentionally empty';

-- The loader (never TableDossier) refreshes the planner statistics, so that the metadata level
-- finds a recorded row count.
ANALYZE analytics.customers, analytics.orders, analytics.order_events, analytics.returns;
