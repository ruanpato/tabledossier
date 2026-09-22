-- TableDossier synthetic demo tables.
--
-- Every value below is generated from integer sequences: no real people,
-- companies or production data. Deterministic (hash-based) expressions make
-- reruns produce the same rows.
--
-- Databricks: open a SQL editor or notebook, select a catalog and schema you
-- own (for example `USE CATALOG demo; USE SCHEMA analytics;`) and run this
-- script. Without a USING clause, tables are created as Delta tables.
-- Local Spark: the same statements create Parquet tables, or Delta tables when the
-- session is configured with delta-spark and spark.sql.sources.default=delta.
--
-- The script DROPS AND RECREATES only these four demo tables in the current
-- schema: customers, orders, order_events, returns.

DROP TABLE IF EXISTS customers;

-- Column names with spaces or dots (`display name`, `a.b`) need Delta column mapping.
CREATE TABLE customers
COMMENT 'Synthetic customers (simple types, special column names, all-null and constant columns)'
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')
AS SELECT
  id + 1 AS customer_id,
  CASE WHEN pmod(hash(id, 1), 10) = 0 THEN NULL ELSE concat('user', id + 1, '@example.com') END AS email,
  CASE pmod(hash(id, 2), 3) WHEN 0 THEN 'retail' WHEN 1 THEN 'smb' ELSE 'enterprise' END AS segment,
  date_add(DATE'2024-01-01', CAST(pmod(hash(id, 3), 700) AS INT)) AS signup_date,
  pmod(hash(id, 4), 4) <> 0 AS is_active,
  'EUR' AS currency,
  CAST(pmod(hash(id, 5), 1000000) / 100.0 AS DECIMAL(12, 2)) AS lifetime_value,
  CASE
    WHEN id % 97 = 0 THEN CAST('NaN' AS DOUBLE)
    WHEN id % 89 = 0 THEN CAST('Infinity' AS DOUBLE)
    WHEN id % 7 = 0 THEN NULL
    ELSE (pmod(hash(id, 6), 2000) - 1000) / 10.0
  END AS score,
  CAST(NULL AS STRING) AS middle_name,
  concat('Customer ', id + 1) AS `display name`,
  CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END AS `a.b`
FROM range(500);

DROP TABLE IF EXISTS orders;

CREATE TABLE orders
COMMENT 'Synthetic orders with nested structs, arrays and maps'
AS SELECT
  concat_ws('-', substr(md5(CAST(id AS STRING)), 1, 8), substr(md5(CAST(id AS STRING)), 9, 4),
            substr(md5(CAST(id AS STRING)), 13, 4), substr(md5(CAST(id AS STRING)), 17, 4),
            substr(md5(CAST(id AS STRING)), 21, 12)) AS order_id,
  pmod(hash(id, 10), 500) + 1 AS customer_id,
  timestamp_seconds(1735689600 + id * 3600) AS order_ts,
  CASE pmod(hash(id, 11), 5) WHEN 0 THEN 'created' WHEN 1 THEN 'paid' WHEN 2 THEN 'shipped'
       WHEN 3 THEN 'delivered' ELSE 'cancelled' END AS status,
  CASE WHEN id = 999 THEN CAST(95000.00 AS DECIMAL(12, 2))
       ELSE CAST(pmod(hash(id, 12), 50000) / 100.0 AS DECIMAL(12, 2)) END AS amount,
  CASE WHEN id % 25 = 0 THEN NULL ELSE named_struct(
    'address', CASE WHEN id % 10 = 0 THEN NULL ELSE named_struct(
      'city', CASE pmod(hash(id, 13), 3) WHEN 0 THEN 'Springfield' WHEN 1 THEN 'Riverside' ELSE 'Lakeside' END,
      'zip', CASE WHEN id % 3 = 0 THEN NULL ELSE lpad(CAST(pmod(hash(id, 14), 99999) AS STRING), 5, '0') END) END,
    'method', CASE WHEN id % 2 = 0 THEN 'standard' ELSE 'express' END) END AS shipping,
  CASE WHEN id = 500 THEN transform(sequence(1, 1500), i -> named_struct(
         'sku', concat('SKU-', i), 'qty', 1, 'price', CAST(1.00 AS DECIMAL(10, 2))))
       ELSE slice(array(
         named_struct('sku', concat('SKU-', pmod(hash(id, 15), 90)), 'qty', CAST(pmod(hash(id, 16), 5) + 1 AS INT),
                      'price', CAST(pmod(hash(id, 17), 10000) / 100.0 AS DECIMAL(10, 2))),
         named_struct('sku', concat('SKU-', pmod(hash(id, 18), 90)), 'qty', CAST(NULL AS INT),
                      'price', CAST(pmod(hash(id, 19), 10000) / 100.0 AS DECIMAL(10, 2))),
         named_struct('sku', concat('SKU-', pmod(hash(id, 20), 90)), 'qty', 2,
                      'price', CAST(pmod(hash(id, 21), 10000) / 100.0 AS DECIMAL(10, 2)))),
         1, CAST(pmod(id, 4) AS INT)) END AS items,
  CASE WHEN id % 8 = 0 THEN NULL
       ELSE map('channel', CASE WHEN id % 3 = 0 THEN 'web' ELSE 'app' END,
                'coupon', CASE WHEN id % 5 = 0 THEN concat('C', id) ELSE NULL END) END AS attributes,
  CASE WHEN id % 6 = 0 THEN array() ELSE array('gift', CASE WHEN id % 4 = 0 THEN NULL ELSE 'priority' END) END AS tags,
  CASE WHEN id % 12 = 0 THEN CAST(pmod(hash(id, 22), 30) AS DOUBLE) ELSE NULL END AS discount
FROM range(1000);

DROP TABLE IF EXISTS order_events;

-- Planted for the exact uniqueness demo: rows 1997-1999 reuse the event_id of rows 0-2
-- (three duplicated ids, six rows).
CREATE TABLE order_events
COMMENT 'Synthetic order events with a JSON payload stored as STRING'
AS SELECT
  concat('evt-', lpad(CAST(CASE WHEN id >= 1997 THEN id - 1997 ELSE id END AS STRING), 6, '0')) AS event_id,
  concat_ws('-', substr(md5(CAST(pmod(id, 1000) AS STRING)), 1, 8), substr(md5(CAST(pmod(id, 1000) AS STRING)), 9, 4),
            substr(md5(CAST(pmod(id, 1000) AS STRING)), 13, 4), substr(md5(CAST(pmod(id, 1000) AS STRING)), 17, 4),
            substr(md5(CAST(pmod(id, 1000) AS STRING)), 21, 12)) AS order_id,
  CASE pmod(id, 4) WHEN 0 THEN 'created' WHEN 1 THEN 'payment' WHEN 2 THEN 'shipment' ELSE 'delivery' END AS event_type,
  CASE
    WHEN id % 50 = 0 THEN '{"status": "broken'
    WHEN id % 45 = 0 THEN 'null'
    WHEN id % 40 = 0 THEN '[1, 2, 3]'
    WHEN id % 333 = 0 THEN to_json(named_struct('status', 'bulk', 'blob', repeat('x', 10000)))
    WHEN id % 30 = 0 THEN NULL
    ELSE to_json(named_struct(
      'status', CASE pmod(id, 3) WHEN 0 THEN 'ok' WHEN 1 THEN 'retry' ELSE 'ok' END,
      'amount', pmod(hash(id, 30), 10000) / 100.0,
      'channel', CASE WHEN id % 2 = 0 THEN 'web' ELSE 'app' END))
  END AS payload,
  CASE WHEN id % 20 = 0 THEN concat('manual note ', id) ELSE NULL END AS notes,
  CASE WHEN id % 2 = 0 THEN 'Y' ELSE 'N' END AS legacy_flag,
  CAST(pmod(hash(id, 31), 100000) / 100.0 AS STRING) AS amount_text,
  CAST(date_add(DATE'2025-01-01', CAST(pmod(id, 365) AS INT)) AS STRING) AS event_date_text,
  concat('https://shop.example.com/orders/', id) AS tracking_url,
  CAST(timestamp_seconds(1735689600 + id * 60) AS TIMESTAMP_NTZ) AS created_at_local
FROM range(2000);

DROP TABLE IF EXISTS returns;

CREATE TABLE returns
COMMENT 'Synthetic returns: intentionally empty'
AS SELECT
  CAST(id AS BIGINT) AS return_id,
  CAST(NULL AS STRING) AS order_id,
  CAST(NULL AS STRING) AS reason,
  CAST(NULL AS DATE) AS created_on
FROM range(0);

ALTER TABLE customers ALTER COLUMN customer_id COMMENT 'Synthetic surrogate key';

ALTER TABLE orders ALTER COLUMN customer_id COMMENT 'Customer who placed the order (synthetic)';
