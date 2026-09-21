-- Optional, Unity Catalog only: declare informational keys on the demo tables so
-- that TableDossier can document them (PK/FK in Unity Catalog are NOT enforced).
-- Run after create_demo_tables.sql, in the same catalog and schema.

ALTER TABLE customers ALTER COLUMN customer_id SET NOT NULL;
ALTER TABLE customers ADD CONSTRAINT customers_pk PRIMARY KEY (customer_id);

ALTER TABLE orders ALTER COLUMN order_id SET NOT NULL;
ALTER TABLE orders ADD CONSTRAINT orders_pk PRIMARY KEY (order_id);
ALTER TABLE orders ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES customers (customer_id);
