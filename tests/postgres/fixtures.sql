-- Synthetic tables for the PostgreSQL integration tests (loaded after the demo, as the administrator).

CREATE SCHEMA td_types;

CREATE TYPE td_types.mood AS ENUM ('sad', 'ok', 'happy');
CREATE DOMAIN td_types.price AS numeric(10, 2);
CREATE TYPE td_types.pair AS (a integer, b text);

-- Type coverage: one row per edge case.
CREATE TABLE td_types.coverage (
  id integer PRIMARY KEY,
  i integer,
  f double precision,
  r real,
  n numeric(10, 2),
  nu numeric,
  p td_types.price,
  tx text,
  vc varchar(10),
  ch char(5),
  u uuid,
  m td_types.mood,
  bo boolean,
  d date,
  ts timestamp with time zone,
  tn timestamp without time zone,
  iv interval,
  by bytea,
  j jsonb,
  js json,
  ar integer[],
  co td_types.pair,
  ip inet
);
INSERT INTO td_types.coverage VALUES
  (1, 1, 1.5, 2.5, 12.34, 1.000001, 3.30, 'abc', 'x', 'ab', '00000000-0000-4000-8000-000000000001',
   'ok', true, '2025-01-01', '2025-01-01 10:00:00+00', '2025-01-01 10:00:00', '1 day', '\x0102',
   '{"a": 1}', '{"b": 2}', ARRAY[1, NULL, 3], ROW(1, 'x'), '10.0.0.1'),
  (2, NULL, 'NaN', 'Infinity', NULL, 'NaN', NULL, '', NULL, '   ', NULL,
   'sad', false, 'infinity', '-infinity', '2025-06-01 00:00:00.123', NULL, NULL, NULL, NULL,
   '{}', ROW(NULL, NULL), NULL),
  (3, -5, -1e10, 0, -1.00, 2, 1.11, 'abc', 'y', NULL, '00000000-0000-4000-8000-000000000003',
   NULL, NULL, '0044-03-15 BC', NULL, NULL, '2 hours', '\x', '[1, 2]', '{"c": 3}',
   ARRAY[[1, 2], [3, NULL]], NULL, NULL),
  (4, 7, '-Infinity', -1.25, 0.00, -3.5, 0.00, ' 	', 'x', 'zz', '00000000-0000-4000-8000-000000000004',
   'happy', true, '2026-12-31', '2027-01-01 00:00:00+00', '2026-12-31 23:59:59.999999', NULL,
   '\xff', NULL, NULL, NULL, ROW(2, NULL), '::1');

-- Float values whose average and deviation overflow double precision (SQLSTATE 22003).
CREATE TABLE td_types.huge_floats (id integer, f double precision, label text);
INSERT INTO td_types.huge_floats VALUES (1, 1e300, 'a'), (2, 1e300, 'b'), (3, 1, 'c');

-- Values that must never leave the database (the sample sees them; profiles and logs must not).
CREATE TABLE td_types.markers (id integer, s text, payload text);
INSERT INTO td_types.markers
SELECT g, 'SECRET-MARKER-' || g, '{"note": "SECRET-JSON-MARKER-' || g || '", "n": ' || g || '}'
FROM generate_series(1, 50) AS g;

-- Error paths that carry values in their messages.
CREATE TABLE td_types.raw (code text);
INSERT INTO td_types.raw VALUES ('7'), ('7" SECRET-ROW-MARKER 999-88-7777');
CREATE VIEW td_types.casted AS SELECT code::integer AS code FROM td_types.raw;
CREATE FUNCTION td_types.check_value(value text) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  IF value LIKE '%MARKER%' THEN
    RAISE EXCEPTION 'bad value % in row', value;
  END IF;
  RETURN value;
END $$;
CREATE VIEW td_types.raising AS SELECT td_types.check_value(code) AS v FROM td_types.raw;

-- A view slower than a one-second statement timeout.
CREATE VIEW td_types.slow AS
SELECT g FROM generate_series(1, 400) AS g WHERE pg_catalog.pg_sleep(0.01) IS NOT NULL;

-- Row-level security: the reader role sees only tenant a.
CREATE TABLE td_types.tenants (tenant text, v integer);
INSERT INTO td_types.tenants VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4), ('b', 5);
ALTER TABLE td_types.tenants ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_a ON td_types.tenants FOR SELECT USING (tenant = 'a');

-- Column privileges are set by the fixture: the reader may read only id.
CREATE TABLE td_types.partly_readable (id integer, secret text);
INSERT INTO td_types.partly_readable VALUES (1, 'SECRET-COLUMN-MARKER');

-- Constraints: a foreign key to a partitioned table (PostgreSQL adds internal copies per
-- partition), a foreign key on a plain unique index, a composite foreign key with permuted
-- columns, a NOT VALID check.
CREATE TABLE td_types.pt (id integer PRIMARY KEY) PARTITION BY RANGE (id);
CREATE TABLE td_types.pt_1 PARTITION OF td_types.pt FOR VALUES FROM (0) TO (100);
CREATE TABLE td_types.pt_2 PARTITION OF td_types.pt FOR VALUES FROM (100) TO (200);
INSERT INTO td_types.pt VALUES (1), (150);
CREATE TABLE td_types.child (pt_id integer CONSTRAINT child_pt_fk REFERENCES td_types.pt (id));
INSERT INTO td_types.child VALUES (1), (150);
CREATE TABLE td_types.refs (code text, id integer);
CREATE UNIQUE INDEX refs_code_id ON td_types.refs (code, id);
CREATE TABLE td_types.refs2 (a integer, b integer, CONSTRAINT refs2_ab UNIQUE (a, b));
CREATE TABLE td_types.linked (
  c text,
  i integer,
  y integer,
  x integer,
  CONSTRAINT linked_idx_fk FOREIGN KEY (c, i) REFERENCES td_types.refs (code, id),
  CONSTRAINT linked_permuted_fk FOREIGN KEY (y, x) REFERENCES td_types.refs2 (b, a)
);
ALTER TABLE td_types.linked ADD CONSTRAINT linked_positive CHECK (i > 0) NOT VALID;

-- Names that differ only by case are distinct relations in PostgreSQL.
CREATE TABLE td_types."Mixed" (v integer);
CREATE TABLE td_types.mixed (v integer, w integer);
INSERT INTO td_types."Mixed" VALUES (1);
INSERT INTO td_types.mixed VALUES (1, 2), (3, 4);

-- Hostile identifiers.
CREATE SCHEMA "Hostile Schema";
CREATE TABLE "Hostile Schema"."x""; DROP TABLE td_types.coverage; --" (
  "a""b" text,
  "c%d" integer,
  "%s" text,
  "%(x)s" text,
  "sp ace" varchar(20),
  "semi;colon" text,
  "naïve ✓" text
);
INSERT INTO "Hostile Schema"."x""; DROP TABLE td_types.coverage; --" VALUES
  ('q"uote', 1, '%s', '%(x)s', 'a b', 'x;y', 'é'),
  (NULL, 2, NULL, '', NULL, NULL, 'ü');

-- A hostile function the reader would pick through its search_path (public) if the connector
-- did not empty it: the built-in char_length must win.
CREATE FUNCTION public.char_length(value character varying) RETURNS integer
  LANGUAGE sql IMMUTABLE AS 'SELECT 999';
CREATE FUNCTION public.substr(value character varying, start integer, count integer)
  RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT 'HIJACKED'$$;

ANALYZE td_types.coverage, td_types.markers;
