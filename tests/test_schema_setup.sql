-- =============================================================
-- mm-ready test schema: trigger every scan-mode check
--
-- IDEMPOTENT: safe to re-run. Uses IF NOT EXISTS, OR REPLACE,
-- and DO $$ blocks with exception handling throughout.
--
-- All objects use the mmr_ prefix. This schema is self-contained
-- and does not depend on any external database.
--
-- Usage:
--   psql -U postgres -d mmready -f test_schema_setup.sql
--   psql -U postgres -d mmready -f test_workload.sql
-- =============================================================


-- ===================== EXTENSIONS =====================

CREATE EXTENSION IF NOT EXISTS btree_gist;   -- needed for exclusion constraints


-- ===================== TYPES =====================

-- enum_types check (CONSIDER)
DO $$ BEGIN
    CREATE TYPE mmr_status AS ENUM ('pending', 'active', 'completed');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


-- ===================== CORE TABLES (for workload) =====================
-- These tables provide a realistic multi-table schema for the workload
-- script to generate diverse pg_stat_statements entries.

CREATE TABLE IF NOT EXISTS mmr_regions (
    region_id int PRIMARY KEY,
    region_name text NOT NULL
);

CREATE TABLE IF NOT EXISTS mmr_categories (
    category_id int PRIMARY KEY,
    category_name text NOT NULL,
    description text
);

CREATE TABLE IF NOT EXISTS mmr_suppliers (
    supplier_id int PRIMARY KEY,
    company_name text NOT NULL,
    contact_name text,
    city text,
    country text,
    phone text
);

CREATE TABLE IF NOT EXISTS mmr_products (
    product_id int PRIMARY KEY,
    product_name text NOT NULL,
    supplier_id int REFERENCES mmr_suppliers(supplier_id),
    category_id int REFERENCES mmr_categories(category_id),
    unit_price numeric DEFAULT 0,
    units_in_stock int DEFAULT 0,
    discontinued int DEFAULT 0
);

CREATE TABLE IF NOT EXISTS mmr_customers (
    customer_id text PRIMARY KEY,
    company_name text NOT NULL,
    contact_name text,
    city text,
    country text,
    phone text
);

CREATE TABLE IF NOT EXISTS mmr_employees (
    employee_id int PRIMARY KEY,
    first_name text NOT NULL,
    last_name text NOT NULL,
    title text,
    hire_date date
);

CREATE TABLE IF NOT EXISTS mmr_orders (
    order_id int PRIMARY KEY,
    customer_id text REFERENCES mmr_customers(customer_id),
    employee_id int REFERENCES mmr_employees(employee_id),
    order_date date,
    ship_country text,
    freight numeric DEFAULT 0
);

CREATE TABLE IF NOT EXISTS mmr_order_details (
    order_id int REFERENCES mmr_orders(order_id),
    product_id int REFERENCES mmr_products(product_id),
    unit_price numeric NOT NULL,
    quantity int NOT NULL,
    discount numeric DEFAULT 0,
    PRIMARY KEY (order_id, product_id)
);


-- ===================== SEED DATA =====================

-- Regions
INSERT INTO mmr_regions VALUES (1, 'North America'), (2, 'Europe'), (3, 'Asia')
    ON CONFLICT DO NOTHING;

-- Categories
INSERT INTO mmr_categories VALUES
    (1, 'Beverages', 'Coffee, tea, and soft drinks'),
    (2, 'Condiments', 'Sauces, spreads, and seasonings'),
    (3, 'Confections', 'Candy, chocolate, and sweets'),
    (4, 'Dairy', 'Cheese, milk, and butter'),
    (5, 'Grains', 'Bread, cereal, and pasta'),
    (6, 'Produce', 'Dried fruit, vegetables, and tofu'),
    (7, 'Seafood', 'Fish, shellfish, and seaweed')
    ON CONFLICT DO NOTHING;

-- Suppliers
INSERT INTO mmr_suppliers VALUES
    (1, 'Nordic Goods', 'Lars Svensson', 'Stockholm', 'Sweden', '46-555-0100'),
    (2, 'Tokyo Trading', 'Yuki Tanaka', 'Tokyo', 'Japan', '81-555-0200'),
    (3, 'Berlin Foods', 'Klaus Weber', 'Berlin', 'Germany', '49-555-0300'),
    (4, 'Lyon Provisions', 'Marie Dupont', 'Lyon', 'France', '33-555-0400'),
    (5, 'Portland Organics', 'Sam Green', 'Portland', 'USA', '1-555-0500')
    ON CONFLICT DO NOTHING;

-- Products
INSERT INTO mmr_products VALUES
    (1,  'Earl Grey Tea',      1, 1, 18.00, 39, 0),
    (2,  'Hot Sauce',          4, 2, 21.35, 52, 0),
    (3,  'Dark Chocolate',     3, 3, 10.00, 15, 0),
    (4,  'Swiss Cheese',       3, 4, 44.00, 30, 0),
    (5,  'Sourdough Bread',    5, 5,  7.00, 20, 0),
    (6,  'Dried Mango',        2, 6, 30.00, 10, 0),
    (7,  'Atlantic Salmon',    1, 7, 25.00, 25, 0),
    (8,  'Green Tea',          2, 1, 12.00, 45, 0),
    (9,  'Dijon Mustard',      4, 2, 14.00, 60, 0),
    (10, 'Milk Chocolate',     3, 3, 16.50, 35, 0),
    (11, 'Gouda Cheese',       3, 4, 32.00, 22, 0),
    (12, 'Rye Bread',          5, 5,  6.50, 18, 0),
    (13, 'Tofu',               2, 6, 23.25, 35, 0),
    (14, 'Smoked Mackerel',    1, 7, 19.50, 17, 0),
    (15, 'Discontinued Soda',  5, 1,  4.50,  0, 1)
    ON CONFLICT DO NOTHING;

-- Employees
INSERT INTO mmr_employees VALUES
    (1, 'Alice',  'Johnson', 'Sales Rep',      '2020-03-15'),
    (2, 'Bob',    'Smith',   'Sales Manager',   '2019-07-01'),
    (3, 'Carol',  'Davis',   'Sales Rep',       '2021-01-10'),
    (4, 'Dave',   'Wilson',  'Account Manager', '2018-11-20'),
    (5, 'Eve',    'Brown',   'Sales Rep',       '2022-06-01')
    ON CONFLICT DO NOTHING;

-- Customers
INSERT INTO mmr_customers VALUES
    ('ACME',  'Acme Corp',       'Wile Coyote',   'Phoenix',    'USA',     '1-555-1000'),
    ('GLOBX', 'GlobEx Inc',      'Homer Simpson',  'Springfield','USA',     '1-555-2000'),
    ('EURTR', 'EuroTrade GmbH',  'Hans Mueller',   'Berlin',    'Germany', '49-555-3000'),
    ('NIPPO', 'Nippon Imports',   'Kenji Yamamoto', 'Tokyo',     'Japan',   '81-555-4000'),
    ('PARIF', 'Paris Foods',      'Jean Moreau',    'Paris',     'France',  '33-555-5000'),
    ('LONDI', 'London Distrib',   'James Watson',   'London',    'UK',      '44-555-6000'),
    ('SYDNY', 'Sydney Supplies',  'Bruce Irwin',    'Sydney',    'Australia','61-555-7000'),
    ('TORON', 'Toronto Traders',  'Wayne Gretzky',  'Toronto',   'Canada',  '1-555-8000')
    ON CONFLICT DO NOTHING;

-- Orders (30 orders across customers/employees)
INSERT INTO mmr_orders VALUES
    (1001, 'ACME',  1, '2025-01-05', 'USA',       45.50),
    (1002, 'GLOBX', 2, '2025-01-06', 'USA',      120.75),
    (1003, 'EURTR', 3, '2025-01-07', 'Germany',   88.00),
    (1004, 'NIPPO', 4, '2025-01-08', 'Japan',    200.00),
    (1005, 'PARIF', 5, '2025-01-09', 'France',    55.25),
    (1006, 'LONDI', 1, '2025-01-10', 'UK',        72.00),
    (1007, 'SYDNY', 2, '2025-01-11', 'Australia', 310.00),
    (1008, 'TORON', 3, '2025-01-12', 'Canada',    65.00),
    (1009, 'ACME',  4, '2025-01-15', 'USA',       95.00),
    (1010, 'GLOBX', 5, '2025-01-16', 'USA',      180.50),
    (1011, 'EURTR', 1, '2025-01-17', 'Germany',  140.00),
    (1012, 'NIPPO', 2, '2025-01-18', 'Japan',    275.00),
    (1013, 'PARIF', 3, '2025-01-19', 'France',    42.00),
    (1014, 'LONDI', 4, '2025-01-20', 'UK',       105.00),
    (1015, 'SYDNY', 5, '2025-01-21', 'Australia', 195.00),
    (1016, 'TORON', 1, '2025-02-01', 'Canada',    78.00),
    (1017, 'ACME',  2, '2025-02-02', 'USA',      225.00),
    (1018, 'GLOBX', 3, '2025-02-03', 'USA',       33.50),
    (1019, 'EURTR', 4, '2025-02-04', 'Germany',  160.00),
    (1020, 'NIPPO', 5, '2025-02-05', 'Japan',    350.00),
    (1021, 'PARIF', 1, '2025-02-06', 'France',    90.00),
    (1022, 'LONDI', 2, '2025-02-07', 'UK',       115.00),
    (1023, 'SYDNY', 3, '2025-02-08', 'Australia', 250.00),
    (1024, 'TORON', 4, '2025-02-09', 'Canada',    55.00),
    (1025, 'ACME',  5, '2025-02-10', 'USA',      400.00),
    (1026, 'GLOBX', 1, '2025-03-01', 'USA',       67.00),
    (1027, 'EURTR', 2, '2025-03-02', 'Germany',  185.00),
    (1028, 'NIPPO', 3, '2025-03-03', 'Japan',    290.00),
    (1029, 'PARIF', 4, '2025-03-04', 'France',    75.00),
    (1030, 'LONDI', 5, '2025-03-05', 'UK',       130.00)
    ON CONFLICT DO NOTHING;

-- Order details (2-3 line items per order)
INSERT INTO mmr_order_details VALUES
    (1001, 1, 18.00, 10, 0),    (1001, 3, 10.00, 5, 0.05),
    (1002, 2, 21.35, 20, 0),    (1002, 7, 25.00, 8, 0.10),
    (1003, 4, 44.00, 3, 0),     (1003, 9, 14.00, 15, 0),
    (1004, 6, 30.00, 12, 0.05), (1004, 8, 12.00, 25, 0),
    (1005, 5, 7.00,  30, 0),    (1005, 10, 16.50, 10, 0),
    (1006, 11, 32.00, 5, 0),    (1006, 14, 19.50, 8, 0),
    (1007, 1, 18.00, 15, 0.10), (1007, 13, 23.25, 10, 0),
    (1008, 3, 10.00, 20, 0),    (1008, 12, 6.50, 25, 0),
    (1009, 2, 21.35, 10, 0),    (1009, 4, 44.00, 2, 0),
    (1010, 7, 25.00, 12, 0.05), (1010, 5, 7.00, 40, 0),
    (1011, 9, 14.00, 20, 0),    (1011, 6, 30.00, 5, 0),
    (1012, 8, 12.00, 30, 0),    (1012, 10, 16.50, 15, 0),
    (1013, 14, 19.50, 6, 0),    (1013, 11, 32.00, 3, 0),
    (1014, 1, 18.00, 8, 0),     (1014, 13, 23.25, 12, 0),
    (1015, 3, 10.00, 15, 0.05), (1015, 2, 21.35, 7, 0),
    (1016, 4, 44.00, 4, 0),     (1016, 12, 6.50, 18, 0),
    (1017, 6, 30.00, 10, 0.10), (1017, 7, 25.00, 15, 0),
    (1018, 5, 7.00, 20, 0),     (1018, 9, 14.00, 8, 0),
    (1019, 10, 16.50, 12, 0),   (1019, 14, 19.50, 5, 0),
    (1020, 1, 18.00, 25, 0.05), (1020, 8, 12.00, 20, 0),
    (1021, 11, 32.00, 6, 0),    (1021, 3, 10.00, 10, 0),
    (1022, 2, 21.35, 15, 0),    (1022, 13, 23.25, 8, 0),
    (1023, 4, 44.00, 5, 0.05),  (1023, 6, 30.00, 10, 0),
    (1024, 7, 25.00, 4, 0),     (1024, 5, 7.00, 30, 0),
    (1025, 9, 14.00, 25, 0.10), (1025, 10, 16.50, 18, 0),
    (1026, 12, 6.50, 20, 0),    (1026, 14, 19.50, 10, 0),
    (1027, 1, 18.00, 12, 0),    (1027, 11, 32.00, 7, 0),
    (1028, 8, 12.00, 18, 0),    (1028, 3, 10.00, 22, 0),
    (1029, 13, 23.25, 9, 0),    (1029, 2, 21.35, 5, 0),
    (1030, 4, 44.00, 3, 0),     (1030, 6, 30.00, 8, 0.05)
    ON CONFLICT DO NOTHING;


-- ===================== CHECK-TRIGGER TABLES =====================

-- primary_keys check (WARNING): table without a PK
CREATE TABLE IF NOT EXISTS mmr_no_pk (
    col1 int,
    col2 text
);

-- sequence_pks check (CRITICAL): PK backed by a sequence
CREATE TABLE IF NOT EXISTS mmr_serial_pk (
    id SERIAL PRIMARY KEY,
    name text
);

-- sequence_pks check (CRITICAL): PK with IDENTITY
CREATE TABLE IF NOT EXISTS mmr_identity_pk (
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    val text
);

-- unlogged_tables check (WARNING)
DO $$ BEGIN
    CREATE UNLOGGED TABLE mmr_unlogged (id int, data text);
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- inheritance check (WARNING): table inheritance (non-partition)
CREATE TABLE IF NOT EXISTS mmr_parent_tbl (id int, name text);
DO $$ BEGIN
    CREATE TABLE mmr_child_tbl () INHERITS (mmr_parent_tbl);
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- deferrable_constraints check (CRITICAL for PK, WARNING for UNIQUE)
DO $$ BEGIN
    CREATE TABLE mmr_deferrable_pk (
        id int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED,
        name text
    );
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TABLE mmr_deferrable_uq (
        id int PRIMARY KEY,
        email text UNIQUE DEFERRABLE
    );
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- missing_fk_indexes check (WARNING): FK without index on referencing side
CREATE TABLE IF NOT EXISTS mmr_ref_parent (id int PRIMARY KEY);
DO $$ BEGIN
    CREATE TABLE mmr_ref_child (
        id int PRIMARY KEY,
        parent_id int REFERENCES mmr_ref_parent(id)
        -- deliberately no index on parent_id
    );
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- foreign_keys check (WARNING for CASCADE, CONSIDER for summary)
CREATE TABLE IF NOT EXISTS mmr_cascade_parent (id int PRIMARY KEY);
DO $$ BEGIN
    CREATE TABLE mmr_cascade_child (
        id int PRIMARY KEY,
        parent_id int REFERENCES mmr_cascade_parent(id) ON DELETE CASCADE
    );
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- row_level_security check (WARNING)
CREATE TABLE IF NOT EXISTS mmr_rls_table (
    id int PRIMARY KEY,
    owner_name text
);

-- exclusion_constraints check (WARNING)
DO $$ BEGIN
    CREATE TABLE mmr_exclusion_tbl (
        room int,
        during tsrange,
        EXCLUDE USING gist (room WITH =, during WITH &&)
    );
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- large_objects check (WARNING): table with OID column
CREATE TABLE IF NOT EXISTS mmr_lob_ref (
    id int PRIMARY KEY,
    file_ref oid
);

-- numeric_columns check: WARNING for nullable, CONSIDER for NOT NULL
CREATE TABLE IF NOT EXISTS mmr_numerics (
    id int PRIMARY KEY,
    total_count bigint,          -- nullable numeric named "total_*" → WARNING
    balance numeric NOT NULL,    -- NOT NULL numeric named "balance" → CONSIDER
    sum_value numeric            -- nullable numeric named "sum_*" → WARNING
);

-- multiple_unique_indexes check (CONSIDER): table with 3 unique indexes
CREATE TABLE IF NOT EXISTS mmr_multi_unique (
    id int PRIMARY KEY,
    email text UNIQUE,
    username text UNIQUE
);

-- generated_columns check (CONSIDER)
CREATE TABLE IF NOT EXISTS mmr_generated (
    id int PRIMARY KEY,
    price numeric,
    tax numeric,
    total numeric GENERATED ALWAYS AS (price + tax) STORED
);

-- column_defaults check (CONSIDER): volatile defaults
CREATE TABLE IF NOT EXISTS mmr_defaults (
    id int PRIMARY KEY,
    created_at timestamp DEFAULT now(),
    rand_val numeric DEFAULT random()
);

-- partitioned_tables check (CONSIDER)
CREATE TABLE IF NOT EXISTS mmr_partitioned (
    id int,
    logdate date,
    val numeric
) PARTITION BY RANGE (logdate);
DO $$ BEGIN
    CREATE TABLE mmr_part_2024 PARTITION OF mmr_partitioned
        FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;
DO $$ BEGIN
    CREATE TABLE mmr_part_2025 PARTITION OF mmr_partitioned
        FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- rules check: need two separate tables (avoid circular rules)
CREATE TABLE IF NOT EXISTS mmr_log_tbl (id int, msg text);
CREATE TABLE IF NOT EXISTS mmr_archive_tbl (id int, msg text);

-- truncate_cascade check: table safe to truncate
CREATE TABLE IF NOT EXISTS mmr_truncate_target (
    id int PRIMARY KEY,
    data text
);


-- ===================== LARGE OBJECTS =====================

-- large_objects check (WARNING) + lolor_check (WARNING without lolor)
DO $$ BEGIN
    PERFORM lo_create(99999);
EXCEPTION WHEN OTHERS THEN NULL;
END $$;


-- ===================== SEQUENCES =====================

-- sequence_data_types check (WARNING): smallint sequence
CREATE SEQUENCE IF NOT EXISTS mmr_small_seq AS smallint;

-- sequence_audit check (WARNING): standalone sequence
CREATE SEQUENCE IF NOT EXISTS mmr_standalone_seq;


-- ===================== ROW LEVEL SECURITY =====================

ALTER TABLE mmr_rls_table ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
    CREATE POLICY mmr_rls_policy ON mmr_rls_table
        FOR ALL USING (owner_name = current_user);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


-- ===================== RULES =====================

-- rules check: INSTEAD rule (WARNING)
DO $$ BEGIN
    CREATE OR REPLACE RULE mmr_instead_rule AS
        ON INSERT TO mmr_archive_tbl
        DO INSTEAD INSERT INTO mmr_log_tbl VALUES (NEW.id, NEW.msg);
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- rules check: ALSO rule (CONSIDER)
DO $$ BEGIN
    CREATE OR REPLACE RULE mmr_also_rule AS
        ON UPDATE TO mmr_log_tbl
        DO ALSO NOTHING;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;


-- ===================== FUNCTIONS =====================

-- notify_listen check (WARNING): function using pg_notify
CREATE OR REPLACE FUNCTION mmr_notify_func()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('mmr_channel', NEW.col1::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- stored_procedures check (CONSIDER): function with write operations
CREATE OR REPLACE FUNCTION mmr_write_func()
RETURNS void AS $$
BEGIN
    INSERT INTO mmr_log_tbl VALUES (1, 'test');
    DELETE FROM mmr_log_tbl WHERE id = 1;
END;
$$ LANGUAGE plpgsql;

-- temp_tables check (CONSIDER): function creating temp tables
CREATE OR REPLACE FUNCTION mmr_temp_func()
RETURNS void AS $$
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS mmr_staging (x int);
    DROP TABLE IF EXISTS mmr_staging;
END;
$$ LANGUAGE plpgsql;


-- ===================== TRIGGERS =====================

-- trigger_functions check (WARNING for ALWAYS)
DO $$ BEGIN
    CREATE TRIGGER mmr_notify_trig
        AFTER INSERT ON mmr_no_pk
        FOR EACH ROW EXECUTE FUNCTION mmr_notify_func();
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

ALTER TABLE mmr_no_pk ENABLE ALWAYS TRIGGER mmr_notify_trig;


-- ===================== VIEWS =====================

-- views_audit check (CONSIDER for regular views)
CREATE OR REPLACE VIEW mmr_view AS SELECT * FROM mmr_no_pk;

-- views_audit check (WARNING for materialized views)
DO $$ BEGIN
    CREATE MATERIALIZED VIEW mmr_matview AS SELECT * FROM mmr_ref_parent;
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;


-- ===================== EVENT TRIGGERS =====================

-- event_triggers check (CONSIDER for default-enabled)
CREATE OR REPLACE FUNCTION mmr_ddl_logger()
RETURNS event_trigger AS $$
BEGIN
    RAISE NOTICE 'DDL event: %', tg_event;
END;
$$ LANGUAGE plpgsql;

DO $$ BEGIN
    CREATE EVENT TRIGGER mmr_ddl_trigger
        ON ddl_command_start
        EXECUTE FUNCTION mmr_ddl_logger();
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


-- ===================== TABLE ACTIVITY =====================

-- tables_update_delete_no_pk check (CRITICAL): needs UPDATE/DELETE stats
INSERT INTO mmr_no_pk VALUES (1, 'test_row_1');
INSERT INTO mmr_no_pk VALUES (2, 'test_row_2');
UPDATE mmr_no_pk SET col2 = 'updated' WHERE col1 = 1;
DELETE FROM mmr_no_pk WHERE col1 = 2;

-- insert-only no-PK table for INFO finding
CREATE TABLE IF NOT EXISTS mmr_insert_only_no_pk (val text);
INSERT INTO mmr_insert_only_no_pk VALUES ('row1');
INSERT INTO mmr_insert_only_no_pk VALUES ('row2');


-- ===================== WORKLOAD PATTERNS =====================

-- NOTIFY in queries (notify_listen CONSIDER for query patterns)
NOTIFY mmr_test_channel, 'hello';

-- TRUNCATE RESTART IDENTITY (truncate_cascade CONSIDER)
TRUNCATE TABLE mmr_serial_pk RESTART IDENTITY;


-- ===================== DONE =====================

SELECT 'mm-ready test schema setup complete' AS status;
