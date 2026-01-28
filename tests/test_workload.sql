-- =============================================================
-- mm-ready test workload for pg_stat_statements population
--
-- Runs entirely against the mmr_ test schema. No external
-- database dependencies.
--
-- IDEMPOTENT: all inserts are deleted, all updates are reverted.
-- After running, the database is in its original state.
--
-- Requires: test_schema_setup.sql has been run first.
--
-- Structure:
--   1. INSERTs (add test rows)
--   2. UPDATEs (modify existing data)
--   3. SELECTs (bulk of the workload)
--   4. UPDATEs (revert modifications)
--   5. DELETEs (remove all inserted rows)
--   6. Pattern statements (DDL, advisory locks, temp tables, truncate)
-- =============================================================


-- ===================== 1. INSERTS =====================

-- New products
INSERT INTO mmr_products (product_id, product_name, supplier_id, category_id, unit_price, units_in_stock, discontinued)
VALUES (101, 'Organic Quinoa', 5, 5, 15.50, 100, 0);
INSERT INTO mmr_products (product_id, product_name, supplier_id, category_id, unit_price, units_in_stock, discontinued)
VALUES (102, 'Wasabi Paste', 2, 2, 22.00, 50, 0);
INSERT INTO mmr_products (product_id, product_name, supplier_id, category_id, unit_price, units_in_stock, discontinued)
VALUES (103, 'Frozen Edamame', 2, 6, 8.75, 200, 0);

-- New customers
INSERT INTO mmr_customers VALUES
    ('TSTER', 'Test Corp',      'Bob Tester',  'Austin',   'USA',     '1-555-9100');
INSERT INTO mmr_customers VALUES
    ('DEVOP', 'DevOps Ltd',     'Alice Dev',   'Seattle',  'USA',     '1-555-9200');
INSERT INTO mmr_customers VALUES
    ('BERLI', 'Berlin Trading', 'Franz Meier', 'Berlin',   'Germany', '49-555-9300');

-- New orders
INSERT INTO mmr_orders VALUES
    (2001, 'TSTER', 1, '2025-04-01', 'USA',     45.50);
INSERT INTO mmr_orders VALUES
    (2002, 'DEVOP', 3, '2025-04-02', 'USA',    120.75);
INSERT INTO mmr_orders VALUES
    (2003, 'BERLI', 5, '2025-04-03', 'Germany', 88.00);

-- New order details
INSERT INTO mmr_order_details VALUES (2001, 101, 15.50, 10, 0);
INSERT INTO mmr_order_details VALUES (2001, 102, 22.00, 5, 0.05);
INSERT INTO mmr_order_details VALUES (2002, 103, 8.75, 50, 0.10);
INSERT INTO mmr_order_details VALUES (2002, 1, 18.00, 20, 0);
INSERT INTO mmr_order_details VALUES (2003, 102, 22.00, 15, 0.05);
INSERT INTO mmr_order_details VALUES (2003, 101, 15.50, 30, 0);


-- ===================== 2. UPDATES part 1 (modify) =====================

-- Employee title changes
UPDATE mmr_employees SET title = 'Senior Sales Rep' WHERE employee_id = 1;

-- Customer info changes
UPDATE mmr_customers SET phone = '1-555-9101' WHERE customer_id = 'TSTER';
UPDATE mmr_customers SET contact_name = 'Alice Developer' WHERE customer_id = 'DEVOP';

-- Product price changes
UPDATE mmr_products SET unit_price = 19.00 WHERE product_id = 1;
UPDATE mmr_products SET unit_price = 22.00 WHERE product_id = 2;
UPDATE mmr_products SET unit_price = 11.00 WHERE product_id = 3;
UPDATE mmr_products SET units_in_stock = units_in_stock + 10 WHERE discontinued = 0 AND units_in_stock < 20;

-- Order updates
UPDATE mmr_orders SET freight = 50.00 WHERE order_id = 2001;
UPDATE mmr_orders SET freight = 130.00 WHERE order_id = 2002;
UPDATE mmr_orders SET ship_country = 'DE' WHERE order_id = 2003;

-- Supplier update
UPDATE mmr_suppliers SET phone = '1-555-0501' WHERE supplier_id = 5;


-- ===================== 3. SELECTS =====================

-- Full table scans
SELECT * FROM mmr_customers;
SELECT * FROM mmr_products;
SELECT * FROM mmr_orders LIMIT 50;
SELECT * FROM mmr_employees;
SELECT * FROM mmr_categories;
SELECT * FROM mmr_suppliers;
SELECT * FROM mmr_regions;
SELECT * FROM mmr_order_details;

-- Filtered selects
SELECT * FROM mmr_customers WHERE country = 'Germany';
SELECT * FROM mmr_customers WHERE country = 'USA';
SELECT * FROM mmr_customers WHERE city = 'Berlin';
SELECT * FROM mmr_products WHERE discontinued = 0;
SELECT * FROM mmr_products WHERE unit_price > 20.0;
SELECT * FROM mmr_products WHERE units_in_stock < 10;
SELECT * FROM mmr_products WHERE category_id = 1;
SELECT * FROM mmr_orders WHERE ship_country = 'France';
SELECT * FROM mmr_orders WHERE freight > 100;
SELECT * FROM mmr_orders WHERE order_date > '2025-02-01';
SELECT * FROM mmr_employees WHERE title = 'Sales Rep';
SELECT * FROM mmr_suppliers WHERE country = 'Germany';

-- Aggregate queries
SELECT country, count(*) AS customer_count FROM mmr_customers GROUP BY country ORDER BY customer_count DESC;
SELECT category_id, count(*) AS product_count, avg(unit_price) AS avg_price FROM mmr_products GROUP BY category_id;
SELECT employee_id, count(*) AS order_count FROM mmr_orders GROUP BY employee_id ORDER BY order_count DESC;
SELECT ship_country, count(*) AS orders, sum(freight) AS total_freight FROM mmr_orders GROUP BY ship_country ORDER BY total_freight DESC;
SELECT customer_id, count(*) AS orders FROM mmr_orders GROUP BY customer_id HAVING count(*) > 3;
SELECT p.product_name, sum(od.quantity) AS total_qty
    FROM mmr_order_details od JOIN mmr_products p ON od.product_id = p.product_id
    GROUP BY p.product_name ORDER BY total_qty DESC LIMIT 10;
SELECT DATE_TRUNC('month', order_date) AS month, count(*) AS orders
    FROM mmr_orders GROUP BY month ORDER BY month;
SELECT country, count(*) AS supplier_count FROM mmr_suppliers GROUP BY country ORDER BY supplier_count DESC;

-- Join queries
SELECT o.order_id, c.company_name, o.order_date, o.freight
    FROM mmr_orders o JOIN mmr_customers c ON o.customer_id = c.customer_id
    WHERE o.freight > 50 ORDER BY o.freight DESC LIMIT 20;

SELECT o.order_id, e.first_name || ' ' || e.last_name AS employee, c.company_name
    FROM mmr_orders o
    JOIN mmr_employees e ON o.employee_id = e.employee_id
    JOIN mmr_customers c ON o.customer_id = c.customer_id
    WHERE o.order_date > '2025-01-15'
    ORDER BY o.order_date DESC LIMIT 25;

SELECT od.order_id, p.product_name, od.quantity, od.unit_price, od.discount,
       (od.quantity * od.unit_price * (1 - od.discount)) AS line_total
    FROM mmr_order_details od
    JOIN mmr_products p ON od.product_id = p.product_id
    WHERE od.order_id = 1001;

SELECT p.product_name, c.category_name, s.company_name AS supplier
    FROM mmr_products p
    JOIN mmr_categories c ON p.category_id = c.category_id
    JOIN mmr_suppliers s ON p.supplier_id = s.supplier_id
    ORDER BY c.category_name, p.product_name;

SELECT c.company_name, count(o.order_id) AS order_count,
       sum(o.freight) AS total_freight,
       avg(o.freight) AS avg_freight
    FROM mmr_customers c
    LEFT JOIN mmr_orders o ON c.customer_id = o.customer_id
    GROUP BY c.company_name
    ORDER BY order_count DESC LIMIT 15;

-- Subquery patterns
SELECT * FROM mmr_products WHERE unit_price > (SELECT avg(unit_price) FROM mmr_products);
SELECT * FROM mmr_customers WHERE customer_id IN (SELECT customer_id FROM mmr_orders WHERE freight > 200);
SELECT p.product_name, p.unit_price,
       (SELECT avg(unit_price) FROM mmr_products p2 WHERE p2.category_id = p.category_id) AS category_avg
    FROM mmr_products p ORDER BY p.category_id, p.unit_price DESC;

-- Window functions
SELECT order_id, customer_id, order_date, freight,
       row_number() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_seq,
       sum(freight) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_freight
    FROM mmr_orders ORDER BY customer_id, order_date LIMIT 50;

SELECT product_name, category_id, unit_price,
       rank() OVER (PARTITION BY category_id ORDER BY unit_price DESC) AS price_rank
    FROM mmr_products;

-- CASE expressions
SELECT product_name, unit_price,
  CASE
    WHEN unit_price < 10 THEN 'Budget'
    WHEN unit_price < 30 THEN 'Standard'
    WHEN unit_price < 60 THEN 'Premium'
    ELSE 'Luxury'
  END AS price_tier
FROM mmr_products ORDER BY unit_price;

-- CTEs
WITH monthly_orders AS (
    SELECT DATE_TRUNC('month', order_date) AS month,
           count(*) AS num_orders,
           sum(freight) AS total_freight
    FROM mmr_orders GROUP BY month
)
SELECT month, num_orders, total_freight,
       avg(num_orders) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
FROM monthly_orders ORDER BY month;

WITH top_customers AS (
    SELECT customer_id, count(*) AS orders
    FROM mmr_orders GROUP BY customer_id ORDER BY orders DESC LIMIT 5
)
SELECT c.company_name, tc.orders, c.country
FROM top_customers tc JOIN mmr_customers c ON tc.customer_id = c.customer_id;

-- DISTINCT and EXISTS
SELECT DISTINCT ship_country FROM mmr_orders ORDER BY ship_country;
SELECT DISTINCT category_id FROM mmr_products;
SELECT * FROM mmr_customers c WHERE EXISTS (SELECT 1 FROM mmr_orders o WHERE o.customer_id = c.customer_id AND o.freight > 300);

-- UNION
SELECT 'customer' AS entity_type, company_name, city, country FROM mmr_customers WHERE country = 'USA'
UNION ALL
SELECT 'supplier', company_name, city, country FROM mmr_suppliers WHERE country = 'USA'
ORDER BY country, entity_type;

-- Count checks
SELECT count(*) FROM mmr_orders;
SELECT count(*) FROM mmr_order_details;
SELECT count(*) FROM mmr_customers;
SELECT count(*) FROM mmr_products;
SELECT count(DISTINCT customer_id) FROM mmr_orders;
SELECT count(DISTINCT ship_country) FROM mmr_orders;

-- Range/batch selects
SELECT * FROM mmr_orders WHERE order_id BETWEEN 1010 AND 1020;
SELECT * FROM mmr_order_details WHERE order_id BETWEEN 1010 AND 1020;
SELECT p.product_name, p.unit_price FROM mmr_products p WHERE p.units_in_stock > 0 ORDER BY p.unit_price DESC;
SELECT s.company_name, s.country FROM mmr_suppliers s ORDER BY s.country;
SELECT c.company_name, c.city FROM mmr_customers c WHERE c.country IN ('USA', 'UK', 'Germany') ORDER BY c.country, c.city;
SELECT DISTINCT e.first_name, e.last_name, e.title FROM mmr_employees e ORDER BY e.last_name;
SELECT r.region_name FROM mmr_regions r ORDER BY r.region_name;
SELECT c.category_name, count(p.product_id) FROM mmr_categories c LEFT JOIN mmr_products p ON c.category_id = p.category_id GROUP BY c.category_name ORDER BY count DESC;

-- Repeat hot queries to build call counts
SELECT * FROM mmr_customers WHERE country = 'Germany';
SELECT * FROM mmr_customers WHERE country = 'USA';
SELECT * FROM mmr_products WHERE discontinued = 0;
SELECT count(*) FROM mmr_orders;
SELECT count(*) FROM mmr_order_details;
SELECT o.order_id, c.company_name, o.freight FROM mmr_orders o JOIN mmr_customers c ON o.customer_id = c.customer_id WHERE o.freight > 50 ORDER BY o.freight DESC LIMIT 20;
SELECT country, count(*) FROM mmr_customers GROUP BY country ORDER BY count DESC;
SELECT * FROM mmr_products WHERE unit_price > 20.0;
SELECT * FROM mmr_orders WHERE ship_country = 'France';
SELECT * FROM mmr_employees;


-- ===================== 4. UPDATES part 2 (revert) =====================

-- Revert employee title
UPDATE mmr_employees SET title = 'Sales Rep' WHERE employee_id = 1;

-- Revert customer info
UPDATE mmr_customers SET phone = '1-555-9100' WHERE customer_id = 'TSTER';
UPDATE mmr_customers SET contact_name = 'Alice Dev' WHERE customer_id = 'DEVOP';

-- Revert product prices
UPDATE mmr_products SET unit_price = 18.00 WHERE product_id = 1;
UPDATE mmr_products SET unit_price = 21.35 WHERE product_id = 2;
UPDATE mmr_products SET unit_price = 10.00 WHERE product_id = 3;
UPDATE mmr_products SET units_in_stock = units_in_stock - 10 WHERE discontinued = 0 AND units_in_stock >= 20 AND units_in_stock <= 30;

-- Revert order updates
UPDATE mmr_orders SET freight = 45.50 WHERE order_id = 2001;
UPDATE mmr_orders SET freight = 120.75 WHERE order_id = 2002;
UPDATE mmr_orders SET ship_country = 'Germany' WHERE order_id = 2003;

-- Revert supplier
UPDATE mmr_suppliers SET phone = '1-555-0500' WHERE supplier_id = 5;


-- ===================== 5. DELETES (remove all inserts) =====================

-- Order details first (FK dependency)
DELETE FROM mmr_order_details WHERE order_id IN (2001, 2002, 2003);

-- Orders
DELETE FROM mmr_orders WHERE order_id IN (2001, 2002, 2003);

-- Customers
DELETE FROM mmr_customers WHERE customer_id IN ('TSTER', 'DEVOP', 'BERLI');

-- Products
DELETE FROM mmr_products WHERE product_id IN (101, 102, 103);


-- ===================== 6. PATTERN STATEMENTS =====================

-- TRUNCATE CASCADE pattern (triggers truncate_cascade check)
TRUNCATE TABLE mmr_truncate_target CASCADE;

-- DDL statements (triggers ddl_statements check)
CREATE INDEX IF NOT EXISTS idx_mmr_orders_customer ON mmr_orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_mmr_orders_employee ON mmr_orders (employee_id);
CREATE INDEX IF NOT EXISTS idx_mmr_orders_ship ON mmr_orders (ship_country);
CREATE INDEX IF NOT EXISTS idx_mmr_details_product ON mmr_order_details (product_id);
CREATE INDEX IF NOT EXISTS idx_mmr_products_category ON mmr_products (category_id);
CREATE INDEX IF NOT EXISTS idx_mmr_products_supplier ON mmr_products (supplier_id);

-- Advisory lock usage (triggers advisory_locks check)
SELECT pg_advisory_lock(12345);
SELECT pg_advisory_unlock(12345);
SELECT pg_try_advisory_lock(67890);
SELECT pg_advisory_unlock(67890);

-- Temp table creation (triggers temp_table_queries check)
CREATE TEMP TABLE IF NOT EXISTS tmp_mmr_high_value AS
SELECT o.order_id, o.customer_id, sum(od.quantity * od.unit_price) AS total_value
FROM mmr_orders o JOIN mmr_order_details od ON o.order_id = od.order_id
GROUP BY o.order_id, o.customer_id
HAVING sum(od.quantity * od.unit_price) > 500;

SELECT * FROM tmp_mmr_high_value ORDER BY total_value DESC LIMIT 10;
DROP TABLE IF EXISTS tmp_mmr_high_value;

-- Concurrent index creation (triggers concurrent_indexes check)
-- Must be run outside a transaction block
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mmr_concurrency_test ON mmr_no_pk (col1);
DROP INDEX CONCURRENTLY IF EXISTS idx_mmr_concurrency_test;
