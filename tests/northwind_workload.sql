-- =============================================================
-- Northwind test workload for pg_stat_statements population
-- IDEMPOTENT: all inserts are deleted, all updates are reverted.
-- After running, the database is in its original state.
--
-- Structure:
--   1. INSERTs (add test rows)
--   2. UPDATEs part 1 (modify existing data)
--   3. SELECTs (bulk of the workload)
--   4. UPDATEs part 2 (revert modifications)
--   5. DELETEs (remove all inserted rows)
--   6. Pattern statements (DDL, advisory locks, temp tables, truncate)
-- =============================================================

-- ===================== 1. INSERTS =====================

-- New categories
INSERT INTO categories (category_id, category_name, description) VALUES (9, 'Organic', 'Organic and natural food products');
INSERT INTO categories (category_id, category_name, description) VALUES (10, 'Frozen', 'Frozen food and ice cream products');
INSERT INTO categories (category_id, category_name, description) VALUES (11, 'Snacks', 'Chips, crackers, and snack foods');

-- New shippers
INSERT INTO shippers (shipper_id, company_name, phone) VALUES (100, 'DHL Express', '(503) 555-1234');
INSERT INTO shippers (shipper_id, company_name, phone) VALUES (101, 'FedEx Ground', '(503) 555-5678');

-- New suppliers
INSERT INTO suppliers (supplier_id, company_name, contact_name, contact_title, address, city, country, phone)
VALUES (30, 'Pacific Farms', 'John Smith', 'Sales Manager', '123 Farm Road', 'Portland', 'USA', '(503) 555-9999');
INSERT INTO suppliers (supplier_id, company_name, contact_name, contact_title, address, city, country, phone)
VALUES (31, 'Alpine Dairy Co', 'Maria Schmidt', 'Owner', '45 Mountain Ave', 'Zurich', 'Switzerland', '41-100-555-1234');

-- New products
INSERT INTO products (product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued)
VALUES (78, 'Organic Quinoa', 30, 9, '12 - 500g bags', 15.50, 100, 0, 25, 0);
INSERT INTO products (product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued)
VALUES (79, 'Swiss Dark Chocolate', 31, 3, '24 bars', 22.00, 50, 30, 15, 0);
INSERT INTO products (product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued)
VALUES (80, 'Frozen Organic Peas', 30, 10, '20 - 400g bags', 8.75, 200, 0, 50, 0);

-- New customers
INSERT INTO customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone)
VALUES ('TSTCO', 'Test Company Inc', 'Bob Test', 'CTO', '100 Test St', 'Testville', 'WA', '98101', 'USA', '(206) 555-0100');
INSERT INTO customers (customer_id, company_name, contact_name, contact_title, address, city, country, phone)
VALUES ('DEVCO', 'DevOps Corp', 'Alice Dev', 'CEO', '200 Code Ave', 'San Francisco', 'USA', '(415) 555-0200');
INSERT INTO customers (customer_id, company_name, contact_name, contact_title, address, city, country, phone)
VALUES ('EURCO', 'EuroTrade GmbH', 'Hans Mueller', 'Manager', 'Hauptstr 10', 'Berlin', 'Germany', '030-555-0300');

-- New orders
INSERT INTO orders (order_id, customer_id, employee_id, order_date, required_date, ship_via, freight, ship_name, ship_address, ship_city, ship_country)
VALUES (11078, 'TSTCO', 1, '1998-06-01', '1998-06-28', 1, 45.50, 'Test Company Inc', '100 Test St', 'Testville', 'USA');
INSERT INTO orders (order_id, customer_id, employee_id, order_date, required_date, ship_via, freight, ship_name, ship_address, ship_city, ship_country)
VALUES (11079, 'DEVCO', 3, '1998-06-02', '1998-06-30', 2, 120.75, 'DevOps Corp', '200 Code Ave', 'San Francisco', 'USA');
INSERT INTO orders (order_id, customer_id, employee_id, order_date, required_date, ship_via, freight, ship_name, ship_address, ship_city, ship_country)
VALUES (11080, 'EURCO', 5, '1998-06-03', '1998-07-01', 3, 88.00, 'EuroTrade GmbH', 'Hauptstr 10', 'Berlin', 'Germany');

-- New order details
INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES (11078, 78, 15.50, 10, 0);
INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES (11078, 79, 22.00, 5, 0.05);
INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES (11079, 80, 8.75, 50, 0.10);
INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES (11079, 1, 18.00, 20, 0);
INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES (11080, 79, 22.00, 15, 0.05);
INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES (11080, 78, 15.50, 30, 0);

-- New region / territories
INSERT INTO region (region_id, region_description) VALUES (5, 'International');
INSERT INTO territories (territory_id, territory_description, region_id) VALUES ('99001', 'London', 5);
INSERT INTO territories (territory_id, territory_description, region_id) VALUES ('99002', 'Berlin', 5);


-- ===================== 2. UPDATES part 1 (modify) =====================

-- Employee title changes
UPDATE employees SET title = 'Senior Sales Representative' WHERE employee_id = 1;
UPDATE employees SET extension = '5555' WHERE employee_id = 2;

-- Customer info changes
UPDATE customers SET phone = '(206) 555-0101' WHERE customer_id = 'TSTCO';
UPDATE customers SET contact_title = 'VP Engineering' WHERE customer_id = 'DEVCO';

-- Product price/stock changes (on original products, save old values in our heads)
-- product_id=1: original unit_price=18, units_in_stock varies
UPDATE products SET unit_price = 19.00 WHERE product_id = 1;
-- product_id=2: original unit_price=19
UPDATE products SET unit_price = 20.00 WHERE product_id = 2;
-- product_id=3: original unit_price=10
UPDATE products SET unit_price = 11.00 WHERE product_id = 3;
-- Bulk: set reorder_level to 10 where it's currently 0 on non-discontinued items
UPDATE products SET reorder_level = 10 WHERE reorder_level = 0 AND discontinued = 0;

-- Order shipping updates
UPDATE orders SET shipped_date = '1998-06-05' WHERE order_id = 11078;
UPDATE orders SET shipped_date = '1998-06-06' WHERE order_id = 11079;
UPDATE orders SET freight = 93.00 WHERE order_id = 11080;

-- Supplier update
UPDATE suppliers SET phone = '(503) 555-0001' WHERE supplier_id = 30;


-- ===================== 3. SELECTS =====================

-- Simple full table scans
SELECT * FROM customers;
SELECT * FROM products;
SELECT * FROM orders LIMIT 50;
SELECT * FROM employees;
SELECT * FROM categories;
SELECT * FROM shippers;
SELECT * FROM suppliers;
SELECT * FROM region;
SELECT * FROM territories;
SELECT * FROM us_states;

-- Filtered selects
SELECT * FROM customers WHERE country = 'Germany';
SELECT * FROM customers WHERE country = 'USA';
SELECT * FROM customers WHERE city = 'London';
SELECT * FROM customers WHERE contact_title = 'Owner';
SELECT * FROM products WHERE discontinued = 0;
SELECT * FROM products WHERE unit_price > 20.0;
SELECT * FROM products WHERE units_in_stock < 10;
SELECT * FROM products WHERE category_id = 1;
SELECT * FROM orders WHERE shipped_date IS NULL;
SELECT * FROM orders WHERE ship_country = 'France';
SELECT * FROM orders WHERE freight > 100;
SELECT * FROM orders WHERE order_date > '1997-01-01';
SELECT * FROM employees WHERE country = 'USA';
SELECT * FROM suppliers WHERE country = 'UK';
SELECT * FROM us_states WHERE state_region = 'south';

-- Aggregate queries
SELECT country, count(*) AS customer_count FROM customers GROUP BY country ORDER BY customer_count DESC;
SELECT category_id, count(*) AS product_count, avg(unit_price) AS avg_price FROM products GROUP BY category_id;
SELECT employee_id, count(*) AS order_count FROM orders GROUP BY employee_id ORDER BY order_count DESC;
SELECT ship_country, count(*) AS orders, sum(freight) AS total_freight FROM orders GROUP BY ship_country ORDER BY total_freight DESC;
SELECT ship_via, count(*) AS shipments, avg(freight) AS avg_freight FROM orders GROUP BY ship_via;
SELECT customer_id, count(*) AS orders FROM orders GROUP BY customer_id HAVING count(*) > 10;
SELECT p.product_name, sum(od.quantity) AS total_qty FROM order_details od JOIN products p ON od.product_id = p.product_id GROUP BY p.product_name ORDER BY total_qty DESC LIMIT 10;
SELECT DATE_TRUNC('month', order_date) AS month, count(*) AS orders FROM orders GROUP BY month ORDER BY month;
SELECT country, count(*) AS supplier_count FROM suppliers GROUP BY country ORDER BY supplier_count DESC;

-- Join queries
SELECT o.order_id, c.company_name, o.order_date, o.freight
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
WHERE o.freight > 50 ORDER BY o.freight DESC LIMIT 20;

SELECT o.order_id, e.first_name || ' ' || e.last_name AS employee, c.company_name
FROM orders o
JOIN employees e ON o.employee_id = e.employee_id
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date > '1997-06-01'
ORDER BY o.order_date DESC LIMIT 25;

SELECT od.order_id, p.product_name, od.quantity, od.unit_price, od.discount,
       (od.quantity * od.unit_price * (1 - od.discount)) AS line_total
FROM order_details od
JOIN products p ON od.product_id = p.product_id
WHERE od.order_id = 10248;

SELECT p.product_name, c.category_name, s.company_name AS supplier
FROM products p
JOIN categories c ON p.category_id = c.category_id
JOIN suppliers s ON p.supplier_id = s.supplier_id
ORDER BY c.category_name, p.product_name;

SELECT e.first_name || ' ' || e.last_name AS employee,
       t.territory_description, r.region_description
FROM employees e
JOIN employee_territories et ON e.employee_id = et.employee_id
JOIN territories t ON et.territory_id = t.territory_id
JOIN region r ON t.region_id = r.region_id
ORDER BY e.last_name;

SELECT c.company_name, count(o.order_id) AS order_count,
       sum(o.freight) AS total_freight,
       avg(o.freight) AS avg_freight
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.company_name
ORDER BY order_count DESC LIMIT 15;

-- Subquery patterns
SELECT * FROM products WHERE unit_price > (SELECT avg(unit_price) FROM products);
SELECT * FROM customers WHERE customer_id IN (SELECT customer_id FROM orders WHERE freight > 200);
SELECT * FROM employees WHERE employee_id NOT IN (SELECT DISTINCT employee_id FROM orders WHERE order_date > '1998-01-01');
SELECT p.product_name, p.unit_price,
       (SELECT avg(unit_price) FROM products p2 WHERE p2.category_id = p.category_id) AS category_avg
FROM products p ORDER BY p.category_id, p.unit_price DESC;

-- Window functions
SELECT order_id, customer_id, order_date, freight,
       row_number() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_seq,
       sum(freight) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_freight
FROM orders ORDER BY customer_id, order_date LIMIT 50;

SELECT product_name, category_id, unit_price,
       rank() OVER (PARTITION BY category_id ORDER BY unit_price DESC) AS price_rank
FROM products;

-- CASE expressions
SELECT product_name, unit_price,
  CASE
    WHEN unit_price < 10 THEN 'Budget'
    WHEN unit_price < 30 THEN 'Standard'
    WHEN unit_price < 60 THEN 'Premium'
    ELSE 'Luxury'
  END AS price_tier
FROM products ORDER BY unit_price;

-- CTEs
WITH monthly_orders AS (
    SELECT DATE_TRUNC('month', order_date) AS month,
           count(*) AS num_orders,
           sum(freight) AS total_freight
    FROM orders GROUP BY month
)
SELECT month, num_orders, total_freight,
       avg(num_orders) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
FROM monthly_orders ORDER BY month;

WITH top_customers AS (
    SELECT customer_id, count(*) AS orders
    FROM orders GROUP BY customer_id ORDER BY orders DESC LIMIT 5
)
SELECT c.company_name, tc.orders, c.country
FROM top_customers tc JOIN customers c ON tc.customer_id = c.customer_id;

-- DISTINCT and EXISTS
SELECT DISTINCT ship_country FROM orders ORDER BY ship_country;
SELECT DISTINCT category_id FROM products;
SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id AND o.freight > 300);

-- UNION
SELECT 'customer' AS entity_type, company_name, city, country FROM customers WHERE country = 'USA'
UNION ALL
SELECT 'supplier', company_name, city, country FROM suppliers WHERE country = 'USA'
ORDER BY country, entity_type;

-- Count checks
SELECT count(*) FROM orders;
SELECT count(*) FROM order_details;
SELECT count(*) FROM customers;
SELECT count(*) FROM products;
SELECT count(DISTINCT customer_id) FROM orders;
SELECT count(DISTINCT ship_country) FROM orders;

-- Range/batch selects
SELECT * FROM orders WHERE order_id BETWEEN 10300 AND 10400;
SELECT * FROM order_details WHERE order_id BETWEEN 10300 AND 10400;
SELECT p.product_name, p.unit_price FROM products p WHERE p.units_in_stock > 0 ORDER BY p.unit_price DESC;
SELECT s.company_name, s.country FROM suppliers s ORDER BY s.country;
SELECT c.company_name, c.city FROM customers c WHERE c.country IN ('USA', 'UK', 'Germany') ORDER BY c.country, c.city;
SELECT o.order_id, o.order_date, o.freight FROM orders o WHERE o.shipped_date IS NOT NULL ORDER BY o.shipped_date DESC LIMIT 30;
SELECT DISTINCT e.first_name, e.last_name, e.title FROM employees e ORDER BY e.last_name;
SELECT t.territory_description, r.region_description FROM territories t JOIN region r ON t.region_id = r.region_id ORDER BY r.region_description, t.territory_description;
SELECT c.category_name, count(p.product_id) FROM categories c LEFT JOIN products p ON c.category_id = p.category_id GROUP BY c.category_name ORDER BY count DESC;
SELECT * FROM us_states ORDER BY state_name;

-- Repeat hot queries to build call counts
SELECT * FROM customers WHERE country = 'Germany';
SELECT * FROM customers WHERE country = 'USA';
SELECT * FROM products WHERE discontinued = 0;
SELECT count(*) FROM orders;
SELECT count(*) FROM order_details;
SELECT o.order_id, c.company_name, o.freight FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE o.freight > 50 ORDER BY o.freight DESC LIMIT 20;
SELECT country, count(*) FROM customers GROUP BY country ORDER BY count DESC;
SELECT * FROM products WHERE unit_price > 20.0;
SELECT * FROM orders WHERE ship_country = 'France';
SELECT * FROM employees;


-- ===================== 4. UPDATES part 2 (revert) =====================

-- Revert employee title changes
UPDATE employees SET title = 'Sales Representative' WHERE employee_id = 1;
UPDATE employees SET extension = '452' WHERE employee_id = 2;

-- Revert customer info (undo the updates on the inserted rows — will be deleted anyway,
-- but still generates UPDATE stats for those tables)
UPDATE customers SET phone = '(206) 555-0100' WHERE customer_id = 'TSTCO';
UPDATE customers SET contact_title = 'CEO' WHERE customer_id = 'DEVCO';

-- Revert product price changes
UPDATE products SET unit_price = 18.00 WHERE product_id = 1;
UPDATE products SET unit_price = 19.00 WHERE product_id = 2;
UPDATE products SET unit_price = 10.00 WHERE product_id = 3;
-- Revert reorder_level (set back to 0 where we set it to 10)
UPDATE products SET reorder_level = 0 WHERE reorder_level = 10 AND discontinued = 0;

-- Revert order shipping (clear shipped_date on our test orders — will be deleted anyway)
UPDATE orders SET shipped_date = NULL WHERE order_id = 11078;
UPDATE orders SET shipped_date = NULL WHERE order_id = 11079;
UPDATE orders SET freight = 88.00 WHERE order_id = 11080;

-- Revert supplier update
UPDATE suppliers SET phone = '(503) 555-9999' WHERE supplier_id = 30;


-- ===================== 5. DELETES (remove all inserts) =====================

-- Order details first (FK dependency)
DELETE FROM order_details WHERE order_id IN (11078, 11079, 11080);

-- Orders
DELETE FROM orders WHERE order_id IN (11078, 11079, 11080);

-- Customers
DELETE FROM customers WHERE customer_id IN ('TSTCO', 'DEVCO', 'EURCO');

-- Products (must delete before suppliers/categories they reference)
DELETE FROM products WHERE product_id IN (78, 79, 80);

-- Suppliers
DELETE FROM suppliers WHERE supplier_id IN (30, 31);

-- Categories
DELETE FROM categories WHERE category_id IN (9, 10, 11);

-- Shippers
DELETE FROM shippers WHERE shipper_id IN (100, 101);

-- Territories first (FK to region)
DELETE FROM territories WHERE territory_id IN ('99001', '99002');

-- Region
DELETE FROM region WHERE region_id = 5;


-- ===================== 6. PATTERN STATEMENTS =====================

-- TRUNCATE CASCADE pattern (triggers truncate_cascade check)
-- customer_demographics is empty, safe to truncate
TRUNCATE TABLE customer_demographics CASCADE;

-- DDL statements (triggers ddl_statements check)
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_employee ON orders (employee_id);
CREATE INDEX IF NOT EXISTS idx_orders_ship_country ON orders (ship_country);
CREATE INDEX IF NOT EXISTS idx_order_details_product ON order_details (product_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON products (category_id);
CREATE INDEX IF NOT EXISTS idx_products_supplier ON products (supplier_id);

-- Advisory lock usage (triggers advisory_locks check)
SELECT pg_advisory_lock(12345);
SELECT pg_advisory_unlock(12345);
SELECT pg_try_advisory_lock(67890);
SELECT pg_advisory_unlock(67890);

-- Temp table creation (triggers temp_table_queries check)
CREATE TEMP TABLE IF NOT EXISTS tmp_high_value_orders AS
SELECT o.order_id, o.customer_id, sum(od.quantity * od.unit_price) AS total_value
FROM orders o JOIN order_details od ON o.order_id = od.order_id
GROUP BY o.order_id, o.customer_id
HAVING sum(od.quantity * od.unit_price) > 1000;

SELECT * FROM tmp_high_value_orders ORDER BY total_value DESC LIMIT 10;
DROP TABLE IF EXISTS tmp_high_value_orders;
