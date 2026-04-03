-- =========================================
-- PHASE 3: SQL ETL PIPELINE (STRUCTURED)
-- =========================================

-- 1. EXTRACT (Load Raw Data)
CREATE OR REPLACE TEMP VIEW customers
USING csv
OPTIONS (
  path "/samples/customers.csv",
  header "true",
  inferSchema "true"
);

CREATE OR REPLACE TEMP VIEW sales
USING csv
OPTIONS (
  path "/samples/sales.csv",
  header "true",
  inferSchema "true"
);


-- 2. CLEANING LAYER (DATA QUALITY FIX)
CREATE OR REPLACE TEMP VIEW sales_clean AS
SELECT *
FROM sales
WHERE customer_id IS NOT NULL
  AND sale_date IS NOT NULL
  AND quantity IS NOT NULL
  AND total_amount IS NOT NULL
  AND quantity > 0
  AND total_amount > 0;

CREATE OR REPLACE TEMP VIEW customers_clean AS
SELECT *
FROM customers
WHERE customer_id IS NOT NULL
  AND city IS NOT NULL;


-- 3. JOIN LAYER (DATA INTEGRATION)
CREATE OR REPLACE TEMP VIEW joined AS
SELECT 
    s.customer_id,
    c.city,
    s.sale_date,
    s.quantity,
    s.total_amount
FROM sales_clean s
LEFT JOIN customers_clean c
ON s.customer_id = c.customer_id;


-- 4. BUSINESS LOGIC LAYER


-- 4.1 Daily Sales (NO JOIN NEEDED)
CREATE OR REPLACE TEMP VIEW daily_sales AS
SELECT 
    sale_date,
    SUM(total_amount) AS daily_sales
FROM sales_clean
GROUP BY sale_date;


-- 4.2 City-wise Revenue (USES JOINED DATA)
CREATE OR REPLACE TEMP VIEW city_revenue AS
SELECT 
    city,
    SUM(total_amount) AS revenue
FROM joined
GROUP BY city;


-- 4.3 Repeat Customers (>2 Orders)
CREATE OR REPLACE TEMP VIEW repeat_customers AS
SELECT 
    customer_id,
    COUNT(*) AS order_count
FROM sales_clean
GROUP BY customer_id
HAVING COUNT(*) > 2;


-- 4.4 Highest Spending Customer per City
CREATE OR REPLACE TEMP VIEW customer_spend AS
SELECT 
    city,
    customer_id,
    SUM(total_amount) AS total_spent
FROM joined
GROUP BY city, customer_id;


CREATE OR REPLACE TEMP VIEW top_customers AS
SELECT city, customer_id, total_spent
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY city ORDER BY total_spent DESC) AS rn
    FROM customer_spend
) t
WHERE rn = 1;


-- 4.5 FINAL REPORTING TABLE
CREATE OR REPLACE TEMP VIEW final_report AS
SELECT 
    customer_id,
    city,
    SUM(total_amount) AS total_spend,
    COUNT(*) AS order_count
FROM joined
GROUP BY customer_id, city;


-- 5. OUTPUT LAYER (VERIFY RESULTS)

SELECT * FROM daily_sales;
SELECT * FROM city_revenue;
SELECT * FROM repeat_customers;
SELECT * FROM top_customers;
SELECT * FROM final_report;
