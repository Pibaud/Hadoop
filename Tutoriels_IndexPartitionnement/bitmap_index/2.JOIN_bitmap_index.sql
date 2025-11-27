
-- Cleanup
BEGIN
    EXECUTE IMMEDIATE 'DROP INDEX idx_bj_product_id';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE Fact_table_sales CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE Dim_table_product CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

-- Step 1: 
-- Create the Dimension Table 
CREATE TABLE Dim_table_product (
    product_id VARCHAR2(10),
    name VARCHAR2(50),
    price NUMBER
);

-- Create the Fact Table 
CREATE TABLE Fact_table_sales (
    sale_id VARCHAR2(10),
    product_id VARCHAR2(10),
    store_id VARCHAR2(10),
    sale_date DATE,
    amount NUMBER
);

Step 2: Populate the tables
-- Populate Dimension Table with 1000 Rows
BEGIN
    FOR i IN 1..1000 LOOP
        INSERT INTO Dim_table_product (product_id, name, price)
        VALUES ('p' || TO_CHAR(i), 'product_' || TO_CHAR(i), MOD(i, 100) + 10);
    END LOOP;
    COMMIT;
END;
/

-- Populate Fact Table with 10,000 Rows and Restrict Products to Even Numbers
BEGIN
    FOR i IN 1..10000 LOOP
        INSERT INTO Fact_table_sales (sale_id, product_id, store_id, sale_date, amount)
        VALUES (
            's' || TO_CHAR(i), 
            'p' || TO_CHAR(2 * MOD(i, 500) + 2), -- Only even product IDs, for data variability
            'store_' || TO_CHAR(MOD(i, 50) + 1), 
            SYSDATE, 
            MOD(i, 500) + 50
        );
    END LOOP;
    COMMIT;
END;
/

-- Step 3: Add Primary Keys
-- Primary Key for Dim_table_product
ALTER TABLE Dim_table_product ADD CONSTRAINT dim_product_pk PRIMARY KEY (product_id);

-- Step 4: Create the Bitmap Join Index on `product_id`
CREATE BITMAP INDEX idx_bj_product_id
ON Fact_table_sales(d.product_id)
FROM Fact_table_sales f, Dim_table_product d
WHERE f.product_id = d.product_id;

-- Step 5: Query execution uses the Bitmap Join Index to compute the result. Note that the index makes the access Dim_table_product superfluous. 
EXPLAIN PLAN FOR
SELECT /*+ INDEX(f idx_bj_product_id) */
       f.sale_id
FROM Fact_table_sales f
JOIN Dim_table_product d
  ON f.product_id = d.product_id
WHERE d.product_id = 'p500';

-- Display Execution Plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);


-- Step 6: Run another query ; force index usage
EXPLAIN PLAN FOR
SELECT /*+ INDEX(f idx_bj_product_id) */
       f.sale_id
FROM Fact_table_sales f
JOIN Dim_table_product d
  ON f.product_id = d.product_id
WHERE d.price = 50;

-- Display Execution Plan Again
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- Step 6: Re-Run the query ; if we do not force index usage this may be neglected on small data. Indeed, the oracle optimizer may estimate a better way to compute the join.
EXPLAIN PLAN FOR
SELECT 
       f.sale_id
FROM Fact_table_sales f
JOIN Dim_table_product d
  ON f.product_id = d.product_id
WHERE d.price = 50;

-- Display Execution Plan Again
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

