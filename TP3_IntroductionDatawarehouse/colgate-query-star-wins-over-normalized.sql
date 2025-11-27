-- This scrip illustrates the performance improvement of using a Star vs Normalized schema.
-- It considers the case of 1M sales facts : this is very small for a datawarehouse, but more manageable by the available machines.
-- It considers the case of the colgate query presented in http://philip.greenspun.com/sql/data-warehousing.html
-- =========================================================

SET SERVEROUTPUT ON
SET FEEDBACK ON
SET TIMING OFF
SET DEFINE OFF

PROMPT Cleaning up old schema objects...

BEGIN
  -- Drop indexes
  FOR i IN (
    SELECT index_name FROM user_indexes
    WHERE table_name IN (
      'MANUFACTURERS_NORM','PRODUCT_CATEGORIES_NORM','PRODUCTS_NORM',
      'CITIES_NORM','STORES_NORM','SALES_NORM',
      'PRODUCT_DIM','CITY_DIM','SALES_STAR'
    )
  ) LOOP
    BEGIN EXECUTE IMMEDIATE 'DROP INDEX '||i.index_name; EXCEPTION WHEN OTHERS THEN NULL; END;
  END LOOP;

  -- Drop tables
  FOR t IN (
    SELECT table_name FROM user_tables
    WHERE table_name IN (
      'SALES_NORM','STORES_NORM','CITIES_NORM','PRODUCTS_NORM',
      'PRODUCT_CATEGORIES_NORM','MANUFACTURERS_NORM',
      'SALES_STAR','PRODUCT_DIM','CITY_DIM'
    )
  ) LOOP
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE '||t.table_name||' CASCADE CONSTRAINTS PURGE';
    EXCEPTION WHEN OTHERS THEN NULL; END;
  END LOOP;
END;
/
PROMPT Cleanup complete.

-- ======================
-- NORMALIZED SCHEMA
-- ======================
CREATE TABLE MANUFACTURERS_NORM (
  MANUFACTURER_ID NUMBER PRIMARY KEY,
  MANUFACTURER_NAME VARCHAR2(100) UNIQUE NOT NULL
);

CREATE TABLE PRODUCT_CATEGORIES_NORM (
  PRODUCT_CATEGORY_ID NUMBER PRIMARY KEY,
  PRODUCT_CATEGORY_NAME VARCHAR2(100) UNIQUE NOT NULL
);

CREATE TABLE PRODUCTS_NORM (
  PRODUCT_ID NUMBER PRIMARY KEY,
  PRODUCT_NAME VARCHAR2(200) NOT NULL,
  PRODUCT_CATEGORY_ID NUMBER NOT NULL,
  MANUFACTURER_ID NUMBER NOT NULL,
  CONSTRAINT FK_PROD_CAT_NORM FOREIGN KEY (PRODUCT_CATEGORY_ID)
    REFERENCES PRODUCT_CATEGORIES_NORM(PRODUCT_CATEGORY_ID),
  CONSTRAINT FK_PROD_MANU_NORM FOREIGN KEY (MANUFACTURER_ID)
    REFERENCES MANUFACTURERS_NORM(MANUFACTURER_ID)
);

CREATE TABLE CITIES_NORM (
  CITY_ID NUMBER PRIMARY KEY,
  CITY_NAME VARCHAR2(200) NOT NULL,
  POPULATION NUMBER NOT NULL
);

CREATE TABLE STORES_NORM (
  STORE_ID NUMBER PRIMARY KEY,
  STORE_NAME VARCHAR2(200) NOT NULL,
  CITY_ID NUMBER NOT NULL,
  CONSTRAINT FK_STORE_CITY_NORM FOREIGN KEY (CITY_ID)
    REFERENCES CITIES_NORM(CITY_ID)
);

CREATE TABLE SALES_NORM (
  SALES_ID NUMBER PRIMARY KEY,
  PRODUCT_ID NUMBER NOT NULL,
  STORE_ID NUMBER NOT NULL,
  QUANTITY_SOLD NUMBER NOT NULL,
  DATE_TIME_OF_SALE DATE NOT NULL,
  CONSTRAINT FK_SALES_PROD_NORM FOREIGN KEY (PRODUCT_ID)
    REFERENCES PRODUCTS_NORM(PRODUCT_ID),
  CONSTRAINT FK_SALES_STORE_NORM FOREIGN KEY (STORE_ID)
    REFERENCES STORES_NORM(STORE_ID)
);

-- ======================
-- STAR / DENORMALIZED SCHEMA
-- ======================
CREATE TABLE PRODUCT_DIM (
  PRODUCT_ID NUMBER PRIMARY KEY, 
  PRODUCT_NAME VARCHAR2(200) NOT NULL,
  MANUFACTURER_NAME VARCHAR2(100) NOT NULL,
  PRODUCT_CATEGORY_NAME VARCHAR2(100) NOT NULL
);

CREATE TABLE CITY_DIM (
  CITY_ID NUMBER PRIMARY KEY,
  CITY_NAME VARCHAR2(200) NOT NULL,
  POPULATION NUMBER NOT NULL
);

            
CREATE TABLE SALES_STAR (
  SALES_ID NUMBER PRIMARY KEY,
  PRODUCT_ID NUMBER NOT NULL,
  CITY_ID NUMBER NOT NULL,
  QUANTITY_SOLD NUMBER NOT NULL,
  DATE_TIME_OF_SALE DATE NOT NULL,
  CONSTRAINT FK_SALES_STAR_PROD FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCT_DIM(PRODUCT_ID),
  CONSTRAINT FK_SALES_STAR_CITY FOREIGN KEY (CITY_ID) REFERENCES CITY_DIM(CITY_ID)
);

-- ======================
-- CONFIG + DATA GENERATION (change that on your taste)
-- ======================
DECLARE
  N_MANUFACTURERS   PLS_INTEGER := 20;
  N_CATEGORIES      PLS_INTEGER := 10;
  N_PRODUCTS        PLS_INTEGER := 5000;
  N_CITIES          PLS_INTEGER := 2000;
  STORES_PER_CITY   PLS_INTEGER := 5;      -- <- lots of stores per city (pressure on normalized join)
  SALES_ROWS        PLS_INTEGER := 1000000; -- <- 1M facts
  YDAY_PCT          PLS_INTEGER := 60;      -- 60% of facts on "yesterday"

  max_store_id NUMBER;
BEGIN
  -- Manufacturers: include Colgate = 1
  INSERT INTO MANUFACTURERS_NORM (MANUFACTURER_ID, MANUFACTURER_NAME) VALUES (1, 'Colgate');
  INSERT INTO MANUFACTURERS_NORM (MANUFACTURER_ID, MANUFACTURER_NAME)
  SELECT 1 + LEVEL, 'Mfg_'||TO_CHAR(LEVEL,'FM0000')
  FROM dual CONNECT BY LEVEL <= GREATEST(N_MANUFACTURERS-1,0);

  -- Categories: include toothpaste = 1
  INSERT INTO PRODUCT_CATEGORIES_NORM (PRODUCT_CATEGORY_ID, PRODUCT_CATEGORY_NAME) VALUES (1, 'toothpaste');
  INSERT INTO PRODUCT_CATEGORIES_NORM (PRODUCT_CATEGORY_ID, PRODUCT_CATEGORY_NAME)
  SELECT 1 + LEVEL, 'cat_'||TO_CHAR(LEVEL,'FM0000')
  FROM dual CONNECT BY LEVEL <= GREATEST(N_CATEGORIES-1,0);

  -- Products: cycle manufacturers/categories; some Colgate toothpaste
  INSERT /*+ APPEND */ INTO PRODUCTS_NORM (PRODUCT_ID, PRODUCT_NAME, PRODUCT_CATEGORY_ID, MANUFACTURER_ID)
  SELECT p_id,
         CASE WHEN MOD(p_id,7)=1 THEN 'Colgate SKU_'||TO_CHAR(p_id,'FM000000')
              ELSE 'Prod_'||TO_CHAR(p_id,'FM000000') END,
         1 + MOD(p_id-1, N_CATEGORIES),
         1 + MOD(p_id-1, N_MANUFACTURERS)
  FROM (SELECT LEVEL AS p_id FROM dual CONNECT BY LEVEL <= N_PRODUCTS);
  COMMIT;

  -- Cities: mix small/large
  INSERT /*+ APPEND */ INTO CITIES_NORM (CITY_ID, CITY_NAME, POPULATION)
  SELECT c_id,
         'City_'||TO_CHAR(c_id,'FM000000'),
         CASE WHEN MOD(c_id,5) IN (1,2)
              THEN 10000 + MOD(c_id*7, 28000)     -- small: < 40k
              ELSE  80000 + MOD(c_id*11,1500000)  -- large
         END
  FROM (SELECT LEVEL AS c_id FROM dual CONNECT BY LEVEL <= N_CITIES);
  COMMIT;

  -- Stores: many per city (fan-out)
  INSERT /*+ APPEND */ INTO STORES_NORM (STORE_ID, STORE_NAME, CITY_ID)
  SELECT (c.CITY_ID - 1) * STORES_PER_CITY + s AS store_id,
         'Store_'||TO_CHAR(c.CITY_ID,'FM000000')||'_'||s,
         c.CITY_ID
  FROM CITIES_NORM c
  CROSS JOIN (SELECT LEVEL AS s FROM dual CONNECT BY LEVEL <= STORES_PER_CITY);
  COMMIT;

  SELECT MAX(STORE_ID) INTO max_store_id FROM STORES_NORM;

  -- Sales: hash-map to products/stores, skew to yesterday
  INSERT /*+ APPEND */ INTO SALES_NORM (SALES_ID, PRODUCT_ID, STORE_ID, QUANTITY_SOLD, DATE_TIME_OF_SALE)
  WITH seq AS (SELECT LEVEL AS id FROM dual CONNECT BY LEVEL <= SALES_ROWS)
  SELECT
    s.id AS sales_id,
    1 + MOD(ABS(DBMS_UTILITY.GET_HASH_VALUE('P'||s.id,1,1000000000)), N_PRODUCTS) AS product_id,
    1 + MOD(ABS(DBMS_UTILITY.GET_HASH_VALUE('S'||s.id,1,1000000000)), max_store_id) AS store_id,
    1 + MOD(s.id, 5) AS quantity_sold,
    CASE WHEN MOD(s.id,100) < YDAY_PCT
         THEN TRUNC(SYSDATE) - 1 + NUMTODSINTERVAL(MOD(s.id,24),'HOUR')
         ELSE TRUNC(SYSDATE) - (2 + MOD(s.id,14)) + NUMTODSINTERVAL(MOD(s.id,24),'HOUR')
    END
  FROM seq s;
  COMMIT;
END;
/

-- Load STAR dims/facts from normalized
INSERT /*+ APPEND */ INTO PRODUCT_DIM (PRODUCT_ID, PRODUCT_NAME, MANUFACTURER_NAME, PRODUCT_CATEGORY_NAME)
SELECT p.PRODUCT_ID, p.PRODUCT_NAME, m.MANUFACTURER_NAME, c.PRODUCT_CATEGORY_NAME
FROM PRODUCTS_NORM p
JOIN MANUFACTURERS_NORM m ON p.MANUFACTURER_ID = m.MANUFACTURER_ID
JOIN PRODUCT_CATEGORIES_NORM c ON p.PRODUCT_CATEGORY_ID = c.PRODUCT_CATEGORY_ID;

INSERT /*+ APPEND */ INTO CITY_DIM (CITY_ID, CITY_NAME, POPULATION)
SELECT CITY_ID, CITY_NAME, POPULATION FROM CITIES_NORM;

INSERT /*+ APPEND */ INTO SALES_STAR (SALES_ID, PRODUCT_ID, CITY_ID, QUANTITY_SOLD, DATE_TIME_OF_SALE)
SELECT s.SALES_ID, s.PRODUCT_ID, st.CITY_ID, s.QUANTITY_SOLD, s.DATE_TIME_OF_SALE
FROM SALES_NORM s
JOIN STORES_NORM st ON s.STORE_ID = st.STORE_ID;
COMMIT;

-- ======================
-- Stats
                                                                                                                      -- ======================
BEGIN
  FOR t IN (
    SELECT table_name FROM user_tables
    WHERE table_name IN ('MANUFACTURERS_NORM','PRODUCT_CATEGORIES_NORM','PRODUCTS_NORM',
                         'CITIES_NORM','STORES_NORM','SALES_NORM',
                         'PRODUCT_DIM','CITY_DIM','SALES_STAR')
  ) LOOP
    DBMS_STATS.GATHER_TABLE_STATS(USER, t.table_name, cascade => TRUE);
  END LOOP;
END;
/

-- =========================================================
-- Benchmark: 3 runs each, average time + execution plan
-- =========================================================
PROMPT
PROMPT ============================
PROMPT Running timed benchmark...
PROMPT ============================

DECLARE
  d0 DATE := TRUNC(SYSDATE-1);
  d1 DATE := TRUNC(SYSDATE);
  v_sum NUMBER;
  t1 INTEGER; t2 INTEGER;
  n NUMBER := 3;           -- repetitions
  avg_norm NUMBER := 0;
  avg_star NUMBER := 0;

  PROCEDURE show_plan IS BEGIN
    FOR r IN (SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(NULL,NULL,'ALLSTATS LAST +OUTLINE'))) LOOP
      DBMS_OUTPUT.PUT_LINE(r.plan_table_output);
    END LOOP;
  END;
BEGIN
  DBMS_OUTPUT.PUT_LINE('--- Normalized schema (n timed runs) ---');
  FOR i IN 1..n LOOP
    -- warm-up each loop to stabilize
    SELECT /*+ gather_plan_statistics */
           SUM(s.QUANTITY_SOLD) INTO v_sum
    FROM SALES_NORM s
    JOIN PRODUCTS_NORM p  ON s.PRODUCT_ID = p.PRODUCT_ID
    JOIN PRODUCT_CATEGORIES_NORM pc ON p.PRODUCT_CATEGORY_ID = pc.PRODUCT_CATEGORY_ID
    JOIN MANUFACTURERS_NORM m ON p.MANUFACTURER_ID = m.MANUFACTURER_ID
    JOIN STORES_NORM st ON s.STORE_ID = st.STORE_ID
    JOIN CITIES_NORM c  ON st.CITY_ID  = c.CITY_ID
    WHERE m.MANUFACTURER_NAME = 'Colgate'
      AND pc.PRODUCT_CATEGORY_NAME = 'toothpaste'
      AND c.POPULATION < 40000
      AND s.DATE_TIME_OF_SALE >= d0 AND s.DATE_TIME_OF_SALE < d1;

    t1 := DBMS_UTILITY.GET_TIME;
    SELECT /*+ gather_plan_statistics */
           SUM(s.QUANTITY_SOLD) INTO v_sum
    FROM SALES_NORM s
    JOIN PRODUCTS_NORM p  ON s.PRODUCT_ID = p.PRODUCT_ID
    JOIN PRODUCT_CATEGORIES_NORM pc ON p.PRODUCT_CATEGORY_ID = pc.PRODUCT_CATEGORY_ID
    JOIN MANUFACTURERS_NORM m ON p.MANUFACTURER_ID = m.MANUFACTURER_ID
    JOIN STORES_NORM st ON s.STORE_ID = st.STORE_ID
    JOIN CITIES_NORM c  ON st.CITY_ID  = c.CITY_ID
    WHERE m.MANUFACTURER_NAME = 'Colgate'
      AND pc.PRODUCT_CATEGORY_NAME = 'toothpaste'
      AND c.POPULATION < 40000
      AND s.DATE_TIME_OF_SALE >= d0 AND s.DATE_TIME_OF_SALE < d1;
    t2 := DBMS_UTILITY.GET_TIME;

    DBMS_OUTPUT.PUT_LINE('Run #'||i||' SUM='||v_sum||' ; elapsed='||TO_CHAR((t2-t1)/100,'FM9990D00')||'s');
    avg_norm := avg_norm + (t2 - t1);
  END LOOP;
  avg_norm := (avg_norm / n) / 100; -- seconds
  DBMS_OUTPUT.PUT_LINE('Normalized AVG elapsed = '||TO_CHAR(avg_norm,'FM9990D00')||'s');
  DBMS_OUTPUT.PUT_LINE('Execution plan (last run):');
  show_plan;

  DBMS_OUTPUT.PUT_LINE(CHR(10)||'--- Star schema (n timed runs) ---');
  FOR i IN 1..n LOOP
    -- warm-up
    SELECT /*+ gather_plan_statistics */
           SUM(s.QUANTITY_SOLD) INTO v_sum
    FROM SALES_STAR s
    JOIN PRODUCT_DIM pd ON s.PRODUCT_ID = pd.PRODUCT_ID
    JOIN CITY_DIM cd    ON s.CITY_ID    = cd.CITY_ID
    WHERE pd.MANUFACTURER_NAME = 'Colgate'
      AND pd.PRODUCT_CATEGORY_NAME = 'toothpaste'
      AND cd.POPULATION < 40000
      AND s.DATE_TIME_OF_SALE >= d0 AND s.DATE_TIME_OF_SALE < d1;

    t1 := DBMS_UTILITY.GET_TIME;
    SELECT /*+ gather_plan_statistics */
           SUM(s.QUANTITY_SOLD) INTO v_sum
    FROM SALES_STAR s
    JOIN PRODUCT_DIM pd ON s.PRODUCT_ID = pd.PRODUCT_ID
    JOIN CITY_DIM cd    ON s.CITY_ID    = cd.CITY_ID
    WHERE pd.MANUFACTURER_NAME = 'Colgate'
      AND pd.PRODUCT_CATEGORY_NAME = 'toothpaste'
      AND cd.POPULATION < 40000
      AND s.DATE_TIME_OF_SALE >= d0 AND s.DATE_TIME_OF_SALE < d1;
    t2 := DBMS_UTILITY.GET_TIME;

    DBMS_OUTPUT.PUT_LINE('Run #'||i||' SUM='||v_sum||' ; elapsed='||TO_CHAR((t2-t1)/100,'FM9990D00')||'s');
    avg_star := avg_star + (t2 - t1);
  END LOOP;
  avg_star := (avg_star / n) / 100; -- seconds
  DBMS_OUTPUT.PUT_LINE('Star AVG elapsed = '||TO_CHAR(avg_star,'FM9990D00')||'s');
  DBMS_OUTPUT.PUT_LINE('Execution plan (last run):');
  show_plan;

  DBMS_OUTPUT.PUT_LINE(CHR(10)||'=== RESULT ===');
  DBMS_OUTPUT.PUT_LINE('Normalized AVG: '||TO_CHAR(avg_norm,'FM9990D00')||'s');
  DBMS_OUTPUT.PUT_LINE('Star       AVG: '||TO_CHAR(avg_star,'FM9990D00')||'s');
  IF avg_star < avg_norm THEN
    DBMS_OUTPUT.PUT_LINE('=> Star schema faster by '||
      TO_CHAR( (avg_norm-avg_star)/avg_norm * 100, 'FM990D00')||'% (approx).');
  ELSE
    DBMS_OUTPUT.PUT_LINE('=> Star schema was not faster. Check your data skew. Add bitmap indexes.');
  END IF;
END;
/

                                                                                                                      