
-- BitMap Indexes (no joins)
BEGIN
    EXECUTE IMMEDIATE 'DROP INDEX idx_rating';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP INDEX idx_gender';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE Customers CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

-- Step 1: Create the Customers table
CREATE TABLE Customers (
    custid NUMBER PRIMARY KEY,
    name VARCHAR2(50),
    gender CHAR(1),
    rating NUMBER
);

-- Step 2: Insert sample data
INSERT INTO Customers (custid, name, gender, rating) VALUES (112, 'Joe', 'M', 3);
INSERT INTO Customers (custid, name, gender, rating) VALUES (115, 'Ram', 'M', 5);
INSERT INTO Customers (custid, name, gender, rating) VALUES (119, 'Sue', 'F', 5);
INSERT INTO Customers (custid, name, gender, rating) VALUES (116, 'Woo', 'M', 4);
INSERT INTO Customers (custid, name, gender, rating) VALUES (120, 'Ana', 'F', 3);
INSERT INTO Customers (custid, name, gender, rating) VALUES (121, 'Tom', 'M', 1);
INSERT INTO Customers (custid, name, gender, rating) VALUES (122, 'Eve', 'F', 2);

COMMIT;

-- Step 3: Create bitmap indexes
CREATE BITMAP INDEX idx_rating ON Customers (rating);
CREATE BITMAP INDEX idx_gender ON Customers (gender);

-- Step 4: Query 
EXPLAIN PLAN FOR
SELECT 
    custid, name, gender, rating
FROM Customers
WHERE rating = 5 AND gender = 'M';

-- Display the execution plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- Step 5: suggest use of idx_rating index ; the optimizer refuses our hint as it finds a better strategy.
EXPLAIN PLAN FOR
SELECT /*+ INDEX(f idx_rating) */
    custid, name, gender, rating
FROM Customers
WHERE rating = 5 AND gender = 'M';

-- Display the execution plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- Step 4: Query using both indexes
EXPLAIN PLAN FOR
SELECT /*+ INDEX(f idx_gender) */
    custid, name, gender, rating
FROM Customers
WHERE rating = 5 AND gender = 'M';

-- Display the execution plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
