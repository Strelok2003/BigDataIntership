CREATE DATABASE IF NOT EXISTS EXAMPLE;

USE DATABASE EXAMPLE;

CREATE SCHEMA IF NOT EXISTS EXAMPLE;

USE SCHEMA EXAMPLE;

CREATE OR REPLACE STAGE raw_files
COMMENT = 'Snowflake internal storage for raw files uploaded from local system'
DIRECTORY = ( ENABLE = TRUE );


-- 1. Create STORES Table
CREATE OR REPLACE TABLE STORES (
    STORE_ID VARCHAR(10),
    STORE_NAME VARCHAR(100),
    CITY VARCHAR(50),
    STATE VARCHAR(2),
    POSTAL_CODE VARCHAR(10),
    MANAGER_ID VARCHAR(10)
);


--
CREATE OR REPLACE TABLE DIM_STORES (
    store_id VARCHAR(10),
    store_name VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(2),
    postal_code VARCHAR(10),
    manager_id VARCHAR(10),
    is_deleted BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

--Create STORES STREAM

CREATE STREAM STORES_STREAM
ON TABLE STORES;

-- 2. Create CUSTOMERS Table
CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID VARCHAR(10),
    FIRST_NAME VARCHAR(50),
    LAST_NAME VARCHAR(50),
    EMAIL VARCHAR(100),
    JOIN_DATE DATE,
    LOYALTY_TIER VARCHAR(20)
);

--
CREATE OR REPLACE TABLE DIM_CUSTOMERS (
    customer_id VARCHAR(10),
    first_name  VARCHAR(50),
    last_name   VARCHAR(50),
    email       VARCHAR(100),
    JOIN_DATE_KEY NUMBER(8,0),
    loyalty_tier VARCHAR(20),
    is_deleted  BOOLEAN DEFAULT FALSE,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


--Create CUSTOMERS STREAM

CREATE STREAM CUSTOMERS_STREAM
ON TABLE CUSTOMERS;

-- 3. Create EMPLOYEES Table
CREATE OR REPLACE TABLE EMPLOYEES (
    EMPLOYEE_ID VARCHAR(10),
    FIRST_NAME VARCHAR(50),
    LAST_NAME VARCHAR(50),
    ROLE VARCHAR(50),
    HIRE_DATE DATE,
    STORE_ID VARCHAR(10),
    SALARY NUMBER(10, 2)
);


--

CREATE OR REPLACE TABLE DIM_EMPLOYEES (
    employee_id VARCHAR(10),
    first_name  VARCHAR(50),
    last_name   VARCHAR(50),
    role        VARCHAR(50),
    hire_date         DATE,
    hire_date_key     NUMBER(8,0),
    store_id    VARCHAR(10),
    salary       NUMBER(10,2),
    is_deleted   BOOLEAN DEFAULT FALSE,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

--Create EMPLOYEES STREAM

CREATE STREAM EMPLOYEES_STREAM
ON TABLE EMPLOYEES;

-- 4. Create PRODUCTS Table
CREATE OR REPLACE TABLE PRODUCTS (
    PRODUCT_ID VARCHAR(10),
    PRODUCT_NAME VARCHAR(100),
    CATEGORY VARCHAR(50),
    PRICE NUMBER(10, 2),
    STOCK_QUANTITY INT
);

--
CREATE OR REPLACE TABLE DIM_PRODUCTS (
    product_id   VARCHAR(10),
    product_name VARCHAR(100),
    category     VARCHAR(50),
    price        NUMBER(10,2),
    stock_quantity INT,
    is_deleted   BOOLEAN DEFAULT FALSE,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

--Create PRODUCTS STREAM

CREATE STREAM PRODUCTS_STREAM
ON TABLE PRODUCTS;

-- 5. Create TRANSACTIONS Table
CREATE OR REPLACE TABLE TRANSACTIONS (
    TRANSACTION_ID VARCHAR(10),
    TIMESTAMP TIMESTAMP_NTZ,
    CUSTOMER_ID VARCHAR(10),
    PRODUCT_ID VARCHAR(10),
    STORE_ID VARCHAR(10),
    QUANTITY INT,
    TOTAL_AMOUNT NUMBER(10, 2)
);


--
CREATE OR REPLACE TABLE FACT_TRANSACTIONS (
    transaction_id VARCHAR(10),
    transaction_timestamp TIMESTAMP_NTZ,
    date_key NUMBER(8,0),
    time_key NUMBER(9,0),
    customer_id VARCHAR(10),
    product_id  VARCHAR(10),
    store_id    VARCHAR(10),
    quantity    INT,
    total_amount NUMBER(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


--Create TRANSACTIONS STREAM

CREATE STREAM TRANSACTIONS_STREAM
ON TABLE TRANSACTIONS
APPEND_ONLY = TRUE;


CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;




CREATE OR REPLACE TABLE load_audit_log (
    table_name STRING,
    stage_name STRING,
    folder_name STRING,
    operation STRING,
    rows_loaded NUMBER,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


CREATE OR REPLACE PROCEDURE load_folder_data(
    table_name STRING,
    stage_name STRING,
    folder_name STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    copy_statement STRING;
    rows_loaded NUMBER DEFAULT 0;

BEGIN

    copy_statement :=
          'COPY INTO ' || table_name
       || ' FROM @' || stage_name || '/' || folder_name
       || ' FILE_FORMAT = (FORMAT_NAME = ''my_csv_format'')';

    EXECUTE IMMEDIATE :copy_statement;

    BEGIN
        SELECT SUM("rows_loaded")
        INTO :rows_loaded
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) t;
    EXCEPTION
        WHEN OTHER THEN
            rows_loaded := 0;
    END;

    -- Insert audit record
    INSERT INTO load_audit_log (
        table_name,
        stage_name,
        folder_name,
        operation,
        rows_loaded,
        load_time
    )
    VALUES (
        :table_name,
        :stage_name,
        :folder_name,
        'COPY INTO',
        :rows_loaded,
        CURRENT_TIMESTAMP()
    );

    RETURN 'Rows loaded: ' || rows_loaded;

END;
$$;


---

CREATE OR REPLACE PROCEDURE load_dim_stores()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    MERGE INTO DIM_STORES d
    USING STORES_STREAM s
    ON d.store_id = s.store_id

    -- Handle DELETES → soft delete
    WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN
        UPDATE SET
            is_deleted = TRUE,
            updated_at = CURRENT_TIMESTAMP()

    -- Handle INSERTS (this also represents UPDATED rows' new version)
    WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
        UPDATE SET
            store_name  = s.store_name,
            city        = s.city,
            state       = s.state,
            postal_code = s.postal_code,
            manager_id  = s.manager_id,
            is_deleted  = FALSE,
            updated_at  = CURRENT_TIMESTAMP()

    -- New store (not yet in dimension)
    WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
        INSERT (
            store_id,
            store_name,
            city,
            state,
            postal_code,
            manager_id,
            is_deleted,
            updated_at
        )
        VALUES (
            s.store_id,
            s.store_name,
            s.city,
            s.state,
            s.postal_code,
            s.manager_id,
            FALSE,
            CURRENT_TIMESTAMP()
        );

    RETURN 'DIM_STORES UPDATED SUCCESSFULLY';

END;
$$;

--
CREATE OR REPLACE PROCEDURE load_dim_customers()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    MERGE INTO dim_customers d
    USING customers_stream s
    ON d.customer_id = s.customer_id

    -- HANDLE DELETES (soft delete)
    WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN
        UPDATE SET
            is_deleted = TRUE,
            updated_at = CURRENT_TIMESTAMP()

    -- HANDLE INSERTS (also covers updated rows’ new version)
    WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
        UPDATE SET
            first_name   = s.first_name,
            last_name    = s.last_name,
            email        = s.email,
            join_date_key    = TO_NUMBER(TO_CHAR(s.join_date, 'YYYYMMDD')),
            loyalty_tier = s.loyalty_tier,
            is_deleted   = FALSE,
            updated_at   = CURRENT_TIMESTAMP()

    -- NEW CUSTOMERS
    WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
        INSERT (
            customer_id,
            first_name,
            last_name,
            email,
            join_date_key,
            loyalty_tier,
            is_deleted,
            updated_at
        )
        VALUES (
            s.customer_id,
            s.first_name,
            s.last_name,
            s.email,
            TO_NUMBER(TO_CHAR(s.join_date, 'YYYYMMDD')),
            s.loyalty_tier,
            FALSE,
            CURRENT_TIMESTAMP()
        );

    RETURN 'DIM_CUSTOMERS LOADED SUCCESSFULLY';

END;
$$;

--

CREATE OR REPLACE PROCEDURE load_dim_employees()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    MERGE INTO dim_employees d
    USING employees_stream s
    ON d.employee_id = s.employee_id

    -- DELETE → soft delete
    WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN
        UPDATE SET
            is_deleted = TRUE,
            updated_at = CURRENT_TIMESTAMP()

    -- INSERT / UPDATE → overwrite current state
    WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
        UPDATE SET
            first_name     = s.first_name,
            last_name      = s.last_name,
            role           = s.role,
            hire_date      = s.hire_date,
            hire_date_key  = TO_NUMBER(TO_CHAR(s.hire_date, 'YYYYMMDD')),
            store_id       = s.store_id,
            salary         = s.salary,
            is_deleted     = FALSE,
            updated_at     = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
        INSERT (
            employee_id,
            first_name,
            last_name,
            role,
            hire_date,
            hire_date_key,
            store_id,
            salary,
            is_deleted,
            updated_at
        )
        VALUES (
            s.employee_id,
            s.first_name,
            s.last_name,
            s.role,
            s.hire_date,
            TO_NUMBER(TO_CHAR(s.hire_date, 'YYYYMMDD')),
            s.store_id,
            s.salary,
            FALSE,
            CURRENT_TIMESTAMP()
        );

    RETURN 'DIM_EMPLOYEES LOADED SUCCESSFULLY';

END;
$$;


--
CREATE OR REPLACE PROCEDURE load_dim_products()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    MERGE INTO dim_products d
    USING products_stream s
    ON d.product_id = s.product_id

    -- DELETE → soft delete
    WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN
        UPDATE SET
            is_deleted = TRUE,
            updated_at = CURRENT_TIMESTAMP()

    -- INSERT / UPDATE → overwrite current state
    WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
        UPDATE SET
            product_name   = s.product_name,
            category       = s.category,
            price          = s.price,
            stock_quantity = s.stock_quantity,
            is_deleted     = FALSE,
            updated_at     = CURRENT_TIMESTAMP()

    -- NEW RECORDS
    WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
        INSERT (
            product_id,
            product_name,
            category,
            price,
            stock_quantity,
            is_deleted,
            updated_at
        )
        VALUES (
            s.product_id,
            s.product_name,
            s.category,
            s.price,
            s.stock_quantity,
            FALSE,
            CURRENT_TIMESTAMP()
        );

    RETURN 'DIM_PRODUCTS LOADED SUCCESSFULLY';

END;
$$;


--

CREATE OR REPLACE PROCEDURE load_fact_transactions()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    INSERT INTO fact_transactions (
        transaction_id,
        transaction_timestamp,
        date_key,
        time_key,
        customer_id,
        product_id,
        store_id,
        quantity,
        total_amount,
        created_at
    )

    SELECT
        s.transaction_id,
        s.timestamp,

        TO_NUMBER(TO_CHAR(s.timestamp, 'YYYYMMDD')) AS date_key,
        TO_NUMBER(TO_CHAR(s.timestamp, 'HH24MISSFF3')) AS time_key,

        s.customer_id,
        s.product_id,
        s.store_id,
        s.quantity,

        COALESCE(s.total_amount, 0) AS total_amount,

        CURRENT_TIMESTAMP()

    FROM transactions_stream s;

    RETURN 'FACT_TRANSACTIONS LOADED SUCCESSFULLY';

END;
$$;



--

CREATE OR REPLACE VIEW DIM_DATE AS
WITH date_range AS (
    SELECT
        DATEADD(DAY, SEQ4(), '2001-01-01') AS dt
    FROM TABLE(GENERATOR(ROWCOUNT => 100000))
    WHERE DATEADD(DAY, SEQ4(), '2001-01-01') <= CURRENT_DATE()
)
SELECT
    dt AS date,

    TO_NUMBER(TO_CHAR(dt, 'YYYYMMDD')) AS date_key,

    YEAR(dt) AS year,
    MONTH(dt) AS month,
    DAY(dt) AS day,

    DAYOFWEEK(dt) AS day_of_week,  -- 1=Sunday in Snowflake
    DAYNAME(dt) AS day_name,

    CASE 
        WHEN DAYOFWEEK(dt) IN (1,7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    WEEKOFYEAR(dt) AS week_of_year,

    QUARTER(dt) AS quarter

FROM date_range;



--

CREATE OR REPLACE TABLE DIM_TIME AS

WITH time_series AS (

    SELECT
        DATEADD(SECOND, SEQ4(), '00:00:00'::TIME) AS tm
    FROM TABLE(GENERATOR(ROWCOUNT => 86400))

)

SELECT

    tm AS full_time,

    TO_NUMBER(TO_CHAR(tm, 'HH24MISS')) AS time_key,

    HOUR(tm)   AS hour,
    MINUTE(tm) AS minute,
    SECOND(tm) AS second,

    CASE
        WHEN HOUR(tm) < 12 THEN 'AM'
        ELSE 'PM'
    END AS am_pm,

    CASE
        WHEN HOUR(tm) BETWEEN 9 AND 17 THEN TRUE
        ELSE FALSE
    END AS is_business_hour

FROM time_series;