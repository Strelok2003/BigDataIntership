-- =========================================================
-- DBT + SNOWFLAKE FLEXIBLE SETUP SCRIPT
-- =========================================================
-- This version allows dbt to access ALL current and future
-- schemas inside the TARGET_DATABASE.
--
-- So later if you create:
--   ANALYTICS.STAGING
--   ANALYTICS.INTERMEDIATE
--   ANALYTICS.MARTS
--   ANALYTICS.ADHOC
--
-- dbt will automatically work there without new grants.
--
-- Run as SECURITYADMIN / SYSADMIN
-- =========================================================



-- =========================================================
-- VARIABLES (EDIT THESE)
-- =========================================================

SET DBT_ROLE        = 'DBT_ROLE';
SET DBT_USER        = 'DBT_USER';
SET DBT_PASSWORD    = 'StrongPassword123!';
SET DBT_WAREHOUSE   = 'DBT_WH';

SET RAW_DATABASE    = 'AIRBYTE_DATABASE';
SET TARGET_DATABASE = 'ANALYTICS';



-- =========================================================
-- 1. CREATE ROLE
-- =========================================================

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS IDENTIFIER($DBT_ROLE);



-- =========================================================
-- 2. CREATE WAREHOUSE
-- =========================================================

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($DBT_WAREHOUSE)
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;



-- =========================================================
-- 3. CREATE USER
-- =========================================================

USE ROLE SECURITYADMIN;

CREATE USER IF NOT EXISTS IDENTIFIER($DBT_USER)
PASSWORD = $DBT_PASSWORD
DEFAULT_ROLE = $DBT_ROLE
DEFAULT_WAREHOUSE = $DBT_WAREHOUSE
MUST_CHANGE_PASSWORD = FALSE;



-- =========================================================
-- 4. ASSIGN ROLE TO USER
-- =========================================================

GRANT ROLE IDENTIFIER($DBT_ROLE)
TO USER IDENTIFIER($DBT_USER);



-- =========================================================
-- 5. WAREHOUSE PERMISSIONS
-- =========================================================

GRANT USAGE
ON WAREHOUSE IDENTIFIER($DBT_WAREHOUSE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT OPERATE
ON WAREHOUSE IDENTIFIER($DBT_WAREHOUSE)
TO ROLE IDENTIFIER($DBT_ROLE);



-- =========================================================
-- 6. CREATE TARGET DATABASE AND SHCEMAS
-- =========================================================

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS IDENTIFIER($TARGET_DATABASE);

USE DATABASE IDENTIFIER($TARGET_DATABASE);

CREATE OR REPLACE SCHEMA STAGING;
CREATE OR REPLACE SCHEMA INTERMEDIATE;
CREATE OR REPLACE SCHEMA MARTS;
CREATE OR REPLACE SCHEMA ANALYTICS;



-- =========================================================
-- 7. DATABASE ACCESS
-- =========================================================

USE ROLE SECURITYADMIN;

GRANT USAGE
ON DATABASE IDENTIFIER($RAW_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT USAGE
ON DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);



-- =========================================================
-- 8. RAW DATABASE ACCESS
-- =========================================================

GRANT USAGE
ON ALL SCHEMAS IN DATABASE IDENTIFIER($RAW_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT USAGE
ON FUTURE SCHEMAS IN DATABASE IDENTIFIER($RAW_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT SELECT
ON ALL TABLES IN DATABASE IDENTIFIER($RAW_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT SELECT
ON FUTURE TABLES IN DATABASE IDENTIFIER($RAW_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT SELECT
ON ALL VIEWS IN DATABASE IDENTIFIER($RAW_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT SELECT
ON FUTURE VIEWS IN DATABASE IDENTIFIER($RAW_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);



-- =========================================================
-- 9. TARGET DATABASE SCHEMA ACCESS
-- =========================================================
-- This is the important section.
-- dbt gets access to ALL current and future schemas.
-- =========================================================

GRANT USAGE
ON ALL SCHEMAS IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT USAGE
ON FUTURE SCHEMAS IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);



-- =========================================================
-- 10. DBT BUILD PERMISSIONS
-- =========================================================
-- Allows dbt to create models in ANY schema.
-- =========================================================

GRANT CREATE TABLE
ON ALL SCHEMAS IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT CREATE TABLE
ON FUTURE SCHEMAS IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);


GRANT CREATE VIEW
ON ALL SCHEMAS IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT CREATE VIEW
ON FUTURE SCHEMAS IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);


GRANT CREATE STAGE
ON ALL SCHEMAS IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT CREATE STAGE
ON FUTURE SCHEMAS IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);


GRANT CREATE FILE FORMAT
ON ALL SCHEMAS IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT CREATE FILE FORMAT
ON FUTURE SCHEMAS IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);



-- =========================================================
-- 11. INCREMENTAL MODEL SUPPORT
-- =========================================================

GRANT INSERT,
      UPDATE,
      DELETE,
      TRUNCATE
ON ALL TABLES IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT INSERT,
      UPDATE,
      DELETE,
      TRUNCATE
ON FUTURE TABLES IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);



-- =========================================================
-- 12. VIEW ACCESS
-- =========================================================

GRANT SELECT
ON ALL VIEWS IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);

GRANT SELECT
ON FUTURE VIEWS IN DATABASE IDENTIFIER($TARGET_DATABASE)
TO ROLE IDENTIFIER($DBT_ROLE);



-- =========================================================
-- 13. OPTIONAL QUERY HISTORY ACCESS
-- =========================================================

GRANT IMPORTED PRIVILEGES
ON DATABASE SNOWFLAKE
TO ROLE IDENTIFIER($DBT_ROLE);
