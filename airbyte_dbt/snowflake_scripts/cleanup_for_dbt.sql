-- =========================================================
-- DBT + SNOWFLAKE CLEANUP SCRIPT
-- =========================================================
-- This removes:
--   - dbt user
--   - dbt role
--   - dbt warehouse
--   - analytics schemas
--   - analytics database (optional)
--
-- Run as ACCOUNTADMIN or SECURITYADMIN/SYSADMIN
-- =========================================================



-- =========================================================
-- VARIABLES
-- =========================================================

SET DBT_ROLE        = 'DBT_ROLE';
SET DBT_USER        = 'DBT_USER';
SET DBT_WAREHOUSE   = 'DBT_WH';

SET TARGET_DATABASE = 'ANALYTICS';



-- =========================================================
-- 1. REMOVE ROLE FROM USER
-- =========================================================

USE ROLE SECURITYADMIN;

REVOKE ROLE IDENTIFIER($DBT_ROLE)
FROM USER IDENTIFIER($DBT_USER);



-- =========================================================
-- 2. DROP USER
-- =========================================================

DROP USER IF EXISTS IDENTIFIER($DBT_USER);



-- =========================================================
-- 3. DROP ROLE
-- =========================================================

DROP ROLE IF EXISTS IDENTIFIER($DBT_ROLE);



-- =========================================================
-- 4. DROP WAREHOUSE
-- =========================================================

USE ROLE SYSADMIN;

DROP WAREHOUSE IF EXISTS IDENTIFIER($DBT_WAREHOUSE);



-- =========================================================
-- 5. DROP SCHEMAS
-- =========================================================

USE DATABASE IDENTIFIER($TARGET_DATABASE);

DROP SCHEMA IF EXISTS STAGING CASCADE;
DROP SCHEMA IF EXISTS INTERMEDIATE CASCADE;
DROP SCHEMA IF EXISTS MARTS CASCADE;
DROP SCHEMA IF EXISTS ANALYTICS CASCADE;



-- =========================================================
-- 6. OPTIONAL: DROP DATABASE
-- =========================================================
-- Uncomment if you want to completely remove ANALYTICS
-- =========================================================

DROP DATABASE IF EXISTS IDENTIFIER($TARGET_DATABASE);



-- =========================================================
-- DONE
-- =========================================================