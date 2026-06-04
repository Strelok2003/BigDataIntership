-- set variables
set airbyte_role = 'AIRBYTE_ROLE';
set airbyte_username = 'AIRBYTE_USER';
set airbyte_warehouse = 'AIRBYTE_WAREHOUSE';
set airbyte_database = 'AIRBYTE_DATABASE';

begin;

-- use admin roles
use role securityadmin;

-- remove role from user first
revoke role identifier($airbyte_role)
from user identifier($airbyte_username);

-- switch to sysadmin for db/warehouse cleanup
use role sysadmin;

-- drop database
drop database if exists identifier($airbyte_database);

-- drop warehouse
drop warehouse if exists identifier($airbyte_warehouse);

-- switch back to securityadmin
use role securityadmin;

-- drop user
drop user if exists identifier($airbyte_username);

-- drop role
drop role if exists identifier($airbyte_role);

commit;