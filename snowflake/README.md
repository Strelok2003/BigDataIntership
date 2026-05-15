# Airflow + snowflake project

# requirements:
Docker must be installed for project to run.
https://docs.docker.com/desktop/setup/install/windows-install/

snowflake account with admin access

poetry installed
https://python-poetry.org/docs/

snowsql cli installed https://docs.snowflake.com/en/user-guide/snowsql-install-config


## setup guide:

clone the repository
    
    git clone git@github.com:Strelok2003/BigDataIntership.git

change into airflow folder

    cd snowflake

set up airflow user and password

    create simple_auth_manager_passwords.json.generated file and populate it with following: 

        {"airflow_ui_user": "airflow_ui_password"}

    this user and password is what you will use when accessing airflow UI


set up snowfalke environmetal variables
    
    create .env file in snowflake directory with following env variables

    SNOWFLAKE_USER=
    SNOWFLAKE_PASS=
    SNOWFLAKE_ACCOUNT=

    thees variables are necessary for airflow to snowflake connection and also to access result after loading data to snowflake


run snowflake set up script, before that make sure you set up credentials in snowsql config https://docs.snowflake.com/en/user-guide/snowsql-install-config#setting-the-download-directory-and-configuration-file-location

    snowsql -c example -f snowflake_setup/setup.sql



spin up docker containers

    docker compose up -d

access airflow ui
    
    localhost:8080
    

clean up snowflake

    snowsql -c example -f snowflake_setup/clean_up.sql