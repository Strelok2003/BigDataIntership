# Airflow + Mongodb project
short project description: File should appear in Data/raw folder and then pipeline proceeds, airflow and mongodb are ran as separate containers using docker compose

# requirements:
Docker must be installed for project to run.
https://docs.docker.com/desktop/setup/install/windows-install/


## setup guide:

clone the repository
    
    git clone git@github.com:Strelok2003/BigDataIntership.git

change into airflow folder

    cd airflow

set up airflow user and password

    create simple_auth_manager_passwords.json.generated file and populate it with following: 

        {"airflow_ui_user": "airflow_ui_password"}

    this user and password is what you will use when accessing airflow UI


set up airflow user and password
    
    create .env file in airflow directory with following env variables

    MONGO_AIRFLOW_USER=
    MONGO_AIRFLOW_PASS=

    thees variables are necessary for airflow mongo connection and also to access result after loading data to mongo

spin up docker containers

    docker compose up -d

access airflow ui
    
    localhost:8080
    