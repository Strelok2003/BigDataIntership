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

change airflow user and password if you want 

    edit simple_auth_manager_passwords.json.generated file, default is admin as user and password, this is what you use when accessing airflow UI


spin up docker containers

    docker compose up -d

access airflow ui
    
    localhost:8080
    