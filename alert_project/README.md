# alert project
short project description: File should appear in Data/incoming folder and then pipeline proceeds, applies defined rules and if condition is met then alert is sent

# requirements:
Docker must be installed for project to run.
https://docs.docker.com/desktop/setup/install/windows-install/


## setup guide:

clone the repository
    
    git clone git@github.com:Strelok2003/BigDataIntership.git

change into airflow folder

    cd alert_project



set up env variables
    
    create .env file in alert_project directory with following env variables

    GOOGLE_CHAT_WEBHOOK_URL=

    this variable is necesary for alert sending

spin up docker containers

    docker compose up -d

    