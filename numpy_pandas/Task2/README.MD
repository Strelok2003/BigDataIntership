
# 1. Sql approach

# requirements:
Docker must be installed for project to run.
https://docs.docker.com/desktop/setup/install/windows-install/


## setup guide:

clone the repository
    
    git clone git@github.com:Strelok2003/BigDataIntership.git

change into numpy_pandas\Task2 folder

    cd numpy_pandas\Task2

set SA password 

    create .env file

        SA_PASSWORD=


spin up docker containers

    docker compose up -d

access sql server
    
    download your favorite sql server editor and connect to it with connection details:

    Server: localhost,1433
    User: sa
    Password: {password you choose}
    Database: Test
    

# 2. python approach

## just create virtual environmet, install requirements and run pandas_approach.py
