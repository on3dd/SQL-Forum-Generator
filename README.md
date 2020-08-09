# sql-forum-generator
Creates SQL database with a certain amount of categories, users and messages

# Usage
## Install
Make sure Docker is already installed on your PC.
Clone this repository and install all required dependencies.

## Setup
### Environment variables
Create `.env` file and setup the following variables:
```
DB_USER = *your_username*
DB_PASS = *your_password*
DB_NAME = *your_database_name*
DB_HOST = localhost
DB_PORT = *postgres_server_port*
```

## Start 
Type the following code in the terminal:
```bash
$ docker-compose up
```
