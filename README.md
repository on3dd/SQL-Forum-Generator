# SQL-Forum-Generator
Creates SQL database with a certain amount of categories, users and messages

# Usage
## Install
Make sure Go is already installed on your PC.

Clone this repository and install all required dependencies.

## Setup
### Environment variables
Create `config.env` file and setup the following variables:
```
db_user = *your_username*
db_pass = *your_password*
db_name = *your_database_name*
db_host = localhost
db_port = *postgres_server_port*
```

## Start 
Type the following code in the terminal:
```
go run main.go
```
