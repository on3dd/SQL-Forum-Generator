package db

import (
	"database/sql"
	"fmt"
	"os"
)

// New returns new instance of db
func New() (db *sql.DB, err error) {
	config, err := loadConfig()
	if err != nil {
		return nil, fmt.Errorf("Error loading config: %v", err)
	}

	db, err = initDatabase(config)
	if err != nil {
		return nil, fmt.Errorf("Error initializing DB: %v", err)
	}

	if err = prepareDatabase(db); err != nil {
		return nil, fmt.Errorf("Error creating schema and tables, error: %v", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("Error pinging DB: %v", err)
	}

	return db, err
}

// Config represents structure of the config.env
type Config struct {
	dbUser string
	dbPass string
	dbName string
	dbHost string
	dbPort string
}

// loadConfig loads env variables from config.env
func loadConfig() (config *Config, err error) {
	config = &Config{
		dbUser: os.Getenv("DB_USER"),
		dbPass: os.Getenv("DB_PASS"),
		dbName: os.Getenv("DB_NAME"),
		dbHost: os.Getenv("DB_HOST"),
		dbPort: os.Getenv("DB_PORT"),
	}

	return config, err
}

// initDatabase returns new database connected to postgres
func initDatabase(c *Config) (db *sql.DB, err error) {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		c.dbHost, c.dbPort, c.dbUser, c.dbPass, c.dbName)

	db, err = sql.Open("postgres", psqlInfo)
	return db, err
}

// prepareDatabase prepares db to generation
func prepareDatabase(db *sql.DB) error {
	_, err := db.Query(SchemaQuery)
	if err != nil {
		return err
	}

	_, err = db.Query(TablesQuery)
	if err != nil {
		return err
	}

	return nil
}
