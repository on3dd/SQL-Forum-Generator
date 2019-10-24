package main

import (
	"SQL-Forum-Generator/gen"
	"database/sql"
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"os"
	"sync"
	"time"
)

func main() {
	config, err := loadConfig()
	if err != nil {
		log.Fatalf("Error loading config.env file: %v", err)
	}

	db, err := initDatabase(config)
	if err != nil {
		log.Fatal(err)
	}

	if err = db.Ping(); err != nil {
		log.Fatalf("Error initializing DB: %v", err)
	}

	// Creating a new generator instance
	g, err := gen.New(db)
	if err != nil {
		log.Fatal(err)
	}

	// Generating the records
	g.GenerateRecords()

	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Query("TRUNCATE TABLE categories, users, messages CASCADE;")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%v: Successfully connected to database.\n\n", time.Now().Format(time.UnixDate))

	mutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	// Total execution time
	var total time.Duration

	// Writing users
	total, err = g.WriteUsers(total, mutex, wg)
	if err != nil {
		log.Fatal(err)
	}

	//return

	// Writing categories
	total, err = g.WriteCategories(total, mutex, wg)
	if err != nil {
		log.Fatal(err)
	}

	// return

	// Writing messages
	total, err = g.WriteMessages(total, mutex, wg)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%v: Total time: %v", time.Now().Format(time.UnixDate), total)
}

// Config represents structure of the config.env
type Config struct {
	dbUser string
	dbPass string
	dbName string
	dbHost string
	dbPort string
}

func loadConfig() (config *Config, err error) {
	err = godotenv.Load("config.env")
	if err != nil {
		log.Fatal("Error loading config.env file")
	}

	config = &Config {
		dbUser : os.Getenv("db_user"),
		dbPass : os.Getenv("db_pass"),
		dbName : os.Getenv("db_name"),
		dbHost : os.Getenv("db_host"),
		dbPort : os.Getenv("db_port"),
	}
	return config, err
}

func initDatabase(c *Config) (db *sql.DB, err error) {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		c.dbHost, c.dbPort, c.dbUser, c.dbPass, c.dbName)

	db, err = sql.Open("postgres", psqlInfo)
	return db, err
}