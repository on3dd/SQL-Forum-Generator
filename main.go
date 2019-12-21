package main

import (
	"SQL-Forum-Generator/gen"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	config, err := loadConfig()
	if err != nil {
		log.Fatalf("Error loading config.env file: %v", err)
	}

	db, err := initDatabase(config)
	if err != nil {
		log.Fatalf("Error initializing DB: %v", err)
	}

	if err := prepareDatabase(db); err != nil {
		log.Fatalf("Error creating schema and tables, error: %v", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatalf("Error pinging DB: %v", err)
	}

	defer db.Close()

	log.Printf("Successfully connected to database.\n\n")

	_, err = db.Query("TRUNCATE TABLE categories, users, messages CASCADE;")
	if err != nil {
		log.Fatal(err)
	}

	// Creating a new generator instance
	g, err := gen.New(db)
	if err != nil {
		log.Fatal(err)
	}

	// Total execution time
	var total time.Duration

	for i := 0; i < gen.IterationsNum; i++ {
		// Generating the records
		g.GenerateRecords()

		// Writing users
		total, err = g.WriteUsers(total)
		if err != nil {
			log.Fatal(err)
		}

		//return

		// Writing categories
		total, err = g.WriteCategories(total)
		if err != nil {
			log.Fatal(err)
		}

		// return

		// Writing messages
		total, err = g.WriteMessages(total)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("Total time: %v", total)
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

	config = &Config{
		dbUser: os.Getenv("db_user"),
		dbPass: os.Getenv("db_pass"),
		dbName: os.Getenv("db_name"),
		dbHost: os.Getenv("db_host"),
		dbPort: os.Getenv("db_port"),
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

func prepareDatabase(db *sql.DB) error {
	_, err := db.Query(`CREATE SCHEMA IF NOT EXISTS "hw_db"`)
	if err != nil {
		return err
	}

	_, err = db.Query(`
		DROP TABLE IF EXISTS  public.categories CASCADE;
		DROP TABLE IF EXISTS  public.users CASCADE;
		DROP TABLE IF EXISTS  public.messages CASCADE;
-- 		CREATE SCHEMA IF NOT EXISTS "generator";
		CREATE UNLOGGED TABLE IF NOT EXISTS public.messages (
		"id" uuid NOT NULL,
		"text" TEXT NOT NULL,
		"category_id" uuid NOT NULL,
		"posted_at" TIME NOT NULL,
		"author_id" uuid NOT NULL
	) WITH (
		OIDS=FALSE
	);
	
	CREATE UNLOGGED TABLE IF NOT EXISTS  public.categories (
		"id" uuid NOT NULL,
		"name" varchar(255) NOT NULL,
		"parent_id" uuid
	) WITH (
		OIDS=FALSE
	);
	
	CREATE UNLOGGED TABLE IF NOT EXISTS  public.users (
		"id" uuid NOT NULL,
		"name" varchar(255) NOT NULL
	) WITH (
		OIDS=FALSE
	);
	
	ALTER TABLE public.users SET (autovacuum_enabled = false);
	ALTER TABLE public.categories SET (autovacuum_enabled = false);
	ALTER TABLE public.messages SET (autovacuum_enabled = false);
`)
	if err != nil {
		return err
	}
	return nil
}
