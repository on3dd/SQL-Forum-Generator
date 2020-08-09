package main

import (
	dbpkg "sql-forum-generator/db"
	"sql-forum-generator/gen"

	"log"
	"time"
)

func main() {
	// Creating a new database instance
	db, err := dbpkg.New()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Println("Successfully connected to database.\n")

	// Creating a new generator instance
	g, err := gen.New(db)
	if err != nil {
		log.Fatal(err)
	}

	// Executing root category
	if err := g.GenerateRootCategory(); err != nil {
		log.Fatalf("Cannot execute root category, error: %v", err)
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

		// Writing categories
		total, err = g.WriteCategories(total)
		if err != nil {
			log.Fatal(err)
		}

		// Writing messages
		total, err = g.WriteMessages(total)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("Total time: %v", total)
}
