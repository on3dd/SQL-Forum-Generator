package main

import (
	dbpkg "SQL-Forum-Generator/db"
	"SQL-Forum-Generator/gen"
	"log"
	"time"
)

func main() {
	db, err := dbpkg.New()
	if err != nil {
		log.Fatal(err)
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