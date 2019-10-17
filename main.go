package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

type User struct {
	id   uuid.UUID
	name string
}

type Category struct {
	id        uuid.UUID
	name      string
	parent_id uuid.UUID
}

type Message struct {
	id          uuid.UUID
	text        string
	category_id uuid.UUID
	posted_at   time.Time
	author_id   uuid.UUID
}

const (
	// Default values are 500000, 5000, 10000000, 10
	usersCount      = 1000
	categoriesCount = 1000
	messagesCount   = 1000
	goroutinesCount = 10
)

var (
	db                 *sql.DB
	firstNames         []string
	lastNames          []string
	words              []string
	existingUsers      []uuid.UUID
	existingCategories []uuid.UUID
)

func main() {
	firstNames, _ = readLines("assets/first-names.txt")
	lastNames, _ = readLines("assets/last-names.txt")
	words, _ = readLines("assets/words.txt")

	err := godotenv.Load("config.env")
	if err != nil {
		log.Fatal("Error loading config.env file")
	}

	dbUser := os.Getenv("db_user")
	dbPass := os.Getenv("db_pass")
	dbName := os.Getenv("db_name")
	dbHost := os.Getenv("db_host")
	dbPort := os.Getenv("db_port")

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPass, dbName)

	fmt.Printf("%v\n", dbPort)

	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	//countTotal(time.Now(), "categories")
	//
	//return

	fmt.Printf("%v: Successfully connected to database.\n", time.Now().Format(time.UnixDate))

	mutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	// Total execution time
	var total time.Duration

	// Write users
	func() {
		start := time.Now()
		fmt.Printf("%v: User insertion started...\n", time.Now().Format(time.UnixDate))

		defer func() {
			total += time.Since(start)
			countTotal(start, "users")
		}()

		wg.Add(usersCount)
		iterationsNum := usersCount / goroutinesCount
		for i := 0; i < goroutinesCount; i++ {
			go func() {
				for j := 0; j < iterationsNum; j++ {
					writeUser(mutex, wg)
				}
			}()
		}
		wg.Wait()
	}()

	// Write categories
	func() {
		start := time.Now()
		fmt.Printf("%v: Categories insertion started...\n", time.Now().Format(time.UnixDate))

		defer func() {
			total += time.Since(start)
			countTotal(start, "categories")
		}()

		func() {
			id, _ := uuid.NewV4()
			existingCategories = append(existingCategories, id)
			_, err := db.Exec("INSERT INTO categories VALUES($1, $2)", id, "Forum")
			if err != nil {
				panic(err)
			}
		}()

		wg.Add(categoriesCount)
		iterationsNum := categoriesCount / goroutinesCount
		for i := 0; i < goroutinesCount; i++ {
			go func() {
				for j := 0; j < iterationsNum; j++ {
					writeCategory(mutex, wg)
				}
			}()
		}
		wg.Wait()
	}()

	// Write messages
	func() {
		start := time.Now()
		fmt.Printf("%v: Messages insertion started...\n", time.Now().Format(time.UnixDate))

		defer func() {
			total += time.Since(start)
			countTotal(start, "messages")
		}()

		wg.Add(messagesCount)
		iterationsNum := messagesCount / goroutinesCount
		for i := 0; i < goroutinesCount; i++ {
			go func() {
				for j := 0; j < iterationsNum; j++ {
					writeMessage(mutex, wg)
				}
			}()
		}
		wg.Wait()
	}()

	fmt.Printf("%v: Total time: %v", time.Now().Format(time.UnixDate), total)
}

// writeUser writes users as INSERT INTO queries in the db
func writeUser(m *sync.Mutex, w *sync.WaitGroup) {
	m.Lock()
	defer func() {
		m.Unlock()
		w.Done()
	}()

	id, _ := uuid.NewV4()
	existingUsers = append(existingUsers, id)

	firstName := firstNames[rand.Intn(len(firstNames))]
	lastName := lastNames[rand.Intn(len(lastNames))]

	user := &User{
		id:   id,
		name: firstName + " " + lastName,
	}

	_, err := db.Exec("INSERT INTO users VALUES($1, $2)", user.id, user.name)
	if err != nil {
		panic(err)
	}
}

// writeCategory writes categories as INSERT INTO queries in the db
func writeCategory(m *sync.Mutex, w *sync.WaitGroup) {
	m.Lock()
	defer func() {
		m.Unlock()
		w.Done()
	}()

	id, _ := uuid.NewV4()
	existingCategories = append(existingCategories, id)

	var name []string
	nameLength := rand.Intn(4-2) + 2

	for i := 0; i < nameLength; i++ {
		name = append(name, words[rand.Intn(len(words))])
	}

	category := &Category{
		id:   id,
		name: strings.Title(strings.ToLower(strings.Join(name, " "))),
	}

	var query string
	var err error
	if hasParent := rand.Float32() < 0.5; hasParent {
		category.parent_id = generateParentId(category.id)
		query = "INSERT INTO categories VALUES($1, $2, $3)"
		_, err = db.Exec(query, category.id, category.name, category.parent_id)
	} else {
		query = "INSERT INTO categories VALUES($1, $2)"
		_, err = db.Exec(query, category.id, category.name)
	}
	if err != nil {
		panic(err)
	}

}

// writeMessage writes messages as INSERT INTO queries in the db
func writeMessage(m *sync.Mutex, w *sync.WaitGroup) {
	m.Lock()
	defer func() {
		m.Unlock()
		w.Done()
	}()

	id, _ := uuid.NewV4()

	var text []string
	textLength := rand.Intn(20-1) + 1

	for i := 0; i < textLength; i++ {
		text = append(text, words[rand.Intn(len(words))])
	}

	message := &Message{
		id:          id,
		text:        strings.Title(strings.ToLower(strings.Join(text, " "))),
		category_id: existingCategories[rand.Intn(len(existingCategories))],
		posted_at:   getRandomTimestamp(),
		author_id:   existingUsers[rand.Intn(len(existingUsers))],
	}

	_, err := db.Exec("INSERT INTO messages VALUES($1, $2, $3, $4, $5)", message.id, message.text,
		message.category_id, message.posted_at, message.author_id)
	if err != nil {
		panic(err)
	}
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// generateParentId generates and returns parent for category
func generateParentId(el uuid.UUID) uuid.UUID {
	res := existingCategories[rand.Intn(len(existingCategories))]
	for res.String() == el.String() {
		res = existingCategories[rand.Intn(len(existingCategories))]
	}
	return res
}

// getRandomTimestamp generates and returns random timestamp
func getRandomTimestamp() time.Time {
	min := time.Date(2015, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2020, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

// countTotal counts total number of elements in table
func countTotal(start time.Time, tableName string) {
	fmt.Printf("%v: Insertion is successfully completed and took %v.\n", time.Now().Format(time.UnixDate), time.Since(start))

	var count int
	row := db.QueryRow("SELECT COUNT(*) FROM " + tableName)
	err := row.Scan(&count)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%v: Total count: %v\n\n", time.Now().Format(time.UnixDate), count)
}
