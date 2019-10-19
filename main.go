package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
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
	// Default values are 500000, 5000, 10000000, 10000
	usersCount      = 500000
	categoriesCount = 5000
	messagesCount   = 1000000
	//goroutinesCount = 10000

	// Default UUID value to check if categories.parent_id field is set
	defaultUuidValue = "00000000-0000-0000-0000-000000000000"
)

var (
	// Global database instance
	db         *sql.DB

	// Slices for storage words from external .txt files
	firstNames []string
	lastNames  []string
	words      []string

	// Slices for storage generated instances of structures before insertion
	users      []*User
	categories []*Category
	messages   []*Message
)

func main() {
	firstNames, _ = readLines("assets/first-names.txt")
	lastNames, _ = readLines("assets/last-names.txt")
	words, _ = readLines("assets/words.txt")

	generateRecords()

	// Getting configuration values
	err := godotenv.Load("config.env")
	if err != nil {
		panic("Error loading config.env file")
	}

	dbUser := os.Getenv("db_user")
	dbPass := os.Getenv("db_pass")
	dbName := os.Getenv("db_name")
	dbHost := os.Getenv("db_host")
	dbPort := os.Getenv("db_port")

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPass, dbName)

	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	_, err = db.Query("TRUNCATE TABLE categories, users, messages CASCADE;")
	if err != nil {
		panic(err)
	}

	fmt.Printf("%v: Successfully connected to database.\n\n", time.Now().Format(time.UnixDate))

	mutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	// Total execution time
	var total time.Duration

	// Writing users
	func() {
		txn, err := db.Begin()
		if err != nil {
			panic(err)
		}

		stmt, _ := txn.Prepare(pq.CopyIn("users", "id", "name"))

		start := time.Now()
		fmt.Printf("%v: User insertion started...\n", time.Now().Format(time.UnixDate))

		defer func() {
			total += time.Since(start)
			countTotal(start, "users")
		}()

		wg.Add(usersCount)
		goroutinesCount := 5000
		iterationsNum := usersCount / goroutinesCount
		for i := 0; i < goroutinesCount; i++ {
			go func() {
				for j := 0; j < iterationsNum; j++ {
					writeUser(stmt, mutex, wg)
				}
			}()
		}
		wg.Wait()

		closeTransaction(txn, stmt)
	}()

	//return

	// Writing categories
	func() {
		txn, err := db.Begin()
		if err != nil {
			panic(err)
		}

		stmt, _ := txn.Prepare(pq.CopyIn("categories", "id", "name", "parent_id"))

		start := time.Now()
		fmt.Printf("%v: Categories insertion started...\n", time.Now().Format(time.UnixDate))

		defer func() {
			total += time.Since(start)
			countTotal(start, "categories")
		}()

		func() {
			var category *Category
			category, categories = categories[0], categories[1:]
			_, err := stmt.Exec(category.id, category.name, sql.NullString{
				String: "",
				Valid:  false,
			})
			if err != nil {
				panic(err)
			}
		}()

		wg.Add(categoriesCount)
		goroutinesCount := 5000
		iterationsNum := categoriesCount / goroutinesCount
		for i := 0; i < goroutinesCount; i++ {
			go func() {
				for j := 0; j < iterationsNum; j++ {
					writeCategory(stmt, mutex, wg)
				}
			}()
		}
		wg.Wait()

		closeTransaction(txn, stmt)
	}()

	// Writing messages
	func() {
		txn, err := db.Begin()
		if err != nil {
			panic(err)
		}

		stmt, _ := txn.Prepare(pq.CopyIn("messages", "id", "text", "category_id", "posted_at", "author_id"))

		start := time.Now()
		fmt.Printf("%v: Messages insertion started...\n", time.Now().Format(time.UnixDate))

		defer func() {
			total += time.Since(start)
			countTotal(start, "messages")
		}()

		wg.Add(messagesCount)
		goroutinesCount := 5000
		iterationsNum := messagesCount / goroutinesCount
		for i := 0; i < goroutinesCount; i++ {
			go func() {
				for j := 0; j < iterationsNum; j++ {
					writeMessage(stmt, mutex, wg)
				}
			}()
		}
		wg.Wait()

		closeTransaction(txn, stmt)
	}()

	fmt.Printf("%v: Total time: %v", time.Now().Format(time.UnixDate), total)
}

// writeUser writes users as INSERT INTO queries in the db
func writeUser(stmt *sql.Stmt, m *sync.Mutex, w *sync.WaitGroup) {
	m.Lock()
	defer func() {
		m.Unlock()
		w.Done()
	}()

	var user *User
	user, users = users[0], users[1:]

	_, err := stmt.Exec(user.id, user.name)
	if err != nil {
		panic(err)
	}
}

// writeCategory writes categories as INSERT INTO queries in the db
func writeCategory(stmt *sql.Stmt, m *sync.Mutex, w *sync.WaitGroup) {
	m.Lock()
	defer func() {
		m.Unlock()
		w.Done()
	}()

	var category *Category

	category, categories = categories[0], categories[1:]

	if category.parent_id.String() != defaultUuidValue {
		_, err := stmt.Exec(category.id, category.name, category.parent_id)
		if err != nil {
			panic(err)
		}
	} else {
		_, err := stmt.Exec(category.id, category.name, sql.NullString{
			String: "",
			Valid:  false,
		})
		if err != nil {
			panic(err)
		}
	}
}

// writeMessage writes messages as INSERT INTO queries in the db
func writeMessage(stmt *sql.Stmt, m *sync.Mutex, w *sync.WaitGroup) {
	m.Lock()
	defer func() {
		m.Unlock()
		w.Done()
	}()

	var message *Message
	message, messages = messages[len(messages)-1], messages[:len(messages)-1]

	_, err := stmt.Exec(message.id, message.text, message.category_id, message.posted_at, message.author_id)
	if err != nil {
		panic(err)
	}
}

// generateRecords generates certain amount of users, categories and messages instances
func generateRecords() {
	var total time.Duration

	// Generate users
	func() {
		start := time.Now()
		defer func() {
			total += time.Since(start)
		}()

		for i := 0; i < usersCount; i++ {
			u := generateUser()
			users = append(users, u)
		}
		fmt.Printf("%v: Successfully created %v users (total time: %v).\n", time.Now().Format(time.UnixDate), len(users), time.Since(start))
	}()

	// Generate categories
	func() {
		start := time.Now()
		defer func() {
			total += time.Since(start)
		}()

		func() {
			id, _ := uuid.NewV4()
			categories = append(categories, &Category{
				id:        id,
				name:      "Forum",
			})
		}()

		for i := 0; i < categoriesCount; i++ {
			c := generateCategory()
			categories = append(categories, c)
		}
		fmt.Printf("%v: Successfully created %v categories (total time: %v).\n", time.Now().Format(time.UnixDate), len(categories), time.Since(start))
	}()

	// Generate messages
	func() {
		start := time.Now()
		defer func() {
			total += time.Since(start)
		}()

		for i := 0; i < messagesCount; i++ {
			m := generateMessage()
			messages = append(messages, m)
		}
		fmt.Printf("%v: Successfully created %v messages (total time: %v).\n\n", time.Now().Format(time.UnixDate), len(messages), time.Since(start))
	}()
}

// generateUser generates and returns single instance of User
func generateUser() (user *User) {
	id, _ := uuid.NewV4()

	firstName := firstNames[rand.Intn(len(firstNames))]
	lastName := lastNames[rand.Intn(len(lastNames))]

	user = &User{
		id:   id,
		name: firstName + " " + lastName,
	}
	return user
}

// generateCategory generates and returns single instance of category
func generateCategory() (category *Category) {
	id, _ := uuid.NewV4()

	var name []string
	nameLength := rand.Intn(4-2) + 2

	for i := 0; i < nameLength; i++ {
		name = append(name, words[rand.Intn(len(words))])
	}

	category = &Category{
		id:   id,
		name: strings.Title(strings.ToLower(strings.Join(name, " "))),
	}

	// 50% chance the category has a parent
	if hasParent := rand.Float32() < 0.5; hasParent {
		category.parent_id = generateParentId(category.id)
	}

	return category
}

// generateMessage generates and returns single instance of message
func generateMessage() (message *Message) {
	id, _ := uuid.NewV4()

	var text []string
	textLength := rand.Intn(20-1) + 1

	for i := 0; i < textLength; i++ {
		text = append(text, words[rand.Intn(len(words))])
	}

	categoryID := categories[rand.Intn(len(categories))].id
	authorID := users[rand.Intn(len(users))].id

	message = &Message{
		id:          id,
		text:        strings.Title(strings.ToLower(strings.Join(text, " "))),
		category_id: categoryID,
		posted_at:   getRandomTimestamp(),
		author_id:   authorID,
	}
	return message
}

// generateParentId generates and returns parent for category
func generateParentId(el uuid.UUID) (id uuid.UUID) {
	id = categories[rand.Intn(len(categories))].id
	for id.String() == el.String() {
		id = categories[rand.Intn(len(categories))].id
	}
	return id
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

// closeTransaction closes transaction with database
func closeTransaction(txn *sql.Tx, stmt *sql.Stmt) {
	_, err := stmt.Exec()
	if err != nil {
		panic(err)
	}
	err = stmt.Close()
	if err != nil {
		panic(err)
	}
	err = txn.Commit()
	if err != nil {
		panic(err)
	}
}