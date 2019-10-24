package gen

import (
	"SQL-Forum-Generator/util"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"
)

const (
	// Default values are 500000, 5000, 10000000, 1000
	usersCount      = 50000
	categoriesCount = 5000
	messagesCount   = 1000000
	goroutinesCount = 1000

	// Default UUID value to check if categories.parent_id field is set
	defaultUuidValue = "00000000-0000-0000-0000-000000000000"
)

var (
	// Slices for storage words from external .txt files
	firstNames []string
	lastNames  []string
	words      []string

	// Slices for storage generated instances of structures before insertion
	users      []*User
	categories []*Category
	messages   []*Message
)


type Gen struct {
	db *sql.DB
}

// New returns a new API instance
func New(db *sql.DB) (*Gen, error) {
	var err error

	if firstNames, err = util.ReadLines("assets/first-names.txt"); err != nil {
		return nil, err
	}

	if lastNames, err = util.ReadLines("assets/last-names.txt"); err != nil {
		return nil, err
	}

	if words, err = util.ReadLines("assets/words.txt"); err != nil {
		return nil, err
	}

	return &Gen{db: db}, nil
}

type User struct {
	Id   uuid.UUID
	Name string
}

type Category struct {
	Id        uuid.UUID
	Name      string
	Parent_id uuid.UUID
}

type Message struct {
	Id          uuid.UUID
	Text        string
	Category_id uuid.UUID
	Posted_at   time.Time
	Author_id   uuid.UUID
}

// WriteUsers starts a new transaction with the DB and writes users there
func (gen *Gen) WriteUsers(total time.Duration, mutex *sync.Mutex, wg *sync.WaitGroup) (time.Duration, error) {
	txn, err := gen.db.Begin()
	if err != nil {
		return 0, err
	}

	stmt, _ := txn.Prepare(pq.CopyIn("users", "id", "name"))

	start := time.Now()
	fmt.Printf("%v: User insertion started...\n", time.Now().Format(time.UnixDate))

	defer gen.countTotal(start, "users")

	wg.Add(usersCount)
	iterationsNum := usersCount / goroutinesCount

	for i := 0; i < goroutinesCount; i++ {
		go func() {
			for j := 0; j < iterationsNum; j++ {
				gen.writeUser(stmt, mutex, wg)
			}
		}()
	}
	wg.Wait()

	if err = gen.closeTransaction(txn, stmt); err != nil {
		return 0, err
	}

	total += time.Since(start)

	return total, nil
}

// writeUser writes a single user in the db
func (gen *Gen) writeUser(stmt *sql.Stmt, m *sync.Mutex, w *sync.WaitGroup) {
	m.Lock()
	defer func() {
		m.Unlock()
		w.Done()
	}()

	var user *User
	user, users = users[0], users[1:]

	_, err := stmt.Exec(user.Id, user.Name)
	if err != nil {
		log.Fatal(err)
	}
}

// WriteCategories starts a new transaction with the DB and writes categories there
func (gen *Gen) WriteCategories(total time.Duration, mutex *sync.Mutex, wg *sync.WaitGroup) (time.Duration, error) {
	txn, err := gen.db.Begin()
	if err != nil {
		return 0, err
	}

	stmt, _ := txn.Prepare(pq.CopyIn("categories", "id", "name", "parent_id"))

	start := time.Now()
	fmt.Printf("%v: Categories insertion started...\n", time.Now().Format(time.UnixDate))

	defer gen.countTotal(start, "categories")

	// Exec the root category
	err = func() error {
		var category *Category
		category, categories = categories[0], categories[1:]
		_, err := stmt.Exec(category.Id, category.Name, sql.NullString{
			String: "",
			Valid:  false,
		})
		return err
	}()
	if err != nil {
		return 0, err
	}

	wg.Add(categoriesCount)
	iterationsNum := categoriesCount / goroutinesCount

	for i := 0; i < goroutinesCount; i++ {
		go func() {
			for j := 0; j < iterationsNum; j++ {
				gen.writeCategory(stmt, mutex, wg)
			}
		}()
	}
	wg.Wait()

	if err = gen.closeTransaction(txn, stmt); err != nil {
		return 0, err
	}

	total += time.Since(start)

	return total, nil
}

// writeCategory writes a single category in the db
func (gen *Gen) writeCategory(stmt *sql.Stmt, m *sync.Mutex, w *sync.WaitGroup) {
	m.Lock()
	defer func() {
		m.Unlock()
		w.Done()
	}()

	var category *Category

	category, categories = categories[0], categories[1:]

	if category.Parent_id.String() != defaultUuidValue {
		_, err := stmt.Exec(category.Id, category.Name, category.Parent_id)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		_, err := stmt.Exec(category.Id, category.Name, sql.NullString{
			String: "",
			Valid:  false,
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}

// WriteMessages starts a new transaction with the DB and writes messages there
func (gen *Gen) WriteMessages(total time.Duration, mutex *sync.Mutex, wg *sync.WaitGroup) (time.Duration, error) {
	txn, err := gen.db.Begin()
	if err != nil {
		return 0, err
	}

	stmt, _ := txn.Prepare(pq.CopyIn("messages", "id", "text", "category_id", "posted_at", "author_id"))

	start := time.Now()
	fmt.Printf("%v: Messages insertion started...\n", time.Now().Format(time.UnixDate))

	defer gen.countTotal(start, "messages")

	wg.Add(messagesCount)
	iterationsNum := messagesCount / goroutinesCount

	for i := 0; i < goroutinesCount; i++ {
		go func() {
			for j := 0; j < iterationsNum; j++ {
				gen.writeMessage(stmt, mutex, wg)
			}
		}()
	}
	wg.Wait()

	if err = gen.closeTransaction(txn, stmt); err != nil {
		return 0, err
	}

	total += time.Since(start)

	return total, nil
}

// writeMessage writes a single message in the db
func (gen *Gen) writeMessage(stmt *sql.Stmt, m *sync.Mutex, w *sync.WaitGroup) {
	m.Lock()
	defer func() {
		m.Unlock()
		w.Done()
	}()

	var message *Message
	message, messages = messages[len(messages)-1], messages[:len(messages)-1]

	_, err := stmt.Exec(message.Id, message.Text, message.Category_id, message.Posted_at, message.Author_id)
	if err != nil {
		log.Fatal(err)
	}
}

// GenerateRecords generates certain amount of users, categories and messages instances
func (gen *Gen) GenerateRecords() {
	var total time.Duration

	// Generate users
	func() {
		start := time.Now()
		defer func() {
			total += time.Since(start)
		}()

		for i := 0; i < usersCount; i++ {
			u := gen.generateUser()
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
				Id:        id,
				Name:      "Forum",
			})
		}()

		for i := 0; i < categoriesCount; i++ {
			c := gen.generateCategory()
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
			m := gen.generateMessage()
			messages = append(messages, m)
		}
		fmt.Printf("%v: Successfully created %v messages (total time: %v).\n\n", time.Now().Format(time.UnixDate), len(messages), time.Since(start))
	}()
}

// generateUser generates and returns single instance of User
func (gen *Gen) generateUser() (user *User) {
	id, _ := uuid.NewV4()

	firstName := firstNames[rand.Intn(len(firstNames))]
	lastName := lastNames[rand.Intn(len(lastNames))]

	user = &User{
		Id:   id,
		Name: firstName + " " + lastName,
	}
	return user
}

// generateCategory generates and returns single instance of category
func (gen *Gen) generateCategory() (category *Category) {
	id, _ := uuid.NewV4()

	var name []string
	nameLength := rand.Intn(4-2) + 2

	for i := 0; i < nameLength; i++ {
		name = append(name, words[rand.Intn(len(words))])
	}

	category = &Category{
		Id:   id,
		Name: strings.Title(strings.ToLower(strings.Join(name, " "))),
	}

	// 50% chance the category has a parent
	if hasParent := rand.Float32() < 0.5; hasParent {
		category.Parent_id = generateParentId(category.Id)
	}

	return category
}

// generateMessage generates and returns single instance of message
func (gen *Gen) generateMessage() (message *Message) {
	id, _ := uuid.NewV4()

	var text []string
	textLength := rand.Intn(20-1) + 1

	for i := 0; i < textLength; i++ {
		text = append(text, words[rand.Intn(len(words))])
	}

	categoryID := categories[rand.Intn(len(categories))].Id
	authorID := users[rand.Intn(len(users))].Id

	message = &Message{
		Id:          id,
		Text:        strings.Title(strings.ToLower(strings.Join(text, " "))),
		Category_id: categoryID,
		Posted_at:   util.GetRandomTimestamp(),
		Author_id:   authorID,
	}
	return message
}

// countTotal counts total number of elements in table
func (gen *Gen) countTotal(start time.Time, tableName string) {
	fmt.Printf("%v: Insertion is successfully completed and took %v.\n", time.Now().Format(time.UnixDate), time.Since(start))

	var count int
	row := gen.db.QueryRow("SELECT COUNT(*) FROM " + tableName)
	err := row.Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%v: Total count: %v\n\n", time.Now().Format(time.UnixDate), count)
}

// closeTransaction closes transaction with database
func (gen *Gen) closeTransaction(txn *sql.Tx, stmt *sql.Stmt) (err error) {
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	err = stmt.Close()
	if err != nil {
		return err
	}
	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

// generateParentId generates and returns parent for category
func generateParentId(el uuid.UUID) (id uuid.UUID) {
	id = categories[rand.Intn(len(categories))].Id
	for id.String() == el.String() {
		id = categories[rand.Intn(len(categories))].Id
	}
	return id
}