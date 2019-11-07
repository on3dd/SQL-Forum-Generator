package gen

import (
	"SQL-Forum-Generator/util"
	"database/sql"
	uuid "github.com/satori/go.uuid"
	"log"
	"math/rand"
	"strings"
	"time"
)

var (
	// Default values are 500000, 5000, 10000000, 1000
	usersNum      = 50000
	categoriesNum = 5000
	messagesNum   = 1000000
	goroutinesNum = 1000

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
	Id       uuid.UUID
	Name     string
	ParentId uuid.UUID
}

type Message struct {
	Id         uuid.UUID
	Text       string
	CategoryId uuid.UUID
	PostedAt   time.Time
	AuthorId   uuid.UUID
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

		for i := 0; i < usersNum; i++ {
			u := generateUser()
			users = append(users, u)
		}
		log.Printf("Successfully created %v users (total time: %v).\n", len(users), time.Since(start))
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
				Id:   id,
				Name: "Forum",
			})
		}()

		for i := 0; i < categoriesNum; i++ {
			c := generateCategory()
			categories = append(categories, c)
		}
		log.Printf("Successfully created %v categories (total time: %v).\n", len(categories), time.Since(start))
	}()

	// Generate messages
	func() {
		start := time.Now()
		defer func() {
			total += time.Since(start)
		}()

		for i := 0; i < messagesNum; i++ {
			m := generateMessage()
			messages = append(messages, m)
		}
		log.Printf("Successfully created %v messages (total time: %v).\n\n", len(messages), time.Since(start))
	}()
}

// generateUser generates and returns single instance of User
func generateUser() (user *User) {
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
func generateCategory() (category *Category) {
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
		category.ParentId = generateParentId(category.Id)
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

	categoryID := categories[rand.Intn(len(categories))].Id
	authorID := users[rand.Intn(len(users))].Id

	message = &Message{
		Id:         id,
		Text:       strings.Title(strings.ToLower(strings.Join(text, " "))),
		CategoryId: categoryID,
		PostedAt:   util.GetRandomTimestamp(),
		AuthorId:   authorID,
	}
	return message
}

// countTotal counts total number of elements in table
func (gen *Gen) countTotal(start time.Time, tableName string) {
	log.Printf("Insertion is successfully completed and took %v.\n", time.Since(start))

	var count int
	row := gen.db.QueryRow("SELECT COUNT(*) FROM " + tableName)
	err := row.Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Total count: %v\n\n", count)
}

// generateParentId generates and returns parent for category
func generateParentId(el uuid.UUID) (id uuid.UUID) {
	id = categories[rand.Intn(len(categories))].Id
	for id.String() == el.String() {
		id = categories[rand.Intn(len(categories))].Id
	}
	return id
}
