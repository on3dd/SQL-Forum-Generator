package gen

import (
	"database/sql"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

// Gen model...
type Gen struct {
	db    *sql.DB
	mutex *sync.Mutex
	wg    *sync.WaitGroup
}

// User model...
type User struct {
	Id   uuid.UUID
	Name string
}

// Category model...
type Category struct {
	Id       uuid.UUID
	Name     string
	ParentId uuid.UUID
}

// Message model...
type Message struct {
	Id         uuid.UUID
	Text       string
	CategoryId uuid.UUID
	PostedAt   time.Time
	AuthorId   uuid.UUID
}
