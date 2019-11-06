package gen

import (
	"database/sql"
	"github.com/lib/pq"
	"log"
	"sync"
	"time"
)

// WriteUsers starts a new transaction with the DB and writes users there
func (gen *Gen) WriteUsers(total time.Duration, mutex *sync.Mutex, wg *sync.WaitGroup) (time.Duration, error) {
	txn, err := gen.db.Begin()
	if err != nil {
		return 0, err
	}

	stmt, _ := txn.Prepare(pq.CopyIn("users", "id", "name"))

	start := time.Now()
	log.Printf("User insertion started...\n")

	defer gen.countTotal(start, "users")

	wg.Add(usersCount)
	iterationsNum := usersCount / goroutinesCount

	for i := 0; i < goroutinesCount; i++ {
		go func() {
			for j := 0; j < iterationsNum; j++ {
				writeUser(stmt, mutex, wg)
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
func writeUser(stmt *sql.Stmt, m *sync.Mutex, w *sync.WaitGroup) {
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
	log.Printf("Categories insertion started...\n")

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
				writeCategory(stmt, mutex, wg)
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
func writeCategory(stmt *sql.Stmt, m *sync.Mutex, w *sync.WaitGroup) {
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
	log.Printf("Messages insertion started...\n")

	defer gen.countTotal(start, "messages")

	wg.Add(messagesCount)
	iterationsNum := messagesCount / goroutinesCount

	for i := 0; i < goroutinesCount; i++ {
		go func() {
			for j := 0; j < iterationsNum; j++ {
				writeMessage(stmt, mutex, wg)
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
func writeMessage(stmt *sql.Stmt, m *sync.Mutex, w *sync.WaitGroup) {
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