package gen

import (
	"database/sql"
	"github.com/lib/pq"
	"log"
	"time"
)

// WriteUsers starts a new transaction with the DB and writes users there
func (gen *Gen) WriteUsers(total time.Duration) (time.Duration, error) {
	txn, err := gen.db.Begin()
	if err != nil {
		return 0, err
	}

	stmt, _ := txn.Prepare(pq.CopyInSchema("public", "users", "id", "name"))

	start := time.Now()
	log.Printf("User insertion started...\n")

	defer gen.countTotal(start, "users")

	gen.wg.Add(usersNum)

	if n := usersNum / goroutinesNum; n >= 1 {
		for i := 0; i < goroutinesNum; i++ {
			go func() {
				for j := 0; j < n; j++ {
					gen.writeUser(stmt)
				}
			}()
		}
	} else {
		for i := 0; i < usersNum; i++ {
			go gen.writeUser(stmt)
		}
	}

	gen.wg.Wait()

	if err = gen.closeTransaction(txn, stmt); err != nil {
		return 0, err
	}

	total += time.Since(start)

	return total, nil
}

// writeUser writes a single user in the db
func (gen *Gen) writeUser(stmt *sql.Stmt) {
	gen.mutex.Lock()
	defer func() {
		gen.mutex.Unlock()
		gen.wg.Done()
	}()

	var user *User
	user, users = users[0], users[1:]

	_, err := stmt.Exec(user.Id, user.Name)
	if err != nil {
		log.Fatal(err)
	}
}

// WriteCategories starts a new transaction with the DB and writes categories there
func (gen *Gen) WriteCategories(total time.Duration) (time.Duration, error) {
	txn, err := gen.db.Begin()
	if err != nil {
		return 0, err
	}

	stmt, _ := txn.Prepare(pq.CopyInSchema("public", "categories", "id", "name", "parent_id"))

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

	gen.wg.Add(categoriesNum)

	if n := categoriesNum / goroutinesNum; n >= 1 {
		for i := 0; i < goroutinesNum; i++ {
			go func() {
				for j := 0; j < n; j++ {
					gen.writeCategory(stmt)
				}
			}()
		}
	} else {
		for i := 0; i < categoriesNum; i++ {
			go gen.writeCategory(stmt)
		}
	}

	gen.wg.Wait()

	if err = gen.closeTransaction(txn, stmt); err != nil {
		return 0, err
	}

	total += time.Since(start)

	return total, nil
}

// writeCategory writes a single category in the db
func (gen *Gen) writeCategory(stmt *sql.Stmt) {
	gen.mutex.Lock()
	defer func() {
		gen.mutex.Unlock()
		gen.wg.Done()
	}()

	var category *Category

	category, categories = categories[0], categories[1:]

	if category.ParentId.String() != defaultUuidValue {
		_, err := stmt.Exec(category.Id, category.Name, category.ParentId)
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
func (gen *Gen) WriteMessages(total time.Duration) (time.Duration, error) {
	txn, err := gen.db.Begin()
	if err != nil {
		return 0, err
	}

	stmt, _ := txn.Prepare(pq.CopyInSchema("public", "messages", "id", "text", "category_id", "posted_at", "author_id"))

	start := time.Now()
	log.Printf("Messages insertion started...\n")

	defer gen.countTotal(start, "messages")

	gen.wg.Add(messagesNum)

	if n := messagesNum / goroutinesNum; n >= 1 {
		for i := 0; i < goroutinesNum; i++ {
			go func() {
				for j := 0; j < n; j++ {
					gen.writeMessage(stmt)
				}
			}()
		}
	} else {
		for i := 0; i < messagesNum; i++ {
			go gen.writeMessage(stmt)
		}
	}

	gen.wg.Wait()

	if err = gen.closeTransaction(txn, stmt); err != nil {
		return 0, err
	}

	total += time.Since(start)

	return total, nil
}

// writeMessage writes a single message in the db
func (gen *Gen) writeMessage(stmt *sql.Stmt) {
	gen.mutex.Lock()
	defer func() {
		gen.mutex.Unlock()
		gen.wg.Done()
	}()

	var message *Message
	message, messages = messages[len(messages)-1], messages[:len(messages)-1]

	_, err := stmt.Exec(message.Id, message.Text, message.CategoryId, message.PostedAt, message.AuthorId)
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