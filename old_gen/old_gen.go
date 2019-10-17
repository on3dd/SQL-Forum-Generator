package old_gen

import (
	"bufio"
	"fmt"
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
	usersCount      = 500000
	categoriesCount = 5000
	messagesCount   = 10000000
	goroutinesCount = 10
)

var (
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

	mutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	var total time.Duration

	// Write users
	func() {
		start := time.Now()
		fmt.Printf("%v: Started recording users...\n", time.Now().Format(time.UnixDate))

		f, err := os.OpenFile("tables/users.sql", os.O_RDWR|os.O_APPEND, 0660)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		wg.Add(usersCount)
		iterationsNum := usersCount / goroutinesCount
		for i := 0; i < goroutinesCount; i++ {
			go func() {
				for j := 0; j < iterationsNum; j++ {
					writeUser(f, mutex, wg)
				}
			}()
		}
		wg.Wait()
		total += time.Since(start)
		fmt.Printf("%v: Recording is successfully completed and took %v.\n\n", time.Now().Format(time.UnixDate), time.Since(start))
	}()

	// Write categories
	func() {
		start := time.Now()
		fmt.Printf("%v: Started recording categories...\n", time.Now().Format(time.UnixDate))

		f, err := os.OpenFile("tables/categories.sql", os.O_RDWR|os.O_APPEND, 0660)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		func(file *os.File) {
			id, _ := uuid.NewV4()
			_, err := f.WriteString("INSERT INTO categories(id, name) VALUES ('" + id.String() + "', 'Forum'); \n")
			if err != nil {
				panic(err)
			}
		}(f)

		wg.Add(categoriesCount)
		iterationsNum := categoriesCount / goroutinesCount
		for i := 0; i < goroutinesCount; i++ {
			go func() {
				for j := 0; j < iterationsNum; j++ {
					writeCategory(f, mutex, wg)
				}
			}()
		}
		wg.Wait()
		total += time.Since(start)
		fmt.Printf("%v: Recording is successfully completed and took %v.\n\n", time.Now().Format(time.UnixDate), time.Since(start))
	}()

	// Write messages
	func() {
		start := time.Now()
		fmt.Printf("%v: Started recording messages...\n", time.Now().Format(time.UnixDate))

		f, err := os.OpenFile("tables/messages.sql", os.O_RDWR|os.O_APPEND, 0660)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		wg.Add(messagesCount)
		iterationsNum := messagesCount / goroutinesCount
		for i := 0; i < goroutinesCount; i++ {
			go func() {
				for j := 0; j < iterationsNum; j++ {
					writeMessage(f, mutex, wg)
				}
			}()
		}
		wg.Wait()
		total += time.Since(start)
		fmt.Printf("%v: Recording is successfully completed and took %v.\n\n", time.Now().Format(time.UnixDate), time.Since(start))
	}()

	fmt.Printf("%v: Total time: %v", time.Now().Format(time.UnixDate), total)
}

// writeUser writes users as INSERT INTO queries in the db
func writeUser(f *os.File, m *sync.Mutex, w *sync.WaitGroup) {
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
	_, err := f.WriteString("INSERT INTO users(id, name) VALUES ('" + id.String() + "', '" + user.name + "'); \n")
	if err != nil {
		panic(err)
	}
}

// writeCategory writes categories as INSERT INTO queries in the db
func writeCategory(f *os.File, m *sync.Mutex, w *sync.WaitGroup) {
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
	if hasParent := rand.Float32() < 0.5; hasParent {
		category.parent_id = generateParentId(category.id)
		query = "INSERT INTO categories(id, name, parent_id) VALUES ('" + id.String() + "', '" + category.name + "', '" + category.parent_id.String() + "'); \n"
	} else {
		query = "INSERT INTO categories(id, name) VALUES ('" + id.String() + "', '" + category.name + "'); \n"
	}

	_, err := f.WriteString(query)
	if err != nil {
		panic(err)
	}
}

// writeMessage writes messages as INSERT INTO queries in the db
func writeMessage(f *os.File, m *sync.Mutex, w *sync.WaitGroup) {
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

	_, err := f.WriteString("INSERT INTO messages(id, text, category_id, posted_at, author_id) VALUES ('" + message.id.String() +
		"', '" + message.text + "', '" + message.category_id.String() + "', '" + message.posted_at.String() + "', '" +
		message.author_id.String() + "'); \n")
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