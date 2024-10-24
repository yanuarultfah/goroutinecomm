package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// with addconcurrent
	// numbers := generateList(1e7)

	// fmt.Println("add :", add(numbers))
	// fmt.Println("add concurrent :", addConcurrent(runtime.NumCPU(), numbers))
	// end with addconcurrent
	var result int64
	numbers := generateList(1e7)
	goroutines := runtime.NumCPU()
	lastGoroutine := goroutines - 1
	totalNumbers := len(numbers)
	strid := totalNumbers / goroutines
	c := make(chan int)
	for g := 0; g < goroutines; g++ {
		start := g * strid
		end := start + strid
		if g == lastGoroutine {
			end = totalNumbers
		}
		go add(numbers[start:end], c)
	}

	for j := 0; j < goroutines; j++ {
		result += int64(<-c)
	}
	// with result deadlock
	// for l := range c {
	// 	result += int64(l)
	// }
	// end with result deadlock
	fmt.Println("add concurrent:", result)

	start := time.Now()
	db, err := openDbConnection()
	if err != nil {
		log.Fatal(err.Error())
	}

	csvReader, csvFile, err := openCsvFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csvFile.Close()

	jobs := make(chan []interface{}, 0)
	wg := new(sync.WaitGroup)

	go dispatchWorker(db, jobs, wg)
	readCsvFilePerLineThenSendToWorker(csvReader, jobs, wg)
	wg.Wait()
	duration := time.Since(start)
	fmt.Println("done in", int(math.Ceil(duration.Seconds())), " seconds")

}

func generateList(totalNumbers int) []int {
	numbers := make([]int, totalNumbers)
	for i := 0; i < totalNumbers; i++ {
		numbers[i] = rand.Intn(totalNumbers)
	}
	return numbers
}

func add(numbers []int, c chan int) {
	var v int
	for _, n := range numbers {
		v += n
	}
	c <- v
}

// func addConcurrent(goroutines int, numbers []int) int {
// 	var v int64
// 	totalNumbers := len(numbers)
// 	lastGoroutine := goroutines - 1
// 	strid := totalNumbers / goroutines
// 	var wg sync.WaitGroup
// 	wg.Add(goroutines)
// 	for g := 0; g < goroutines; g++ {
// 		go func(g int) {
// 			start := g * strid
// 			end := start + strid
// 			if g == lastGoroutine {
// 				end = totalNumbers
// 			}
// 			var lv int
// 			for _, n := range numbers[start:end] {
// 				lv += n
// 			}
// 			atomic.AddInt64(&v, int64(lv))
// 			wg.Done()
// 		}(g)
// 	}
// 	wg.Wait()
// 	return int(v)

// }
var (
	Client   *sql.DB
	username = "root"
	password = "root"
	host     = "127.0.0.1:3306"
	schema   = "users_db"
	// dbConnString   = "root:root@127.0.0.1:3306127.0.0.1:3306/users_db"
	dbMaxConns     = 100
	dbMaxIdleConns = 4
	totalWorker    = 100
	csvFile        = "majestic_million.csv"
	dataHeaders    = make([]string, 0)
)

// func init() {
// dataSourceName := fmt.Sprintf("%s:%s@tcp(%s)/%s",
// 	username,
// 	password,
// 	host,
// 	schema,
// )

// 	var err error
// 	Client, err = sql.Open("mysql", dataSourceName)
// 	if err != nil {
// 		panic(err)
// 	}
// 	if err = Client.Ping(); err != nil {
// 		panic(err)
// 	}
// 	log.Println("database successfully configured")

// }
func openDbConnection() (*sql.DB, error) {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		username,
		password,
		host,
		schema,
	)
	log.Println("=> open db connection")

	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(dbMaxConns)
	db.SetMaxIdleConns(dbMaxIdleConns)

	return db, nil
}

func openCsvFile() (*csv.Reader, *os.File, error) {
	log.Println("=> Open Csv File")
	f, err := os.Open(csvFile)
	if err != nil {
		return nil, nil, err
	}

	reader := csv.NewReader(f)
	return reader, f, nil
}

func dispatchWorker(db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= totalWorker; workerIndex++ {
		go func(workerIndex int, db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			counter := 0

			for job := range jobs {
				doTheJob(workerIndex, counter, db, job)
				wg.Done()
				counter++
			}
		}(workerIndex, db, jobs, wg)
	}
}

func readCsvFilePerLineThenSendToWorker(csvreader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	for {
		row, err := csvreader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if len(dataHeaders) == 0 {
			dataHeaders = row
			continue
		}

		rowOrdered := make([]interface{}, 0)
		// make([]interface{}, 0)
		for _, each := range row {
			rowOrdered = append(rowOrdered, each)
		}

		wg.Add(1)
		jobs <- rowOrdered

	}

	close(jobs)
}

func doTheJob(workerIndex, counter int, db *sql.DB, values []interface{}) {
	for {
		var outerError error
		func(outerError *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerError = fmt.Errorf("%v", err)
				}

			}()
			conn, err := db.Conn(context.Background())
			if err != nil {
				fmt.Println("error connection")
			}
			query := fmt.Sprintf("insert into domain(%s) values(%s)",
				strings.Join(dataHeaders, ","),
				strings.Join(generateQuestionMark(len(dataHeaders)), ","),
			)

			_, err = conn.ExecContext(context.Background(), query, values...)
			if err != nil {
				log.Fatal(err.Error())
			}

			err = conn.Close()
			if err != nil {
				log.Fatal(err.Error())
			}
		}(&outerError)
		if outerError == nil {
			break
		}
	}

	if counter%100 == 0 {
		log.Println("=> worker", workerIndex, "inserted", counter, "data")
	}
}

func generateQuestionMark(n int) []string {
	s := make([]string, 0)
	for i := 0; i < n; i++ {
		s = append(s, "?")
	}

	return s
}
