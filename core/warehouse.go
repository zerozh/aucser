package auccore

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

const TableShards = 8 // sharding table, 2^N, N>=0

// Warehouse handle bid data write/read in storage
type Warehouse interface {
	Initialize()
	Terminate()
	Add(bid *Bid) error              // Add data to log warehouse
	Commit(bid *Bid) error           // Add data to result warehouse
	Restore(store *Store, c *Config) // Restore data from log warehouse to Store
}

// MemoryWarehouse store data in memory, for debug and high concurrency test
// MySQL and Postgres are hardly handle more than 10k TPS
type MemoryWarehouse struct {
	store     *Store
	simulator *ConcurrencySimulator
}

func NewMemoryWarehouse() *MemoryWarehouse {
	return &MemoryWarehouse{}
}

func (w *MemoryWarehouse) Initialize() {
	w.store = NewStore(0)
	w.simulator = NewConcurrencySimulator(11000 + rand.Intn(2000))
}

func (w *MemoryWarehouse) Terminate() {
}

func (w *MemoryWarehouse) Add(bid *Bid) error {
	w.simulator.Run()

	bid.Time = time.Now().Truncate(time.Microsecond)

	bidCopy := *bid
	w.store.Add(&bidCopy)

	return nil
}

func (w *MemoryWarehouse) Commit(bid *Bid) error {
	return nil
}

func (w *MemoryWarehouse) Restore(store *Store, c *Config) {
	for _, key := range w.store.BidderChain.Index {
		b := w.store.BidderChain.Blocks[key]
		for _, bid := range b.Bids {
			bidCopy := *bid
			bidCopy.Active = true
			if bid.Sequence == 1 && bid.Time.After(c.StartTime) && bid.Time.Before(c.HalfTime) {
				store.Add(&bidCopy)
			} else if bid.Sequence > 1 && bid.Time.After(c.HalfTime) && bid.Time.Before(c.EndTime) {
				store.Add(&bidCopy)
			} else {
				// ignore invalid bid
			}
		}
	}
}

type PostgresWarehouse struct {
	table string // table prefix
	db    *sql.DB
	loc   *time.Location
	once  sync.Once
	log   *log.Logger
}

func NewPostgresWarehouse(table string, db *sql.DB, logger *log.Logger) *PostgresWarehouse {
	return &PostgresWarehouse{
		table: table,
		db:    db,
		log:   logger,
	}
}

func (w *PostgresWarehouse) Initialize() {
	w.once.Do(func() {
		w.loc, _ = time.LoadLocation("Asia/Shanghai")
		for i := 0; i < TableShards; i++ {
			_, err := w.db.Exec(fmt.Sprintf(`CREATE TABLE %s (
			id BIGSERIAL PRIMARY KEY,
    		client INT,
    		price INT,
    		sequence SMALLINT,
    		ts TIMESTAMP(6) DEFAULT now());`, w.table+fmt.Sprintf("%04d", i)))

			if err != nil {
				w.log.Panicln(err)
			}
		}

		_, err := w.db.Exec(fmt.Sprintf(`CREATE TABLE %s (
			id BIGSERIAL PRIMARY KEY,
    		client INT,
    		price INT,
    		sequence SMALLINT,
    		ts TIMESTAMP(6) DEFAULT now());`, w.getTableResult()))

		if err != nil {
			w.log.Panicln(err)
		}
	})
}

func (w *PostgresWarehouse) Terminate() {
	w.db.Close()
}

func (w *PostgresWarehouse) Add(bid *Bid) error {
	ctx := context.Background()
	conn, err := w.db.Conn(ctx)
	defer conn.Close()

	if err != nil {
		w.log.Println("ERR:GetConn")
		w.log.Println(err)
		return Error{Code: CodeServerSaveError0, Message: "Add err"}
	}

	var ts string
	if err := conn.QueryRowContext(ctx, "INSERT INTO "+w.getTableByClient(bid.Client)+" (client, price, sequence) VALUES ($1, $2, $3) RETURNING ts", bid.Client, bid.Price, bid.Sequence).Scan(&ts); err != nil {
		w.log.Println("ERR:GetRow")
		w.log.Println(err)
		return Error{Code: CodeServerSaveError3, Message: "Add err"}
	}

	t, e := time.ParseInLocation("2006-01-02T15:04:05.999999999Z", ts, w.loc)
	if e != nil {
		w.log.Println(e)
		return Error{Code: CodeServerSaveError4, Message: "Add err"}
	}

	// set process time
	bid.Time = t.Truncate(time.Microsecond)

	return nil
}

func (w *PostgresWarehouse) Commit(bid *Bid) error {
	_, e := w.db.Exec("INSERT INTO "+w.getTableResult()+" (client, price, sequence, ts) VALUES ($1, $2, $3, $4)", bid.Client, bid.Price, bid.Sequence, bid.Time.Format("2006-01-02 15:04:05.000000"))
	if e != nil {
		w.log.Println("ERR:INSERT INTO")
		w.log.Println(e)
		return Error{Code: CodeServerSaveError5, Message: "Commit err"}
	}

	return nil
}

func (w *PostgresWarehouse) Restore(store *Store, c *Config) {
	pageSize := 1000

	for t := 0; t < TableShards; t++ {
		id := 0
		for {
			curI := 0
			rows, err := w.db.Query("SELECT id,client,price,sequence,ts FROM "+w.table+fmt.Sprintf("%04d", t)+" WHERE id > $1 ORDER BY id ASC LIMIT "+strconv.Itoa(pageSize), id)
			for rows.Next() {
				bid := &Bid{Active: true}
				var ts string
				err := rows.Scan(&id, &bid.Client, &bid.Price, &bid.Sequence, &ts)
				if err != nil {
					log.Fatal(err)
				}
				t, e := time.ParseInLocation("2006-01-02T15:04:05.999999999Z", ts, w.loc)
				if e != nil {
					log.Fatal(err)
				}
				bid.Time = t.Truncate(time.Microsecond)
				if bid.Sequence == 1 && bid.Time.After(c.StartTime) && bid.Time.Before(c.HalfTime) {
					store.Add(bid)
				} else if bid.Sequence > 1 && bid.Time.After(c.HalfTime) && bid.Time.Before(c.EndTime) {
					store.Add(bid)
				} else {
					// ignore invalid bid
				}

				curI++
			}
			err = rows.Err()
			if err != nil {
				log.Fatal(err)
			}

			if curI < pageSize {
				break
			}
		}
	}
}

func (w *PostgresWarehouse) getTableByClient(client int) string {
	return w.table + fmt.Sprintf("%04d", client&(TableShards-1))
}

func (w *PostgresWarehouse) getTableResult() string {
	return w.table + "f"
}

type MysqlWarehouse struct {
	table string // table prefix
	db    *sql.DB
	loc   *time.Location
	once  sync.Once
	log   *log.Logger
}

func NewMysqlWarehouse(table string, db *sql.DB, logger *log.Logger) *MysqlWarehouse {
	return &MysqlWarehouse{
		table: table,
		db:    db,
		log:   logger,
	}
}

func (w *MysqlWarehouse) Initialize() {
	w.once.Do(func() {
		w.loc, _ = time.LoadLocation("Asia/Shanghai")
		for i := 0; i < TableShards; i++ {
			_, err := w.db.Exec(fmt.Sprintf(`CREATE TABLE %s (
			id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
			client INT(10) UNSIGNED NOT NULL,
			price INT(10) UNSIGNED NOT NULL,
			sequence TINYINT(3) UNSIGNED NOT NULL,
			ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
			PRIMARY KEY (id)) ENGINE = MyISAM;`, w.table+fmt.Sprintf("%04d", i)))

			if err != nil {
				w.log.Panicln(err)
			}
		}

		_, err := w.db.Exec(fmt.Sprintf(`CREATE TABLE %s (
			id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
			client INT(10) UNSIGNED NOT NULL,
			price INT(10) UNSIGNED NOT NULL,
			sequence TINYINT(3) UNSIGNED NOT NULL,
			ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
			PRIMARY KEY (id)) ENGINE = MyISAM;`, w.getTableResult()))

		if err != nil {
			w.log.Panicln(err)
		}
	})
}

func (w *MysqlWarehouse) Terminate() {
	w.db.Close()
}

func (w *MysqlWarehouse) Add(bid *Bid) error {
	ctx := context.Background()
	conn, err := w.db.Conn(ctx)
	defer conn.Close()

	if err != nil {
		w.log.Println("ERR:GetConn")
		w.log.Println(err)
		return Error{Code: CodeServerSaveError0, Message: "Add err"}
	}

	r, err := conn.ExecContext(ctx, "INSERT INTO "+w.getTableByClient(bid.Client)+" (client, price, sequence) VALUES (?, ?, ?)", bid.Client, bid.Price, bid.Sequence)
	if err != nil {
		w.log.Println("ERR:INSERT INTO")
		w.log.Println(err)
		return Error{Code: CodeServerSaveError1, Message: "Add err"}
	}

	l, err := r.LastInsertId()
	if err != nil {
		w.log.Println("ERR:LastInsertId")
		w.log.Println(err)
		return Error{Code: CodeServerSaveError2, Message: "Add err"}
	}

	var ts string
	if err := conn.QueryRowContext(ctx, "SELECT ts FROM "+w.getTableByClient(bid.Client)+" WHERE id = ? LIMIT 1", l).Scan(&ts); err != nil {
		w.log.Println("ERR:GetRow")
		w.log.Println(err)
		return Error{Code: CodeServerSaveError3, Message: "Add err"}
	}

	t, e := time.ParseInLocation("2006-01-02 15:04:05.000000", ts, w.loc)
	if e != nil {
		w.log.Println(e)
		return Error{Code: CodeServerSaveError4, Message: "Add err"}
	}

	// set process time
	bid.Time = t.Truncate(time.Microsecond)

	return nil
}

func (w *MysqlWarehouse) Commit(bid *Bid) error {
	_, e := w.db.Exec("INSERT INTO "+w.getTableResult()+" (client, price, sequence, ts) VALUES (?, ?, ?, ?)", bid.Client, bid.Price, bid.Sequence, bid.Time.Format("2006-01-02 15:04:05.000000"))
	if e != nil {
		w.log.Println("ERR:INSERT INTO")
		w.log.Println(e)
		return Error{Code: CodeServerSaveError5, Message: "Commit err"}
	}

	return nil
}

func (w *MysqlWarehouse) Restore(store *Store, c *Config) {
	pageSize := 1000

	for t := 0; t < TableShards; t++ {
		id := 0
		for {
			curI := 0
			rows, err := w.db.Query("SELECT id,client,price,sequence,ts FROM "+w.table+fmt.Sprintf("%04d", t)+" WHERE id > ? ORDER BY id ASC LIMIT "+strconv.Itoa(pageSize), id)
			for rows.Next() {
				bid := &Bid{Active: true}
				var ts string
				err := rows.Scan(&id, &bid.Client, &bid.Price, &bid.Sequence, &ts)
				if err != nil {
					log.Fatal(err)
				}
				t, e := time.ParseInLocation("2006-01-02 15:04:05.000000", ts, w.loc)
				if e != nil {
					log.Fatal(err)
				}
				bid.Time = t.Truncate(time.Microsecond)
				if bid.Sequence == 1 && bid.Time.After(c.StartTime) && bid.Time.Before(c.HalfTime) {
					store.Add(bid)
				} else if bid.Sequence > 1 && bid.Time.After(c.HalfTime) && bid.Time.Before(c.EndTime) {
					store.Add(bid)
				} else {
					// ignore invalid bid
				}

				curI++
			}
			err = rows.Err()
			if err != nil {
				log.Fatal(err)
			}

			if curI < pageSize {
				break
			}
		}
	}
}

func (w *MysqlWarehouse) getTableByClient(client int) string {
	return w.table + fmt.Sprintf("%04d", client&(TableShards-1))
}

func (w *MysqlWarehouse) getTableResult() string {
	return w.table + "f"
}
