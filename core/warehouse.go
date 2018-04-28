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
	Save(bid *Bid) error
	FinalSave(bid *Bid) error
	DumpToStore(store *Store, c *Config)
}

// MemoryWarehouse store data in memory, for debug and high concurrency test
// MySQL and Postgres are hardly handle more than 10k TPS
type MemoryWarehouse struct {
	store *Store
	sim   *ConcurrencySimulator
}

func NewMemoryWarehouse() *MemoryWarehouse {
	return &MemoryWarehouse{}
}

func (d *MemoryWarehouse) Initialize() {
	d.store = NewStore(0)
	d.sim = NewConcurrencySimulator(11000 + rand.Intn(2000))
}

func (d *MemoryWarehouse) Terminate() {
}

func (d *MemoryWarehouse) Save(bid *Bid) error {
	d.sim.Run()

	bid.Time = time.Now()
	d.store.Add(bid)

	return nil
}

func (d *MemoryWarehouse) FinalSave(bid *Bid) error {
	return nil
}

func (d *MemoryWarehouse) DumpToStore(store *Store, c *Config) {
	*store = *d.store
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

func (d *PostgresWarehouse) Initialize() {
	d.once.Do(func() {
		d.loc, _ = time.LoadLocation("Asia/Shanghai")
		for i := 0; i < TableShards; i++ {
			_, err := d.db.Exec(fmt.Sprintf(`CREATE TABLE %s (
			id BIGSERIAL PRIMARY KEY,
    		client INT,
    		price INT,
    		sequence SMALLINT,
    		ts TIMESTAMP(6) DEFAULT now());`, d.table+fmt.Sprintf("%04d", i)))

			if err != nil {
				d.log.Panicln(err)
			}
		}

		_, err := d.db.Exec(fmt.Sprintf(`CREATE TABLE %s (
			id BIGSERIAL PRIMARY KEY,
    		client INT,
    		price INT,
    		sequence SMALLINT,
    		ts TIMESTAMP(6) DEFAULT now());`, d.getTableResult()))

		if err != nil {
			d.log.Panicln(err)
		}
	})
}

func (d *PostgresWarehouse) Terminate() {
	d.db.Close()
}

func (d *PostgresWarehouse) Save(bid *Bid) error {
	ctx := context.Background()
	conn, err := d.db.Conn(ctx)
	defer conn.Close()

	if err != nil {
		d.log.Println("ERR:GetConn")
		d.log.Println(err)
		return Error{Code: 30, Message: "Save err"}
	}

	var ts string
	if err := conn.QueryRowContext(ctx, "INSERT INTO "+d.getTableByClient(bid.Client)+" (client, price, sequence) VALUES ($1, $2, $3) RETURNING ts", bid.Client, bid.Price, bid.Sequence).Scan(&ts); err != nil {
		d.log.Println("ERR:GetRow")
		d.log.Println(err)
		return Error{Code: 33, Message: "Save err"}
	}

	t, e := time.ParseInLocation("2006-01-02T15:04:05.999999999Z", ts, d.loc)
	if e != nil {
		d.log.Println(e)
		return Error{Code: 34, Message: "Save err"}
	}

	// set process time
	bid.Time = t

	return nil
}

func (d *PostgresWarehouse) FinalSave(bid *Bid) error {
	_, e := d.db.Exec("INSERT INTO "+d.getTableResult()+" (client, price, sequence, ts) VALUES ($1, $2, $3, $4)", bid.Client, bid.Price, bid.Sequence, bid.Time.Format("2006-01-02 15:04:05.000000"))
	if e != nil {
		d.log.Println("ERR:INSERT INTO")
		d.log.Println(e)
		return Error{Code: 500, Message: "FinalSave err"}
	}

	return nil
}

func (d *PostgresWarehouse) DumpToStore(store *Store, c *Config) {
	pageSize := 1000

	for t := 0; t < TableShards; t++ {
		id := 0
		for {
			curI := 0
			rows, err := d.db.Query("SELECT id,client,price,sequence,ts FROM "+d.table+fmt.Sprintf("%04d", t)+" WHERE id > $1 ORDER BY id ASC LIMIT "+strconv.Itoa(pageSize), id)
			for rows.Next() {
				bid := &Bid{Active: true}
				var ts string
				err := rows.Scan(&id, &bid.Client, &bid.Price, &bid.Sequence, &ts)
				if err != nil {
					log.Fatal(err)
				}
				t, e := time.ParseInLocation("2006-01-02T15:04:05.999999999Z", ts, d.loc)
				if e != nil {
					log.Fatal(err)
				}
				bid.Time = t
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

func (d *PostgresWarehouse) getTableByClient(client int) string {
	return d.table + fmt.Sprintf("%04d", client&(TableShards-1))
}

func (d *PostgresWarehouse) getTableResult() string {
	return d.table + "f"
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

func (d *MysqlWarehouse) Initialize() {
	d.once.Do(func() {
		d.loc, _ = time.LoadLocation("Asia/Shanghai")
		for i := 0; i < TableShards; i++ {
			_, err := d.db.Exec(fmt.Sprintf(`CREATE TABLE %s (
			id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
			client INT(10) UNSIGNED NOT NULL,
			price INT(10) UNSIGNED NOT NULL,
			sequence TINYINT(3) UNSIGNED NOT NULL,
			ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
			PRIMARY KEY (id)) ENGINE = MyISAM;`, d.table+fmt.Sprintf("%04d", i)))

			if err != nil {
				d.log.Panicln(err)
			}
		}

		_, err := d.db.Exec(fmt.Sprintf(`CREATE TABLE %s (
			id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
			client INT(10) UNSIGNED NOT NULL,
			price INT(10) UNSIGNED NOT NULL,
			sequence TINYINT(3) UNSIGNED NOT NULL,
			ts TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
			PRIMARY KEY (id)) ENGINE = MyISAM;`, d.getTableResult()))

		if err != nil {
			d.log.Panicln(err)
		}
	})
}

func (d *MysqlWarehouse) Terminate() {
	d.db.Close()
}

func (d *MysqlWarehouse) Save(bid *Bid) error {
	ctx := context.Background()
	conn, err := d.db.Conn(ctx)
	defer conn.Close()

	if err != nil {
		d.log.Println("ERR:GetConn")
		d.log.Println(err)
		return Error{Code: 30, Message: "Save err"}
	}

	r, err := conn.ExecContext(ctx, "INSERT INTO "+d.getTableByClient(bid.Client)+" (client, price, sequence) VALUES (?, ?, ?)", bid.Client, bid.Price, bid.Sequence)
	if err != nil {
		d.log.Println("ERR:INSERT INTO")
		d.log.Println(err)
		return Error{Code: 31, Message: "Save err"}
	}

	l, err := r.LastInsertId()
	if err != nil {
		d.log.Println("ERR:LastInsertId")
		d.log.Println(err)
		return Error{Code: 32, Message: "Save err"}
	}

	var ts string
	if err := conn.QueryRowContext(ctx, "SELECT ts FROM "+d.getTableByClient(bid.Client)+" WHERE id = ? LIMIT 1", l).Scan(&ts); err != nil {
		d.log.Println("ERR:GetRow")
		d.log.Println(err)
		return Error{Code: 33, Message: "Save err"}
	}

	t, e := time.ParseInLocation("2006-01-02 15:04:05.000000", ts, d.loc)
	if e != nil {
		d.log.Println(e)
		return Error{Code: 34, Message: "Save err"}
	}

	// set process time
	bid.Time = t

	return nil
}

func (d *MysqlWarehouse) FinalSave(bid *Bid) error {
	_, e := d.db.Exec("INSERT INTO "+d.getTableResult()+" (client, price, sequence, ts) VALUES (?, ?, ?, ?)", bid.Client, bid.Price, bid.Sequence, bid.Time.Format("2006-01-02 15:04:05.000000"))
	if e != nil {
		d.log.Println("ERR:INSERT INTO")
		d.log.Println(e)
		return Error{Code: 500, Message: "FinalSave err"}
	}

	return nil
}

func (d *MysqlWarehouse) DumpToStore(store *Store, c *Config) {
	pageSize := 1000

	for t := 0; t < TableShards; t++ {
		id := 0
		for {
			curI := 0
			rows, err := d.db.Query("SELECT id,client,price,sequence,ts FROM "+d.table+fmt.Sprintf("%04d", t)+" WHERE id > ? ORDER BY id ASC LIMIT "+strconv.Itoa(pageSize), id)
			for rows.Next() {
				bid := &Bid{Active: true}
				var ts string
				err := rows.Scan(&id, &bid.Client, &bid.Price, &bid.Sequence, &ts)
				if err != nil {
					log.Fatal(err)
				}
				t, e := time.ParseInLocation("2006-01-02 15:04:05.000000", ts, d.loc)
				if e != nil {
					log.Fatal(err)
				}
				bid.Time = t
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

func (d *MysqlWarehouse) getTableByClient(client int) string {
	return d.table + fmt.Sprintf("%04d", client&(TableShards-1))
}

func (d *MysqlWarehouse) getTableResult() string {
	return d.table + "f"
}
