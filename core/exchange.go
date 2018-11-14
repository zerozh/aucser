package auccore

import (
	"database/sql"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/lib/pq"
)

const Threshold = 10000
const PricingDelta = 3
const BidsPerBidder = 3

// Exchange hold a exchange instance for bid,
// including time period control, system log
type Exchange struct {
	// exchange uuid
	uuid string
	// conf
	c *Config
	// runtime status, collect per second
	r *State

	// state
	session  int // 0 before start, 1 first half, 2 second half, 3 end
	verified int // dump all data and verify

	serial      uint64 // serial number for each Bid, atomic increasing
	lowestPrice int
	lowestTime  time.Time
	bidders     int // total bidders

	// timer & locker
	startTimer      *time.Timer
	halfTimer       *time.Timer
	endTimer        *time.Timer
	stateTicker     *time.Ticker
	quitStateTicker chan struct{}
	updateLock      sync.Mutex // update state lock
	conLock         chan bool  // concurrency lock channel

	// storage
	st *Store
	wh Warehouse

	// util
	sysLog *log.Logger
	bidLog *log.Logger
	resLog *log.Logger
	loc    *time.Location

	counterHit     uint64
	counterProcess uint64
	counterReq     *Counter
	counterRes     *Counter
}

type Config struct {
	StartTime time.Time `json:"startTime"`
	HalfTime  time.Time `json:"halfTime"`
	EndTime   time.Time `json:"endTime"`

	Capacity     int `json:"capacity"`
	WarningPrice int `json:"warningPrice"` // warning price of first half, 0 for disable
}

type State struct {
	Time    time.Time
	Session int

	LowestPrice int
	LowestTime  time.Time
	Bidders     int
}

type Counter struct {
	sync.RWMutex
	ct map[string]int
}

func newCounter() *Counter {
	return &Counter{ct: make(map[string]int)}
}

func NewExchange(conf Config) *Exchange {
	pid := conf.StartTime.Format("060102150405")

	// init log files
	logFile1, _ := os.OpenFile("./logs/"+pid+"_server_sys.txt", os.O_CREATE|os.O_WRONLY, 0666)
	mw1 := io.MultiWriter(logFile1)
	sysLogger := log.New(mw1, "", log.LstdFlags)

	logFile2, _ := os.OpenFile("./logs/"+pid+"_server_bid.txt", os.O_CREATE|os.O_WRONLY, 0666)
	mw2 := io.MultiWriter(logFile2)
	bidLogger := log.New(mw2, "", log.LstdFlags)

	logFile3, _ := os.OpenFile("./logs/"+pid+"_server_res.txt", os.O_CREATE|os.O_WRONLY, 0666)
	mw3 := io.MultiWriter(logFile3)
	resLogger := log.New(mw3, "", 0)

	// init warehouse
	var wh Warehouse
	if os.Getenv("DB_DRIVER") == "mysql" {
		db, _ := sql.Open("mysql", os.Getenv("MYSQL_DSN"))
		// default max connections of mysql is 151
		db.SetMaxIdleConns(150)
		db.SetMaxOpenConns(150)
		wh = NewMysqlWarehouse("pp_"+pid+"_", db, sysLogger)
	} else if os.Getenv("DB_DRIVER") == "postgres" {
		db, _ := sql.Open("postgres", os.Getenv("POSTGRES_DSN"))
		// default max connections of postgres is 100
		db.SetMaxIdleConns(99)
		db.SetMaxOpenConns(99)
		wh = NewPostgresWarehouse("pp_"+pid+"_", db, sysLogger)
	} else {
		wh = NewMemoryWarehouse()
	}
	wh.Initialize()

	loc, _ := time.LoadLocation("Asia/Shanghai")

	return &Exchange{
		uuid:   pid,
		c:      &conf,
		sysLog: sysLogger,
		bidLog: bidLogger,
		resLog: resLogger,
		loc:    loc,
		wh:     wh,
		st:     NewStore(conf.Capacity),
	}
}

// Serve start to serve incoming request
func (e *Exchange) Serve() {
	e.session = 0

	e.lowestPrice = 1
	e.lowestTime = e.c.StartTime

	e.r = &State{}
	e.collectStat()

	// runtime state
	e.conLock = make(chan bool, Threshold)
	e.quitStateTicker = make(chan struct{})

	// add clock
	now := time.Now()
	tStartDuration := time.Duration(0)
	if now.Before(e.c.StartTime) {
		tStartDuration = e.c.StartTime.Sub(now)
	}
	e.startTimer = time.AfterFunc(tStartDuration, e.toggleStart)
	e.halfTimer = time.AfterFunc(e.c.HalfTime.Sub(now), e.toggleHalf)
	e.endTimer = time.AfterFunc(e.c.EndTime.Sub(now), e.toggleEnd)

	// init counter
	e.counterReq = newCounter()
}

// Shutdown stop all service gracefully (save & exit)
func (e *Exchange) Shutdown() {
	e.stopTimer()

	if e.verified == 0 {
		e.Verify()
	}

	e.releaseResource()
}

// Close stop all service right now (exit)
func (e *Exchange) Close() {
	e.stopTimer()
	e.releaseResource()
}

func (e *Exchange) stopTimer() {
	if e.stateTicker != nil {
		close(e.quitStateTicker)
	}
	if e.startTimer != nil {
		e.startTimer.Stop()
	}
	if e.halfTimer != nil {
		e.halfTimer.Stop()
	}
	if e.endTimer != nil {
		e.endTimer.Stop()
	}
}

func (e *Exchange) releaseResource() {
	if e.wh != nil {
		e.wh.Terminate()
	}
}

// Verify check all data correct
// it should be called before Shutdown
func (e *Exchange) Verify() {
	e.sysLog.Println("===============================")
	e.sysLog.Printf(">>> Start Verify @ %s", time.Now().Format("15:04:05.000000"))
	e.sysLog.Println("===============================")

	// protect duplicate dump
	if e.verified > 0 {
		return
	}
	e.verified++

	whStore := NewStore(0)
	e.wh.DumpToStore(whStore, e.c)
	if !e.st.Equal(whStore) {
		// ERROR
	}

	whStore.SetCapacity(e.c.Capacity)
	whStore.SortAllBlocks()

	// export final result
	e.Dump()
	e.Output()

	// bid stats
	//e.counterReq.Lock()
	//names := make([]string, 0, len(e.counterReq.ct))
	//for name := range e.counterReq.ct {
	//	names = append(names, name)
	//}
	//sort.Strings(names) //sort by key
	//for _, name := range names {
	//	e.sysLog.Printf("%s %d\n", name, e.counterReq.ct[name])
	//}
	//e.counterReq.Unlock()

	e.sysLog.Println("===============================")
	e.sysLog.Printf(">>> End Verify @ %s", time.Now().Format("15:04:05.000000"))
	e.sysLog.Println("===============================")

	// log memory use
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	e.sysLog.Printf(">>> Memory Alloc %d, TotalAlloc %d, HeapAlloc %d, HeapSys %d", mem.Alloc, mem.TotalAlloc, mem.HeapAlloc, mem.HeapSys)
}

func (e *Exchange) BidderStatus(client int) (*Bid, error) {
	bidder := e.st.GetBidderBlock(client)
	if bidder != nil {
		return bidder.Bids[len(bidder.Bids)-1], nil
	}

	return nil, Error{Code: CodeRequestNotAttend, Message: "Not attend"}
}

func (e *Exchange) Config() *Config {
	return e.c
}

func (e *Exchange) State() *State {
	return e.r
}

func (e *Exchange) BiddersCount() int {
	return e.st.CountBidders()
}

func (e *Exchange) BidsCount() int {
	return e.st.CountBids()
}

func (e *Exchange) BeforeStart() bool {
	return e.session == 0
}

func (e *Exchange) AfterEnd() bool {
	return e.session == 3
}

func (e *Exchange) IsSession() bool {
	return e.session == 1 || e.session == 2
}

func (e *Exchange) IsSession1() bool {
	return e.session == 1
}

func (e *Exchange) IsSession2() bool {
	return e.session == 2
}

func (e *Exchange) toggleStart() {
	e.session++
	e.sysLog.Println("===============================")
	e.sysLog.Printf(">>> Session %d @ %s", e.session, time.Now().Format("15:04:05.000000"))
	e.sysLog.Println("===============================")

	go e.startCollector()
}

func (e *Exchange) toggleHalf() {
	e.session++
	e.sysLog.Println("===============================")
	e.sysLog.Printf(">>> Session %d @ %s", e.session, time.Now().Format("15:04:05.000000"))
	e.sysLog.Println("===============================")

	// count bidders last time
	e.collectCountBidders()
}

func (e *Exchange) toggleEnd() {
	e.session++
	e.sysLog.Println("===============================")
	e.sysLog.Printf(">>> Session %d @ %s", e.session, time.Now().Format("15:04:05.000000"))
	e.sysLog.Println("===============================")

	if e.stateTicker != nil {
		close(e.quitStateTicker)
	}

	// collect stat last time
	e.collectStat()
}

func (e *Exchange) incrRequestCount() {
	k := time.Now().Format("150405")
	e.counterReq.Lock()
	e.counterReq.ct[k] = e.counterReq.ct[k] + 1
	atomic.AddUint64(&e.counterHit, 1)
	e.counterReq.Unlock()
}

// Bid accept a *Bid request with only Bid.Client and Bid.Price
// If success, *Bid will be fulfill with Bid.Time, Bid.Sequence
func (e *Exchange) Bid(bid *Bid) error {
	e.incrRequestCount()

	// assign a serial number
	bid.Serial = int(atomic.AddUint64(&e.serial, 1))
	tInit := time.Now()
	err := e.bid(bid)

	if err != nil {
		var pTime time.Time
		if bid.Time.IsZero() {
			pTime = time.Now()
		} else {
			pTime = bid.Time
		}
		e.bidLog.Printf("<<< %d %4d @ %s (%6d) %.4fs ✘ %d %s", bid.Client, bid.Price, tInit.Format("15:04:05.000"), bid.Serial, pTime.Sub(tInit).Seconds(), err.(Error).Code, err.(Error).Message)
	} else {
		e.bidLog.Printf("<<< %d %4d @ %s (%6d) %.4fs ✔ ", bid.Client, bid.Price, tInit.Format("15:04:05.000"), bid.Serial, bid.Time.Sub(tInit).Seconds())
	}

	return err
}

func (e *Exchange) bid(bid *Bid) error {
	if !bid.Time.IsZero() || bid.Sequence != 0 || bid.Active {
		return Error{Code: CodeRequestInvalid, Message: "Invalid request"}
	}

	if !e.IsSession() {
		if e.BeforeStart() {
			return Error{Code: CodeServerNotReady, Message: "Not ready"}
		} else {
			return Error{Code: CodeServerEnd, Message: "Invalid time"}
		}
	}

	// concurrency lock
	e.conLock <- true

	err := e.bidProcess(bid)
	// concurrency release
	<-e.conLock

	return err
}

func (e *Exchange) bidProcess(bid *Bid) error {
	if bid.Price < 1 {
		return Error{Code: CodeRequestInvalidPrice, Message: "Invalid price"}
	}

	bid.Active = true

	var err error
	if e.IsSession1() {
		err = e.bidSession1(bid)
	} else if e.IsSession2() {
		err = e.bidSession2(bid)
	} else {
		bid.Active = false
		return Error{Code: CodeRequestInvalidTime, Message: "Invalid time"}
	}

	if err != nil {
		bid.Active = false
		return err
	}

	// bid success, update LowestTenderableBid
	e.lowestPrice = e.st.LowestTenderableBid.Price
	e.lowestTime = e.st.LowestTenderableBid.Time

	return nil
}

func (e *Exchange) bidSession1(bid *Bid) error {
	if b := e.st.GetBidderBlock(bid.Client); b != nil {
		return Error{Code: CodeRequestAttendFirstRound, Message: "Attend first round"}
	}

	if e.c.WarningPrice > 0 && bid.Price > e.c.WarningPrice {
		return Error{Code: CodeRequestGTWarningPrice, Message: "Greater than WarningPrice"}
	}

	// save to warehouse
	bid.Sequence = 1
	if err := e.wh.Save(bid); err != nil {
		return err
	}

	// check db save time
	if bid.Time.After(e.c.HalfTime) || bid.Time.Equal(e.c.HalfTime) {
		return Error{Code: CodeRequestEnd1, Message: "End"}
	}

	// save to store
	e.st.Add(bid)
	atomic.AddUint64(&e.counterProcess, 1)

	return nil
}

func (e *Exchange) bidSession2(bid *Bid) error {
	bidder := e.st.GetBidderBlock(bid.Client)
	if bidder == nil {
		return Error{Code: CodeRequestNotAttendFirstRound, Message: "Not attend first round"}
	}

	if bidder.Total >= BidsPerBidder {
		return Error{Code: CodeRequestAllIn, Message: "Allin"}
	}

	// compare with previous bid
	for _, preBid := range bidder.Bids {
		if preBid.Price == bid.Price {
			return Error{Code: CodeRequestSamePrice, Message: "Same price"}
		}
	}

	// we should check price FIRST to avoid consumption above, but we have to qualify first
	if bid.Price-e.lowestPrice > PricingDelta || e.lowestPrice-bid.Price > PricingDelta {
		return Error{Code: CodeRequestOutOfRange, Message: "Out of Range"}
	}

	// save to warehouse
	bid.Sequence = int(bidder.Total) + 1
	if err := e.wh.Save(bid); err != nil {
		return err
	}

	// check db save time
	if bid.Time.After(e.c.EndTime) || bid.Time.Equal(e.c.EndTime) {
		return Error{Code: CodeRequestEnd2, Message: "End"}
	}

	// save to store
	e.st.Add(bid)
	atomic.AddUint64(&e.counterProcess, 1)

	return nil
}

// startCollector start a time.Ticker to collect system state per second
func (e *Exchange) startCollector() {
	e.stateTicker = time.NewTicker(time.Second * 1)
	//for ; true; <-e.stateTicker.C {
	//	e.collectStat()
	//}
	for {
		select {
		case <-e.stateTicker.C:
			e.collectStat()
		case <-e.quitStateTicker:
			// release resources avoid memory leak
			e.stateTicker.Stop()
			e.stateTicker = nil
			return
		}
	}
}

func (e *Exchange) collectStat() {
	if e.IsSession1() {
		e.collectCountBidders()
	}

	e.r.Time = time.Now()
	e.r.Session = e.session
	e.r.Bidders = e.bidders
	e.r.LowestPrice = e.lowestPrice
	e.r.LowestTime = e.lowestTime

	e.sysLog.Printf("%s %3.0f %4d @ %s, B %6d, O %6d, G %6d, H %6d, P %6d\n", time.Now().Format("15:04:05.000000"), e.c.EndTime.Sub(time.Now()).Seconds(), e.r.LowestPrice, e.r.LowestTime.Format("15:04:05"), e.r.Bidders, e.BidsCount(), runtime.NumGoroutine(), atomic.SwapUint64(&e.counterHit, 0), atomic.SwapUint64(&e.counterProcess, 0))
}

func (e *Exchange) collectCountBidders() {
	e.bidders = e.st.CountBidders()
}

// Dump save all final result to log
func (e *Exchange) Dump() {
	DumpAll(e.resLog, e.st)
}

// Output save final tender to storage
func (e *Exchange) Output() {
	success := 0
	for _, key := range e.st.PriceChain.Index {
		b := e.st.PriceChain.Blocks[key]
		for _, bid := range b.Bids {
			if success < e.st.Capacity && bid.Active {
				e.wh.FinalSave(bid)
				success++
			}
		}
	}
}
