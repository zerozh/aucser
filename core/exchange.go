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

const BidProcessThreshold = 10000
const PricingDelta = 3
const BidsPerBidder = 3

const (
	SessionUnprepared = iota
	SessionFirstHalf
	SessionSecondHalf
	SessionFinished
)

// Exchange hold a exchange instance for bid,
// including time period control, system log
type Exchange struct {
	// exchange uuid
	uuid string

	config *Config
	state  *State // runtime status, collect per second
	final  *Final

	// state
	session int  // 0 before start, 1 first half, 2 second half, 3 end
	sealed  bool // finish dump all data and verify

	serial      uint64 // serial number for each Bid, atomic increasing
	lowestPrice int
	lowestTime  time.Time
	bidders     int // total bidders

	// timer & locker
	startTimer          *time.Timer
	halfTimer           *time.Timer
	endTimer            *time.Timer
	stateTicker         *time.Ticker
	quitServe           chan struct{}
	quitStateTickerSign chan struct{}
	bidConcurrentLock   chan struct{} // concurrency lock channel

	// storage
	store     *Store
	warehouse Warehouse

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
	StartTime time.Time
	HalfTime  time.Time
	EndTime   time.Time

	Capacity     int
	WarningPrice int // warning price of first half, 0 for disable
}

type State struct {
	Time    time.Time
	Session int

	LowestPrice int
	LowestTime  time.Time
	Bidders     int
}

type Final struct {
	Capacity int
	Bidders  int

	LowestPrice    int
	LowestTime     time.Time
	LowestSequence int

	AveragePrice int
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
	var warehouse Warehouse
	if os.Getenv("DB_DRIVER") == "mysql" {
		db, _ := sql.Open("mysql", os.Getenv("MYSQL_DSN"))
		// default max connections of mysql is 151
		db.SetMaxIdleConns(150)
		db.SetMaxOpenConns(150)
		warehouse = NewMysqlWarehouse("pp_"+pid+"_", db, sysLogger)
	} else if os.Getenv("DB_DRIVER") == "postgres" {
		db, _ := sql.Open("postgres", os.Getenv("POSTGRES_DSN"))
		// default max connections of postgres is 100
		db.SetMaxIdleConns(99)
		db.SetMaxOpenConns(99)
		warehouse = NewPostgresWarehouse("pp_"+pid+"_", db, sysLogger)
	} else {
		warehouse = NewMemoryWarehouse()
	}
	warehouse.Initialize()

	loc, _ := time.LoadLocation("Asia/Shanghai")

	return &Exchange{
		uuid:      pid,
		config:    &conf,
		state:     &State{},
		sysLog:    sysLogger,
		bidLog:    bidLogger,
		resLog:    resLogger,
		loc:       loc,
		warehouse: warehouse,
		store:     NewStore(conf.Capacity),
	}
}

// Serve start to serve incoming request
func (e *Exchange) Serve() {
	// runtime state
	e.bidConcurrentLock = make(chan struct{}, BidProcessThreshold)
	e.quitStateTickerSign = make(chan struct{})
	e.quitServe = make(chan struct{})

	// add clock
	now := time.Now()
	tStartDuration := time.Duration(0)
	if now.Before(e.config.StartTime) {
		tStartDuration = e.config.StartTime.Sub(now)
	}
	e.startTimer = time.NewTimer(tStartDuration)
	e.halfTimer = time.NewTimer(e.config.HalfTime.Sub(now))
	e.endTimer = time.NewTimer(e.config.EndTime.Sub(now))

	// init counter
	e.counterReq = newCounter()

	for {
		select {
		case <-e.startTimer.C:
			e.session = SessionFirstHalf
			//e.toggleStart()
			go e.startCollector()
		case <-e.halfTimer.C:
			e.session = SessionSecondHalf
			//e.toggleHalf()
			e.collectLowestPrice()
			e.collectCountBidders()
		case <-e.endTimer.C:
			e.session = SessionFinished
			//e.toggleEnd()
			e.stopCollector()
			return
		case <-e.quitServe:
			e.session = SessionFinished
			//e.toggleEnd()
			e.stopCollector()
			return
		}
	}
}

// Close stop all service gracefully (save & exit)
func (e *Exchange) Close() {
	e.stopTimer()

	if !e.sealed {
		e.Seal()
	}

	e.releaseResource()
}

// Halt stop all service right now (exit)
func (e *Exchange) Halt() {
	e.stopTimer()

	if e.session != SessionFinished {
		e.quitServe <- struct{}{}
	}

	e.releaseResource()
}

func (e *Exchange) stopTimer() {
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
	if e.warehouse != nil {
		e.warehouse.Terminate()
	}
}

// Seal check all data correct and judge final result
func (e *Exchange) Seal() *Final {
	// avoid duplicate sealing
	if e.sealed {
		return e.final
	}
	e.sealed = true

	e.sysLog.Println("===============================")
	e.sysLog.Printf(">>> Start Sealing @ %s", time.Now().Format("15:04:05.000000"))
	e.sysLog.Println("===============================")

	// compare store in memory with store restored from warehouse
	// make all data correct
	restoreStore := NewStore(0)
	e.warehouse.Restore(restoreStore, e.config)
	//restoreStore.SetCapacity(e.config.Capacity)
	//restoreStore.SortAllBlocks()
	if !e.store.Equal(restoreStore) {
		e.sysLog.Println("*** Store is not equal to store restored from warehouse !!!")
	} else {
		e.sysLog.Println("warehouse raw data check done!")
	}

	// sort blocks in case time in store different from warehouse
	e.store.SortAllBlocks()
	// export final result
	seq, avg := e.store.Judge()
	e.commitResults()
	e.dump()

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
	e.sysLog.Printf(">>> End Sealing @ %s", time.Now().Format("15:04:05.000000"))
	e.sysLog.Println("===============================")

	// log memory use
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	e.sysLog.Printf(">>> Memory Alloc %d, TotalAlloc %d, HeapAlloc %d, HeapSys %d", mem.Alloc, mem.TotalAlloc, mem.HeapAlloc, mem.HeapSys)

	if e.store.TailBid != nil {
		e.final = &Final{
			Capacity:       e.config.Capacity,
			Bidders:        e.state.Bidders,
			LowestPrice:    e.store.TailBid.Price,
			LowestTime:     e.store.TailBid.Time,
			LowestSequence: seq,
			AveragePrice:   int(avg * 100),
		}
	}
	return e.final
}

// Enquiry enquiries bidder's latest Bid
func (e *Exchange) Enquiry(client int) (*Bid, error) {
	bidder := e.store.GetBidderBlock(client)
	if bidder != nil {
		return bidder.Bids[len(bidder.Bids)-1], nil
	}

	return nil, Error{Code: CodeRequestNotAttend, Message: "Not attend"}
}

// SuccessfulBids list all successful bids
func (e *Exchange) SuccessfulBids() []*Bid {
	return e.store.FinalBids
}

func (e *Exchange) Config() *Config {
	return e.config
}

func (e *Exchange) State() *State {
	return e.state
}

func (e *Exchange) Final() *Final {
	return e.final
}

func (e *Exchange) BiddersCount() int {
	return e.store.CountBidders()
}

func (e *Exchange) BidsCount() int {
	return e.store.CountBids()
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

// traffic control
func (e *Exchange) bid(bid *Bid) error {
	if !bid.Time.IsZero() || bid.Sequence != 0 || bid.Active {
		return Error{Code: CodeRequestInvalid, Message: "Invalid request"}
	}

	if e.session == SessionUnprepared {
		return Error{Code: CodeServerNotReady, Message: "Not ready"}
	} else if e.session == SessionFinished {
		return Error{Code: CodeServerEnd, Message: "Invalid time"}
	}

	// concurrency lock
	e.bidConcurrentLock <- struct{}{}

	err := e.bidProcess(bid)
	// concurrency release
	<-e.bidConcurrentLock

	return err
}

// actually bid process
func (e *Exchange) bidProcess(bid *Bid) error {
	if bid.Price < 1 {
		return Error{Code: CodeRequestInvalidPrice, Message: "Invalid price"}
	}

	bid.Active = true

	var err error
	if e.session == SessionFirstHalf {
		err = e.bidSession1(bid)
	} else if e.session == SessionSecondHalf {
		err = e.bidSession2(bid)
	} else {
		bid.Active = false
		return Error{Code: CodeRequestInvalidTime, Message: "Invalid time"}
	}

	if err != nil {
		bid.Active = false
		return err
	}

	// bid success, update TailBid
	// only if bidders gte capacity in first half
	// and second half
	if e.session == SessionSecondHalf || e.BiddersCount() >= e.config.Capacity {
		e.lowestPrice = e.store.TailBid.Price
		e.lowestTime = e.store.TailBid.Time
	}

	return nil
}

func (e *Exchange) bidSession1(bid *Bid) error {
	if b := e.store.GetBidderBlock(bid.Client); b != nil {
		return Error{Code: CodeRequestAttendFirstRound, Message: "Attend first round"}
	}

	if e.config.WarningPrice > 0 && bid.Price > e.config.WarningPrice {
		return Error{Code: CodeRequestGTWarningPrice, Message: "Greater than WarningPrice"}
	}

	// save to warehouse
	bid.Sequence = 1
	if err := e.warehouse.Add(bid); err != nil {
		return err
	}

	// check db save time
	if bid.Time.After(e.config.HalfTime) || bid.Time.Equal(e.config.HalfTime) {
		return Error{Code: CodeRequestEnd1, Message: "End"}
	}

	// save to store
	e.store.Add(bid)
	atomic.AddUint64(&e.counterProcess, 1)

	return nil
}

func (e *Exchange) bidSession2(bid *Bid) error {
	b := e.store.GetBidderBlock(bid.Client) // bidder's block
	if b == nil {
		return Error{Code: CodeRequestNotAttendFirstRound, Message: "Not attend first round"}
	}

	if b.Total >= BidsPerBidder {
		return Error{Code: CodeRequestAllIn, Message: "Allin"}
	}

	// compare with previous bid
	for _, preBid := range b.Bids {
		if preBid.Price == bid.Price {
			return Error{Code: CodeRequestSamePrice, Message: "Same price"}
		}
	}

	// check price in bound
	if bid.Price-e.lowestPrice > PricingDelta || e.lowestPrice-bid.Price > PricingDelta {
		return Error{Code: CodeRequestOutOfRange, Message: "Out of Range"}
	}

	// save to warehouse
	bid.Sequence = int(b.Total) + 1
	if err := e.warehouse.Add(bid); err != nil {
		return err
	}

	// check db save time
	if bid.Time.After(e.config.EndTime) || bid.Time.Equal(e.config.EndTime) {
		return Error{Code: CodeRequestEnd2, Message: "End"}
	}

	// save to store
	e.store.Add(bid)
	atomic.AddUint64(&e.counterProcess, 1)

	return nil
}

func (e *Exchange) toggleStart() {
	e.sysLog.Println("===============================")
	e.sysLog.Printf(">>> Session %d @ %s", e.session, time.Now().Format("15:04:05.000000"))
	e.sysLog.Println("===============================")
}

func (e *Exchange) toggleHalf() {
	e.sysLog.Println("===============================")
	e.sysLog.Printf(">>> Session %d @ %s", e.session, time.Now().Format("15:04:05.000000"))
	e.sysLog.Println("===============================")
}

func (e *Exchange) toggleEnd() {
	e.sysLog.Println("===============================")
	e.sysLog.Printf(">>> Session %d @ %s", e.session, time.Now().Format("15:04:05.000000"))
	e.sysLog.Println("===============================")
}

// startCollector start a time.Ticker to collect system state per second
func (e *Exchange) startCollector() {
	e.collectStat()
	defer e.collectStat()

	e.stateTicker = time.NewTicker(time.Millisecond * 1000)
	for {
		select {
		case <-e.stateTicker.C:
			e.collectStat()
		case <-e.quitStateTickerSign:
			// release resources avoid memory leak
			e.stateTicker.Stop()
			e.stateTicker = nil
			return
		}
	}
}

func (e *Exchange) stopCollector() {
	if e.stateTicker != nil {
		e.quitStateTickerSign <- struct{}{}
	}
}

func (e *Exchange) collectStat() {
	if e.session == SessionFirstHalf {
		e.collectCountBidders()
	}

	e.state.Time = time.Now()
	e.state.Session = e.session
	e.state.Bidders = e.bidders
	e.state.LowestPrice = e.lowestPrice
	e.state.LowestTime = e.lowestTime

	e.sysLog.Printf("%s %3.0f %4d @ %s, B %6d, O %6d, G %6d, H %6d, P %6d\n", time.Now().Format("15:04:05.000000"), e.config.EndTime.Sub(time.Now()).Seconds(), e.state.LowestPrice, e.state.LowestTime.Format("15:04:05"), e.state.Bidders, e.BidsCount(), runtime.NumGoroutine(), atomic.SwapUint64(&e.counterHit, 0), atomic.SwapUint64(&e.counterProcess, 0))
}

func (e *Exchange) collectLowestPrice() {
	if e.store.TailBid != nil {
		e.lowestPrice = e.store.TailBid.Price
		e.lowestTime = e.store.TailBid.Time
	} else {
		// no one attend...
	}
}

func (e *Exchange) collectCountBidders() {
	e.bidders = e.store.CountBidders()
}

// Dump save all final result to log
func (e *Exchange) dump() {
	DumpAll(e.resLog, e.store)
}

// save final tender to storage
func (e *Exchange) commitResults() {
	for _, bid := range e.store.FinalBids {
		e.warehouse.Commit(bid)
	}
}
