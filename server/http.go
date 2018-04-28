package aucserver

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	//_ "net/http/pprof"
	"os"
	"strconv"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/zerozh/aucser/core"
)

var exchange *auccore.Exchange
var loc, _ = time.LoadLocation("Asia/Shanghai")
var S1Duration = time.Second * 1800
var S2Duration = time.Second * 1800

// register http handles for incoming requests
func init() {
	http.HandleFunc("/bid", bidHandle)
	http.HandleFunc("/status", statusHandle)
	http.HandleFunc("/system/status", statusHandle)
	http.HandleFunc("/system/boot", systemHandle)

	go func() {
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()
	//go func() {
	//	log.Fatal(http.ListenAndServe(":6060", nil))
	//}()
}

func SetExchange(e *auccore.Exchange) {
	exchange = e
}

// listen incoming bid request
func bidHandle(w http.ResponseWriter, r *http.Request) {
	res := &BidResponse{}
	if exchange == nil {
		res.Code = 100
		res.Message = "No game"
	}

	if err := r.ParseForm(); err != nil {
		fmt.Println(err)
	}

	client, _ := strconv.Atoi(r.Form.Get("client"))
	price, _ := strconv.Atoi(r.Form.Get("price"))

	bid := &auccore.Bid{
		Client: client,
		Price:  price,
	}
	if err := exchange.Bid(bid); err != nil {
		res.Code = err.(auccore.Error).Code
		res.Message = err.(auccore.Error).Message
	} else {
		res.Bid = bid
	}

	retString, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(retString))
	w.Write([]byte("\n"))
}

// listen incoming status request
func statusHandle(w http.ResponseWriter, r *http.Request) {
	res := &StatusResponse{}
	printInfo(res)

	retString, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(retString))
	w.Write([]byte("\n"))
}

// listen incoming boot system request
func systemHandle(w http.ResponseWriter, r *http.Request) {
	e := r.ParseForm()
	if e != nil {
		fmt.Println(e)
	}

	qStartTime := r.Form.Get("startTime")
	qHalfTime := r.Form.Get("halfTime")
	qEndTime := r.Form.Get("endTime")
	qCapacity := r.Form.Get("capacity")
	qWarningPrice := r.Form.Get("warningPrice")

	var startTime, halfTime, endTime time.Time
	var capacity, warningPrice int

	if qStartTime == "" || qHalfTime == "" || qEndTime == "" {
		t := time.Now()
		startTime = t
		halfTime = t.Add(S1Duration)
		endTime = t.Add(S2Duration)
	} else {
		startTime, _ = time.ParseInLocation("2006-01-02 15:04:05", qStartTime, loc)
		halfTime, _ = time.ParseInLocation("2006-01-02 15:04:05", qHalfTime, loc)
		endTime, _ = time.ParseInLocation("2006-01-02 15:04:05", qEndTime, loc)
	}
	if qCapacity == "" {
		capacity, _ = strconv.Atoi(os.Getenv("CAPACITY"))
	} else {
		capacity, _ = strconv.Atoi(qCapacity)
	}
	if qWarningPrice == "" {
		warningPrice, _ = strconv.Atoi(os.Getenv("WARNINGPRICE"))
	} else {
		warningPrice, _ = strconv.Atoi(qWarningPrice)
	}

	log.Println("===============================")
	log.Println("=========reinitSystem==========")
	log.Println("===============================")

	if exchange != nil {
		exchange.Close()
	}

	conf := auccore.Config{
		StartTime:    startTime,
		HalfTime:     halfTime,
		EndTime:      endTime,
		Capacity:     capacity,
		WarningPrice: warningPrice,
	}

	exchange = auccore.NewExchange(conf)
	exchange.Serve()

	res := &StatusResponse{}
	printInfo(res)

	retString, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(retString))
	w.Write([]byte("\n"))
}

func printInfo(res *StatusResponse) {
	res.Info.StartTime = exchange.Config().StartTime.Format("2006-01-02 15:04:05")
	res.Info.HalfTime = exchange.Config().HalfTime.Format("2006-01-02 15:04:05")
	res.Info.EndTime = exchange.Config().EndTime.Format("2006-01-02 15:04:05")

	res.Info.WarningPrice = exchange.Config().WarningPrice
	res.Info.Capacity = exchange.Config().Capacity

	res.Info.Time = exchange.State().Time.Format("2006-01-02 15:04:05")
	res.Info.Session = exchange.State().Session

	res.Info.Bidders = exchange.State().Bidders
	res.Info.LowestPrice = exchange.State().LowestPrice
	res.Info.LowestTime = exchange.State().LowestTime.Format("2006-01-02 15:04:05")
}
