package auccore

import (
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"
)

func DumpAll(log *log.Logger, st *Store) {
	if st.TailBid == nil {
		log.Println("*** no st.TailBid")
		return
	}

	success := 0
	totalPrice := 0
	for _, key := range st.PriceChain.Index {
		b := st.PriceChain.Blocks[key]
		for e := b.Front(); e != nil; e = e.Next() {
			bid := e.Bid
			if !bid.Active {
			} else if success < st.Capacity {
				success++
				totalPrice += bid.Price
			} else {
			}
		}
	}

	minPriceSuccess := 0
	minPriceLastSecondAll := 0
	minPriceLastSecondSuccess := 0
	b := st.PriceChain.Blocks[st.TailBid.Price] // min price block
	for e := b.Front(); e != nil; e = e.Next() {
		bid := e.Bid
		if bid.Time.Before(st.TailBid.Time) || bid == st.TailBid {
			minPriceSuccess++
			// success
		}

		if bid.Time.Unix() == st.TailBid.Time.Unix() {
			minPriceLastSecondAll++

			if bid.Time.Before(st.TailBid.Time) || bid == st.TailBid {
				minPriceLastSecondSuccess++ // success
			}
		}
	}

	log.Println("=============================")
	log.Printf("AVG PRICE %.2f\n", float64(totalPrice)/float64(success))
	log.Printf("MIN PRICE %d\n", st.TailBid.Price)
	log.Printf("TAIL BID %d @ %s No. %d\n", st.TailBid.Price, st.TailBid.Time.Format("15:04:05"), minPriceLastSecondSuccess)
	log.Printf("MIN PRICE BIDS %d\n", st.PriceChain.Blocks[st.TailBid.Price].Valid)
	log.Printf("MIN PRICE DEALS %d\n", minPriceSuccess)
	log.Printf("MIN PRICE LAST SECOND BIDS %d\n", minPriceLastSecondAll)
	log.Printf("MIN PRICE LAST SECOND DEALS %d\n", minPriceLastSecondSuccess)
	log.Println("=============================")

	log.Println()
	log.Println("=============Dump=============")
	success = 0
	totalPrice = 0
	for _, key := range st.PriceChain.Index {
		b := st.PriceChain.Blocks[key]
		log.Printf("====Batch  %4d %6d %6d====\n", b.Key, b.Total, b.Valid)
		for e := b.Front(); e != nil; e = e.Next() {
			bid := e.Bid
			var mark = "" //  ✂ ✔ ✘
			if !bid.Active {
				mark = "✂"
			} else if success < st.Capacity {
				success++
				totalPrice += bid.Price
				mark = "✔"
			} else {
				mark = "✘"
			}
			log.Printf("%s   %d  %4d    %s\n", bid.Time.Format("15:04:05.000000"), bid.Client, bid.Price, mark)
		}
	}
	log.Println("=============================")
	log.Println()
}

func RestoreStoreStatus(st *Store) {
	logFile2, _ := os.OpenFile("../logs/0_sys.txt", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	mw2 := io.MultiWriter(logFile2)
	logger2 := log.New(mw2, "", 0)

	logFile3, _ := os.OpenFile("../logs/0_bid.txt", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	mw3 := io.MultiWriter(logFile3)
	logger := log.New(mw3, "", 0)

	var bids []*Bid

	for _, key := range st.PriceChain.Index {
		b := st.PriceChain.Blocks[key]
		for e := b.Front(); e != nil; e = e.Next() {
			bid := e.Bid
			bids = append(bids, bid)
		}
	}

	sort.Slice(bids, func(i, j int) bool {
		return bids[i].Time.Before(bids[j].Time)
	})

	ost := NewStore(st.Capacity)
	var currentT time.Time
	var currentP int
	var ct = 0
	for _, bid := range bids {
		ct++
		// mark all active then input
		bid.Active = true
		ost.Add(bid)
		if currentT.Unix() != bid.Time.Unix() {
			logger2.Printf("%s %4d @ %s, B %6d, O %6d, P %6d\n", bid.Time.Format("15:04:05"), ost.TailBid.Price, ost.TailBid.Time.Format("15:04:05"), ost.CountBidders(), ost.CountBids(), ct)
			currentT = bid.Time
			ct = 0
		}

		if ost.TailBid.Price != currentP {
			fmt.Printf("%s %4d @ %s, B %6d, O %6d, P %6d\n", bid.Time.Format("15:04:05"), ost.TailBid.Price, ost.TailBid.Time.Format("15:04:05"), ost.CountBidders(), ost.CountBids(), ct)
			currentP = ost.TailBid.Price
		}

		logger.Printf("<<< %d %4d @ %s ✔ ", bid.Client, bid.Price, bid.Time.Format("15:04:05.000"))
	}
}
