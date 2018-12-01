package auccore

import (
	"math"
	"sync"
	"testing"
	"time"
)

func newBid(client, price, seq int) *Bid {
	return &Bid{
		Client:   client,
		Price:    price,
		Time:     time.Now(),
		Sequence: seq,
		Active:   true,
	}
}

func TestFunctionally(t *testing.T) {
	var bid *Bid
	store := NewStore(3)

	if store.CountBidders() != 0 {
		t.Error("store.CountBidders() != 0")
	}
	if store.CountBids() != 0 {
		t.Error("store.CountBids() != 0")
	}

	bid = newBid(1, 1, 1)
	store.Add(bid)
	if store.TailBid != nil {
		t.Error("store.TailBid != nil")
	}

	bid = newBid(2, 1, 1)
	store.Add(bid)
	if store.TailBid != nil {
		t.Error("store.TailBid != nil")
	}

	bid = newBid(3, 1, 1)
	store.Add(bid)
	if store.TailBid != bid {
		t.Error("store.TailBid != bid")
	}

	bid = newBid(4, 5, 1)
	store.Add(bid)
	if store.TailBid.Price != 1 {
		t.Error("store.TailBid.Price != 1")
	}

	bid = newBid(5, 2, 1)
	store.Add(bid)
	if store.TailBid.Price != 1 {
		t.Error("store.TailBid.Price != 1")
	}

	bid = newBid(6, 3, 1)
	store.Add(bid)
	if store.TailBid.Price != 2 {
		t.Error("store.TailBid.Price != 2")
	}

	bid = newBid(7, 1, 1)
	store.Add(bid)
	if store.TailBid.Price != 2 {
		t.Error("store.TailBid.Price != 2")
	}

	bid = newBid(8, 5, 1)
	store.Add(bid)
	if store.TailBid.Price != 3 {
		t.Error("store.TailBid.Price != 3")
	}

	bid = newBid(9, 4, 1)
	store.Add(bid)
	if store.TailBid.Price != 4 {
		t.Error("store.TailBid.Price != 4")
	}

	bid = newBid(10, 5, 1)
	store.Add(bid)
	if store.TailBid.Price != 5 {
		t.Error("store.TailBid.Price != 5")
	}

	bid = newBid(10, 6, 2)
	store.Add(bid)
	if store.TailBid.Price != 5 {
		t.Error("store.TailBid.Price != 5")
	}

	bid = newBid(7, 7, 2)
	store.Add(bid)
	if store.TailBid.Client != 4 {
		t.Error("store.TailBid.Client != 4")
	}

	bid = newBid(6, 6, 2)
	store.Add(bid)
	if store.TailBid.Price != 6 {
		t.Error("store.TailBid.Price != 6")
	}

	bid = newBid(4, 8, 2)
	store.Add(bid)
	if store.TailBid.Client != 10 {
		t.Error("store.TailBid.Client != 10")
		t.Error(store.TailBid)
	}

	bid = newBid(2, 9, 2)
	store.Add(bid)
	if store.TailBid.Price != 7 {
		t.Error("store.TailBid.Price != 7")
	}

	bid = newBid(3, 7, 2)
	store.Add(bid)
	if store.TailBid.Client != 7 {
		t.Error("store.TailBid.Client != 7")
	}

	bid = newBid(3, 8, 3)
	store.Add(bid)
	if store.TailBid.Client != 3 {
		t.Error("store.TailBid.Client != 3")
	}

	bid = newBid(10, 9, 3)
	store.Add(bid)
	if store.TailBid.Price != 8 {
		t.Error("store.TailBid.Price != 8")
	}

	bid = newBid(4, 6, 3)
	store.Add(bid)
	if store.TailBid.Client != 3 {
		t.Error("store.TailBid.Client != 3")
	}
}

func TestSequentiallyAdd(t *testing.T) {
	for i := 1; i <= 3; i++ {
		n := int(math.Pow10(i))
		testSequentiallyAdd(t, n)
	}
}

func testSequentiallyAdd(t *testing.T, clientEnd int) {
	store := NewStore(clientEnd / 10)
	for i := 1; i <= clientEnd; i++ {
		bid := &Bid{
			Client:   i,
			Price:    1,
			Time:     time.Now(),
			Sequence: 1,
			Active:   true,
		}
		store.Add(bid)
	}

	if store.CountBidders() != clientEnd {
		t.Error("store.CountBidders() != clientEnd")
	}
	if store.CountBids() != clientEnd {
		t.Error("store.CountBids() != clientEnd")
	}
}

func TestConcurrencyAdd(t *testing.T) {
	for i := 1; i <= 3; i++ {
		n := int(math.Pow10(i))
		testConcurrencyAdd(t, n)
	}
}

func testConcurrencyAdd(t *testing.T, clientEnd int) {
	store := NewStore(clientEnd / 10)
	wg := sync.WaitGroup{}
	for i := 1; i <= clientEnd; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bid := &Bid{
				Client:   i,
				Price:    1,
				Time:     time.Now(),
				Sequence: 1,
				Active:   true,
			}
			store.Add(bid)
		}(i)
	}

	wg.Wait()
	if store.CountBidders() != clientEnd {
		t.Error("store.CountBidders() != clientEnd")
	}
	if store.CountBids() != clientEnd {
		t.Error("store.CountBids() != clientEnd")
	}
}
