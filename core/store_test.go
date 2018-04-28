package auccore

import (
	"math"
	"sync"
	"testing"
	"time"
)

var store *Store
var capacity = 100
var ltBid *Bid // Lowest Tenderable Bid

func TestMain(m *testing.M) {
	store = NewStore(capacity)

	m.Run()
}

func TestInitStatus(t *testing.T) {
	if store.CountBidders() != 0 {
		t.Error("store.CountBidders() != 0")
	}
	if store.CountBids() != 0 {
		t.Error("store.CountBids() != 0")
	}
}

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

	bid = newBid(1, 1, 1)
	store.Add(bid)
	if store.LowestTenderableBid != bid {
		t.Error("store.LowestTenderableBid != bid")
	}

	bid = newBid(2, 1, 1)
	store.Add(bid)
	if store.LowestTenderableBid != bid {
		t.Error("store.LowestTenderableBid != bid")
	}

	bid = newBid(3, 1, 1)
	store.Add(bid)
	if store.LowestTenderableBid != bid {
		t.Error("store.LowestTenderableBid != bid")
	}

	bid = newBid(4, 5, 1)
	store.Add(bid)
	if store.LowestTenderableBid.Price != 1 {
		t.Error("store.LowestTenderableBid.Price != 1")
	}

	bid = newBid(5, 2, 1)
	store.Add(bid)
	if store.LowestTenderableBid.Price != 1 {
		t.Error("store.LowestTenderableBid.Price != 1")
	}

	bid = newBid(6, 3, 1)
	store.Add(bid)
	if store.LowestTenderableBid.Price != 2 {
		t.Error("store.LowestTenderableBid.Price != 2")
	}

	bid = newBid(7, 1, 1)
	store.Add(bid)
	if store.LowestTenderableBid.Price != 2 {
		t.Error("store.LowestTenderableBid.Price != 2")
	}

	bid = newBid(8, 5, 1)
	store.Add(bid)
	if store.LowestTenderableBid.Price != 3 {
		t.Error("store.LowestTenderableBid.Price != 3")
	}

	bid = newBid(9, 4, 1)
	store.Add(bid)
	if store.LowestTenderableBid.Price != 4 {
		t.Error("store.LowestTenderableBid.Price != 4")
	}

	bid = newBid(10, 5, 1)
	store.Add(bid)
	if store.LowestTenderableBid.Price != 5 {
		t.Error("store.LowestTenderableBid.Price != 5")
	}

	bid = newBid(10, 6, 2)
	store.Add(bid)
	if store.LowestTenderableBid.Price != 5 {
		t.Error("store.LowestTenderableBid.Price != 5")
	}

	bid = newBid(7, 7, 2)
	store.Add(bid)
	if store.LowestTenderableBid.Client != 4 {
		t.Error("store.LowestTenderableBid.Client != 4")
	}

	bid = newBid(6, 6, 2)
	store.Add(bid)
	if store.LowestTenderableBid.Price != 6 {
		t.Error("store.LowestTenderableBid.Price != 6")
	}

	bid = newBid(4, 8, 2)
	store.Add(bid)
	if store.LowestTenderableBid.Client != 10 {
		t.Error("store.LowestTenderableBid.Client != 10")
		t.Error(store.LowestTenderableBid)
	}

	bid = newBid(2, 9, 2)
	store.Add(bid)
	if store.LowestTenderableBid.Price != 7 {
		t.Error("store.LowestTenderableBid.Price != 7")
	}

	bid = newBid(3, 7, 2)
	store.Add(bid)
	if store.LowestTenderableBid.Client != 7 {
		t.Error("store.LowestTenderableBid.Client != 7")
	}

	bid = newBid(3, 8, 3)
	store.Add(bid)
	if store.LowestTenderableBid.Client != 3 {
		t.Error("store.LowestTenderableBid.Client != 3")
	}

	bid = newBid(10, 9, 3)
	store.Add(bid)
	if store.LowestTenderableBid.Price != 8 {
		t.Error("store.LowestTenderableBid.Price != 8")
	}

	bid = newBid(4, 6, 3)
	store.Add(bid)
	if store.LowestTenderableBid.Client != 3 {
		t.Error("store.LowestTenderableBid.Client != 3")
	}
}

func TestCapacityAddS1(t *testing.T) {
	clientStart := 1
	clientEnd := capacity

	for i := clientStart; i <= clientEnd; i++ {
		bid := &Bid{
			Client:   i,
			Price:    1,
			Time:     time.Now(),
			Sequence: 1,
			Active:   true,
		}
		store.Add(bid)
		ltBid = bid

		if store.LowestTenderableBid != ltBid {
			t.Errorf("store.LowestTenderableBid != ltBid %v %v", store.LowestTenderableBid, ltBid)
		}
	}

	if store.CountBidders() != clientEnd {
		t.Error("store.CountBidders() != clientEnd")
	}
	if store.CountBids() != clientEnd {
		t.Error("store.CountBids() != clientEnd")
	}
}

func TestCapacityAddS2(t *testing.T) {
	clientStart := 1
	clientEnd := capacity - 1

	for i := clientStart; i <= clientEnd; i++ {
		bid := &Bid{
			Client:   i,
			Price:    2,
			Time:     time.Now(),
			Sequence: 1,
			Active:   true,
		}
		store.Add(bid)

		if store.LowestTenderableBid.Price != 1 {
			t.Errorf("store.LowestTenderableBid.Price != 1 %v", store.LowestTenderableBid)
		}
	}

	bid := &Bid{
		Client:   capacity,
		Price:    2,
		Time:     time.Now(),
		Sequence: 1,
		Active:   true,
	}
	store.Add(bid)
	if store.LowestTenderableBid.Price != 2 {
		t.Errorf("store.LowestTenderableBid.Price != 2 %v", store.LowestTenderableBid)
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
