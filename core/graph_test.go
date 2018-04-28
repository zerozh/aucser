package auccore

import (
	"testing"
	"time"
)

func newBidWithTime(client, price, seq int, ts string) *Bid {
	t, _ := time.Parse("15:04:05", ts)
	return &Bid{
		Client:   client,
		Price:    price,
		Time:     t,
		Sequence: seq,
		Active:   true,
	}
}

func TestGraph11(t *testing.T) {
	graph := NewGraph()

	var bid *Bid
	store := NewStore(3)

	bid = newBidWithTime(1, 100, 1, "10:30:01")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(2, 100, 1, "10:30:02")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(3, 100, 1, "10:30:03")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(4, 100, 1, "10:30:04")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(5, 100, 1, "10:30:05")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(6, 500, 1, "10:30:07")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(7, 500, 1, "10:30:07")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(8, 500, 1, "10:30:07")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(9, 500, 1, "10:30:08")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(10, 1000, 1, "10:30:10")
	store.Add(bid)
	graph.Snapshot(store)

	graph.Output()
}

func TestGraph12(t *testing.T) {
	graph := NewGraph()

	var bid *Bid
	store := NewStore(3)

	bid = newBidWithTime(1, 100, 1, "10:30:01")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(2, 100, 1, "10:30:02")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(3, 100, 1, "10:30:03")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(4, 100, 1, "10:30:04")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(5, 100, 1, "10:30:05")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(6, 500, 1, "10:30:07")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(7, 500, 1, "10:30:07")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(8, 500, 1, "10:30:07")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(9, 500, 1, "10:30:08")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(10, 1000, 1, "10:30:10")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(9, 800, 1, "11:00:02")
	store.Add(bid)
	graph.Snapshot(store)

	graph.Output()
}

func TestGraph2(t *testing.T) {
	graph := NewGraph()

	var bid *Bid
	store := NewStore(3)

	bid = newBidWithTime(11, 1, 1, "10:30:02")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(12, 1, 1, "10:30:03")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(13, 1, 1, "10:30:04")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(14, 5, 1, "10:30:05")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(15, 2, 1, "10:30:06")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(16, 3, 1, "10:30:07")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(17, 1, 1, "10:30:08")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(18, 5, 1, "10:30:09")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(19, 4, 1, "10:30:12")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(10, 5, 1, "10:30:13")
	store.Add(bid)
	graph.Snapshot(store)

	// Second half

	bid = newBidWithTime(10, 6, 2, "11:01:02")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(17, 7, 2, "11:12:32")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(16, 6, 2, "11:29:15")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(14, 8, 2, "11:29:30")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(12, 9, 2, "11:29:48")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(13, 7, 2, "11:29:52")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(13, 8, 3, "11:29:55")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(10, 9, 3, "11:29:58")
	store.Add(bid)
	graph.Snapshot(store)

	bid = newBidWithTime(14, 6, 3, "11:29:59")
	store.Add(bid)
	graph.Snapshot(store)

	graph.Output()
}
