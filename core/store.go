package auccore

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Bid struct {
	Serial   int
	Client   int
	Price    int
	Time     time.Time
	Sequence int
	Active   bool
}

type Block struct {
	Key   int
	Total uint64
	Valid uint64
	Bids  []*Bid
}

type Chain struct {
	sync.RWMutex
	Index  []int // DESC order in PriceChain
	Blocks map[int]*Block
}

// Store maintain configurations and two chains.
// A Chain to store bids by bidder identifier, another Chain store bids by bidding price.
// attach all blocks to BidderChain and PriceChain.
type Store struct {
	sync.RWMutex
	BidderChain *Chain
	PriceChain  *Chain
	Capacity    int
	TailBid     *Bid   // last one successful bid
	FinalBids   []*Bid // all successful bids
}

// NewStore return new *Store instance
// set capacity=0 to disable auto update lowest bid
func NewStore(capacity int) *Store {
	return &Store{
		BidderChain: NewChain(),
		PriceChain:  NewChain(),
		Capacity:    capacity,
	}
}

func NewChain() *Chain {
	return &Chain{
		Blocks: make(map[int]*Block),
	}
}

// SetCapacity set capacity manually
// sorting and updateState may required
func (s *Store) SetCapacity(capacity int) {
	s.Capacity = capacity
}

// CountBidders return the count of all bidders
func (s *Store) CountBidders() int {
	return s.BidderChain.Length()
}

// CountBids return the count of all bids
func (s *Store) CountBids() int {
	return s.PriceChain.Sum()
}

// GetBidderBlock return the *Block of specific bidder
func (s *Store) GetBidderBlock(key int) *Block {
	return s.BidderChain.GetBlock(key)
}

func (s *Store) GetPriceBlock(key int) *Block {
	return s.PriceChain.GetBlock(key)
}

// Add add *Bid to PriceChain and BidderChain, also handle bidder's previous bids carefully
func (s *Store) Add(bid *Bid) {
	s.Lock()
	defer s.Unlock()

	s.BidderChain.Insert(bid.Client, bid, false)
	s.PriceChain.Insert(bid.Price, bid, true)

	// decrease Block.Valid
	b := s.BidderChain.GetBlock(bid.Client)
	if b.Total > 1 {
		preBid := b.Bids[b.Total-2]
		s.PriceChain.DecrActiveCount(preBid.Price)
		preBid.Active = false
		b.Valid = 1
	}

	s.updateState()
}

// SortAllBlocks sort all blocks' Block.Bids in time ASC order
// usually called after end
func (s *Store) SortAllBlocks() {
	s.Lock()
	defer s.Unlock()

	for _, key := range s.PriceChain.Index {
		s.PriceChain.SortBlock(key)
	}

	s.updateState()

	//for _, key := range s.BidderChain.Index {
	//	s.BidderChain.SortBlock(key)
	//}
}

// updateState update the lowest bid
// may inaccurate due to bids in block is NOT in order
// it is different between the time in warehouse and inserting to store
func (s *Store) updateState() {
	// do not update TailBid if no capacity or bidders less than capacity
	if s.Capacity == 0 || s.CountBidders() < s.Capacity {
		return
	}

	c := 0
	for _, key := range s.PriceChain.Index {
		cPrevious := c

		b := s.PriceChain.Blocks[key]
		c += int(b.Valid)
		if c >= s.Capacity {
			offset := s.Capacity - cPrevious
			j := 0
			for _, bid := range b.Bids {
				if !bid.Active {
					continue
				}

				j++
				if j == offset {
					s.TailBid = bid
					return
				}
			}
		}
	}
}

// Equal check two store are equal deeply
func (s *Store) Equal(c *Store) bool {
	for _, key := range s.BidderChain.Index {
		b := s.BidderChain.Blocks[key]
		bc := c.BidderChain.GetBlock(key)
		if bc == nil {
			return false
		}
		if b.Total != bc.Total {
			return false
		}

		for i, bid := range b.Bids {
			bidC := bc.Bids[i]
			if bid.Client != bidC.Client || bid.Price != bidC.Price ||
				bid.Sequence != bidC.Sequence || bid.Active != bidC.Active ||
				!bid.Time.Truncate(time.Microsecond).Equal(bidC.Time.Truncate(time.Microsecond)) {
				return false
			}
		}
	}
	return true
}

// Judge final result
func (s *Store) Judge() (seq int, avg float64) {
	if s.Capacity <= 0 {
		return 0, 0
	}
	if s.TailBid == nil {
		return 0, 0
	}

	s.Lock()
	defer s.Unlock()

	success := 0
	totalPrice := 0
	s.FinalBids = make([]*Bid, s.Capacity)
	for _, key := range s.PriceChain.Index {
		b := s.PriceChain.Blocks[key]
		for _, bid := range b.Bids {
			if success < s.Capacity && bid.Active {
				s.FinalBids[success] = bid
				success++
				totalPrice += bid.Price
			}
		}
	}

	minPriceSuccess := 0
	minPriceLastSecondAll := 0
	minPriceLastSecondSuccess := 0
	b := s.PriceChain.Blocks[s.TailBid.Price] // min price block
	for _, bid := range b.Bids {
		if !bid.Active {
			continue // ignore non-active bid
		}

		if bid.Time.Before(s.TailBid.Time) || bid == s.TailBid {
			minPriceSuccess++ // success
		}

		if bid.Time.Unix() == s.TailBid.Time.Unix() {
			minPriceLastSecondAll++
			if bid.Time.Before(s.TailBid.Time) || bid == s.TailBid {
				minPriceLastSecondSuccess++ // success
			}
		}
	}

	return minPriceLastSecondSuccess, float64(totalPrice) / float64(success)
}

// Insert insert *Bid to specific *Block
// if sortIndex apply, eg, insert the bid to a Chain of PriceChain, also sort the Block.Index
func (c *Chain) Insert(key int, bid *Bid, sortIndex bool) {
	c.Lock()
	defer c.Unlock()

	c.initBlock(key, sortIndex)
	c.insert(key, bid)
}

func (c *Chain) insert(key int, bid *Bid) {
	b := c.Blocks[key]
	b.Bids = append(b.Bids, bid)

	// increase Block.Total and Block.Valid
	atomic.AddUint64(&b.Total, 1)
	atomic.AddUint64(&b.Valid, 1)
}

func (c *Chain) initBlock(key int, sortIndex bool) bool {
	if b := c.Blocks[key]; b == nil {
		c.Blocks[key] = &Block{Key: key}
		c.Index = append(c.Index, key)
		if sortIndex {
			sort.Sort(sort.Reverse(sort.IntSlice(c.Index)))
		}
		return true
	} else {
		return false
	}
}

// SortBlock sort the Block.Bids in time ASC order
func (c *Chain) SortBlock(key int) bool {
	c.Lock()
	defer c.Unlock()

	return c.sortBlock(key)
}

func (c *Chain) sortBlock(key int) bool {
	if b := c.Blocks[key]; b == nil {
		return false
	} else {
		sort.SliceStable(b.Bids, func(i, j int) bool { return b.Bids[i].Time.Before(b.Bids[j].Time) })
		return true
	}
}

// GetBlock return the *Block
func (c *Chain) GetBlock(key int) *Block {
	c.RLock()
	defer c.RUnlock()

	return c.getBlock(key)
}

func (c *Chain) getBlock(key int) *Block {
	return c.Blocks[key]
}

// DecrActiveCount decrease the Block.Valid
func (c *Chain) DecrActiveCount(key int) {
	c.RLock()
	defer c.RUnlock()

	c.decrActiveCount(key)
}

func (c *Chain) decrActiveCount(key int) {
	atomic.AddUint64(&c.Blocks[key].Valid, ^uint64(0))
}

// Length return length of blocks
func (c *Chain) Length() int {
	c.RLock()
	defer c.RUnlock()

	return c.length()
}

func (c *Chain) length() int {
	return len(c.Blocks)
}

// Sum return sum of all blocks' Block.Total
func (c *Chain) Sum() int {
	c.RLock()
	defer c.RUnlock()

	return c.sum()
}

func (c *Chain) sum() int {
	var r uint64
	for _, b := range c.Blocks {
		r += b.Total
	}
	return int(r)
}
