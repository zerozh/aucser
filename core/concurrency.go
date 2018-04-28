package auccore

import (
	"math"
	"math/rand"
	"time"
)

// Simulate concurrent latency, specially for database concurrent write/read latency
// typical latency of db write/read is 15ms, plus about 10ms for high concurrency
type ConcurrencySimulator struct {
	Threshold int
	conLock   chan bool
}

func NewConcurrencySimulator(threshold int) *ConcurrencySimulator {
	return &ConcurrencySimulator{
		Threshold: threshold,
		conLock:   make(chan bool, threshold/44),
	}
}

// Run run time.Sleep simulating latency for each database write/read
func (c *ConcurrencySimulator) Run() {
	c.conLock <- true
	c.run()
	<-c.conLock
}

func (c *ConcurrencySimulator) run() {
	time.Sleep(time.Millisecond * time.Duration(10+rand.Intn(5)))
	t := float64(1) / (math.Log(float64(cap(c.conLock))/float64(len(c.conLock)+1)) + 1)
	time.Sleep(time.Microsecond * time.Duration(int64(10000*t)))
}
