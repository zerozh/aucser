package auccore

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrency(t *testing.T) {
	c := NewConcurrencySimulator(15000)
	wg := &sync.WaitGroup{}
	allMap := make(map[int64]int)
	allMapLock := sync.Mutex{}
	allTimeMap := make(map[int64]int64)
	allTimeMapLock := sync.Mutex{}
	var counterStart uint64
	var counterDone uint64

	for i := 0; i < 230000; i++ {
		ts := time.Duration(generateRequest(60) * 1e9)
		wg.Add(1)
		time.AfterFunc(ts, func() {
			defer wg.Done()
			t1 := time.Now()
			key := t1.Unix()
			atomic.AddUint64(&counterStart, 1)
			c.Run()
			atomic.AddUint64(&counterDone, 1)
			t2 := time.Now()

			allMapLock.Lock()
			if _, got := allMap[key]; !got {
				allMap[key] = 0
			}
			allMap[key]++
			allMapLock.Unlock()

			allTimeMapLock.Lock()
			if _, got := allTimeMap[key]; !got {
				allTimeMap[key] = 0
			}
			allTimeMap[key] += t2.Sub(t1).Nanoseconds()
			allTimeMapLock.Unlock()

		})
	}

	go func() {
		ticker := time.NewTicker(time.Millisecond * 1000)
		for ; true; <-ticker.C {
			fmt.Printf("start %d, done %d\n", atomic.SwapUint64(&counterStart, 0), atomic.SwapUint64(&counterDone, 0))
		}
	}()

	wg.Wait()
	keys := make([]int64, 0, len(allMap))
	for k := range allMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	for _, k := range keys {
		fmt.Printf("%d T %6d AllAvgTime %02.3f\n", k, allMap[k], float64(allTimeMap[k])/1e9/float64(allMap[k]))
	}
}

func generateRequest(d float64) float64 {
	r := rand.Intn(100)
	if r > 80 {
		mean := d - 4.1
		for {
			r := rand.NormFloat64()*1.1 + mean
			if r >= 0 && r <= d {
				return r
			}
		}
	} else if r > 32 {
		mean := d - 5.5
		for {
			r := rand.NormFloat64()*1.5 + mean
			if r >= 0 && r <= d {
				return r
			}
		}
	} else if r > 15 {
		mean := d - 10
		for {
			r := rand.NormFloat64()*2.5 + mean
			if r >= 0 && r <= d {
				return r
			}
		}
	} else {
		mean := d - 20
		for {
			r := rand.NormFloat64()*15 + mean
			if r >= 0 && r <= d {
				return r
			}
		}
	}
}
