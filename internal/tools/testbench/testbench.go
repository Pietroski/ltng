package testbench

import (
	"fmt"
	"time"
)

const (
	chanLimit = 50 // 1 << 5
)

type BenchData struct {
	min, max, avg time.Duration
	counter       int
	CounterChan   chan struct{}
	TimeChan      chan time.Duration

	startTime time.Time
	endTime   time.Time

	quitCounterChan chan struct{}
	quitTimeChan    chan struct{}
}

func New() *BenchData {
	bd := &BenchData{
		CounterChan:     make(chan struct{}, chanLimit),
		TimeChan:        make(chan time.Duration, chanLimit),
		quitCounterChan: make(chan struct{}),
		quitTimeChan:    make(chan struct{}),
	}

	// bd.ComputeAsync()

	return bd
}

func (bd *BenchData) CalcAvg(elapsed time.Duration) {
	if bd.min == 0 {
		bd.min = elapsed
		bd.max = elapsed
	} else {
		if bd.min > elapsed {
			bd.min = elapsed
		}
		if bd.max < elapsed {
			bd.max = elapsed
		}
	}

	bd.avg = (bd.avg + elapsed) / 2
}

func (bd *BenchData) Count() {
	bd.counter++
}

func (bd *BenchData) ComputeCalcAvg() {
	for elapsed := range bd.TimeChan {
		bd.CalcAvg(elapsed)
	}
}

func (bd *BenchData) ComputeCounter() {
	for _ = range bd.CounterChan {
		bd.Count()
	}
}

func (bd *BenchData) Compute() {
	bd.ComputeCalcAvg()
	bd.ComputeCounter()
}

func (bd *BenchData) ComputeCalcAvgAsync() {
	for {
		select {
		case elapsed := <-bd.TimeChan:
			bd.CalcAvg(elapsed)
		case <-bd.quitTimeChan:
			break
		}
	}
}

func (bd *BenchData) ComputeCounterAsync() {
	for {
		select {
		case <-bd.CounterChan:
			bd.Count()
		case <-bd.quitCounterChan:
			break
		}
	}
}

func (bd *BenchData) ComputeAsync() {
	go bd.ComputeCalcAvgAsync()
	go bd.ComputeCounterAsync()
}

func (bd *BenchData) QuitClose() {
	bd.CloseChannels()
	bd.quitCounterChan <- struct{}{}
	bd.quitTimeChan <- struct{}{}
	close(bd.quitCounterChan)
	close(bd.quitTimeChan)
}

func (bd *BenchData) CloseChannels() {
	close(bd.CounterChan)
	close(bd.TimeChan)
}

func (bd *BenchData) String() string {
	return fmt.Sprintf("\nmin: %v\nmax: %v\navg: %v\n", bd.min, bd.max, bd.avg)
}

func (bd *BenchData) StartTimer() {
	bd.startTime = time.Now()
}

func (bd *BenchData) StopTimer() {
	bd.endTime = time.Now()
}

func (bd *BenchData) Elapsed() time.Duration {
	return bd.endTime.Sub(bd.startTime)
}

func (bd *BenchData) CalcElapsed(fn func()) time.Duration {
	bd.StartTimer()
	fn()
	bd.StopTimer()
	return bd.Elapsed()
}
