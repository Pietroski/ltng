package testingx

import (
	"fmt"
	"time"
)

type BenchSync struct {
	min, max, avg time.Duration
	counter       int64
	startTime     time.Time
	endTime       time.Time
}

func NewBenchSync() *BenchSync {
	tb := &BenchSync{}

	return tb
}

func (tb *BenchSync) Elapsed() time.Duration {
	return tb.endTime.Sub(tb.startTime)
}

func (tb *BenchSync) StartTimer() {
	tb.startTime = time.Now()
}

func (tb *BenchSync) StopTimer() {
	tb.endTime = time.Now()
}

func (tb *BenchSync) Count() {
	tb.counter++
}

func (tb *BenchSync) CalcElapsedAvg(fn func()) *BenchSync {
	tb.StartTimer()
	fn()
	tb.StopTimer()
	tb.CalcStats(tb.Elapsed())
	return tb
}

func (tb *BenchSync) CalcElapsed(fn func()) time.Duration {
	tb.StartTimer()
	fn()
	tb.StopTimer()
	return tb.Elapsed()
}

func (tb *BenchSync) CalcStats(elapsed time.Duration) {
	tb.calcMinMax(elapsed)
	tb.calcAvg(elapsed)
}

func (tb *BenchSync) calcMinMax(elapsed time.Duration) {
	if tb.min == 0 {
		tb.min = elapsed
		tb.max = elapsed
	} else {
		if elapsed < tb.min {
			tb.min = elapsed
		}
		if elapsed > tb.max {
			tb.max = elapsed
		}
	}
}

func (tb *BenchSync) calcAvg(elapsed time.Duration) {
	tb.avg = ((tb.avg * time.Duration(tb.counter)) + elapsed) / (time.Duration(tb.counter) + 1)
	tb.counter++
}

func (tb *BenchSync) String() string {
	return fmt.Sprintf("min: %v, max: %v, avg: %v", tb.min, tb.max, tb.avg)
}

func (tb *BenchSync) Min() time.Duration {
	return tb.min
}

func (tb *BenchSync) Max() time.Duration {
	return tb.max
}

func (tb *BenchSync) Avg() time.Duration {
	return tb.avg
}
