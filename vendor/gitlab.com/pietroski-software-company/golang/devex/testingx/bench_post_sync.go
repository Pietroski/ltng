package testingx

import (
	"fmt"
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"
)

type BenchPostSync struct {
	elapsedTimeChan *syncx.Channel[time.Duration]
	bs              *BenchSync
}

func NewBenchPostSync(opts ...options.Option) *BenchPostSync {
	tb := &BenchPostSync{
		elapsedTimeChan: syncx.NewChannel[time.Duration](
			syncx.WithChannelSize[time.Duration](
				chanLimit,
			),
		),
		bs: NewBenchSync(),
	}
	options.ApplyOptions(tb, opts...)

	return tb
}

func (tb *BenchPostSync) CalcElapsedTime(fn func()) *BenchPostSync {
	startTime := time.Now()
	fn()
	endTime := time.Now()
	elapsed := endTime.Sub(startTime)

	tb.elapsedTimeChan.Send(elapsed)

	return tb
}

func (tb *BenchPostSync) CalcElapsedAvg() *BenchPostSync {
	tb.elapsedTimeChan.Close()
	for t := range tb.elapsedTimeChan.Ch {
		tb.bs.CalcStats(t)
	}

	return tb
}

func (tb *BenchPostSync) String() string {
	return fmt.Sprintf("min: %v, max: %v, avg: %v",
		tb.Min(), tb.Max(), tb.Avg())
}

func (tb *BenchPostSync) Min() time.Duration {
	return tb.bs.min
}

func (tb *BenchPostSync) Max() time.Duration {
	return tb.bs.max
}

func (tb *BenchPostSync) Avg() time.Duration {
	return tb.bs.avg
}
