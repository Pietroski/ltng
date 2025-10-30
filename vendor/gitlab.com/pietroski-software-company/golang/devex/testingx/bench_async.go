package testingx

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"
)

const chanLimit = 1 << 8

type BenchAsync struct {
	min, max, avg *atomic.Uint64
	counter       *atomic.Uint64

	elapsedTimeChan *syncx.Channel[time.Duration]
	testBench       *BenchSync
	async           *syncx.OffThread
}

func WithCountLimit(limit uint64) options.Option {
	return func(i interface{}) {
		if tb, ok := i.(*BenchAsync); ok {
			tb.elapsedTimeChan =
				syncx.NewChannel[time.Duration](
					syncx.WithChannelSize[time.Duration](
						int(limit),
					),
				)
		}

		if tb, ok := i.(*BenchPostSync); ok {
			tb.elapsedTimeChan =
				syncx.NewChannel[time.Duration](
					syncx.WithChannelSize[time.Duration](
						int(limit),
					),
				)
		}
	}
}

func NewBenchAsync(opts ...options.Option) *BenchAsync {
	tb := &BenchAsync{
		min:     &atomic.Uint64{},
		max:     &atomic.Uint64{},
		avg:     &atomic.Uint64{},
		counter: &atomic.Uint64{},

		elapsedTimeChan: syncx.NewChannel[time.Duration](
			syncx.WithChannelSize[time.Duration](
				chanLimit,
			),
		),
		testBench: NewBenchSync(),
		async:     syncx.NewThreadOperator("live-processor"),
	}
	options.ApplyOptions(tb, opts...)

	tb.async.Op(tb.CalcLiveElapsedAvg)

	return tb
}

func (tb *BenchAsync) CalcElapsedTime(fn func()) *BenchAsync {
	startTime := time.Now()
	fn()
	endTime := time.Now()
	elapsed := endTime.Sub(startTime)

	tb.elapsedTimeChan.Send(elapsed)

	return tb
}

func (tb *BenchAsync) CalcLiveElapsedAvg() {
	for t := range tb.elapsedTimeChan.Ch {
		tb.testBench.CalcStats(t)
		tb.min.Store(uint64(tb.testBench.Min()))
		tb.max.Store(uint64(tb.testBench.Max()))
		tb.avg.Store(uint64(tb.testBench.Avg()))
	}

	slogx.DefaultLogger().Debug(context.Background(), "live ended", "results", tb.String())
}

func (tb *BenchAsync) String() string {
	return fmt.Sprintf("min: %v, max: %v, avg: %v",
		tb.Min(), tb.Max(), tb.Avg())
}

func (tb *BenchAsync) Min() time.Duration {
	return time.Duration(tb.min.Load())
}

func (tb *BenchAsync) Max() time.Duration {
	return time.Duration(tb.max.Load())
}

func (tb *BenchAsync) Avg() time.Duration {
	return time.Duration(tb.avg.Load())
}

func (tb *BenchAsync) CloseWait() {
	tb.elapsedTimeChan.Close()
	tb.async.Wait()
}
