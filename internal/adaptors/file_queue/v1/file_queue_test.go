package filequeuev1

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/devex/golang/concurrent"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/testbench"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/execx"
)

type TestData struct {
	StrField  string
	IntField  int
	BoolField bool
}

func TestFileQueue(t *testing.T) {
	ctx := context.Background()
	_, err := execx.DelHardExec(ctx, ltngFileQueueBasePath)
	fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
	require.NoError(t, err)

	testData := &TestData{
		StrField:  "str",
		IntField:  10,
		BoolField: true,
	}
	err = fq.Write(ctx, testData)
	require.NoError(t, err)

	bs, err := fq.Read(ctx)
	require.NoError(t, err)
	var td TestData
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	err = fq.Pop(ctx)
	require.NoError(t, err)

	err = fq.Write(ctx, testData)
	require.NoError(t, err)

	bs, err = fq.Read(ctx)
	require.NoError(t, err)
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	err = fq.Pop(ctx)
	require.NoError(t, err)

	err = fq.Write(ctx, testData)
	require.NoError(t, err)

	bs, err = fq.Read(ctx)
	require.NoError(t, err)
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	err = fq.Pop(ctx)
	require.NoError(t, err)

	bs, err = fq.Read(ctx)
	require.Error(t, err)
	require.Nil(t, bs)
}

func TestFileQueueBench(t *testing.T) {
	ctx := context.Background()
	fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
	require.NoError(t, err)

	testData := &TestData{
		StrField:  "str",
		IntField:  10,
		BoolField: true,
	}
	err = fq.Write(ctx, testData)
	require.NoError(t, err)

	bs, err := fq.Read(ctx)
	require.NoError(t, err)
	var td TestData
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	err = fq.Pop(ctx)
	require.NoError(t, err)

	bs, err = fq.Read(ctx)
	require.Error(t, err)
	require.Nil(t, bs)
}

func TestFileQueueDisordered(t *testing.T) {
	ctx := context.Background()
	fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
	require.NoError(t, err)

	testData := &TestData{
		StrField:  "str",
		IntField:  10,
		BoolField: true,
	}
	err = fq.Write(ctx, testData)
	require.NoError(t, err)

	bs, err := fq.Read(ctx)
	require.NoError(t, err)
	var td TestData
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	err = fq.Pop(ctx)
	require.NoError(t, err)

	err = fq.Write(ctx, testData)
	require.NoError(t, err)

	err = fq.Write(ctx, testData)
	require.NoError(t, err)

	err = fq.Write(ctx, testData)
	require.NoError(t, err)

	err = fq.Write(ctx, testData)
	require.NoError(t, err)

	bs, err = fq.Read(ctx)
	require.NoError(t, err)
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	err = fq.Pop(ctx)
	require.NoError(t, err)

	bs, err = fq.Read(ctx)
	require.NoError(t, err)
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	err = fq.Pop(ctx)
	require.NoError(t, err)

	bs, err = fq.Read(ctx)
	require.NoError(t, err)
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	err = fq.Pop(ctx)
	require.NoError(t, err)

	bs, err = fq.Read(ctx)
	require.NoError(t, err)
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	err = fq.Pop(ctx)
	require.NoError(t, err)

	bs, err = fq.Read(ctx)
	require.Error(t, err)
	require.Nil(t, bs)
}

func TestFileQueueMultipleWrites(t *testing.T) {
	ctx := context.Background()
	_, err := execx.DelHardExec(ctx, ltngFileQueueBasePath)
	fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
	require.NoError(t, err)

	op := concurrent.New("file_queue")
	limit := 50
	for idx := 0; idx < limit; idx++ {
		//op.OpX(func() (any, error) {
		//	testData := &TestData{
		//		StrField:  "str",
		//		IntField:  10,
		//		BoolField: true,
		//	}
		//	err = fq.Write(ctx, testData)
		//	require.NoError(t, err)
		//
		//	time.Sleep(time.Millisecond * 100)
		//
		//	bs, err := fq.Read(ctx)
		//	require.NoError(t, err)
		//	var td TestData
		//	err = fq.serializer.Deserialize(bs, &td)
		//	require.NoError(t, err)
		//	require.Equal(t, testData, &td)
		//	//t.Log(testData)
		//	//t.Log(td)
		//
		//	err = fq.Pop(ctx)
		//	require.NoError(t, err)
		//
		//	return nil, nil
		//})

		testData := &TestData{
			StrField:  "str",
			IntField:  10,
			BoolField: true,
		}
		err = fq.Write(ctx, testData)
		require.NoError(t, err)

		bs, err := fq.Read(ctx)
		require.NoError(t, err)
		var td TestData
		err = fq.serializer.Deserialize(bs, &td)
		require.NoError(t, err)
		require.Equal(t, testData, &td)
		//t.Log(testData)
		//t.Log(td)

		err = fq.Pop(ctx)
		require.NoError(t, err)
	}

	err = op.WaitAndWrapErr()
	require.NoError(t, err)

	bs, err := fq.Read(ctx)
	require.Error(t, err)
	require.Nil(t, bs)
}

func TestFileQueueConcurrent(t *testing.T) {
	ctx := context.Background()
	_, err := execx.DelHardExec(ctx, ltngFileQueueBasePath)
	require.NoError(t, err)
	fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
	require.NoError(t, err)

	op := concurrent.New("file_queue")
	limit := 50
	for idx := 0; idx < limit; idx++ {
		op.OpX(func() (any, error) {
			testData := &TestData{
				StrField:  "str",
				IntField:  10,
				BoolField: true,
			}
			err := fq.Write(ctx, testData)
			require.NoError(t, err)

			time.Sleep(time.Millisecond * 100)

			err = fq.ReadAndPop(ctx, func(ctx context.Context, bs []byte) error {
				var td TestData
				err = fq.serializer.Deserialize(bs, &td)
				require.NoError(t, err)
				require.Equal(t, testData, &td)
				return err
			})
			require.NoError(t, err)

			return nil, nil
		})
	}

	err = op.WaitAndWrapErr()
	require.NoError(t, err)

	bs, err := fq.Read(ctx)
	require.Error(t, err)
	require.Nil(t, bs)
}

func BenchmarkFileQueueActions(b *testing.B) {
	ctx := context.Background()
	fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
	require.NoError(b, err)

	testData := &TestData{
		StrField:  "str",
		IntField:  10,
		BoolField: true,
	}

	b.Run("write - read - pop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err = fq.Write(ctx, testData)
			require.NoError(b, err)

			var bs []byte
			bs, err = fq.Read(ctx)
			require.NoError(b, err)
			var td TestData
			err = fq.serializer.Deserialize(bs, &td)
			require.NoError(b, err)
			require.Equal(b, testData, &td)

			err = fq.Pop(ctx)
			require.NoError(b, err)
		}
	})

	b.Run("(write - read - pop) stats", func(b *testing.B) {
		writeBench := &testbench.BenchData{}
		readBench := &testbench.BenchData{}
		popBench := &testbench.BenchData{}

		limit := 250
		for i := 0; i < limit; i++ {
			writeBench.Count()
			readBench.Count()
			popBench.Count()

			b.ResetTimer()
			b.StartTimer()
			err = fq.Write(ctx, testData)
			b.StopTimer()
			elapsed := b.Elapsed()
			b.ResetTimer()
			writeBench.CalcAvg(elapsed)
			require.NoError(b, err)

			var bs []byte
			b.ResetTimer()
			b.StartTimer()
			bs, err = fq.Read(ctx)
			b.StopTimer()
			elapsed = b.Elapsed()
			b.ResetTimer()
			readBench.CalcAvg(elapsed)
			require.NoError(b, err)

			var td TestData
			err = fq.serializer.Deserialize(bs, &td)
			require.NoError(b, err)
			require.Equal(b, testData, &td)

			b.ResetTimer()
			b.StartTimer()
			err = fq.Pop(ctx)
			b.StopTimer()
			elapsed = b.Elapsed()
			b.ResetTimer()
			popBench.CalcAvg(elapsed)
			b.ResetTimer()
			require.NoError(b, err)
		}

		b.Logf("writeBench - %s", writeBench.String())
		b.Logf("readBench - %s", readBench.String())
		b.Logf("popBench - %s", popBench.String())
	})

	b.Run("(write - read and pop) stats", func(b *testing.B) {
		writeBench := &testbench.BenchData{}
		readAndPopBench := &testbench.BenchData{}

		limit := 250
		for i := 0; i < limit; i++ {
			writeBench.Count()
			readAndPopBench.Count()

			b.ResetTimer()
			b.StartTimer()
			err = fq.Write(ctx, testData)
			b.StopTimer()
			elapsed := b.Elapsed()
			b.ResetTimer()
			writeBench.CalcAvg(elapsed)
			require.NoError(b, err)

			b.ResetTimer()
			b.StartTimer()
			err = fq.ReadAndPop(ctx, func(ctx context.Context, bs []byte) error {
				var td TestData
				err = fq.serializer.Deserialize(bs, &td)
				require.NoError(b, err)
				require.Equal(b, testData, &td)
				return err
			})
			b.StopTimer()
			elapsed = b.Elapsed()
			readAndPopBench.CalcAvg(elapsed)
			b.ResetTimer()
			require.NoError(b, err)
		}

		b.Logf("writeBench - %s", writeBench.String())
		b.Logf("readBench - %s", readAndPopBench.String())
	})

	b.Run("(write - read - pop) stats", func(b *testing.B) {
		writeBench := &testbench.BenchData{}
		readBench := &testbench.BenchData{}

		limit := 1 << 14
		for i := 0; i < limit; i++ {
			writeBench.Count()
			readBench.Count()

			b.ResetTimer()
			b.StartTimer()
			err = fq.WriteOnCursor(ctx, testData)
			b.StopTimer()
			elapsed := b.Elapsed()
			b.ResetTimer()
			writeBench.CalcAvg(elapsed)
			require.NoError(b, err)

			var bs []byte
			b.ResetTimer()
			b.StartTimer()
			bs, err = fq.ReadFromCursor(ctx)
			b.StopTimer()
			elapsed = b.Elapsed()
			b.ResetTimer()
			readBench.CalcAvg(elapsed)
			require.NoError(b, err)

			var td TestData
			err = fq.serializer.Deserialize(bs, &td)
			require.NoError(b, err)
			require.Equal(b, testData, &td)
		}

		bs, err := fq.ReadFromCursor(ctx)
		require.Error(b, err)
		require.Empty(b, bs)

		b.Logf("writeBench - %s", writeBench.String())
		b.Logf("readBench - %s", readBench.String())
	})
}

func BenchmarkFileQueueActionsConcurrent(b *testing.B) {
	b.Run("write, read & pop", func(b *testing.B) {
		ctx := context.Background()
		_, err := execx.DelHardExec(ctx, ltngFileQueueBasePath)
		require.NoError(b, err)
		fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
		require.NoError(b, err)

		op := concurrent.New("file_queue")
		limit := 50
		for idx := 0; idx < limit; idx++ {
			op.OpX(func() (any, error) {
				testData := &TestData{
					StrField:  "str",
					IntField:  10,
					BoolField: true,
				}
				err := fq.Write(ctx, testData)
				require.NoError(b, err)

				time.Sleep(time.Millisecond * 100)

				err = fq.ReadAndPop(ctx, func(ctx context.Context, bs []byte) error {
					var td TestData
					err = fq.serializer.Deserialize(bs, &td)
					require.NoError(b, err)
					require.Equal(b, testData, &td)
					return err
				})
				require.NoError(b, err)

				return nil, nil
			})
		}

		err = op.WaitAndWrapErr()
		require.NoError(b, err)

		bs, err := fq.Read(ctx)
		require.Error(b, err)
		require.Nil(b, bs)
	})

	b.Run("write, read, pop", func(b *testing.B) {
		ctx := context.Background()
		_, err := execx.DelHardExec(ctx, ltngFileQueueBasePath)
		require.NoError(b, err)
		fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
		require.NoError(b, err)

		op := concurrent.New("file_queue")
		limit := 50
		for idx := 0; idx < limit; idx++ {
			op.OpX(func() (any, error) {
				testData := &TestData{
					StrField:  "str",
					IntField:  10,
					BoolField: true,
				}
				err := fq.Write(ctx, testData)
				require.NoError(b, err)

				time.Sleep(time.Millisecond * 100)

				var bs []byte
				bs, err = fq.Read(ctx)
				require.NoError(b, err)

				var td TestData
				err = fq.serializer.Deserialize(bs, &td)
				require.NoError(b, err)
				require.Equal(b, testData, &td)

				err = fq.Pop(ctx)
				require.NoError(b, err)

				return nil, nil
			})
		}

		err = op.WaitAndWrapErr()
		require.NoError(b, err)

		bs, err := fq.Read(ctx)
		require.Error(b, err)
		require.Nil(b, bs)
	})

	b.Run("write, read, pop", func(b *testing.B) {
		ctx := context.Background()
		_, err := execx.DelHardExec(ctx, ltngFileQueueBasePath)
		require.NoError(b, err)
		fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
		require.NoError(b, err)

		op := concurrent.New("file_queue")
		limit := 50
		for idx := 0; idx < limit; idx++ {
			op.OpX(func() (any, error) {
				testData := &TestData{
					StrField:  "str",
					IntField:  10,
					BoolField: true,
				}
				err := fq.WriteOnCursor(ctx, testData)
				require.NoError(b, err)

				time.Sleep(time.Millisecond * 100)

				var bs []byte
				bs, err = fq.ReadFromCursor(ctx)
				require.NoError(b, err)

				var td TestData
				err = fq.serializer.Deserialize(bs, &td)
				require.NoError(b, err)
				require.Equal(b, testData, &td)

				return nil, nil
			})
		}

		err = op.WaitAndWrapErr()
		require.NoError(b, err)

		bs, err := fq.ReadFromCursor(ctx)
		require.Error(b, err)
		require.Nil(b, bs)
	})

	b.Run("write, read, pop", func(b *testing.B) {
		ctx := context.Background()
		_, err := execx.DelHardExec(ctx, ltngFileQueueBasePath)
		require.NoError(b, err)
		fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
		require.NoError(b, err)

		writerOnCursorBench := testbench.New()
		readFromCursorBench := testbench.New()

		testData := &TestData{
			StrField:  "str",
			IntField:  10,
			BoolField: true,
		}

		op := concurrent.New("file_queue")
		limit := 50

		canRead := make(chan struct{}, limit)
		for idx := 0; idx < limit; idx++ {
			op.OpX(func() (any, error) {
				writerOnCursorBench.CounterChan <- struct{}{}
				bd := testbench.New()
				var err error
				elapsed := bd.CalcElapsed(func() {
					err = fq.WriteOnCursor(ctx, testData)
				})
				writerOnCursorBench.TimeChan <- elapsed
				require.NoError(b, err)

				canRead <- struct{}{}
				return nil, nil
			})

			op.OpX(func() (any, error) {
				<-canRead
				readFromCursorBench.CounterChan <- struct{}{}
				bd := testbench.New()
				var err error
				var bs []byte
				elapsed := bd.CalcElapsed(func() {
					bs, err = fq.ReadFromCursor(ctx)
				})
				readFromCursorBench.TimeChan <- elapsed
				require.NoError(b, err)

				var td TestData
				err = fq.serializer.Deserialize(bs, &td)
				require.NoError(b, err)
				require.Equal(b, testData, &td)

				return nil, nil
			})
		}

		err = op.WaitAndWrapErr()
		require.NoError(b, err)

		for !fq.CheckAndClose() {
			runtime.Gosched()
		}

		writerOnCursorBench.CloseChannels()
		readFromCursorBench.CloseChannels()
		writerOnCursorBench.Compute()
		readFromCursorBench.Compute()

		b.Logf("writeBench - %s", writerOnCursorBench.String())
		b.Logf("readBench - %s", readFromCursorBench.String())

		bs, err := fq.ReadFromCursor(ctx)
		require.Error(b, err)
		require.Nil(b, bs)
	})

	b.Run("write, read, pop", func(b *testing.B) {
		ctx := context.Background()
		_, err := execx.DelHardExec(ctx, ltngFileQueueBasePath)
		require.NoError(b, err)
		fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
		require.NoError(b, err)

		writerOnCursorBench := testbench.New()
		readFromCursorBench := testbench.New()

		writerOnCursorBench.ComputeAsync()
		readFromCursorBench.ComputeAsync()

		testData := &TestData{
			StrField:  "str",
			IntField:  10,
			BoolField: true,
		}

		op := concurrent.New("file_queue")
		limit := 50

		canRead := make(chan struct{}, limit)
		for idx := 0; idx < limit; idx++ {
			op.OpX(func() (any, error) {
				writerOnCursorBench.CounterChan <- struct{}{}
				bd := testbench.New()
				var err error
				elapsed := bd.CalcElapsed(func() {
					err = fq.WriteOnCursor(ctx, testData)
				})
				writerOnCursorBench.TimeChan <- elapsed
				require.NoError(b, err)

				canRead <- struct{}{}
				return nil, nil
			})

			op.OpX(func() (any, error) {
				<-canRead
				readFromCursorBench.CounterChan <- struct{}{}
				bd := testbench.New()
				var err error
				var bs []byte
				elapsed := bd.CalcElapsed(func() {
					bs, err = fq.ReadFromCursor(ctx)
				})
				readFromCursorBench.TimeChan <- elapsed
				require.NoError(b, err)

				var td TestData
				err = fq.serializer.Deserialize(bs, &td)
				require.NoError(b, err)
				require.Equal(b, testData, &td)

				return nil, nil
			})
		}

		err = op.WaitAndWrapErr()
		require.NoError(b, err)

		for !fq.CheckAndClose() {
			runtime.Gosched()
		}

		writerOnCursorBench.QuitClose()
		readFromCursorBench.QuitClose()

		b.Logf("writeBench - %s", writerOnCursorBench.String())
		b.Logf("readBench - %s", readFromCursorBench.String())

		bs, err := fq.ReadFromCursor(ctx)
		require.Error(b, err)
		require.Nil(b, bs)
	})
}

func getTestData() *TestData {
	return &TestData{
		StrField:  "str",
		IntField:  10,
		BoolField: true,
	}
}

func TestReadFromFQ(t *testing.T) {
	ctx := context.Background()
	fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
	require.NoError(t, err)

	var counter int
	for {
		_, err = fq.Read(ctx)
		if err != nil {
			t.Log(err)
			break
		}

		err = fq.Pop(ctx)
		assert.NoError(t, err)

		counter++
	}
	t.Log(counter)
}
