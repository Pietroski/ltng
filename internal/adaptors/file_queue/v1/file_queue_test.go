package filequeuev1

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/devex/golang/concurrent"
	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	go_random "gitlab.com/pietroski-software-company/tools/random/go-random/pkg/tools/random"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/testbench"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/execx"
)

var nonExistentIndex = []byte("non existent index")

type TestData struct {
	IDField   string
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

func TestFileQueue_PopFromIndex(t *testing.T) {
	t.Run("single item", func(t *testing.T) {
		t.Run("index found", func(t *testing.T) {
			ctx := context.Background()

			path := GenericFileQueueFilePath + "/" + "pop_from_index"
			filename := GenericFileQueueFileName

			err := os.RemoveAll(getFilePath(path, filename))
			require.NoError(t, err)

			fq, err := New(ctx, path, filename)
			require.NoError(t, err)

			testData := getTestData(t)
			err = fq.WriteOnCursor(ctx, testData)
			require.NoError(t, err)

			bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
			require.NoError(t, err)

			var td TestData
			err = serializer.
				NewRawBinarySerializer().
				Deserialize(bs, &td)
			require.NoError(t, err)
			require.EqualValues(t, testData, &td)

			err = fq.PopFromIndex(ctx, []byte(testData.IDField))
			require.NoError(t, err)
			assertFQCount(t, ctx, fq, 0)
		})

		t.Run("index not found", func(t *testing.T) {
			ctx := context.Background()

			path := GenericFileQueueFilePath + "/" + "pop_from_index"
			filename := GenericFileQueueFileName

			err := os.RemoveAll(getFilePath(path, filename))
			require.NoError(t, err)

			fq, err := New(ctx, path, filename)
			require.NoError(t, err)

			testData := getTestData(t)
			err = fq.WriteOnCursor(ctx, testData)
			require.NoError(t, err)

			bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
			require.NoError(t, err)

			var td TestData
			err = serializer.
				NewRawBinarySerializer().
				Deserialize(bs, &td)
			require.NoError(t, err)
			require.EqualValues(t, testData, &td)

			err = fq.PopFromIndex(ctx, nonExistentIndex)
			require.Error(t, err)
			assertFQCount(t, ctx, fq, 1)
		})
	})

	t.Run("first item", func(t *testing.T) {
		t.Run("index found", func(t *testing.T) {
			ctx := context.Background()

			path := GenericFileQueueFilePath + "/" + "pop_from_index"
			filename := GenericFileQueueFileName

			err := os.RemoveAll(getFilePath(path, filename))
			require.NoError(t, err)

			fq, err := New(ctx, path, filename)
			require.NoError(t, err)

			testDataList := getTestDataList(t, 3)

			for _, testData := range testDataList {
				err = fq.WriteOnCursor(ctx, testData)
				require.NoError(t, err)
			}

			for _, testData := range testDataList {
				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
				require.NoError(t, err)

				var td TestData
				err = serializer.
					NewRawBinarySerializer().
					Deserialize(bs, &td)
				require.NoError(t, err)
				require.EqualValues(t, testData, &td)
			}

			err = fq.PopFromIndex(ctx, []byte(testDataList[0].IDField))
			require.NoError(t, err)
			assertFQCount(t, ctx, fq, 2)
		})

		t.Run("index not found", func(t *testing.T) {
			ctx := context.Background()

			path := GenericFileQueueFilePath + "/" + "pop_from_index"
			filename := GenericFileQueueFileName

			err := os.RemoveAll(getFilePath(path, filename))
			require.NoError(t, err)

			fq, err := New(ctx, path, filename)
			require.NoError(t, err)

			testDataList := getTestDataList(t, 3)

			for _, testData := range testDataList {
				err = fq.WriteOnCursor(ctx, testData)
				require.NoError(t, err)
			}

			for _, testData := range testDataList {
				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
				require.NoError(t, err)

				var td TestData
				err = serializer.
					NewRawBinarySerializer().
					Deserialize(bs, &td)
				require.NoError(t, err)
				require.EqualValues(t, testData, &td)
			}

			err = fq.PopFromIndex(ctx, nonExistentIndex)
			require.Error(t, err)
			assertFQCount(t, ctx, fq, 3)
		})
	})

	t.Run("last item", func(t *testing.T) {
		t.Run("index found", func(t *testing.T) {
			ctx := context.Background()

			path := GenericFileQueueFilePath + "/" + "pop_from_index"
			filename := GenericFileQueueFileName

			err := os.RemoveAll(getFilePath(path, filename))
			require.NoError(t, err)

			fq, err := New(ctx, path, filename)
			require.NoError(t, err)

			testDataList := getTestDataList(t, 3)

			for _, testData := range testDataList {
				err = fq.WriteOnCursor(ctx, testData)
				require.NoError(t, err)
			}

			for _, testData := range testDataList {
				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
				require.NoError(t, err)

				var td TestData
				err = serializer.
					NewRawBinarySerializer().
					Deserialize(bs, &td)
				require.NoError(t, err)
				require.EqualValues(t, testData, &td)
			}

			err = fq.PopFromIndex(ctx, []byte(testDataList[len(testDataList)-1].IDField))
			require.NoError(t, err)
			assertFQCount(t, ctx, fq, 2)
		})

		t.Run("index not found", func(t *testing.T) {
			ctx := context.Background()

			path := GenericFileQueueFilePath + "/" + "pop_from_index"
			filename := GenericFileQueueFileName

			err := os.RemoveAll(getFilePath(path, filename))
			require.NoError(t, err)

			fq, err := New(ctx, path, filename)
			require.NoError(t, err)

			testDataList := getTestDataList(t, 3)

			for _, testData := range testDataList {
				err = fq.WriteOnCursor(ctx, testData)
				require.NoError(t, err)
			}

			for _, testData := range testDataList {
				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
				require.NoError(t, err)

				var td TestData
				err = serializer.
					NewRawBinarySerializer().
					Deserialize(bs, &td)
				require.NoError(t, err)
				require.EqualValues(t, testData, &td)
			}

			err = fq.PopFromIndex(ctx, nonExistentIndex)
			require.Error(t, err)
			assertFQCount(t, ctx, fq, 3)
		})
	})

	t.Run("middle item", func(t *testing.T) {
		t.Run("index found", func(t *testing.T) {
			ctx := context.Background()

			path := GenericFileQueueFilePath + "/" + "pop_from_index"
			filename := GenericFileQueueFileName

			err := os.RemoveAll(getFilePath(path, filename))
			require.NoError(t, err)

			fq, err := New(ctx, path, filename)
			require.NoError(t, err)

			testDataList := getTestDataList(t, 3)

			for _, testData := range testDataList {
				err = fq.WriteOnCursor(ctx, testData)
				require.NoError(t, err)
			}

			for _, testData := range testDataList {
				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
				require.NoError(t, err)

				var td TestData
				err = serializer.
					NewRawBinarySerializer().
					Deserialize(bs, &td)
				require.NoError(t, err)
				require.EqualValues(t, testData, &td)
			}

			err = fq.PopFromIndex(ctx, []byte(testDataList[len(testDataList)/2].IDField))
			require.NoError(t, err)
			assertFQCount(t, ctx, fq, 2)
		})

		t.Run("index not found", func(t *testing.T) {
			ctx := context.Background()

			path := GenericFileQueueFilePath + "/" + "pop_from_index"
			filename := GenericFileQueueFileName

			err := os.RemoveAll(getFilePath(path, filename))
			require.NoError(t, err)

			fq, err := New(ctx, path, filename)
			require.NoError(t, err)

			testDataList := getTestDataList(t, 3)

			for _, testData := range testDataList {
				err = fq.WriteOnCursor(ctx, testData)
				require.NoError(t, err)
			}

			for _, testData := range testDataList {
				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
				require.NoError(t, err)

				var td TestData
				err = serializer.
					NewRawBinarySerializer().
					Deserialize(bs, &td)
				require.NoError(t, err)
				require.EqualValues(t, testData, &td)
			}

			err = fq.PopFromIndex(ctx, nonExistentIndex)
			require.Error(t, err)
			assertFQCount(t, ctx, fq, 3)
		})
	})

	t.Run("any item", func(t *testing.T) {
		amount := go_random.RandomInt(1, 50)
		indexPosition := go_random.RandomInt(0, amount-1)

		t.Log("amount", amount)
		t.Log("indexPosition:", indexPosition)

		t.Run("index found", func(t *testing.T) {
			ctx := context.Background()

			path := GenericFileQueueFilePath + "/" + "pop_from_index"
			filename := GenericFileQueueFileName

			err := os.RemoveAll(getFilePath(path, filename))
			require.NoError(t, err)

			fq, err := New(ctx, path, filename)
			require.NoError(t, err)

			testDataList := getTestDataList(t, int(amount))

			for _, testData := range testDataList {
				err = fq.WriteOnCursor(ctx, testData)
				require.NoError(t, err)
			}

			for _, testData := range testDataList {
				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
				require.NoError(t, err)

				var td TestData
				err = serializer.
					NewRawBinarySerializer().
					Deserialize(bs, &td)
				require.NoError(t, err)
				require.EqualValues(t, testData, &td)
			}

			err = fq.PopFromIndex(ctx, []byte(testDataList[indexPosition].IDField))
			require.NoError(t, err)
			assertFQCount(t, ctx, fq, int(amount)-1)
		})

		t.Run("index not found", func(t *testing.T) {
			ctx := context.Background()

			path := GenericFileQueueFilePath + "/" + "pop_from_index"
			filename := GenericFileQueueFileName

			err := os.RemoveAll(getFilePath(path, filename))
			require.NoError(t, err)

			fq, err := New(ctx, path, filename)
			require.NoError(t, err)

			testDataList := getTestDataList(t, int(amount))

			for _, testData := range testDataList {
				err = fq.WriteOnCursor(ctx, testData)
				require.NoError(t, err)
			}

			for _, testData := range testDataList {
				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
				require.NoError(t, err)

				var td TestData
				err = serializer.
					NewRawBinarySerializer().
					Deserialize(bs, &td)
				require.NoError(t, err)
				require.EqualValues(t, testData, &td)
			}

			err = fq.PopFromIndex(ctx, nonExistentIndex)
			require.Error(t, err)
			assertFQCount(t, ctx, fq, int(amount))
		})
	})

	t.Run("one after another", func(t *testing.T) {
		amount := go_random.RandomInt(1, 10)
		indexPosition := go_random.RandomInt(0, amount-1)
		anotherIndexPosition := go_random.RandomInt(0, amount-1)
		for indexPosition == anotherIndexPosition {
			anotherIndexPosition = go_random.RandomInt(0, amount-1)
		}

		t.Log("amount", amount)
		t.Log("indexPosition:", indexPosition)

		t.Run("single-thread", func(t *testing.T) {
			t.Run("index found", func(t *testing.T) {
				ctx := context.Background()

				path := GenericFileQueueFilePath + "/" + "pop_from_index"
				filename := GenericFileQueueFileName

				err := os.RemoveAll(getFilePath(path, filename))
				require.NoError(t, err)

				fq, err := New(ctx, path, filename)
				require.NoError(t, err)

				testDataList := getTestDataList(t, int(amount))

				for _, testData := range testDataList {
					err = fq.WriteOnCursor(ctx, testData)
					require.NoError(t, err)
				}

				for _, testData := range testDataList {
					bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
					require.NoError(t, err)

					var td TestData
					err = serializer.
						NewRawBinarySerializer().
						Deserialize(bs, &td)
					require.NoError(t, err)
					require.EqualValues(t, testData, &td)
				}

				t.Log(testDataList[indexPosition].IDField)
				t.Log(testDataList[anotherIndexPosition].IDField)

				err = fq.PopFromIndex(ctx, []byte(testDataList[indexPosition].IDField))
				require.NoError(t, err)
				err = fq.PopFromIndex(ctx, []byte(testDataList[anotherIndexPosition].IDField))
				require.NoError(t, err)

				assertFQCount(t, ctx, fq, int(amount)-2)
			})

			t.Run("index not found", func(t *testing.T) {
				ctx := context.Background()

				path := GenericFileQueueFilePath + "/" + "pop_from_index"
				filename := GenericFileQueueFileName

				err := os.RemoveAll(getFilePath(path, filename))
				require.NoError(t, err)

				fq, err := New(ctx, path, filename)
				require.NoError(t, err)

				testDataList := getTestDataList(t, int(amount))

				for _, testData := range testDataList {
					err = fq.WriteOnCursor(ctx, testData)
					require.NoError(t, err)
				}

				for _, testData := range testDataList {
					bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
					require.NoError(t, err)

					var td TestData
					err = serializer.
						NewRawBinarySerializer().
						Deserialize(bs, &td)
					require.NoError(t, err)
					require.EqualValues(t, testData, &td)
				}

				err = fq.PopFromIndex(ctx, nonExistentIndex)
				require.Error(t, err)
				err = fq.PopFromIndex(ctx, nonExistentIndex)
				require.Error(t, err)
				assertFQCount(t, ctx, fq, int(amount))
			})
		})

		t.Run("concurrently", func(t *testing.T) {
			t.Run("index found", func(t *testing.T) {
				ctx := context.Background()

				path := GenericFileQueueFilePath + "/" + "pop_from_index"
				filename := GenericFileQueueFileName

				err := os.RemoveAll(getFilePath(path, filename))
				require.NoError(t, err)

				fq, err := New(ctx, path, filename)
				require.NoError(t, err)

				testDataList := getTestDataList(t, int(amount))

				for _, testData := range testDataList {
					err = fq.WriteOnCursor(ctx, testData)
					require.NoError(t, err)
				}

				for _, testData := range testDataList {
					bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
					require.NoError(t, err)

					var td TestData
					err = serializer.
						NewRawBinarySerializer().
						Deserialize(bs, &td)
					require.NoError(t, err)
					require.EqualValues(t, testData, &td)
				}

				t.Log(testDataList[indexPosition].IDField)
				t.Log(testDataList[anotherIndexPosition].IDField)

				op := concurrent.New("simultaneous popping")
				op.OpX(func() (any, error) {
					err := fq.PopFromIndex(ctx, []byte(testDataList[indexPosition].IDField))
					require.NoError(t, err)

					return nil, nil
				})
				op.OpX(func() (any, error) {
					err := fq.PopFromIndex(ctx, []byte(testDataList[anotherIndexPosition].IDField))
					require.NoError(t, err)

					return nil, nil
				})
				err = op.WaitAndWrapErr()
				require.NoError(t, err)

				assertFQCount(t, ctx, fq, int(amount)-2)
			})

			t.Run("index not found", func(t *testing.T) {
				ctx := context.Background()

				path := GenericFileQueueFilePath + "/" + "pop_from_index"
				filename := GenericFileQueueFileName

				err := os.RemoveAll(getFilePath(path, filename))
				require.NoError(t, err)

				fq, err := New(ctx, path, filename)
				require.NoError(t, err)

				testDataList := getTestDataList(t, int(amount))

				for _, testData := range testDataList {
					err = fq.WriteOnCursor(ctx, testData)
					require.NoError(t, err)
				}

				for _, testData := range testDataList {
					bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
					require.NoError(t, err)

					var td TestData
					err = serializer.
						NewRawBinarySerializer().
						Deserialize(bs, &td)
					require.NoError(t, err)
					require.EqualValues(t, testData, &td)
				}

				op := concurrent.New("simultaneous popping")
				op.OpX(func() (any, error) {
					err := fq.PopFromIndex(ctx, nonExistentIndex)
					require.Error(t, err)

					return nil, nil
				})
				op.OpX(func() (any, error) {
					err := fq.PopFromIndex(ctx, nonExistentIndex)
					require.Error(t, err)

					return nil, nil
				})
				err = op.WaitAndWrapErr()
				require.NoError(t, err)

				assertFQCount(t, ctx, fq, int(amount))
			})
		})
	})

	t.Run("empty queue", func(t *testing.T) {
		ctx := context.Background()

		path := GenericFileQueueFilePath + "/" + "pop_from_index"
		filename := GenericFileQueueFileName

		err := os.RemoveAll(getFilePath(path, filename))
		require.NoError(t, err)

		fq, err := New(ctx, path, filename)
		require.NoError(t, err)

		testData := getTestData(t)
		bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
		require.Error(t, err)
		require.Empty(t, bs)

		err = fq.PopFromIndex(ctx, []byte(testData.IDField))
		require.Error(t, err)
		assertFQCount(t, ctx, fq, 0)
	})
}

func getTestData(t *testing.T) *TestData {
	newUUID, err := uuid.NewRandom()
	require.NoError(t, err)

	return &TestData{
		IDField:   newUUID.String(),
		StrField:  "str",
		IntField:  10,
		BoolField: true,
	}
}

func getTestDataList(t *testing.T, amount int) []*TestData {
	testDataList := make([]*TestData, amount)
	for i := 0; i < amount; i++ {
		testDataList[i] = getTestData(t)
	}

	return testDataList
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

func TestReadFromFQWithoutTruncation(t *testing.T) {
	ctx := context.Background()
	path := GenericFileQueueFilePath + "/" + "pop_from_index"
	filename := GenericFileQueueFileName
	defer func() {
		err := os.RemoveAll(getFilePath(path, filename))
		require.NoError(t, err)
	}()
	fq, err := New(ctx, path, filename)
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

func assertFQCount(
	t *testing.T, ctx context.Context,
	fq *FileQueue, expected int,
) {
	var counter int
	for {
		_, err := fq.Read(ctx)
		if err != nil {
			t.Log(err)
			break
		}

		err = fq.Pop(ctx)
		assert.NoError(t, err)

		counter++
	}
	t.Log(counter)
	assert.Equal(t, expected, counter)
}
