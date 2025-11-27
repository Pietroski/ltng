package mmap

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/pietroski-software-company/golang/devex/testingx"

	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	fileiomodels "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/models"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

var nonExistentIndex = []byte("non existent index")

type TestData struct {
	IDField   string
	StrField  string
	IntField  int
	BoolField bool
}

func TestMmapFileQueue(t *testing.T) {
	ctx := context.Background()
	err := osx.DelHard(ctx, fileiomodels.FileQueueBasePath)

	fq, err := NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
		fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
	require.NoError(t, err)

	testData := &TestData{
		StrField:  "str",
		IntField:  10,
		BoolField: true,
	}
	_, err = fq.Write(testData)
	require.NoError(t, err)

	bs, err := fq.Read()
	require.NoError(t, err)
	var td TestData
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	_, err = fq.Pop()
	require.NoError(t, err)

	_, err = fq.Write(testData)
	require.NoError(t, err)

	bs, err = fq.Read()
	require.NoError(t, err)
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	_, err = fq.Pop()
	require.NoError(t, err)

	_, err = fq.Write(testData)
	require.NoError(t, err)

	bs, err = fq.Read()
	require.NoError(t, err)
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	_, err = fq.Pop()
	require.NoError(t, err)

	bs, err = fq.Read()
	require.Error(t, err)
	require.Nil(t, bs)
}

func TestFileQueueBench(t *testing.T) {
	fq, err := NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
		fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
	require.NoError(t, err)

	testData := &TestData{
		StrField:  "str",
		IntField:  10,
		BoolField: true,
	}
	_, err = fq.Write(testData)
	require.NoError(t, err)

	bs, err := fq.Read()
	require.NoError(t, err)
	var td TestData
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	_, err = fq.Pop()
	require.NoError(t, err)

	bs, err = fq.Read()
	require.Error(t, err)
	require.Nil(t, bs)
}

func TestFileQueueDisordered(t *testing.T) {
	fq, err := NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
		fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
	require.NoError(t, err)

	testData := &TestData{
		StrField:  "str",
		IntField:  10,
		BoolField: true,
	}
	_, err = fq.Write(testData)
	require.NoError(t, err)

	bs, err := fq.Read()
	require.NoError(t, err)
	var td TestData
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	_, err = fq.Pop()
	require.NoError(t, err)

	_, err = fq.Write(testData)
	require.NoError(t, err)

	_, err = fq.Write(testData)
	require.NoError(t, err)

	_, err = fq.Write(testData)
	require.NoError(t, err)

	_, err = fq.Write(testData)
	require.NoError(t, err)

	bs, err = fq.Read()
	require.NoError(t, err)
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	_, err = fq.Pop()
	require.NoError(t, err)

	bs, err = fq.Read()
	require.NoError(t, err)
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	_, err = fq.Pop()
	require.NoError(t, err)

	bs, err = fq.Read()
	require.NoError(t, err)
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	_, err = fq.Pop()
	require.NoError(t, err)

	bs, err = fq.Read()
	require.NoError(t, err)
	err = fq.serializer.Deserialize(bs, &td)
	require.NoError(t, err)
	require.Equal(t, testData, &td)
	t.Log(testData)
	t.Log(td)

	_, err = fq.Pop()
	require.NoError(t, err)

	bs, err = fq.Read()
	require.Error(t, err)
	require.Nil(t, bs)
}

func TestFileQueueMultipleWrites(t *testing.T) {
	ctx := context.Background()
	err := osx.DelHard(ctx, fileiomodels.FileQueueBasePath)

	fq, err := NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
		fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
	require.NoError(t, err)

	op := syncx.NewThreadOperator("file_queue")
	limit := 50
	for idx := 0; idx < limit; idx++ {
		//op.OpX(func() (any, error) {
		//	testData := &TestData{
		//		StrField:  "str",
		//		IntField:  10,
		//		BoolField: true,
		//	}
		//	_, err = fq.Write(testData)
		//	require.NoError(t, err)
		//
		//	time.Sleep(time.Millisecond * 100)
		//
		//	bs, err := fq.Read()
		//	require.NoError(t, err)
		//	var td TestData
		//	err = fq.serializer.Deserialize(bs, &td)
		//	require.NoError(t, err)
		//	require.Equal(t, testData, &td)
		//	//t.Log(testData)
		//	//t.Log(td)
		//
		//	_, err = fq.Pop()
		//	require.NoError(t, err)
		//
		//	return nil, nil
		//})

		testData := &TestData{
			StrField:  "str",
			IntField:  10,
			BoolField: true,
		}
		_, err = fq.Write(testData)
		require.NoError(t, err)

		bs, err := fq.Read()
		require.NoError(t, err)
		var td TestData
		err = fq.serializer.Deserialize(bs, &td)
		require.NoError(t, err)
		require.Equal(t, testData, &td)
		//t.Log(testData)
		//t.Log(td)

		_, err = fq.Pop()
		require.NoError(t, err)
	}

	err = op.WaitAndWrapErr()
	require.NoError(t, err)

	bs, err := fq.Read()
	require.Error(t, err)
	require.Nil(t, bs)
}

//func TestFileQueueConcurrent(t *testing.T) {
//	ctx := context.Background()
//	err := osx.DelHard(ctx, fileiomodels.FileQueueBasePath)
//	require.NoError(t, err)
//
//	fq, err := NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
//		fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
//	require.NoError(t, err)
//
//	op := syncx.NewThreadOperator("file_queue")
//	limit := 50
//	for idx := 0; idx < limit; idx++ {
//		op.OpX(func() (any, error) {
//			testData := &TestData{
//				StrField:  "str",
//				IntField:  10,
//				BoolField: true,
//			}
//			_, err := fq.Write(testData)
//			require.NoError(t, err)
//
//			time.Sleep(time.Millisecond * 100)
//
//			err = fq.ReadAndPop(ctx, func(ctx context.Context, bs []byte) error {
//				var td TestData
//				err = fq.serializer.Deserialize(bs, &td)
//				require.NoError(t, err)
//				require.Equal(t, testData, &td)
//				return err
//			})
//			require.NoError(t, err)
//
//			return nil, nil
//		})
//	}
//
//	err = op.WaitAndWrapErr()
//	require.NoError(t, err)
//
//	bs, err := fq.Read()
//	require.Error(t, err)
//	require.Nil(t, bs)
//}

func BenchmarkFileQueueActions(b *testing.B) {
	fq, err := NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
		fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
	require.NoError(b, err)

	testData := &TestData{
		StrField:  "str",
		IntField:  10,
		BoolField: true,
	}

	b.Run("write - read - pop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = fq.Write(testData)
			require.NoError(b, err)

			var bs []byte
			bs, err = fq.Read()
			require.NoError(b, err)
			var td TestData
			err = fq.serializer.Deserialize(bs, &td)
			require.NoError(b, err)
			require.Equal(b, testData, &td)

			_, err = fq.Pop()
			require.NoError(b, err)
		}
	})

	b.Run("(write - read - pop) stats", func(b *testing.B) {
		writeBench := testingx.NewBenchSync()
		readBench := testingx.NewBenchSync()
		popBench := testingx.NewBenchSync()

		limit := 250
		for i := 0; i < limit; i++ {
			writeBench.CalcElapsedAvg(func() {
				_, err = fq.Write(testData)
			})
			require.NoError(b, err)

			var bs []byte
			readBench.CalcElapsedAvg(func() {
				bs, err = fq.Read()
			})
			require.NoError(b, err)

			var td TestData
			err = fq.serializer.Deserialize(bs, &td)
			require.NoError(b, err)
			require.Equal(b, testData, &td)

			popBench.CalcElapsedAvg(func() {
				_, err = fq.Pop()
			})
			require.NoError(b, err)
		}

		b.Logf("writeBench - %s", writeBench.String())
		b.Logf("readBench - %s", readBench.String())
		b.Logf("popBench - %s", popBench.String())
	})

	//b.Run("(write - read and pop) stats", func(b *testing.B) {
	//	writeBench := &testbench.BenchData{}
	//	readAndPopBench := &testbench.BenchData{}
	//
	//	limit := 250
	//	for i := 0; i < limit; i++ {
	//		writeBench.Count()
	//		readAndPopBench.Count()
	//
	//		b.ResetTimer()
	//		b.StartTimer()
	//		_, err = fq.Write(testData)
	//		b.StopTimer()
	//		elapsed := b.Elapsed()
	//		b.ResetTimer()
	//		writeBench.CalcAvg(elapsed)
	//		require.NoError(b, err)
	//
	//		b.ResetTimer()
	//		b.StartTimer()
	//		err = fq.ReadAndPop(ctx, func(ctx context.Context, bs []byte) error {
	//			var td TestData
	//			err = fq.serializer.Deserialize(bs, &td)
	//			require.NoError(b, err)
	//			require.Equal(b, testData, &td)
	//			return err
	//		})
	//		b.StopTimer()
	//		elapsed = b.Elapsed()
	//		readAndPopBench.CalcAvg(elapsed)
	//		b.ResetTimer()
	//		require.NoError(b, err)
	//	}
	//
	//	b.Logf("writeBench - %s", writeBench.String())
	//	b.Logf("readBench - %s", readAndPopBench.String())
	//})
	//
	//b.Run("(write - read - pop) stats", func(b *testing.B) {
	//	writeBench := &testbench.BenchData{}
	//	readBench := &testbench.BenchData{}
	//
	//	limit := 1 << 14
	//	for i := 0; i < limit; i++ {
	//		writeBench.Count()
	//		readBench.Count()
	//
	//		b.ResetTimer()
	//		b.StartTimer()
	//		err = fq.WriteOnCursor(ctx, testData)
	//		b.StopTimer()
	//		elapsed := b.Elapsed()
	//		b.ResetTimer()
	//		writeBench.CalcAvg(elapsed)
	//		require.NoError(b, err)
	//
	//		var bs []byte
	//		b.ResetTimer()
	//		b.StartTimer()
	//		bs, err = fq.ReadFromCursor(ctx)
	//		b.StopTimer()
	//		elapsed = b.Elapsed()
	//		b.ResetTimer()
	//		readBench.CalcAvg(elapsed)
	//		require.NoError(b, err)
	//
	//		var td TestData
	//		err = fq.serializer.Deserialize(bs, &td)
	//		require.NoError(b, err)
	//		require.Equal(b, testData, &td)
	//	}
	//
	//	bs, err := fq.ReadFromCursor(ctx)
	//	require.Error(b, err)
	//	require.Empty(b, bs)
	//
	//	b.Logf("writeBench - %s", writeBench.String())
	//	b.Logf("readBench - %s", readBench.String())
	//})
}

func BenchmarkFileQueueActionsConcurrent(b *testing.B) {
	//b.Run("write, read & pop", func(b *testing.B) {
	//	ctx := context.Background()
	//	err := osx.DelHard(ctx, fileiomodels.FileQueueBasePath)
	//	require.NoError(b, err)
	//
	//	fq, err := NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
	//		fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
	//	require.NoError(b, err)
	//
	//	op := syncx.NewThreadOperator("file_queue")
	//	limit := 50
	//	for idx := 0; idx < limit; idx++ {
	//		op.OpX(func() (any, error) {
	//			testData := &TestData{
	//				StrField:  "str",
	//				IntField:  10,
	//				BoolField: true,
	//			}
	//			_, err := fq.Write(testData)
	//			require.NoError(b, err)
	//
	//			err = fq.ReadAndPop(ctx, func(ctx context.Context, bs []byte) error {
	//				var td TestData
	//				err = fq.serializer.Deserialize(bs, &td)
	//				require.NoError(b, err)
	//				require.Equal(b, testData, &td)
	//				return err
	//			})
	//			require.NoError(b, err)
	//
	//			return nil, nil
	//		})
	//	}
	//
	//	err = op.WaitAndWrapErr()
	//	require.NoError(b, err)
	//
	//	bs, err := fq.Read()
	//	require.Error(b, err)
	//	require.Nil(b, bs)
	//})

	//b.Run("write, read, pop", func(b *testing.B) {
	//	ctx := context.Background()
	//	_, err := execx.DelHard(ctx, ltngFileQueueBasePath)
	//	require.NoError(b, err)
	//	fq, err := New(ctx, GenericFileQueueFilePath, GenericFileQueueFileName)
	//	require.NoError(b, err)
	//
	//	op := syncx.NewThreadOperator("file_queue")
	//	limit := 5
	//	for idx := 0; idx < limit; idx++ {
	//		op.OpX(func() (any, error) {
	//			testData := &TestData{
	//				StrField:  "str",
	//				IntField:  10,
	//				BoolField: true,
	//			}
	//			err := fq.Write(testData)
	//			require.NoError(b, err)
	//
	//			var bs []byte
	//			bs, err = fq.Read()
	//			require.NoError(b, err)
	//
	//			var td TestData
	//			err = fq.serializer.Deserialize(bs, &td)
	//			require.NoError(b, err)
	//			require.Equal(b, testData, &td)
	//
	//			_, err = fq.Pop()
	//			require.NoError(b, err)
	//
	//			return nil, nil
	//		})
	//	}
	//
	//	err = op.WaitAndWrapErr()
	//	require.NoError(b, err)
	//
	//	bs, err := fq.Read()
	//	b.Logf("%s", bs)
	//	assert.Error(b, err)
	//	assert.Nil(b, bs)
	//})

	b.Run("write, read, pop", func(b *testing.B) {
		ctx := context.Background()
		err := osx.DelHard(ctx, fileiomodels.FileQueueBasePath)
		require.NoError(b, err)

		fq, err := NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
			fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
		require.NoError(b, err)

		op := syncx.NewThreadOperator("file_queue")
		limit := 50
		for idx := 0; idx < limit; idx++ {
			op.OpX(func() (any, error) {
				testData := &TestData{
					StrField:  "str",
					IntField:  10,
					BoolField: true,
				}
				_, err := fq.Write(testData)
				require.NoError(b, err)

				time.Sleep(time.Millisecond * 100)

				var bs []byte
				bs, err = fq.Read()
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

		bs, err := fq.Read()
		require.Error(b, err)
		require.Nil(b, bs)
	})

	b.Run("write, read, pop", func(b *testing.B) {
		ctx := context.Background()
		err := osx.DelHard(ctx, fileiomodels.FileQueueBasePath)
		require.NoError(b, err)

		fq, err := NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
			fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
		require.NoError(b, err)

		writerOnCursorBench := testingx.NewBenchAsync()
		readFromCursorBench := testingx.NewBenchAsync()

		testData := &TestData{
			StrField:  "str",
			IntField:  10,
			BoolField: true,
		}

		op := syncx.NewThreadOperator("file_queue")
		limit := 50

		canRead := make(chan struct{}, limit)
		for idx := 0; idx < limit; idx++ {
			op.OpX(func() (any, error) {
				var err error
				writerOnCursorBench.CalcElapsedTime(func() {
					_, err = fq.Write(testData)
				})
				require.NoError(b, err)

				canRead <- struct{}{}
				return nil, nil
			})

			op.OpX(func() (any, error) {
				<-canRead
				var err error
				var bs []byte
				readFromCursorBench.CalcElapsedTime(func() {
					bs, err = fq.Read()
				})
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

		//for !fq.Close() {
		//	runtime.Gosched()
		//}

		err = fq.Close()
		require.NoError(b, err)

		writerOnCursorBench.CloseWait()
		readFromCursorBench.CloseWait()

		b.Logf("writeBench - %s", writerOnCursorBench.String())
		b.Logf("readBench - %s", readFromCursorBench.String())
	})

	b.Run("write, read, pop", func(b *testing.B) {
		ctx := context.Background()
		err := osx.DelHard(ctx, fileiomodels.FileQueueBasePath)
		require.NoError(b, err)

		fq, err := NewFileQueue(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
			fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
		require.NoError(b, err)

		writerOnCursorBench := testingx.NewBenchAsync()
		readFromCursorBench := testingx.NewBenchAsync()

		testData := &TestData{
			StrField:  "str",
			IntField:  10,
			BoolField: true,
		}

		op := syncx.NewThreadOperator("file_queue")
		limit := 50

		canRead := make(chan struct{}, limit)
		for idx := 0; idx < limit; idx++ {
			op.OpX(func() (any, error) {
				var err error
				writerOnCursorBench.CalcElapsedTime(func() {
					_, err = fq.Write(testData)
				})
				require.NoError(b, err)

				canRead <- struct{}{}
				return nil, nil
			})

			op.OpX(func() (any, error) {
				<-canRead
				var err error
				var bs []byte
				readFromCursorBench.CalcElapsedTime(func() {
					bs, err = fq.Read()
				})
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

		err = fq.Close()
		require.NoError(b, err)

		writerOnCursorBench.CloseWait()
		readFromCursorBench.CloseWait()

		b.Logf("writeBench - %s", writerOnCursorBench.String())
		b.Logf("readBench - %s", readFromCursorBench.String())
	})
}

//func TestFileQueue_PopFromIndex(t *testing.T) {
//	t.Run("single item", func(t *testing.T) {
//		t.Run("index found", func(t *testing.T) {
//			ctx := context.Background()
//
//			path := GenericFileQueueFilePath + "/" + "pop_from_index"
//			filename := GenericFileQueueFileName
//
//			err := os.RemoveAll(getFilePath(path, filename))
//			require.NoError(t, err)
//
//			fq, err := New(ctx, path, filename)
//			require.NoError(t, err)
//
//			testData := getTestData(t)
//			err = fq.WriteOnCursor(ctx, testData)
//			require.NoError(t, err)
//
//			bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//			require.NoError(t, err)
//
//			var td TestData
//			err = serializer.
//				NewRawBinarySerializer().
//				Deserialize(bs, &td)
//			require.NoError(t, err)
//			require.EqualValues(t, testData, &td)
//
//			err = fq.PopFromIndex(ctx, []byte(testData.IDField))
//			require.NoError(t, err)
//			assertFQCount(t, ctx, fq, 0)
//		})
//
//		t.Run("index not found", func(t *testing.T) {
//			ctx := context.Background()
//
//			path := GenericFileQueueFilePath + "/" + "pop_from_index"
//			filename := GenericFileQueueFileName
//
//			err := os.RemoveAll(getFilePath(path, filename))
//			require.NoError(t, err)
//
//			fq, err := New(ctx, path, filename)
//			require.NoError(t, err)
//
//			testData := getTestData(t)
//			err = fq.WriteOnCursor(ctx, testData)
//			require.NoError(t, err)
//
//			bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//			require.NoError(t, err)
//
//			var td TestData
//			err = serializer.
//				NewRawBinarySerializer().
//				Deserialize(bs, &td)
//			require.NoError(t, err)
//			require.EqualValues(t, testData, &td)
//
//			err = fq.PopFromIndex(ctx, nonExistentIndex)
//			require.Error(t, err)
//			assertFQCount(t, ctx, fq, 1)
//		})
//	})
//
//	t.Run("first item", func(t *testing.T) {
//		t.Run("index found", func(t *testing.T) {
//			ctx := context.Background()
//
//			path := GenericFileQueueFilePath + "/" + "pop_from_index"
//			filename := GenericFileQueueFileName
//
//			err := os.RemoveAll(getFilePath(path, filename))
//			require.NoError(t, err)
//
//			fq, err := New(ctx, path, filename)
//			require.NoError(t, err)
//
//			testDataList := getTestDataList(t, 3)
//
//			for _, testData := range testDataList {
//				err = fq.WriteOnCursor(ctx, testData)
//				require.NoError(t, err)
//			}
//
//			for _, testData := range testDataList {
//				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//				require.NoError(t, err)
//
//				var td TestData
//				err = serializer.
//					NewRawBinarySerializer().
//					Deserialize(bs, &td)
//				require.NoError(t, err)
//				require.EqualValues(t, testData, &td)
//			}
//
//			err = fq.PopFromIndex(ctx, []byte(testDataList[0].IDField))
//			require.NoError(t, err)
//			assertFQCount(t, ctx, fq, 2)
//		})
//
//		t.Run("index not found", func(t *testing.T) {
//			ctx := context.Background()
//
//			path := GenericFileQueueFilePath + "/" + "pop_from_index"
//			filename := GenericFileQueueFileName
//
//			err := os.RemoveAll(getFilePath(path, filename))
//			require.NoError(t, err)
//
//			fq, err := New(ctx, path, filename)
//			require.NoError(t, err)
//
//			testDataList := getTestDataList(t, 3)
//
//			for _, testData := range testDataList {
//				err = fq.WriteOnCursor(ctx, testData)
//				require.NoError(t, err)
//			}
//
//			for _, testData := range testDataList {
//				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//				require.NoError(t, err)
//
//				var td TestData
//				err = serializer.
//					NewRawBinarySerializer().
//					Deserialize(bs, &td)
//				require.NoError(t, err)
//				require.EqualValues(t, testData, &td)
//			}
//
//			err = fq.PopFromIndex(ctx, nonExistentIndex)
//			require.Error(t, err)
//			assertFQCount(t, ctx, fq, 3)
//		})
//	})
//
//	t.Run("last item", func(t *testing.T) {
//		t.Run("index found", func(t *testing.T) {
//			ctx := context.Background()
//
//			path := GenericFileQueueFilePath + "/" + "pop_from_index"
//			filename := GenericFileQueueFileName
//
//			err := os.RemoveAll(getFilePath(path, filename))
//			require.NoError(t, err)
//
//			fq, err := New(ctx, path, filename)
//			require.NoError(t, err)
//
//			testDataList := getTestDataList(t, 3)
//
//			for _, testData := range testDataList {
//				err = fq.WriteOnCursor(ctx, testData)
//				require.NoError(t, err)
//			}
//
//			for _, testData := range testDataList {
//				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//				require.NoError(t, err)
//
//				var td TestData
//				err = serializer.
//					NewRawBinarySerializer().
//					Deserialize(bs, &td)
//				require.NoError(t, err)
//				require.EqualValues(t, testData, &td)
//			}
//
//			err = fq.PopFromIndex(ctx, []byte(testDataList[len(testDataList)-1].IDField))
//			require.NoError(t, err)
//			assertFQCount(t, ctx, fq, 2)
//		})
//
//		t.Run("index not found", func(t *testing.T) {
//			ctx := context.Background()
//
//			path := GenericFileQueueFilePath + "/" + "pop_from_index"
//			filename := GenericFileQueueFileName
//
//			err := os.RemoveAll(getFilePath(path, filename))
//			require.NoError(t, err)
//
//			fq, err := New(ctx, path, filename)
//			require.NoError(t, err)
//
//			testDataList := getTestDataList(t, 3)
//
//			for _, testData := range testDataList {
//				err = fq.WriteOnCursor(ctx, testData)
//				require.NoError(t, err)
//			}
//
//			for _, testData := range testDataList {
//				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//				require.NoError(t, err)
//
//				var td TestData
//				err = serializer.
//					NewRawBinarySerializer().
//					Deserialize(bs, &td)
//				require.NoError(t, err)
//				require.EqualValues(t, testData, &td)
//			}
//
//			err = fq.PopFromIndex(ctx, nonExistentIndex)
//			require.Error(t, err)
//			assertFQCount(t, ctx, fq, 3)
//		})
//	})
//
//	t.Run("middle item", func(t *testing.T) {
//		t.Run("index found", func(t *testing.T) {
//			ctx := context.Background()
//
//			path := GenericFileQueueFilePath + "/" + "pop_from_index"
//			filename := GenericFileQueueFileName
//
//			err := os.RemoveAll(getFilePath(path, filename))
//			require.NoError(t, err)
//
//			fq, err := New(ctx, path, filename)
//			require.NoError(t, err)
//
//			testDataList := getTestDataList(t, 3)
//
//			for _, testData := range testDataList {
//				err = fq.WriteOnCursor(ctx, testData)
//				require.NoError(t, err)
//			}
//
//			for _, testData := range testDataList {
//				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//				require.NoError(t, err)
//
//				var td TestData
//				err = serializer.
//					NewRawBinarySerializer().
//					Deserialize(bs, &td)
//				require.NoError(t, err)
//				require.EqualValues(t, testData, &td)
//			}
//
//			err = fq.PopFromIndex(ctx, []byte(testDataList[len(testDataList)/2].IDField))
//			require.NoError(t, err)
//			assertFQCount(t, ctx, fq, 2)
//		})
//
//		t.Run("index not found", func(t *testing.T) {
//			ctx := context.Background()
//
//			path := GenericFileQueueFilePath + "/" + "pop_from_index"
//			filename := GenericFileQueueFileName
//
//			err := os.RemoveAll(getFilePath(path, filename))
//			require.NoError(t, err)
//
//			fq, err := New(ctx, path, filename)
//			require.NoError(t, err)
//
//			testDataList := getTestDataList(t, 3)
//
//			for _, testData := range testDataList {
//				err = fq.WriteOnCursor(ctx, testData)
//				require.NoError(t, err)
//			}
//
//			for _, testData := range testDataList {
//				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//				require.NoError(t, err)
//
//				var td TestData
//				err = serializer.
//					NewRawBinarySerializer().
//					Deserialize(bs, &td)
//				require.NoError(t, err)
//				require.EqualValues(t, testData, &td)
//			}
//
//			err = fq.PopFromIndex(ctx, nonExistentIndex)
//			require.Error(t, err)
//			assertFQCount(t, ctx, fq, 3)
//		})
//	})
//
//	t.Run("any item", func(t *testing.T) {
//		amount := random.Int(1, 50)
//		indexPosition := random.Int(0, amount-1)
//
//		t.Log("amount", amount)
//		t.Log("indexPosition:", indexPosition)
//
//		t.Run("index found", func(t *testing.T) {
//			ctx := context.Background()
//
//			path := GenericFileQueueFilePath + "/" + "pop_from_index"
//			filename := GenericFileQueueFileName
//
//			err := os.RemoveAll(getFilePath(path, filename))
//			require.NoError(t, err)
//
//			fq, err := New(ctx, path, filename)
//			require.NoError(t, err)
//
//			testDataList := getTestDataList(t, int(amount))
//
//			for _, testData := range testDataList {
//				err = fq.WriteOnCursor(ctx, testData)
//				require.NoError(t, err)
//			}
//
//			for _, testData := range testDataList {
//				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//				require.NoError(t, err)
//
//				var td TestData
//				err = serializer.
//					NewRawBinarySerializer().
//					Deserialize(bs, &td)
//				require.NoError(t, err)
//				require.EqualValues(t, testData, &td)
//			}
//
//			err = fq.PopFromIndex(ctx, []byte(testDataList[indexPosition].IDField))
//			require.NoError(t, err)
//			assertFQCount(t, ctx, fq, int(amount)-1)
//		})
//
//		t.Run("index not found", func(t *testing.T) {
//			ctx := context.Background()
//
//			path := GenericFileQueueFilePath + "/" + "pop_from_index"
//			filename := GenericFileQueueFileName
//
//			err := os.RemoveAll(getFilePath(path, filename))
//			require.NoError(t, err)
//
//			fq, err := New(ctx, path, filename)
//			require.NoError(t, err)
//
//			testDataList := getTestDataList(t, int(amount))
//
//			for _, testData := range testDataList {
//				err = fq.WriteOnCursor(ctx, testData)
//				require.NoError(t, err)
//			}
//
//			for _, testData := range testDataList {
//				bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//				require.NoError(t, err)
//
//				var td TestData
//				err = serializer.
//					NewRawBinarySerializer().
//					Deserialize(bs, &td)
//				require.NoError(t, err)
//				require.EqualValues(t, testData, &td)
//			}
//
//			err = fq.PopFromIndex(ctx, nonExistentIndex)
//			require.Error(t, err)
//			assertFQCount(t, ctx, fq, int(amount))
//		})
//	})
//
//	t.Run("one after another", func(t *testing.T) {
//		amount := random.Int(1, 10)
//		indexPosition := random.Int(0, amount-1)
//		anotherIndexPosition := random.Int(0, amount-1)
//		for indexPosition == anotherIndexPosition {
//			anotherIndexPosition = random.Int(0, amount-1)
//		}
//
//		t.Log("amount", amount)
//		t.Log("indexPosition:", indexPosition)
//		t.Log("anotherIndexPosition:", anotherIndexPosition)
//
//		t.Run("single-thread", func(t *testing.T) {
//			t.Run("index found", func(t *testing.T) {
//				ctx := context.Background()
//
//				path := GenericFileQueueFilePath + "/" + "pop_from_index"
//				filename := GenericFileQueueFileName
//
//				err := os.RemoveAll(getFilePath(path, filename))
//				require.NoError(t, err)
//
//				fq, err := New(ctx, path, filename)
//				require.NoError(t, err)
//
//				testDataList := getTestDataList(t, int(amount))
//
//				for _, testData := range testDataList {
//					err = fq.WriteOnCursor(ctx, testData)
//					require.NoError(t, err)
//				}
//
//				for _, testData := range testDataList {
//					bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//					require.NoError(t, err)
//
//					var td TestData
//					err = serializer.
//						NewRawBinarySerializer().
//						Deserialize(bs, &td)
//					require.NoError(t, err)
//					require.EqualValues(t, testData, &td)
//				}
//
//				t.Log(testDataList[indexPosition].IDField)
//				t.Log(testDataList[anotherIndexPosition].IDField)
//
//				err = fq.PopFromIndex(ctx, []byte(testDataList[indexPosition].IDField))
//				require.NoError(t, err)
//				err = fq.PopFromIndex(ctx, []byte(testDataList[anotherIndexPosition].IDField))
//				require.NoError(t, err)
//
//				assertFQCount(t, ctx, fq, int(amount)-2)
//			})
//
//			t.Run("index not found", func(t *testing.T) {
//				ctx := context.Background()
//
//				path := GenericFileQueueFilePath + "/" + "pop_from_index"
//				filename := GenericFileQueueFileName
//
//				err := os.RemoveAll(getFilePath(path, filename))
//				require.NoError(t, err)
//
//				fq, err := New(ctx, path, filename)
//				require.NoError(t, err)
//
//				testDataList := getTestDataList(t, int(amount))
//
//				for _, testData := range testDataList {
//					err = fq.WriteOnCursor(ctx, testData)
//					require.NoError(t, err)
//				}
//
//				for _, testData := range testDataList {
//					bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//					require.NoError(t, err)
//
//					var td TestData
//					err = serializer.
//						NewRawBinarySerializer().
//						Deserialize(bs, &td)
//					require.NoError(t, err)
//					require.EqualValues(t, testData, &td)
//				}
//
//				err = fq.PopFromIndex(ctx, nonExistentIndex)
//				require.Error(t, err)
//				err = fq.PopFromIndex(ctx, nonExistentIndex)
//				require.Error(t, err)
//				assertFQCount(t, ctx, fq, int(amount))
//			})
//		})
//
//		t.Run("concurrently", func(t *testing.T) {
//			t.Run("index found", func(t *testing.T) {
//				ctx := context.Background()
//
//				path := GenericFileQueueFilePath + "/" + "pop_from_index"
//				filename := GenericFileQueueFileName
//
//				err := os.RemoveAll(getFilePath(path, filename))
//				require.NoError(t, err)
//
//				fq, err := New(ctx, path, filename)
//				require.NoError(t, err)
//
//				testDataList := getTestDataList(t, int(amount))
//
//				for _, testData := range testDataList {
//					err = fq.WriteOnCursor(ctx, testData)
//					require.NoError(t, err)
//				}
//
//				for _, testData := range testDataList {
//					bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//					require.NoError(t, err)
//
//					var td TestData
//					err = serializer.
//						NewRawBinarySerializer().
//						Deserialize(bs, &td)
//					require.NoError(t, err)
//					require.EqualValues(t, testData, &td)
//				}
//
//				t.Log(testDataList[indexPosition].IDField)
//				t.Log(testDataList[anotherIndexPosition].IDField)
//
//				op := syncx.NewThreadOperator("simultaneous popping")
//				op.OpX(func() (any, error) {
//					err := fq.PopFromIndex(ctx, []byte(testDataList[indexPosition].IDField))
//					require.NoError(t, err)
//
//					return nil, nil
//				})
//				op.OpX(func() (any, error) {
//					err := fq.PopFromIndex(ctx, []byte(testDataList[anotherIndexPosition].IDField))
//					require.NoError(t, err)
//
//					return nil, nil
//				})
//				err = op.WaitAndWrapErr()
//				require.NoError(t, err)
//
//				assertFQCount(t, ctx, fq, int(amount)-2)
//			})
//
//			t.Run("index not found", func(t *testing.T) {
//				ctx := context.Background()
//
//				path := GenericFileQueueFilePath + "/" + "pop_from_index"
//				filename := GenericFileQueueFileName
//
//				err := os.RemoveAll(getFilePath(path, filename))
//				require.NoError(t, err)
//
//				fq, err := New(ctx, path, filename)
//				require.NoError(t, err)
//
//				testDataList := getTestDataList(t, int(amount))
//
//				for _, testData := range testDataList {
//					err = fq.WriteOnCursor(ctx, testData)
//					require.NoError(t, err)
//				}
//
//				for _, testData := range testDataList {
//					bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//					require.NoError(t, err)
//
//					var td TestData
//					err = serializer.
//						NewRawBinarySerializer().
//						Deserialize(bs, &td)
//					require.NoError(t, err)
//					require.EqualValues(t, testData, &td)
//				}
//
//				op := syncx.NewThreadOperator("simultaneous popping")
//				op.OpX(func() (any, error) {
//					err := fq.PopFromIndex(ctx, nonExistentIndex)
//					require.Error(t, err)
//
//					return nil, nil
//				})
//				op.OpX(func() (any, error) {
//					err := fq.PopFromIndex(ctx, nonExistentIndex)
//					require.Error(t, err)
//
//					return nil, nil
//				})
//				err = op.WaitAndWrapErr()
//				require.NoError(t, err)
//
//				assertFQCount(t, ctx, fq, int(amount))
//			})
//		})
//	})
//
//	t.Run("empty queue", func(t *testing.T) {
//		ctx := context.Background()
//
//		path := GenericFileQueueFilePath + "/" + "pop_from_index"
//		filename := GenericFileQueueFileName
//
//		err := os.RemoveAll(getFilePath(path, filename))
//		require.NoError(t, err)
//
//		fq, err := New(ctx, path, filename)
//		require.NoError(t, err)
//
//		testData := getTestData(t)
//		bs, err := fq.ReadFromCursorWithoutTruncation(ctx)
//		require.Error(t, err)
//		require.Empty(t, bs)
//
//		err = fq.PopFromIndex(ctx, []byte(testData.IDField))
//		require.Error(t, err)
//		assertFQCount(t, ctx, fq, 0)
//	})
//}

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
	fq, err := NewFileQueue(fileiomodels.GetTemporaryFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
		fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
	require.NoError(t, err)

	var counter int
	for {
		_, err = fq.Read()
		if err != nil {
			t.Log(err)
			break
		}

		_, err = fq.Pop()
		assert.NoError(t, err)

		counter++
	}
	t.Log(counter)
}

func TestReadFromFQWithoutTruncation(t *testing.T) {
	path := fileiomodels.GenericFileQueueFilePath + "/" + "pop_from_index"
	filename := fileiomodels.GenericFileQueueFileName
	defer func() {
		err := os.RemoveAll(fileiomodels.GetFileQueueFilePath(fileiomodels.FileQueueMmapVersion, path, filename))
		require.NoError(t, err)
	}()

	fq, err := NewFileQueue(fileiomodels.GetTemporaryFileQueueFilePath(fileiomodels.FileQueueMmapVersion,
		fileiomodels.GenericFileQueueFilePath, fileiomodels.GenericFileQueueFileName))
	require.NoError(t, err)

	var counter int
	for {
		_, err = fq.Read()
		if err != nil {
			t.Log(err)
			break
		}

		_, err = fq.Pop()
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
		_, err := fq.Read()
		if err != nil {
			t.Log(err)
			break
		}

		_, err = fq.Pop()
		assert.NoError(t, err)

		counter++
	}
	t.Log(counter)
	assert.Equal(t, expected, counter)
}
