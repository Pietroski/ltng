package filequeuev1

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"sync"

	"golang.org/x/sys/unix"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	serializermodels "gitlab.com/pietroski-software-company/golang/devex/serializer/models"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesx"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/ctx/ctxhandler"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/execx"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
)

const truncateLimit = 1 << 14

type FileQueue struct {
	mtx   *sync.RWMutex
	kvMtx *syncx.KVLock

	serializer serializermodels.Serializer

	readerCursor uint64
	writeCursor  uint64

	fullPath    string
	fullTmpPath string
	file        *os.File
	reader      *bufio.Reader
	writer      *bufio.Writer

	readerCursorLocker map[uint64]uint64
}

func New(_ context.Context, path, filename string) (*FileQueue, error) {
	if err := os.MkdirAll(ltngFileQueueBasePath+sep+path, dbFilePerm); err != nil {
		return nil, err
	}
	fullPath := getFilePath(path, filename)
	fullTmpPath := getTmpFilePath(path, filename)
	file, err := os.OpenFile(fullPath,
		os.O_RDWR|os.O_CREATE|os.O_APPEND, dbFilePerm,
	)
	if err != nil { // && !strings.Contains(err.Error(), "file already exist")
		return nil, err
	}

	fq := &FileQueue{
		kvMtx: syncx.NewKVLock(),
		mtx:   &sync.RWMutex{},

		serializer: serializer.NewRawBinarySerializer(),

		fullPath:    fullPath,
		fullTmpPath: fullTmpPath,
		file:        file,
		reader:      bufio.NewReader(file),
		writer:      bufio.NewWriter(file),

		readerCursorLocker: make(map[uint64]uint64),
	}

	return fq, nil
}

func (fq *FileQueue) resetReader() error {
	if _, err := fq.file.Seek(0, 0); err != nil {
		return err
	}

	fq.reader.Reset(fq.file)
	fq.readerCursor = 0

	return nil
}

func (fq *FileQueue) resetWriter() error {
	if _, err := fq.file.Seek(0, 2); err != nil {
		return err
	}

	fq.writer.Reset(fq.file)

	return nil
}

func (fq *FileQueue) Reset() error {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	if err := fq.resetReader(); err != nil {
		return err
	}

	if err := fq.resetWriter(); err != nil {
		return err
	}

	return nil
}

func (fq *FileQueue) Read(ctx context.Context) ([]byte, error) {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	return fq.read(ctx)
}

func (fq *FileQueue) read(ctx context.Context) ([]byte, error) {
	if err := fq.resetReader(); err != nil {
		return nil, err
	}

	return fq.readWithNoReset(ctx)
}

// readWithNoReset does not reset the reader before reading it.
// it is recommented for using inside a for loop so it does not loop forever,
// otherwise, the file would be endlessly reset on every for loop iteration.
func (fq *FileQueue) readWithNoReset(_ context.Context) ([]byte, error) {
	rawRowLen := make([]byte, 4)
	if _, err := fq.reader.Read(rawRowLen); err != nil {
		return nil, err
	}
	rowLen := bytesx.Uint32(rawRowLen)

	row := make([]byte, rowLen)
	if _, err := fq.reader.Read(row); err != nil {
		return nil, err
	}

	fq.readerCursor += uint64(4 + rowLen)

	return row, nil
}

func (fq *FileQueue) yieldReader() uint64 {
	return fq.readerCursor
}

func (fq *FileQueue) yieldWriter() uint64 {
	return fq.writeCursor
}

func (fq *FileQueue) Write(_ context.Context, data interface{}) error {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	if err := fq.resetWriter(); err != nil {
		return err
	}

	bs, err := fq.serializer.Serialize(data)
	if err != nil {
		return err
	}
	bsLen := bytesx.AddUint32(uint32(len(bs)))

	bbw := bytesx.NewWriter(make([]byte, len(bs)+4))
	bbw.Write(bsLen)
	bbw.Write(bs)
	if _, err = fq.writer.Write(bbw.Bytes()); err != nil {
		return err
	}

	if err = fq.writer.Flush(); err != nil {
		return err
	}

	return nil
}

func (fq *FileQueue) Pop(ctx context.Context) error {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	return fq.safelyTruncateFromStart(ctx)
}

func (fq *FileQueue) PopFromIndex(ctx context.Context, index []byte) error {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	return fq.safelyTruncateFromIndex(ctx, index)
}

func (fq *FileQueue) safelyTruncateFromIndex(ctx context.Context, index []byte) (err error) {
	if err = fq.resetReader(); err != nil {
		return err
	}

	var found bool
	var upTo, from uint64
	for {
		bs, err := fq.readWithNoReset(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		}

		// fmt.Printf("index: %s - bs: %s\n", index, bs)
		if bytes.Contains(bs, index) {
			found = true
			from = fq.yieldReader()
			upTo = from - uint64(len(bs)+4)
			break
		}
	}

	// fmt.Printf("from: %v - upTo: %v\n", from, upTo)
	if !found {
		return errorsx.Errorf("index not found: %s", string(index))
	}

	{
		// TODO: implement file truncation
		// create temporary file - ok
		// defer file removal in case of any error - ok
		// copy upto to temporary file - ok
		// copy from to temporary file - ok
		// rename temporary file to main file - ok
		// recreate the pointer references to the new file - ok

		var tmpFile *os.File
		tmpFile, err = os.OpenFile(fq.fullTmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, dbFilePerm)
		if err != nil {
			return err
		}

		defer func() {
			if err != nil {
				_ = tmpFile.Close()
				_ = os.Remove(fq.fullTmpPath)
			}
		}()

		// seek the file to the beginning.
		if _, err = fq.file.Seek(0, 0); err != nil {
			return errorsx.Wrap(err, "error seeking to file-queue")
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fq.file, int64(upTo)); err != nil {
			return errorsx.Wrap(err, "error copying first part of the file-queue to tmp file-queue")
		}

		if _, err = fq.file.Seek(int64(from), 0); err != nil {
			return errorsx.Wrap(err, "error seeking to file-queue")
		}

		if _, err = io.Copy(tmpFile, fq.file); err != nil {
			return errorsx.Wrap(err, "error copying second part of the file-queue to tmp file-queue")
		}

		if err = tmpFile.Sync(); err != nil {
			return errorsx.Wrapf(err, "failed to sync file-queue: %s", tmpFile.Name())
		}

		if err = tmpFile.Close(); err != nil {
			return errorsx.Wrapf(err, "failed to close file-queue: %s", tmpFile.Name())
		}

		if err = os.Rename(fq.fullTmpPath, fq.fullPath); err != nil {
			return errorsx.Wrap(err, "error renaming tmp file-queue to file-queue")
		}

		{ // reset file pointers
			var file *os.File
			file, err = os.OpenFile(fq.fullPath,
				os.O_RDWR|os.O_APPEND, dbFilePerm,
			)
			if err != nil {
				return err
			}

			fq.file = file
			deduct := from - upTo
			fq.readerCursor -= deduct
			fq.writeCursor -= deduct

			//fmt.Printf("writeCursor from %d to %d - %v -> %v\n", upTo, deduct,
			//	fq.writeCursor, fq.writeCursor-deduct)
			//fq.writeCursor -= deduct
			//fq.reader = bufio.NewReader(file)
			//fq.writer = bufio.NewWriter(file)
		}
	}

	return nil
}

func (fq *FileQueue) PopAndUnlockItFromIndex(ctx context.Context, index []byte) error {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	return fq.safelyTruncateAndUnlockItFromIndex(ctx, index)
}

func (fq *FileQueue) safelyTruncateAndUnlockItFromIndex(ctx context.Context, index []byte) (err error) {
	if err = fq.resetReader(); err != nil {
		return err
	}

	var found bool
	var upTo, from uint64
	for {
		bs, err := fq.readWithNoReset(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		}

		// fmt.Printf("index: %s - bs: %s\n", index, bs)
		if bytes.Contains(bs, index) {
			found = true
			from = fq.yieldReader()
			upTo = from - uint64(len(bs)+4)
			break
		}
	}

	// fmt.Printf("from: %v - upTo: %v\n", from, upTo)
	if !found {
		return errorsx.Errorf("index not found: %s", string(index))
	}

	{
		// TODO: implement file truncation
		// create temporary file - ok
		// defer file removal in case of any error - ok
		// copy upto to temporary file - ok
		// copy from to temporary file - ok
		// rename temporary file to main file - ok
		// recreate the pointer references to the new file - ok

		var tmpFile *os.File
		tmpFile, err = os.OpenFile(fq.fullTmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, dbFilePerm)
		if err != nil {
			return err
		}

		defer func() {
			if err != nil {
				_ = tmpFile.Close()
				_ = os.Remove(fq.fullTmpPath)
			}
		}()

		// seek the file to the beginning.
		if _, err = fq.file.Seek(0, 0); err != nil {
			return errorsx.Wrap(err, "error seeking to file-queue")
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fq.file, int64(upTo)); err != nil {
			return errorsx.Wrap(err, "error copying first part of the file-queue to tmp file-queue")
		}

		if _, err = fq.file.Seek(int64(from), 0); err != nil {
			return errorsx.Wrap(err, "error seeking to file-queue")
		}

		if _, err = io.Copy(tmpFile, fq.file); err != nil {
			return errorsx.Wrap(err, "error copying second part of the file-queue to tmp file-queue")
		}

		if err = tmpFile.Sync(); err != nil {
			return errorsx.Wrapf(err, "failed to sync file-queue - %s", tmpFile.Name())
		}

		if err = tmpFile.Close(); err != nil {
			return errorsx.Wrapf(err, "failed to close file-queue - %s", tmpFile.Name())
		}

		if err = os.Rename(fq.fullTmpPath, fq.fullPath); err != nil {
			return errorsx.Wrap(err, "error renaming tmp file-queue to file-queue")
		}

		{ // reset file pointers
			var file *os.File
			file, err = os.OpenFile(fq.fullPath,
				os.O_RDWR|os.O_APPEND, dbFilePerm,
			)
			if err != nil {
				return err
			}

			fq.file = file
			deduct := from - upTo
			fq.readerCursor -= deduct
			fq.writeCursor -= deduct

			newReaderCursorLocker := make(map[uint64]uint64)
			for key, value := range fq.readerCursorLocker {
				if key == 0 {
					continue
				}

				newReaderCursorLocker[key-deduct] = value - deduct
			}
			fq.readerCursorLocker = newReaderCursorLocker

			//for cursor, ok := fq.readerCursorLocker[fq.readerCursor]; ok; cursor, ok = fq.readerCursorLocker[fq.readerCursor] {
			//	fq.readerCursor = cursor
			//}

			//fmt.Printf("writeCursor from %d to %d - %v -> %v\n", upTo, deduct,
			//	fq.writeCursor, fq.writeCursor-deduct)
			//fq.writeCursor -= deduct
			//fq.reader = bufio.NewReader(file)
			//fq.writer = bufio.NewWriter(file)
		}
	}

	return nil
}

func (fq *FileQueue) RepublishIndex(ctx context.Context, index []byte, data any) error {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	return fq.safelyRepublishIndex(ctx, index, data)
}

func (fq *FileQueue) safelyRepublishIndex(ctx context.Context, index []byte, data any) (err error) {
	if err = fq.resetReader(); err != nil {
		return errorsx.Wrap(err, "error republishing index: error resetting reader")
	}

	_, err = execx.CpFileExec(ctx, fq.fullPath, fq.fullTmpPath)
	if err != nil {
		return errorsx.Wrap(err, "error republishing index: error executing cpfile")
	}

	if err = fq.PopFromIndex(ctx, index); err != nil {
		return errorsx.Wrap(err, "error republishing index: error popping from index")
	}

	if err = fq.WriteOnCursor(ctx, data); err != nil {
		_, err = execx.MvFileExec(ctx, fq.fullTmpPath, fq.fullPath)
		if err != nil {
			return errorsx.Wrap(err, "error republishing index: error executing reverse cpfile")
		}

		return errorsx.Wrap(err, "error republishing index: error writing cursor")
	}

	return nil
}

func (fq *FileQueue) safelyTruncateFromStart(_ context.Context) error {
	// Get file size
	fi, err := fq.file.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()

	// If cursor is at start or file is empty, nothing to do
	if fq.readerCursor == 0 || size == 0 {
		return nil
	}

	// Memory map the file
	data, err := unix.Mmap(
		int(fq.file.Fd()),
		0,
		int(size),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		return err
	}

	// Copy the remaining data to the start
	remaining := data[fq.readerCursor:]
	remainingLen := len(remaining)
	copy(data[:len(remaining)], remaining)

	// Sync changes to disk
	if err = unix.Msync(data, unix.MS_SYNC); err != nil {
		return err
	}

	//// Force a disk sync before unmapping
	//if err = fq.file.Sync(); err != nil {
	//	return err
	//}

	if err = unix.Munmap(data); err != nil {
		return err
	}

	// Truncate the file to the new size
	if err = fq.file.Truncate(int64(remainingLen)); err != nil {
		return err
	}

	// Reset cursor and readers/writers
	fq.readerCursor = 0
	fq.writeCursor = uint64(remainingLen)
	//fq.reader = bufio.NewReader(fq.file)
	//fq.writer = bufio.NewWriter(fq.file)

	return nil
}

func (fq *FileQueue) safelyTruncateToCursor(_ context.Context) error {
	// Get file size
	fi, err := fq.file.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()

	// If cursor is at start or file is empty, nothing to do
	if fq.readerCursor == 0 || size == 0 {
		return nil
	}

	// Memory map the file
	data, err := unix.Mmap(
		int(fq.file.Fd()),
		0,
		int(size),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		return err
	}

	// Copy the remaining data to the start
	remaining := data[fq.readerCursor:]
	copy(data[:len(remaining)], remaining)

	// Sync changes to disk
	if err = unix.Msync(data, unix.MS_SYNC); err != nil {
		return err
	}

	if err = unix.Munmap(data); err != nil {
		return err
	}

	// Truncate the file to the new size
	if err = fq.file.Truncate(int64(len(remaining))); err != nil {
		return err
	}

	// Reset cursor and readers/writers
	fq.writeCursor -= fq.readerCursor
	fq.readerCursor = 0
	//fq.reader = bufio.NewReader(fq.file)
	//fq.writer = bufio.NewWriter(fq.file)

	return nil
}

func (fq *FileQueue) ReadAndPop(
	ctx context.Context,
	handler func(ctx context.Context, bs []byte) error,
) error {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	bs, err := fq.read(ctx)
	if err != nil {
		return err
	}

	if err = handler(ctx, bs); err != nil {
		log.Printf("error handling file queue: %v", err)
	}

	return fq.safelyTruncateFromStart(ctx)
}

func (fq *FileQueue) ReadFromCursor(ctx context.Context) ([]byte, error) {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	return fq.readFromCursor(ctx)
}

func (fq *FileQueue) readFromCursor(ctx context.Context) ([]byte, error) {
	if fq.readerCursor == fq.writeCursor {
		_ = fq.file.Truncate(0)
		fq.writeCursor = 0
		fq.readerCursor = 0
		fq.reader = bufio.NewReader(fq.file)
		fq.writer = bufio.NewWriter(fq.file)

		return nil, io.EOF
	}

	if fq.readerCursor >= truncateLimit {
		if err := fq.safelyTruncateToCursor(ctx); err != nil {
			return nil, err
		}
	}

	if _, err := fq.file.Seek(int64(fq.readerCursor), 0); err != nil {
		return nil, err
	}
	fq.reader.Reset(fq.file)

	rawRowLen := make([]byte, 4)
	if _, err := fq.reader.Read(rawRowLen); err != nil {
		if err == io.EOF {
			_ = fq.file.Truncate(0)
			fq.writeCursor = 0
			fq.readerCursor = 0
			fq.reader = bufio.NewReader(fq.file)
			fq.writer = bufio.NewWriter(fq.file)
		}

		return nil, err
	}
	rowLen := bytesx.Uint32(rawRowLen)

	row := make([]byte, rowLen)
	if _, err := fq.reader.Read(row); err != nil {
		return nil, err
	}

	fq.readerCursor += uint64(4 + rowLen)

	return row, nil
}

func (fq *FileQueue) ReadFromCursorWithoutTruncation(ctx context.Context) ([]byte, error) {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	return fq.readFromCursorWithoutTruncation(ctx)
}

func (fq *FileQueue) readFromCursorWithoutTruncation(_ context.Context) ([]byte, error) {
	if _, err := fq.file.Seek(int64(fq.readerCursor), 0); err != nil {
		return nil, err
	}
	fq.reader.Reset(fq.file)

	rawRowLen := make([]byte, 4)
	if _, err := fq.reader.Read(rawRowLen); err != nil {
		return nil, err
	}
	rowLen := bytesx.Uint32(rawRowLen)

	row := make([]byte, rowLen)
	if _, err := fq.reader.Read(row); err != nil {
		return nil, err
	}

	fq.readerCursor += uint64(4 + rowLen)

	return row, nil
}

func (fq *FileQueue) ReadFromCursorAndLockItWithoutTruncation(ctx context.Context) ([]byte, error) {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	return fq.readFromCursorAndLockItWithoutTruncation(ctx)
}

func (fq *FileQueue) readFromCursorAndLockItWithoutTruncation(_ context.Context) ([]byte, error) {
	for cursor, ok := fq.readerCursorLocker[fq.readerCursor]; ok; cursor, ok = fq.readerCursorLocker[fq.readerCursor] {
		fq.readerCursor = cursor
	}

	if _, err := fq.file.Seek(int64(fq.readerCursor), 0); err != nil {
		return nil, err
	}
	fq.reader.Reset(fq.file)

	rawRowLen := make([]byte, 4)
	if _, err := fq.reader.Read(rawRowLen); err != nil {
		return nil, err
	}
	rowLen := bytesx.Uint32(rawRowLen)

	row := make([]byte, rowLen)
	if _, err := fq.reader.Read(row); err != nil {
		return nil, err
	}

	newReadCursor := fq.readerCursor + uint64(4+rowLen)
	fq.readerCursorLocker[fq.readerCursor] = newReadCursor
	fq.readerCursor = newReadCursor
	//fq.readerCursor += uint64(4 + rowLen)

	return row, nil
}

func (fq *FileQueue) WriteOnCursor(_ context.Context, data interface{}) error {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	if err := fq.resetWriter(); err != nil {
		return err
	}

	bs, err := fq.serializer.Serialize(data)
	if err != nil {
		return err
	}
	bsLen := len(bs)
	bsBsLen := bytesx.AddUint32(uint32(bsLen))

	bbw := bytesx.NewWriter(make([]byte, bsLen+4))
	bbw.Write(bsBsLen)
	bbw.Write(bs)
	if _, err = fq.writer.Write(bbw.Bytes()); err != nil {
		return err
	}

	if err = fq.writer.Flush(); err != nil {
		return err
	}

	fq.writeCursor += 4 + uint64(bsLen)

	return nil
}

func (fq *FileQueue) ReaderPooler(
	ctx context.Context,
	handler func(ctx context.Context, bs []byte) error,
) error {
	ctxhandler.WithCancellation(ctx, func() error {
		//time.Sleep(time.Millisecond * 50)
		bs, err := fq.Read(ctx)
		if err != nil {
			// log.Printf("error reading file queue: %v", err)
			if err == io.EOF {
				return ctxhandler.ErrEnded
			}

			return err
		}

		if err = handler(ctx, bs); err != nil {
			if !errorsx.IsNonRetriable(err) {
				// TODO: fix republish here
				//err = fq.RepublishIndex(ctx, bs)
				//if err != nil {
				//	return fmt.Errorf("error republishing index %s to file queue: %v", bs, err)
				//}
			}

			return errorsx.New("error handling file queue").
				Wrap(err, "error from handler").
				WithRetriable()
		}

		err = fq.PopFromIndex(ctx, bs)
		if err != nil {
			return errorsx.Wrapf(err, "error popping from file queue with index %s", bs)
		}

		return nil
	})

	return fq.Close()
}

func (fq *FileQueue) Close() error {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	if err := fq.file.Close(); err != nil {
		return err
	}

	return nil
}

func (fq *FileQueue) Stats() (os.FileInfo, error) {
	return fq.file.Stat()
}

func (fq *FileQueue) IsEmpty() (bool, error) {
	fileStat, err := fq.file.Stat()
	if err != nil {
		return false, err
	}

	return fileStat.Size() == 0, nil
}

func (fq *FileQueue) CheckAndClose() bool {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	if fq.readerCursor == fq.writeCursor {
		fq.writeCursor = 0
		fq.readerCursor = 0
		if err := fq.file.Truncate(0); err != nil {
			log.Printf("error truncating file: %v", err)
		}

		if err := fq.file.Close(); err != nil {
			log.Printf("error closing file: %v", err)
		}

		return true
	}

	return false
}

func (fq *FileQueue) Init() error {
	fq.kvMtx.Lock(fileQueueKey, struct{}{})
	defer fq.kvMtx.Unlock(fileQueueKey)

	if !rw.IsFileClosed(fq.file) {
		return nil
	}

	file, err := os.OpenFile(fq.fullPath,
		os.O_RDWR|os.O_CREATE|os.O_APPEND, dbFilePerm,
	)
	if err != nil {
		return err
	}
	fq.file = file
	fq.writer = bufio.NewWriter(fq.file)
	fq.reader = bufio.NewReader(fq.file)

	return nil
}

func (fq *FileQueue) Restart() error {
	fq.CheckAndClose()
	return fq.Init()
}
