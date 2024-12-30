package filequeuev1

import (
	"bufio"
	"context"
	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	serializermodels "gitlab.com/pietroski-software-company/devex/golang/serializer/models"
	"golang.org/x/sys/unix"
	"io"
	"log"
	"os"
	"sync"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesx"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/lock"
)

const truncateLimit = 1 << 14

type FileQueue struct {
	mtx      *sync.RWMutex
	opMtx    *lock.EngineLock
	opPopMtx *lock.EngineLock

	serializer serializermodels.Serializer
	cursor     uint64

	readerCursor uint64
	writeCursor  uint64

	fullPath    string
	fullTmpPath string
	file        *os.File
	reader      *bufio.Reader
	writer      *bufio.Writer
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
	if err != nil {
		return nil, err
	}

	fq := &FileQueue{
		opMtx:    lock.NewEngineLock(),
		opPopMtx: lock.NewEngineLock(),
		mtx:      &sync.RWMutex{},

		serializer: serializer.NewRawBinarySerializer(),
		cursor:     0,

		fullPath:    fullPath,
		fullTmpPath: fullTmpPath,
		file:        file,
		reader:      bufio.NewReader(file),
		writer:      bufio.NewWriter(file),
	}

	return fq, nil
}

func (fq *FileQueue) resetReader() error {
	if _, err := fq.file.Seek(0, 0); err != nil {
		return err
	}

	fq.reader.Reset(fq.file)
	fq.cursor = 0
	return nil
}

func (fq *FileQueue) resetWriter() error {
	if _, err := fq.file.Seek(0, 2); err != nil {
		return err
	}

	fq.writer.Reset(fq.file)
	return nil
}

func (fq *FileQueue) Read(ctx context.Context) ([]byte, error) {
	fq.opMtx.Lock(fileQueueKey, struct{}{})
	//defer fq.opMtx.Unlock(fileQueueKey)

	return fq.read(ctx)
}

func (fq *FileQueue) read(_ context.Context) ([]byte, error) {
	if err := fq.resetReader(); err != nil {
		return nil, err
	}

	rawRowLen := make([]byte, 4)
	if _, err := fq.reader.Read(rawRowLen); err != nil {
		return nil, err
	}
	rowLen := bytesx.Uint32(rawRowLen)

	row := make([]byte, rowLen)
	if _, err := fq.reader.Read(row); err != nil {
		return nil, err
	}

	fq.cursor = uint64(4 + rowLen)

	return row, nil
}

func (fq *FileQueue) Write(_ context.Context, data interface{}) error {
	fq.opMtx.Lock(fileQueueKey, struct{}{})
	defer fq.opMtx.Unlock(fileQueueKey)

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
	//fq.opMtx.Lock(fileQueueKey, struct{}{})
	defer fq.opMtx.Unlock(fileQueueKey)

	return fq.safelyTruncateFromStart(ctx)
}

func (fq *FileQueue) safelyTruncateFromStart(_ context.Context) error {
	// Get file size
	fi, err := fq.file.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()

	// If cursor is at start or file is empty, nothing to do
	if fq.cursor == 0 || size == 0 {
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
	remaining := data[fq.cursor:]
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
	if err = fq.file.Truncate(int64(len(remaining))); err != nil {
		return err
	}

	// Reset cursor and readers/writers
	fq.cursor = 0
	fq.reader = bufio.NewReader(fq.file)
	fq.writer = bufio.NewWriter(fq.file)

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
	if fq.cursor == 0 || size == 0 {
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

	writeCursor := fq.readerCursor
	// Copy the remaining data to the start
	remaining := data[fq.readerCursor:]
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
	if err = fq.file.Truncate(int64(len(remaining))); err != nil {
		return err
	}

	// Reset cursor and readers/writers
	fq.readerCursor = 0
	fq.writeCursor = -writeCursor
	fq.reader = bufio.NewReader(fq.file)
	fq.writer = bufio.NewWriter(fq.file)

	return nil
}

func (fq *FileQueue) ReadAndPop(
	ctx context.Context,
	handler func(ctx context.Context, bs []byte) error,
) error {
	fq.opMtx.Lock(fileQueueKey, struct{}{})
	defer fq.opMtx.Unlock(fileQueueKey)

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
	fq.opMtx.Lock(fileQueueKey, struct{}{})
	defer fq.opMtx.Unlock(fileQueueKey)

	if fq.readerCursor == fq.writeCursor {
		_ = fq.file.Truncate(0)
		fq.readerCursor = 0
		fq.writeCursor = 0
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
			fq.readerCursor = 0
			fq.writeCursor = 0
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

func (fq *FileQueue) WriteOnCursor(_ context.Context, data interface{}) error {
	fq.opMtx.Lock(fileQueueKey, struct{}{})
	defer fq.opMtx.Unlock(fileQueueKey)

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

	fq.writeCursor += 4 + uint64(len(bs))

	return nil
}

func (fq *FileQueue) Close() error {
	fq.opMtx.Lock(fileQueueKey, struct{}{})
	defer fq.opMtx.Unlock(fileQueueKey)

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
	fq.opMtx.Lock(fileQueueKey, struct{}{})
	defer fq.opMtx.Unlock(fileQueueKey)

	if fq.writeCursor == fq.readerCursor {
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
