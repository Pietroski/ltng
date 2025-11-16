package mmap

import (
	"os"
	"sync"

	"golang.org/x/sys/unix"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesx"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

type (
	FileManager struct {
		mtx      sync.Mutex
		file     *os.File
		data     []byte
		size     uint64
		dataSize uint64

		serializer *serializer.RawBinarySerializer
	}
)

var ErrFileCannotBeOverWritten = errorsx.New("file cannot be over-written")

func NewFileManager(
	filePath string,
) (*FileManager, error) {
	file, err := osx.OpenCreateFile(filePath)
	if err != nil {
		return nil, err
	}

	return NewFileManagerFromFile(file)
}

func NewFileManagerFromFile(
	file *os.File,
) (*FileManager, error) {
	// Ensure file is at least initialSize
	fi, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	fileSize := uint64(fi.Size())
	if fileSize == 0 {
		fileSize = initialSize
		if err = file.Truncate(int64(fileSize)); err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	mmap, err := unix.Mmap(
		int(file.Fd()),
		0,
		int(fileSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	dataSize, err := readDataSize(mmap, fileSize)
	if err != nil {
		_ = unix.Munmap(mmap)
		_ = file.Close()
		return nil, err
	}

	fm := &FileManager{
		file:     file,
		data:     mmap,
		size:     fileSize,
		dataSize: dataSize,

		serializer: serializer.NewRawBinarySerializer(),
	}

	return fm, nil
}

// Write writes to an empty file only, it does not append not upsert data.
// Use Rewrite or upserting / rewriting a file.
func (fm *FileManager) Write(data any) ([]byte, error) {
	fm.mtx.Lock()
	defer fm.mtx.Unlock()

	if fm.dataSize != 0 {
		return nil, ErrFileCannotBeOverWritten
	}

	return fm.write(data)
}

func (fm *FileManager) Rewrite(data any) ([]byte, error) {
	fm.mtx.Lock()
	defer fm.mtx.Unlock()

	return fm.write(data)
}

func (fm *FileManager) write(data any) ([]byte, error) {
	bs, err := fm.serializer.Serialize(data)
	if err != nil {
		return nil, err
	}
	bsLen := uint32(len(bs))

	requiredSize := uint64(4 + bsLen)

	// GROWING: Must resize first (no choice)
	if requiredSize > fm.size {
		if err = fm.resize(requiredSize); err != nil {
			return nil, err
		}
	}

	// Write new data (either overwrites old data or writes to new space)
	bytesx.PutUint32(fm.data[0:4], bsLen)
	copy(fm.data[4:], bs)

	// Flush BEFORE truncating
	if err = partialFlushMmap(fm.data, requiredSize); err != nil {
		return nil, err
	}

	// SHRINKING: Truncate AFTER write is safe on disk
	if requiredSize < fm.size {
		if err = fm.resize(requiredSize); err != nil {
			return nil, err
		}
	}

	fm.dataSize = requiredSize

	return bs, nil
}

// Read returns the exact data that was written to the file.
func (fm *FileManager) Read() ([]byte, error) {
	fm.mtx.Lock()
	defer fm.mtx.Unlock()

	// Check if we have data
	if fm.dataSize == 0 {
		return nil, errorsx.New("file is empty")
	}

	if err := validateData(fm.data, fm.dataSize, fm.size); err != nil {
		// file is very likely corrupted and return it all
		// and let the user decide what to do with it or what is left from it.
		return fm.data[4:fm.dataSize], errorsx.Wrap(err, "corrupted file")
	}

	// return what was originally written.
	// Return a COPY of the data, not the mmap slice
	length := bytesx.Uint32(fm.data[0:4])
	payload := make([]byte, length)
	copy(payload, fm.data[4:4+length])
	return payload, nil
}

// ReadRaw returns a direct slice into mmap (caller must not modify)
// Caller must not use after Close() or concurrent Write/Rewrite
func (fm *FileManager) ReadRaw() ([]byte, error) {
	fm.mtx.Lock()
	defer fm.mtx.Unlock()

	if fm.dataSize == 0 {
		return nil, errorsx.New("file is empty")
	}

	return fm.data[4:fm.dataSize], nil
}

// resize must be called with lock held
func (fm *FileManager) resize(newSize uint64) error {
	// 1. Unmap
	if err := unix.Munmap(fm.data); err != nil {
		return errorsx.Wrap(err, "unmap failed")
	}

	// 2. Truncate
	if err := fm.file.Truncate(int64(newSize)); err != nil {
		return errorsx.Wrap(err, "truncate failed")
	}

	// 3. Remap
	newMmap, err := unix.Mmap(
		int(fm.file.Fd()),
		0,
		int(newSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		return errorsx.Wrap(err, "remap failed")
	}

	fm.data = newMmap
	fm.size = newSize
	return nil
}

func (fm *FileManager) Sync() error {
	fm.mtx.Lock()
	defer fm.mtx.Unlock()

	return flushMmap(fm.data)
}

// Close unmaps and closes the file queue.
func (fm *FileManager) Close() error {
	fm.mtx.Lock()
	defer fm.mtx.Unlock()

	if osx.IsFileClosed(fm.file) {
		return nil
	}

	if err := flushMmap(fm.data); err != nil {
		return errorsx.Wrap(err, "flush failed")
	}

	if err := unix.Munmap(fm.data); err != nil {
		_ = fm.file.Close()
		return errorsx.Wrap(err, "unmap failed")
	}

	return fm.file.Close()
}
