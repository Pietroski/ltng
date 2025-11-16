package mmap

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesx"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

type FileQueue struct {
	mtx         sync.Mutex
	file        *os.File
	data        []byte
	size        uint64
	writeOffset uint64
	readOffset  uint64

	serializer *serializer.RawBinarySerializer
}

func NewFileQueue(filePath string) (*FileQueue, error) {
	if err := os.MkdirAll(filepath.Dir(filePath), osx.FileRW); err != nil {
		return nil, err
	}

	file, err := osx.OpenCreateFile(filePath)
	if err != nil {
		return nil, err
	}

	// Ensure file is at least initialSize
	fi, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	fileSize := uint64(fi.Size())

	if fileSize < initialSize {
		fileSize = initialSize
		if err := file.Truncate(int64(fileSize)); err != nil {
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

	writeOffset, err := scanForValidDataEnd(mmap, fileSize)
	if err != nil {
		_ = unix.Munmap(mmap)
		_ = file.Close()
		return nil, err
	}

	return &FileQueue{
		file: file,
		data: mmap,
		size: fileSize,

		writeOffset: writeOffset,

		serializer: serializer.NewRawBinarySerializer(),
	}, nil
}

// Write writes an item to the end of the file queue.
func (fq *FileQueue) Write(data any) ([]byte, error) {
	fq.mtx.Lock()
	defer fq.mtx.Unlock()

	bs, err := fq.serializer.Serialize(data)
	if err != nil {
		return nil, err
	}

	bsLen := uint64(len(bs))
	totalLen := 4 + bsLen

	requiredSize := fq.writeOffset + totalLen

	// Check if we need to grow
	if requiredSize > fq.size {
		if err = fq.growUnsafe(requiredSize); err != nil {
			return nil, err
		}
	}

	// Write directly to mmap - no intermediate buffer
	bytesx.PutUint32(fq.data[fq.writeOffset:], uint32(bsLen))
	copy(fq.data[fq.writeOffset+4:], bs)

	newOffset := fq.writeOffset + totalLen
	if err = partialFlushMmap(fq.data, newOffset); err != nil {
		return nil, err
	}

	// Update writeOffset
	fq.writeOffset = newOffset

	return bs, nil
}

// Read reads the next item from the file queue.
// Returns io.EOF if no more items are available.
func (fq *FileQueue) Read() ([]byte, error) {
	fq.mtx.Lock()
	defer fq.mtx.Unlock()

	if fq.readOffset >= fq.writeOffset {
		if fq.readOffset != 0 {

			if err := fq.clear(); err != nil {
				return nil, err
			}
		}

		return nil, io.EOF
	}

	// Ensure we can read the length header
	if fq.readOffset+4 > fq.writeOffset {
		return nil, errorsx.New("corrupted data: incomplete length header")
	}

	bsLen := bytesx.Uint32(fq.data[fq.readOffset : fq.readOffset+4])
	fq.readOffset += 4

	// Ensure we can read the full payload
	if fq.readOffset+uint64(bsLen) > fq.writeOffset {
		return nil, errorsx.New("corrupted data: incomplete payload")
	}

	payload := make([]byte, bsLen)
	copy(payload, fq.data[fq.readOffset:fq.readOffset+uint64(bsLen)])
	fq.readOffset += uint64(bsLen)

	return payload, nil
}

// Pop pops the first item from the file queue.
// Returns io.EOF if no more items are available.
func (fq *FileQueue) Pop() ([]byte, error) {
	fq.mtx.Lock()
	defer fq.mtx.Unlock()

	// Check if empty
	if fq.writeOffset == 0 {
		return nil, io.EOF
	}

	// Check for corrupt header
	if fq.writeOffset < 4 {
		return nil, errorsx.New("corrupted data: incomplete length header")
	}

	bsLen := bytesx.Uint32(fq.data[0:4])
	fq.readOffset = 4

	// Ensure we can read the full payload
	if fq.readOffset+uint64(bsLen) > fq.writeOffset {
		return nil, errorsx.New("corrupted data: incomplete payload")
	}

	// read the first item
	payload := make([]byte, bsLen)
	copy(payload, fq.data[fq.readOffset:fq.readOffset+uint64(bsLen)])

	// set the read offset to the next item
	fq.readOffset += uint64(bsLen)

	// move remaining data to the beginning
	copy(fq.data, fq.data[fq.readOffset:])

	// clear from new write position to old write position
	clear(fq.data[fq.writeOffset-fq.readOffset:])

	// flush entire mmap to persist both moved data and cleared area
	if err := flushMmap(fq.data); err != nil {
		return nil, err
	}

	fq.writeOffset = fq.writeOffset - fq.readOffset
	fq.readOffset = 0

	if err := fq.compactIfNecessary(); err != nil {
		return nil, err
	}

	return payload, nil
}

func (fq *FileQueue) compactIfNecessary() error {
	// Only compact if:
	// 1. File is larger than initial size (no point compacting small files)
	// 2. We're using less than 25% of the allocated space
	if fq.size > initialSize && fq.writeOffset < fq.size/4 {
		// New size should be at least initialSize or 2x current data
		newSize := fq.writeOffset * 2
		if newSize < initialSize {
			newSize = initialSize
		}

		return fq.resize(newSize)
	}

	return nil
}

func (fq *FileQueue) Compact() error {
	fq.mtx.Lock()
	defer fq.mtx.Unlock()

	if fq.size <= initialSize {
		return nil // Already at minimum
	}

	// Shrink to 2x current data size, minimum initialSize
	newSize := fq.writeOffset * 2
	if newSize < initialSize {
		newSize = initialSize
	}

	if newSize < fq.size {
		return fq.resize(newSize)
	}

	return nil
}

// growUnsafe must be called with lock held
func (fq *FileQueue) growUnsafe(minimumRequiredSize uint64) error {
	// Round up to page size or some reasonable increment
	//pageSize := int64(os.Getpagesize())
	//newSize = ((newSize + pageSize - 1) / pageSize) * pageSize
	newSize := fq.size * 2
	if newSize < minimumRequiredSize {
		newSize = minimumRequiredSize * 2
	}

	// 1. Unmap
	if err := unix.Munmap(fq.data); err != nil {
		return errorsx.Wrap(err, "unmap failed")
	}

	// 2. Truncate
	if err := fq.file.Truncate(int64(newSize)); err != nil {
		return errorsx.Wrap(err, "truncate failed")
	}

	// 3. Remap
	newMmap, err := unix.Mmap(
		int(fq.file.Fd()),
		0,
		int(newSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		return errorsx.Wrap(err, "remap failed")
	}

	fq.data = newMmap
	fq.size = newSize
	return nil
}

// resize must be called with lock held
func (fq *FileQueue) resize(newSize uint64) error {
	// 1. Unmap
	if err := unix.Munmap(fq.data); err != nil {
		return errorsx.Wrap(err, "unmap failed")
	}

	// 2. Truncate
	if err := fq.file.Truncate(int64(newSize)); err != nil {
		return errorsx.Wrap(err, "truncate failed")
	}

	// 3. Remap
	newMmap, err := unix.Mmap(
		int(fq.file.Fd()),
		0,
		int(newSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		return errorsx.Wrap(err, "remap failed")
	}

	fq.data = newMmap
	fq.size = newSize
	return nil
}

// clear is called internally after all data is consumed
func (fq *FileQueue) clear() error {
	clearSize := fq.readOffset

	// Only clear the used portion
	clear(fq.data[:clearSize])

	fq.readOffset = 0
	fq.writeOffset = 0

	// Only sync what we cleared, not the entire mmap
	return partialFlushMmap(fq.data, clearSize)
}

func (fq *FileQueue) Sync() error {
	fq.mtx.Lock()
	defer fq.mtx.Unlock()

	return flushMmap(fq.data)
}

// Close unmaps and closes the file queue.
func (fq *FileQueue) Close() error {
	fq.mtx.Lock()
	defer fq.mtx.Unlock()

	if err := flushMmap(fq.data); err != nil {
		return err
	}

	if err := unix.Munmap(fq.data); err != nil {
		_ = fq.file.Close()
		return err
	}

	return fq.file.Close()
}
