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
func (g *FileManager) Write(data any) ([]byte, error) {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	if g.dataSize != 0 {
		return nil, errorsx.New("file cannot be over-written")
	}

	return g.write(data)
}

func (g *FileManager) Rewrite(data any) ([]byte, error) {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	return g.write(data)
}

func (g *FileManager) write(data any) ([]byte, error) {
	bs, err := g.serializer.Serialize(data)
	if err != nil {
		return nil, err
	}
	bsLen := uint32(len(bs))

	requiredSize := uint64(4 + bsLen)

	// GROWING: Must resize first (no choice)
	if requiredSize > g.size {
		if err = g.resize(requiredSize); err != nil {
			return nil, err
		}
	}

	// Write new data (either overwrites old data or writes to new space)
	bytesx.PutUint32(g.data[0:4], bsLen)
	copy(g.data[4:], bs)

	// Flush BEFORE truncating
	if err = partialFlushMmap(g.data, requiredSize); err != nil {
		return nil, err
	}

	// SHRINKING: Truncate AFTER write is safe on disk
	if requiredSize < g.size {
		if err = g.resize(requiredSize); err != nil {
			return nil, err
		}
	}

	g.dataSize = requiredSize

	return bs, nil
}

// Read returns the exact data that was written to the file.
func (g *FileManager) Read() ([]byte, error) {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	// Check if we have data
	if g.dataSize == 0 {
		return nil, errorsx.New("file is empty")
	}

	if err := validateData(g.data, g.dataSize, g.size); err != nil {
		// file is very likely corrupted and return it all
		// and let the user decide what to do with it or what is left from it.
		return g.data[4:g.dataSize], errorsx.Wrap(err, "corrupted file")
	}

	// return what was originally written.
	// Return a COPY of the data, not the mmap slice
	length := bytesx.Uint32(g.data[0:4])
	payload := make([]byte, length)
	copy(payload, g.data[4:4+length])
	return payload, nil
}

// ReadRaw returns a direct slice into mmap (caller must not modify)
// Caller must not use after Close() or concurrent Write/Rewrite
func (g *FileManager) ReadRaw() ([]byte, error) {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	if g.dataSize == 0 {
		return nil, errorsx.New("file is empty")
	}

	return g.data[4:g.dataSize], nil
}

// resize must be called with lock held
func (g *FileManager) resize(newSize uint64) error {
	// 1. Unmap
	if err := unix.Munmap(g.data); err != nil {
		return errorsx.Wrap(err, "unmap failed")
	}

	// 2. Truncate
	if err := g.file.Truncate(int64(newSize)); err != nil {
		return errorsx.Wrap(err, "truncate failed")
	}

	// 3. Remap
	newMmap, err := unix.Mmap(
		int(g.file.Fd()),
		0,
		int(newSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		return errorsx.Wrap(err, "remap failed")
	}

	g.data = newMmap
	g.size = newSize
	return nil
}

func (g *FileManager) Sync() error {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	return flushMmap(g.data)
}

// Close unmaps and closes the file queue.
func (g *FileManager) Close() error {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	if err := flushMmap(g.data); err != nil {
		return err
	}

	if err := unix.Munmap(g.data); err != nil {
		_ = g.file.Close()
		return err
	}

	return g.file.Close()
}
