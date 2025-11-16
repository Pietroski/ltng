package mmap

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"

	"golang.org/x/sys/unix"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesx"
	fileiomodels "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/models"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

type (
	RelationalFileManager struct {
		mtx  sync.Mutex
		file *os.File
		data []byte
		size uint64

		writeOffset uint64
		readOffset  uint64

		serializer *serializer.RawBinarySerializer
	}
)

func NewRelationalFileManager(filePath string) (*RelationalFileManager, error) {
	file, err := osx.OpenCreateFile(filePath)
	if err != nil {
		return nil, err
	}

	return NewRelationalFileManagerFromFile(file)
}

func NewRelationalFileManagerFromFile(file *os.File) (*RelationalFileManager, error) {
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

	return &RelationalFileManager{
		file: file,
		data: mmap,
		size: fileSize,

		writeOffset: writeOffset,

		serializer: serializer.NewRawBinarySerializer(),
	}, nil
}

func (rfm *RelationalFileManager) Write(data any) ([]byte, error) {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	bs, err := rfm.serializer.Serialize(data)
	if err != nil {
		return nil, err
	}

	bsLen := uint64(len(bs))
	totalLen := 4 + bsLen

	requiredSize := rfm.writeOffset + totalLen

	// Check if we need to grow
	if requiredSize > rfm.size {
		if err = rfm.grow(requiredSize); err != nil {
			return nil, err
		}
	}

	// Write directly to mmap - no intermediate buffer
	bytesx.PutUint32(rfm.data[rfm.writeOffset:], uint32(bsLen))
	copy(rfm.data[rfm.writeOffset+4:], bs)

	newOffset := rfm.writeOffset + totalLen
	if err = partialFlushMmap(rfm.data, newOffset); err != nil {
		return nil, err
	}

	// Update writeOffset
	rfm.writeOffset = newOffset

	return bs, nil
}

func (rfm *RelationalFileManager) Read() ([]byte, error) {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	return rfm.read()
}

func (rfm *RelationalFileManager) read() ([]byte, error) {
	if rfm.readOffset >= rfm.writeOffset {
		return nil, io.EOF
	}

	// Ensure we can read the length header
	if rfm.readOffset+4 > rfm.writeOffset {
		return nil, errorsx.New("corrupted data: incomplete length header")
	}

	bsLen := bytesx.Uint32(rfm.data[rfm.readOffset : rfm.readOffset+4])
	if bsLen > maxRecordSize {
		return nil, errorsx.Errorf("corrupted: invalid size %d at offset %d", bsLen, rfm.readOffset)
	}
	rfm.readOffset += 4

	// Ensure we can read the full payload
	if rfm.readOffset+uint64(bsLen) > rfm.writeOffset {
		return nil, errorsx.New("corrupted data: incomplete payload")
	}

	payload := make([]byte, bsLen)
	copy(payload, rfm.data[rfm.readOffset:rfm.readOffset+uint64(bsLen)])
	rfm.readOffset += uint64(bsLen)

	return payload, nil
}

func (rfm *RelationalFileManager) ReadAll() ([][]byte, error) {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	var records [][]byte
	offset := uint64(0)

	for offset < rfm.writeOffset {
		if offset+4 > rfm.writeOffset {
			break
		}

		bsLen := bytesx.Uint32(rfm.data[offset : offset+4])
		if bsLen == 0 || bsLen > maxRecordSize {
			return records, errorsx.Errorf("corrupted: invalid size %d at offset %d", bsLen, offset)
		}

		if offset+4+uint64(bsLen) > rfm.writeOffset {
			return records, errorsx.Errorf("corrupted: incomplete record at offset %d", offset)
		}

		payload := make([]byte, bsLen)
		copy(payload, rfm.data[offset+4:offset+4+uint64(bsLen)])
		records = append(records, payload)

		offset += 4 + uint64(bsLen)
	}

	return records, nil
}

// grow must be called with lock held
func (rfm *RelationalFileManager) grow(minimumRequiredSize uint64) error {
	// Round up to page size or some reasonable increment
	//pageSize := int64(os.Getpagesize())
	//newSize = ((newSize + pageSize - 1) / pageSize) * pageSize
	newSize := rfm.size * 2
	if newSize < minimumRequiredSize {
		newSize = minimumRequiredSize * 2
	}

	// 1. Unmap
	if err := unix.Munmap(rfm.data); err != nil {
		return errorsx.Wrap(err, "unmap failed")
	}

	// 2. Truncate
	if err := rfm.file.Truncate(int64(newSize)); err != nil {
		return errorsx.Wrap(err, "truncate failed")
	}

	// 3. Remap
	newMmap, err := unix.Mmap(
		int(rfm.file.Fd()),
		0,
		int(newSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		return errorsx.Wrap(err, "remap failed")
	}

	rfm.data = newMmap
	rfm.size = newSize
	return nil
}

func (rfm *RelationalFileManager) Sync() error {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	return partialFlushMmap(rfm.data, rfm.writeOffset)
}

// Reset reading position to start
func (rfm *RelationalFileManager) Reset() {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	rfm.readOffset = 0
}

// Seek to specific record index
func (rfm *RelationalFileManager) Seek(recordIndex uint64) error {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	var offset uint64
	var index uint64

	for offset < rfm.writeOffset && index < recordIndex {
		if offset+4 > rfm.writeOffset {
			// Reached end of data - record doesn't exist
			return errorsx.Errorf("record %d not found (only %d records exist)",
				recordIndex, index)
		}

		length := bytesx.Uint32(rfm.data[offset : offset+4])

		// Validate we can read the full record
		recordSize := 4 + uint64(length)
		if offset+recordSize > rfm.writeOffset {
			// Incomplete record = actual corruption
			return errorsx.Errorf("corrupted data at offset %d: incomplete record", offset)
		}

		offset += recordSize
		index++
	}

	if index != recordIndex {
		return errorsx.Errorf("record index %d not found", recordIndex)
	}

	rfm.readOffset = offset
	return nil
}

// Count total records
func (rfm *RelationalFileManager) Count() (uint64, error) {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	var count uint64
	var offset uint64

	for offset < rfm.writeOffset {
		if offset+4 > rfm.writeOffset {
			// Reached end of valid data
			break
		}

		length := bytesx.Uint32(rfm.data[offset : offset+4])
		recordSize := 4 + uint64(length)

		// Validate complete record
		if offset+recordSize > rfm.writeOffset {
			// Incomplete record - stop counting
			return count, errorsx.Errorf("corrupted: incomplete record at offset %d", offset)
		}

		offset += recordSize
		count++
	}

	return count, nil
}

// Close unmaps and closes the file queue.
func (rfm *RelationalFileManager) Close() error {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	if osx.IsFileClosed(rfm.file) {
		return nil
	}

	if err := partialFlushMmap(rfm.data, rfm.writeOffset); err != nil {
		return errorsx.Wrap(err, "flush failed")
	}

	if err := unix.Munmap(rfm.data); err != nil {
		_ = rfm.file.Close()
		return errorsx.Wrap(err, "munmap failed")
	}

	// Truncate to actual written size
	if err := rfm.file.Truncate(int64(rfm.writeOffset)); err != nil {
		_ = rfm.file.Close()
		return errorsx.Wrap(err, "truncate failed")
	}

	return rfm.file.Close()
}

// FindResult is the result definition of FindInFile.
// As for reference:
// bs is the found bs payload.
// index is the record index, starting at 0.
// upTo and from, giving the following []byte{}...
// []byte{...upTo, ..., from...}
// example: upTo = 105; from = 120
// so when copying, you:
// target := make([]byte, len(data)-(from-upTo))
// copy(target, data[:upTo]); copy(target, data[from:])
// So,
// upTo is the offset until the found record.
// from is the offset from the from record onwards.
type FindResult struct {
	bs    []byte
	index uint64
	upTo  uint64
	from  uint64
}

func (rfm *RelationalFileManager) Find(
	ctx context.Context,
	key []byte,
) (
	result FindResult,
	err error,
) {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	return rfm.findInFile(ctx, key)
}

func (rfm *RelationalFileManager) findInFile(
	ctx context.Context,
	key []byte,
) (
	result FindResult,
	err error,
) {
	rfm.readOffset = 0

	var found bool
	for {
		if err = ctx.Err(); err != nil {
			return
		}

		var bs []byte
		bs, err = rfm.read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return FindResult{}, errorsx.Wrapf(err, "error reading %s file", rfm.file.Name())
		}

		if bytes.Contains(bs, key) {
			found = true
			result.bs = bs
			result.from = rfm.readOffset
			result.upTo = result.from - uint64(len(bs)+4)

			break
		}

		result.index++
	}

	if !found {
		return FindResult{}, fileiomodels.KeyNotFoundError.Errorf("key '%s' not found", key)
	}

	return
}

func (rfm *RelationalFileManager) GetByIndex(
	ctx context.Context,
	index uint64,
) (FindResult, error) {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	return rfm.getByIndex(ctx, index)
}

func (rfm *RelationalFileManager) getByIndex(
	ctx context.Context,
	index uint64,
) (result FindResult, err error) {
	rfm.readOffset = 0

	var found bool
	var count uint64
	for {
		if err = ctx.Err(); err != nil {
			return
		}

		var bs []byte
		bs, err = rfm.read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return FindResult{}, errorsx.Wrapf(err, "error reading %s file", rfm.file.Name())
		}

		if count == index {
			found = true
			result.bs = bs
			result.index = count
			result.from = rfm.readOffset
			result.upTo = result.from - uint64(len(bs)+4)

			break
		}

		count++
	}
	if !found {
		return FindResult{}, errorsx.Errorf("record index %d not found in %s file", index, rfm.file.Name())
	}

	return
}

func (rfm *RelationalFileManager) UpsertByKey(
	ctx context.Context,
	key []byte,
	data any,
) ([]byte, error) {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	findResult, err := rfm.findInFile(ctx, key)
	if err != nil {
		return nil, err
	}

	return rfm.upsertData(ctx, findResult, data)
}

func (rfm *RelationalFileManager) UpsertByIndex(
	ctx context.Context,
	index uint64,
	data any,
) ([]byte, error) {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	findResult, err := rfm.getByIndex(ctx, index)
	if err != nil {
		return nil, err
	}

	return rfm.upsertData(ctx, findResult, data)
}

func (rfm *RelationalFileManager) upsertData(
	ctx context.Context,
	result FindResult,
	data any,
) ([]byte, error) {
	bs, err := rfm.serializer.Serialize(data)
	if err != nil {
		return nil, err
	}

	newRecordLen := uint64(4 + len(bs))
	oldRecordLen := result.from - result.upTo

	// Calculate new writeOffset
	newWriteOffset := rfm.writeOffset - oldRecordLen + newRecordLen

	// Check if we need to grow
	if newWriteOffset > rfm.size {
		if err = rfm.grow(newWriteOffset); err != nil {
			return nil, err
		}
	}

	// If sizes differ, shift data after the record
	if newRecordLen != oldRecordLen {
		copy(rfm.data[result.upTo+newRecordLen:], rfm.data[result.from:rfm.writeOffset])
	}

	// Write new record
	bytesx.PutUint32(rfm.data[result.upTo:], uint32(len(bs)))
	copy(rfm.data[result.upTo+4:], bs)

	// If shrinking, clear freed tail
	if newWriteOffset < rfm.writeOffset {
		clear(rfm.data[newWriteOffset:rfm.writeOffset])
		// Flush to old offset to persist cleared bytes
		if err = partialFlushMmap(rfm.data, rfm.writeOffset); err != nil {
			return nil, err
		}
	} else {
		// Flush to new offset
		if err = partialFlushMmap(rfm.data, newWriteOffset); err != nil {
			return nil, err
		}
	}

	rfm.writeOffset = newWriteOffset
	rfm.readOffset = 0

	return bs, nil
}

// DeleteByKeyResult is the result definition of DeleteByKey.
// As for reference:
// bs is the found bs payload.
// index is the record index, starting at 0.
// upTo and from, giving the following []byte{}...
// []byte{...upTo, ..., from...}
// example: upTo = 105; from = 120
// so when copying, you:
// target := make([]byte, len(data)-(from-upTo))
// copy(target, data[:upTo]); copy(target, data[from:])
// So,
// upTo is the offset until the found record.
// from is the offset from the from record onwards.
type DeleteByKeyResult struct {
	bs    []byte
	index uint64
	upTo  uint64
	from  uint64
}

func (rfm *RelationalFileManager) DeleteByKey(
	ctx context.Context,
	key []byte,
) (DeleteByKeyResult, error) {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	result, err := rfm.findInFile(ctx, key)
	if err != nil {
		return DeleteByKeyResult{}, err
	}

	return rfm.deleteByResult(ctx, result)
}

func (rfm *RelationalFileManager) deleteByResult(
	_ context.Context,
	result FindResult,
) (DeleteByKeyResult, error) {
	// delete found result item from data
	// []byte{...upTo, ..., from...}
	copy(rfm.data[result.upTo:], rfm.data[result.from:])

	newSize := rfm.writeOffset - (result.from - result.upTo)
	clear(rfm.data[newSize:rfm.writeOffset])
	if err := partialFlushMmap(rfm.data, rfm.writeOffset); err != nil {
		return DeleteByKeyResult{}, err
	}

	rfm.writeOffset = newSize
	rfm.readOffset = 0

	return DeleteByKeyResult{
		bs:    result.bs,
		index: result.index,
		upTo:  result.upTo,
		from:  result.from,
	}, nil
}

// DeleteByIndex deletes and return the deleted []byte.
func (rfm *RelationalFileManager) DeleteByIndex(
	ctx context.Context,
	index uint64,
) ([]byte, error) {
	rfm.mtx.Lock()
	defer rfm.mtx.Unlock()

	return rfm.deleteByIndex(ctx, index)
}

func (rfm *RelationalFileManager) deleteByIndex(
	ctx context.Context,
	index uint64,
) (bs []byte, err error) {
	rfm.readOffset = 0

	var found bool
	var count uint64
	var result FindResult
	for {
		if err = ctx.Err(); err != nil {
			return
		}

		bs, err = rfm.read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, errorsx.Wrapf(err, "error reading %s file", rfm.file.Name())
		}

		if count == index {
			found = true
			result.bs = bs
			result.index = count
			result.from = rfm.readOffset
			result.upTo = result.from - uint64(len(bs)+4)

			break
		}

		count++
	}
	if !found {
		return nil, errorsx.Errorf("record index %d not found in %s file", index, rfm.file.Name())
	}

	if _, err = rfm.deleteByResult(ctx, result); err != nil {
		return nil, err
	}

	return result.bs, nil
}
