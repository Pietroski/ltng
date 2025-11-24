package mmap

import (
	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesx"
	"golang.org/x/sys/unix"
)

const (
	initialSize   = 1 << 10  // 1024B // 1 << 20   // 1MB
	maxRecordSize = 10 << 20 // 100MB
)

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

func flushMmap(mmap []byte) error {
	return unix.Msync(mmap, unix.MS_SYNC)
}

func partialFlushMmap(mmap []byte, upTo uint64) error {
	return unix.Msync(mmap[:upTo], unix.MS_SYNC)
}

func scanForValidDataEnd(data []byte, maxSize uint64) (uint64, error) {
	var offset uint64

	for offset+4 <= maxSize {
		// Read the 4-byte length prefix
		length := bytesx.Uint32(data[offset : offset+4])

		// Zero length means we've hit unwritten/cleared space
		if length == 0 {
			break
		}

		{
			// Sanity check: reject absurdly large lengths
			// 100MB per record seems reasonable max
			if length > maxRecordSize {
				clear(data[offset:])
				return offset, flushMmap(data[offset:])
			}
		}

		totalRecordSize := uint64(4 + length)

		// Validate we have the full record
		if offset+totalRecordSize > maxSize {
			// Incomplete record - corruption or file was grown mid-write

			clear(data[offset:])

			return offset, flushMmap(data[offset:])
		}

		// Move to next record
		offset += totalRecordSize
	}

	return offset, nil
}

// readDataSize reads the 4-byte length prefix to determine actual data size
func readDataSize(
	data []byte,
	maxSize uint64,
) (uint64, error) {
	if maxSize == 0 {
		return 0, nil
	}

	// Read length prefix
	length := bytesx.Uint32(data[0:4])
	if length == 0 {
		return 0, nil // Empty file
	}

	// Sanity check
	if length > maxRecordSize {
		return 0, errorsx.Errorf("corrupted file: length %d exceeds max %d", length, maxRecordSize)
	}

	totalSize := uint64(4 + length)
	if totalSize > maxSize {
		return 0, errorsx.Errorf("corrupted file: data size %d exceeds file size %d", totalSize, maxSize)
	}

	return totalSize, nil
}

func validateData(
	data []byte,
	dataSize uint64,
	size uint64,
) error {
	if dataSize == 0 {
		return nil // Empty is valid
	}

	if dataSize < 4 {
		return errorsx.New("corrupted: insufficient data")
	}

	length := bytesx.Uint32(data[0:4])
	expectedSize := uint64(4 + length)

	if expectedSize != dataSize {
		return errorsx.Errorf("corrupted: length mismatch (expected %d, got %d)",
			expectedSize, dataSize)
	}

	if expectedSize > size {
		return errorsx.New("corrupted: data exceeds file size")
	}

	return nil
}
