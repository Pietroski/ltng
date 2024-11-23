package v1

import (
	"context"
	"fmt"
	"io"
	"os"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/tools/bytesx"
)

func (e *LTNGEngine) openReadFile(
	ctx context.Context,
	filePath string,
) ([]byte, *os.File, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("file does not exist: %v", err)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_SYNC, dbFileOp)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	bs, err := e.readFile(ctx, file)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading %s file: %v", filePath, err)
	}

	return bs, file, nil
}

func (e *LTNGEngine) readFile(
	ctx context.Context,
	file *os.File,
) ([]byte, error) {
	row, err := e.read(ctx, file)
	if err != nil {
		return nil, err
	}

	// Seek back to start
	_, err = file.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("error seeking/resetting file from %s: %v", file.Name(), err)
	}

	return row, nil
}

func (e *LTNGEngine) read(
	_ context.Context,
	file *os.File,
) ([]byte, error) {
	rawRowSize := make([]byte, 4)
	_, err := file.Read(rawRowSize)
	if err != nil {
		return nil, fmt.Errorf("error reading row lenght from file from %s: %v", file.Name(), err)
	}

	rowSize := bytesx.Uint32(rawRowSize)
	row := make([]byte, rowSize)
	_, err = file.Read(row)
	if err != nil {
		return nil, fmt.Errorf("error reading raw file from %s: %v", file.Name(), err)
	}

	return row, nil
}

func (e *LTNGEngine) writeToFile(
	_ context.Context,
	file *os.File,
	data interface{},
) ([]byte, error) {
	bs, err := e.serializer.Serialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize db info - %s | err: %v", file.Name(), err)
	}

	bsLen := bytesx.AddUint32(uint32(len(bs)))

	if _, err = file.Write(bsLen); err != nil {
		return nil, fmt.Errorf("failed to write db info lenght to file - %s | err: %v", file.Name(), err)
	}

	if _, err = file.Write(bs); err != nil {
		return nil, fmt.Errorf("failed to write db info to file - %s | err: %v", file.Name(), err)
	}

	// set the file ready to be read
	if _, err = file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to reset file cursor - %s | err: %v", file.Name(), err)
	}

	return bs, nil
}

type (
	fileReader struct {
		file       *os.File
		dbInfo     *DBInfo
		headerSize uint32
		hasHeader  bool
		headerData []byte
		cursor     uint32
	}
)

func newFileReader(
	ctx context.Context, fi *fileInfo,
) (*fileReader, error) {
	fr := &fileReader{
		file:       fi.File,
		dbInfo:     fi.DBInfo,
		headerSize: fi.HeaderSize,
	}

	_, err := fr.file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	bs, err := fr.read(ctx)
	if err != nil {
		return nil, err
	}

	fr.headerSize = uint32(len(bs))
	fr.hasHeader = true
	fr.headerData = bs

	return fr, nil
}

func (fr *fileReader) read(_ context.Context) (bs []byte, err error) {
	defer func() {
		if err != nil {
			_ = fr.file.Close()
		}
	}()

	rawRowSize := make([]byte, 4)
	_, err = fr.file.Read(rawRowSize)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}

		return nil, err
	}
	fr.cursor += 4

	rowSize := bytesx.Uint32(rawRowSize)
	row := make([]byte, rowSize)
	_, err = fr.file.Read(row)
	if err != nil {
		return nil, err
	}
	fr.cursor += rowSize

	return row, nil
}

func (fr *fileReader) yield() uint32 {
	return fr.cursor
}
