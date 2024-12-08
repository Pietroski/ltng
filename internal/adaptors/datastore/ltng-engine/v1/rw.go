package v1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	serializermodels "gitlab.com/pietroski-software-company/devex/golang/serializer/models"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/tools/bytesx"
)

func (e *LTNGEngine) openCreateTruncatedFile(
	_ context.Context,
	filePath string,
) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL|os.O_TRUNC, dbFileOp)
	if err != nil {
		return nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	if err = file.Truncate(0); err != nil {
		return nil, fmt.Errorf("error truncating %s file: %v", filePath, err)
	}

	return file, nil
}

func (e *LTNGEngine) openReadWholeFile(
	ctx context.Context,
	filePath string,
) ([]byte, *os.File, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("file does not exist: %s: %v", filePath, err)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_SYNC, dbFileOp)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	bs, err := e.readAll(ctx, file)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading %s file: %v", filePath, err)
	}

	if _, err = file.Seek(0, 0); err != nil {
		return nil, nil, fmt.Errorf("error seeking %s file: %v", filePath, err)
	}

	return bs, file, nil
}

func (e *LTNGEngine) openFile(
	_ context.Context, filePath string,
) (*os.File, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("file does not exist: %v", err)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_SYNC, dbFileOp)
	if err != nil {
		return nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	return file, nil
}

// #####################################################################################################################

func (e *LTNGEngine) readAll(
	_ context.Context,
	file *os.File,
) ([]byte, error) {
	row, err := io.ReadAll(file)
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

func (e *LTNGEngine) readRelationalRow(
	_ context.Context,
	file *os.File,
) ([]byte, error) {
	rawRowSize := make([]byte, 4)
	_, err := file.Read(rawRowSize)
	if err != nil {
		if err != io.EOF {
			return nil, io.EOF
		}

		return nil, fmt.Errorf("error reading row lenght from file from %s: %v", file.Name(), err)
	}

	rowSize := bytesx.Uint32(rawRowSize)
	row := make([]byte, rowSize)
	_, err = file.Read(row)
	if err != nil {
		if err != io.EOF {
			return nil, io.EOF
		}

		return nil, fmt.Errorf("error reading raw file from %s: %v", file.Name(), err)
	}

	return row, nil
}

func (e *LTNGEngine) getRelationalFileInfo(
	ctx context.Context, file *os.File,
) (*fileInfo, error) {
	bs, err := e.readRelationalRow(ctx, file)
	if err != nil {
		return nil, err
	}

	var fileData FileData
	if err = e.serializer.Deserialize(bs, &fileData); err != nil {
		return nil, fmt.Errorf("failed to deserialize store stats header from relational file: %v", err)
	}
	fileData.Header.StoreInfo.LastOpenedAt = time.Now().UTC().Unix()

	relationalFileInfo := &fileInfo{
		File:       file,
		FileData:   &fileData,
		HeaderSize: uint32(len(bs)),
		DataSize:   uint32(len(fileData.Data)),
	}

	return relationalFileInfo, nil
}

// #####################################################################################################################

func (e *LTNGEngine) writeToFile(
	ctx context.Context,
	file *os.File,
	data interface{},
) ([]byte, error) {
	bs, err := e.serializer.Serialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize db info - %s | err: %v", file.Name(), err)
	}

	return e.writeAndSeek(ctx, file, bs)
}

func (e *LTNGEngine) writeToRelationalFile(
	ctx context.Context,
	file *os.File,
	data interface{},
) ([]byte, error) {
	bs, err := e.serializer.Serialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize data - %s | err: %v", file.Name(), err)
	}

	bsLen := bytesx.AddUint32(uint32(len(bs)))
	if _, err = file.Write(bsLen); err != nil {
		return nil, fmt.Errorf("failed to write data length to file - %s | err: %v", file.Name(), err)
	}

	return e.writeAndSeek(ctx, file, bs)
}

func (e *LTNGEngine) writeAndSeek(
	_ context.Context,
	file *os.File,
	data []byte,
) ([]byte, error) {
	if _, err := file.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write data to file - %s | err: %v", file.Name(), err)
	}

	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync file - %s | err: %v", file.Name(), err)
	}

	// set the file ready to be read
	if _, err := file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to reset file cursor - %s | err: %v", file.Name(), err)
	}

	return data, nil
}

func (e *LTNGEngine) writeToRelationalFileWithNoSeek(
	_ context.Context,
	file *os.File,
	data interface{},
) ([]byte, error) {
	bs, err := e.serializer.Serialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize data - %s | err: %v", file.Name(), err)
	}

	bsLen := bytesx.AddUint32(uint32(len(bs)))
	if _, err = file.Write(bsLen); err != nil {
		return nil, fmt.Errorf("failed to write data length to file - %s | err: %v", file.Name(), err)
	}

	if _, err = file.Write(bs); err != nil {
		return nil, fmt.Errorf("failed to write data to file - %s | err: %v", file.Name(), err)
	}

	if err = file.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync file - %s | err: %v", file.Name(), err)
	}

	return bs, nil
}

// #####################################################################################################################

func isFileClosed(file *os.File) bool {
	_, err := file.Stat()
	return errors.Is(err, os.ErrClosed)
}

// #####################################################################################################################

type (
	fileWriter struct {
		file       *os.File
		serializer serializermodels.Serializer
	}
)

func newFileWriter(
	ctx context.Context, fi *fileInfo,
) (*fileWriter, error) {
	return &fileWriter{
		file:       fi.File,
		serializer: serializer.NewRawBinarySerializer(),
	}, nil
}

// #####################################################################################################################

type (
	fileReader struct {
		file       *os.File
		header     *Header
		headerSize uint32
		hasHeader  bool
		rawHeader  []byte
		cursor     uint32
	}
)

func newFileReader(
	ctx context.Context, fi *fileInfo, readHeader bool,
) (*fileReader, error) {
	fr := &fileReader{
		file:       fi.File,
		headerSize: fi.HeaderSize,
	}
	if fi.FileData != nil {
		fr.header = fi.FileData.Header
	}

	_, err := fr.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	if !readHeader {
		return fr, nil
	}

	bs, err := fr.read(ctx)
	if err != nil {
		return nil, err
	}

	fr.headerSize = uint32(len(bs))
	fr.hasHeader = true
	fr.rawHeader = bs

	return fr, nil
}

func (fr *fileReader) readAll(_ context.Context) (bs []byte, err error) {
	fileStats, err := fr.file.Stat()
	if err != nil {
		return nil, err
	}

	fr.cursor = uint32(fileStats.Size())
	return io.ReadAll(fr.file)
}

func (fr *fileReader) read(_ context.Context) (bs []byte, err error) {
	defer func() {
		if err != nil && err != io.EOF {
			_ = fr.file.Close()
		}
	}()

	rawRowSize := make([]byte, 4)
	_, err = fr.file.Read(rawRowSize)
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
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

func (fr *fileReader) setCursor(_ context.Context, cursor uint64) error {
	if _, err := fr.file.Seek(int64(cursor), 0); err != nil {
		return err
	}

	return nil
}

func (fr *fileReader) yield() uint32 {
	return fr.cursor
}
