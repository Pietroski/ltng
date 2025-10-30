package rw

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	serializermodels "gitlab.com/pietroski-software-company/golang/devex/serializer/models"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesx"
)

type FileManager struct {
	serializer serializermodels.Serializer
}

func NewFileManager(_ context.Context, opts ...options.Option) *FileManager {
	fm := &FileManager{
		serializer: serializer.NewRawBinarySerializer(),
	}
	options.ApplyOptions(fm, opts...)

	return fm
}

func (e *FileManager) OpenCreateTruncatedFile(
	_ context.Context,
	filePath string,
) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, ltngenginemodels.DBFileOp) // |os.O_EXCL
	if err != nil {
		return nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	if err = file.Truncate(0); err != nil {
		return nil, fmt.Errorf("error truncating %s file: %v", filePath, err)
	}

	return file, nil
}

func (e *FileManager) OpenReadWholeFile(
	ctx context.Context,
	filePath string,
) ([]byte, *os.File, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("file does not exist: %s: %v", filePath, err)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, ltngenginemodels.DBFileOp)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	bs, err := e.ReadAll(ctx, file)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading %s file: %v", filePath, err)
	}

	if _, err = file.Seek(0, 0); err != nil {
		return nil, nil, fmt.Errorf("error seeking %s file: %v", filePath, err)
	}

	return bs, file, nil
}

func (e *FileManager) OpenFile(
	_ context.Context, filePath string,
) (*os.File, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("file does not exist: %v", err)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, ltngenginemodels.DBFileOp)
	if err != nil {
		return nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	return file, nil
}

// #####################################################################################################################

func (e *FileManager) ReadAll(
	_ context.Context,
	file *os.File,
) ([]byte, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := stat.Size()

	reader := bufio.NewReaderSize(file, int(size))

	buf := make([]byte, size)
	if _, err = reader.Read(buf); err != nil {
		return nil, err
	}

	//row, err := io.ReadAll(file)
	//if err != nil {
	//	return nil, err
	//}

	// Seek back to start
	_, err = file.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("error seeking/resetting file from %s: %v", file.Name(), err)
	}

	return buf, nil
}

func (e *FileManager) ReadRelationalRow(
	_ context.Context,
	file *os.File,
) ([]byte, error) {
	reader := bufio.NewReader(file)
	rawRowSize := make([]byte, 4)
	_, err := reader.Read(rawRowSize)
	//rawRowSize := make([]byte, 4)
	//_, err := file.Read(rawRowSize)
	if err != nil {
		if err != io.EOF {
			return nil, io.EOF
		}

		return nil, fmt.Errorf("error reading row lenght from file from %s: %v", file.Name(), err)
	}

	rowSize := bytesx.Uint32(rawRowSize)
	row := make([]byte, rowSize)
	_, err = reader.Read(row)
	//_, err = file.Read(row)
	if err != nil {
		if err != io.EOF {
			return nil, io.EOF
		}

		return nil, fmt.Errorf("error reading raw file from %s: %v", file.Name(), err)
	}

	return row, nil
}

func (e *FileManager) GetRelationalFileInfo(
	ctx context.Context, file *os.File,
) (*ltngenginemodels.FileInfo, error) {
	bs, err := e.ReadRelationalRow(ctx, file)
	if err != nil {
		return nil, err
	}

	var fileData ltngenginemodels.FileData
	if err = e.serializer.Deserialize(bs, &fileData); err != nil {
		return nil, fmt.Errorf("failed to deserialize store stats header from relational file: %v", err)
	}
	fileData.Header.StoreInfo.LastOpenedAt = time.Now().UTC().Unix()

	relationalFileInfo := &ltngenginemodels.FileInfo{
		File:       file,
		FileData:   &fileData,
		HeaderSize: uint32(len(bs)),
		DataSize:   uint32(len(fileData.Data)),
	}

	return relationalFileInfo, nil
}

// #####################################################################################################################

func (e *FileManager) WriteToFile(
	ctx context.Context,
	file *os.File,
	data interface{},
) ([]byte, error) {
	bs, err := e.serializer.Serialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize db info - %s | err: %v", file.Name(), err)
	}

	return e.WriteAndSeek(ctx, file, bs)
}

func (e *FileManager) WriteToRelationalFile(
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

	return e.WriteAndSeek(ctx, file, bs)
}

func (e *FileManager) WriteAndSeek(
	_ context.Context,
	file *os.File,
	data []byte,
) ([]byte, error) {
	writer := bufio.NewWriter(file)
	if _, err := writer.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write data to file - %s | err: %v", file.Name(), err)
	}

	if err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to sync file - %s | err: %v", file.Name(), err)
	}

	// set the file ready to be read
	if _, err := file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to reset file cursor - %s | err: %v", file.Name(), err)
	}

	return data, nil
}

func (e *FileManager) WriteToRelationalFileWithNoSeek(
	_ context.Context,
	file *os.File,
	data interface{},
) ([]byte, error) {
	bs, err := e.serializer.Serialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize data - %s | err: %v", file.Name(), err)
	}

	bsLen := bytesx.AddUint32(uint32(len(bs)))

	writer := bufio.NewWriter(file)
	if _, err = writer.Write(bsLen); err != nil {
		return nil, fmt.Errorf("failed to write data length to file - %s | err: %v", file.Name(), err)
	}

	if _, err = writer.Write(bs); err != nil {
		return nil, fmt.Errorf("failed to write data to file - %s | err: %v", file.Name(), err)
	}

	if err = writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to sync file - %s | err: %v", file.Name(), err)
	}

	return bs, nil
}

// #####################################################################################################################

func IsFileClosed(file *os.File) bool {
	if file == nil {
		return true
	}

	_, err := file.Stat()
	// fmt.Printf("isFileClosed: err: %v\n", err)
	return errors.Is(err, os.ErrClosed)
}

// #####################################################################################################################

type (
	FileWriter struct {
		file       *os.File
		serializer serializermodels.Serializer
	}
)

func newFileWriter(
	ctx context.Context, fi *ltngenginemodels.FileInfo,
) (*FileWriter, error) {
	return &FileWriter{
		file:       fi.File,
		serializer: serializer.NewRawBinarySerializer(),
	}, nil
}

// #####################################################################################################################

type (
	FileReader struct {
		file       *os.File
		header     *ltngenginemodels.Header
		headerSize uint32
		hasHeader  bool
		RawHeader  []byte
		Cursor     uint32
		reader     *bufio.Reader
	}
)

func NewFileReader(
	ctx context.Context, fi *ltngenginemodels.FileInfo, readHeader bool,
) (*FileReader, error) {
	fr := &FileReader{
		file:       fi.File,
		headerSize: fi.HeaderSize,
		reader:     bufio.NewReader(fi.File),
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

	bs, err := fr.Read(ctx)
	if err != nil {
		return nil, err
	}

	fr.headerSize = uint32(len(bs))
	fr.hasHeader = true
	fr.RawHeader = bs

	return fr, nil
}

func (fr *FileReader) ReadAll(_ context.Context) (bs []byte, err error) {
	fileStats, err := fr.file.Stat()
	if err != nil {
		return nil, err
	}

	fr.Cursor = uint32(fileStats.Size())
	//bs = make([]byte, fr.cursor)
	//_, err = fr.reader.Read(bs)
	//if err != nil {
	//	return nil, err
	//}
	//
	//return bs, nil
	return io.ReadAll(fr.file)
}

func (fr *FileReader) Read(_ context.Context) (bs []byte, err error) {
	rawRowSize := make([]byte, 4)
	//_, err = fr.reader.Read(rawRowSize)
	_, err = fr.file.Read(rawRowSize)
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}

		return nil, err
	}
	fr.Cursor += 4

	rowSize := bytesx.Uint32(rawRowSize)
	row := make([]byte, rowSize)
	//_, err = fr.reader.Read(row)
	_, err = fr.file.Read(row)
	if err != nil {
		return nil, err
	}
	fr.Cursor += rowSize

	return row, nil
}

func (fr *FileReader) SetCursor(_ context.Context, cursor uint64) error {
	if _, err := fr.file.Seek(int64(cursor), 0); err != nil {
		return err
	}

	return nil
}

func (fr *FileReader) Yield() uint32 {
	return fr.Cursor
}
