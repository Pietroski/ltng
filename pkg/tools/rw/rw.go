package rw

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	serializermodels "gitlab.com/pietroski-software-company/golang/devex/serializer/models"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesx"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
)

// TODO: Extract to two separate libs: filemanager & fileio

// FileManager is responsible for file operations.
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

// OpenCreateFile opens or creates a file if it does not exist.
func (e *FileManager) OpenCreateFile(
	_ context.Context,
	filePath string,
) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, ltngdata.DBFileRW)
	if err != nil {
		return nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	return file, nil
}

// OpenTruncateOrCreateFile opens and truncates or creates a file.
func (e *FileManager) OpenTruncateOrCreateFile(
	_ context.Context,
	filePath string,
) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_TRUNC|os.O_CREATE, ltngdata.DBFileRW)
	if err != nil {
		return nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	return file, nil
}

// CreateFileIfNotExists creates a file if it does not exist.
func (e *FileManager) CreateFileIfNotExists(
	_ context.Context,
	filePath string,
) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, ltngdata.DBFileRW)
	if err != nil {
		return nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	return file, nil
}

// OpenReadWholeFile opens and reads the whole file.
// This method is recommended to be used with known small to-be files.
func (e *FileManager) OpenReadWholeFile(
	ctx context.Context,
	filePath string,
) ([]byte, *os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, ltngdata.DBFileRW)
	if err != nil {
		return nil, nil, errorsx.Wrapf(err, "error opening %s file", filePath)
	}

	if _, err = file.Seek(0, 0); err != nil {
		return nil, nil, errorsx.Wrapf(err, "error seeking %s file", filePath)
	}

	bs, err := e.ReadAll(ctx, file)
	if err != nil {
		return nil, nil, errorsx.Wrapf(err, "error reading %s file", filePath)
	}

	if _, err = file.Seek(0, 0); err != nil {
		return nil, nil, errorsx.Wrapf(err, "error seeking %s file", filePath)
	}

	return bs, file, nil
}

// OpenFile opens a file and it errors out if it does not exist.
func (e *FileManager) OpenFile(
	_ context.Context, filePath string,
) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, ltngenginemodels.DBFileOp)
	if err != nil {
		return nil, errorsx.Wrapf(err, "error opening %s file", filePath)
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
		return nil, errorsx.Wrapf(err, "error stat %s file", file.Name())
	}
	size := stat.Size()

	reader := bufio.NewReaderSize(file, int(size))

	buf := make([]byte, size)
	if _, err = reader.Read(buf); err != nil {
		return nil, errorsx.Wrapf(err, "error reading %s file", file.Name())
	}

	//row, err := io.ReadAll(file)
	//if err != nil {
	//	return nil, err
	//}

	// Seek back to start
	_, err = file.Seek(0, 0)
	if err != nil {
		return nil, errorsx.Wrapf(err, "error seeking/resetting %s file", file.Name())
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

		return nil, errorsx.Wrapf(err, "error reading row lenght from %s file", file.Name())
	}

	rowSize := bytesx.Uint32(rawRowSize)
	row := make([]byte, rowSize)
	_, err = reader.Read(row)
	//_, err = file.Read(row)
	if err != nil {
		if err != io.EOF {
			return nil, io.EOF
		}

		return nil, errorsx.Wrapf(err, "error reading row from %s file", file.Name())
	}

	return row, nil
}

// #####################################################################################################################

func (e *FileManager) WriteToFile(
	ctx context.Context,
	file *os.File,
	data interface{},
) ([]byte, error) {
	bs, err := e.serializer.Serialize(data)
	if err != nil {
		return nil, errorsx.Wrapf(err, "failed to serialize data - %s", file.Name())
	}

	return e.writeToFile(ctx, file, bs)
}

func (e *FileManager) WriteToRelationalFile(
	ctx context.Context,
	file *os.File,
	data interface{},
) ([]byte, error) {
	bs, err := e.serializer.Serialize(data)
	if err != nil {
		return nil, errorsx.Wrapf(err, "failed to serialize data - %s", file.Name())
	}

	bsLen := bytesx.AddUint32(uint32(len(bs)))
	if _, err = file.Write(bsLen); err != nil {
		return nil, errorsx.Wrapf(err, "failed to write data length to file - %s", file.Name())
	}

	return e.writeToFile(ctx, file, bs)
}

func (e *FileManager) writeToFile(
	_ context.Context,
	file *os.File,
	data []byte,
) ([]byte, error) {
	if _, err := file.Write(data); err != nil {
		return nil, errorsx.Wrapf(err, "failed to write data to file - %s", file.Name())
	}

	if err := file.Sync(); err != nil {
		return nil, errorsx.Wrapf(err, "failed to sync file - %s", file.Name())
	}

	return data, nil
}

// #####################################################################################################################

func IsFileClosed(file *os.File) bool {
	if file == nil {
		return true
	}

	_, err := file.Stat()
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
		header     *ltngdata.Header
		headerSize uint32
		hasHeader  bool
		RawHeader  []byte
		Cursor     uint32
		reader     *bufio.Reader
	}
)

func NewFileReader(
	ctx context.Context, fi *ltngdata.FileInfo, readHeader bool,
) (*FileReader, error) {
	fr := &FileReader{
		file:       fi.File,
		headerSize: fi.HeaderSize,
		reader:     bufio.NewReader(fi.File),
	}
	if fi.FileData != nil {
		fr.header = fi.FileData.Header
	}

	if _, err := fr.file.Seek(0, io.SeekStart); err != nil {
		return nil, errorsx.Wrapf(err, "failed to seek file - %s", fr.file.Name())
	}

	if !readHeader {
		return fr, nil
	}

	bs, err := fr.Read(ctx)
	if err != nil {
		return nil, errorsx.Wrapf(err, "failed to read header - %s", fr.file.Name())
	}

	fr.headerSize = uint32(len(bs))
	fr.hasHeader = true
	fr.RawHeader = bs

	return fr, nil
}

func (fr *FileReader) ReadAll(_ context.Context) (bs []byte, err error) {
	fileStats, err := fr.file.Stat()
	if err != nil {
		return nil, errorsx.Wrapf(err, "failed to stat file - %s", fr.file.Name())
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
		return errorsx.Wrapf(err, "failed to seek file - %s", fr.file.Name())
	}

	fr.Cursor = uint32(cursor)

	return nil
}

func (fr *FileReader) Yield() uint32 {
	return fr.Cursor
}

func (fr *FileReader) Close() error {
	return fr.file.Close()
}

func (fr *FileReader) FindInFile(
	ctx context.Context,
	key []byte,
) (
	upTo, from uint32,
	err error,
) {
	var found bool
	for {
		var bs []byte
		bs, err = fr.Read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return 0, 0, errorsx.Wrapf(err,
				"error reading %s file", fr.file.Name())
		}

		if bytes.Contains(bs, key) {
			found = true
			from = fr.Yield()
			upTo = from - uint32(len(bs)+4)
			break
		}
	}

	if !found {
		return 0, 0, KeyNotFoundError.Errorf("key '%s' not found", key)
	}

	return
}

var KeyNotFoundError = errorsx.New("key not found")
