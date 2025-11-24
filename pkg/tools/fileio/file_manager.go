package fileio

import (
	"context"
	"io"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	serializermodels "gitlab.com/pietroski-software-company/golang/devex/serializer/models"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesx"
)

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

func (e *FileManager) ReadRelationalRow(
	_ context.Context,
	file *os.File,
) ([]byte, error) {
	rawRowSize := make([]byte, 4)
	if _, err := file.Read(rawRowSize); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}

		return nil, errorsx.Wrapf(err, "error reading row lenght from %s file", file.Name())
	}

	rowSize := bytesx.Uint32(rawRowSize)
	row := make([]byte, rowSize)
	if _, err := file.Read(row); err != nil {
		if err != io.EOF {
			return nil, io.EOF
		}

		return nil, errorsx.Wrapf(err, "error reading row from %s file", file.Name())
	}

	return row, nil
}

func (e *FileManager) ReadFile(
	_ context.Context,
	file *os.File,
) ([]byte, error) {
	bsLen := make([]byte, 4)
	if _, err := io.ReadFull(file, bsLen); err != nil {
		return nil, errorsx.Wrapf(err, "error reading file lenght from %s file", file.Name())
	}

	fileSize := bytesx.Uint32(bsLen)
	bs := make([]byte, fileSize)
	if _, err := io.ReadFull(file, bs); err != nil {
		return nil, errorsx.Wrapf(err, "error reading file content from %s file", file.Name())
	}

	return bs, nil
}

func (e *FileManager) WriteToFile(
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
