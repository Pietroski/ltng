package fileio

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	fileiomodels "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/models"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesx"
)

type (
	FileReader struct {
		file       *os.File
		RawHeader  []byte
		headerSize uint32
		hasHeader  bool
		cursor     uint32
		reader     *bufio.Reader
	}
)

func NewFileReader(
	ctx context.Context,
	file *os.File,
	readHeader bool,
) (*FileReader, error) {
	fr := &FileReader{
		file:   file,
		reader: bufio.NewReader(file),
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

	fr.cursor = uint32(fileStats.Size())

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
	fr.cursor += 4

	rowSize := bytesx.Uint32(rawRowSize)
	row := make([]byte, rowSize)
	//_, err = fr.reader.Read(row)
	_, err = fr.file.Read(row)
	if err != nil {
		return nil, err
	}
	fr.cursor += rowSize

	return row, nil
}

func (fr *FileReader) SetCursor(_ context.Context, cursor uint64) error {
	if _, err := fr.file.Seek(int64(cursor), 0); err != nil {
		return errorsx.Wrapf(err, "failed to seek file - %s", fr.file.Name())
	}

	fr.cursor = uint32(cursor)

	return nil
}

func (fr *FileReader) Yield() uint32 {
	return fr.cursor
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
		return 0, 0, fileiomodels.KeyNotFoundError.Errorf("key '%s' not found", key)
	}

	return
}
