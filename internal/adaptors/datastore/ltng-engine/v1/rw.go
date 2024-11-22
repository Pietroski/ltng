package v1

import (
	"context"
	"fmt"
	"os"
)

func (e *LTNGEngine) openReadFile(
	ctx context.Context,
	filePath string,
) ([]byte, *os.File, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("file does not exist: %v", err)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, dbFileOp)
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
	_ context.Context,
	file *os.File,
) ([]byte, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("error getting file %s stats: %v", file.Name(), err)
	}

	bs := make([]byte, stat.Size())
	_, err = file.Read(bs)
	if err != nil {
		return nil, fmt.Errorf("error reading raw file from %s: %v", file.Name(), err)
	}

	// Seek back to start
	_, err = file.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("error seeking/resetting file from %s: %v", file.Name(), err)
	}

	return bs, nil
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

	_, err = file.Write(bs)
	if err != nil {
		return nil, fmt.Errorf("failed to write db info to file - %s | err: %v", file.Name(), err)
	}

	// set the file ready to be read
	if _, err = file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to reset file cursor - %s | err: %v", file.Name(), err)
	}

	return bs, nil
}
