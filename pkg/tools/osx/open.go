package osx

import (
	"errors"
	"fmt"
	"io"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
)

const (
	FileExec  = 0700
	FileRW    = 0700
	FileWrite = 0400
	FileRead  = 0200
)

// OpenCreateFile opens or creates a file if it does not exist.
func OpenCreateFile(filePath string) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, FileRW)
	if err != nil {
		return nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	return file, nil
}

// OpenTruncateOrCreateFile opens and truncates or creates a file.
func OpenTruncateOrCreateFile(filePath string) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_TRUNC|os.O_CREATE, FileRW)
	if err != nil {
		return nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	return file, nil
}

// OpenAppendOrCreateFile opens and appends or creates a file.
func OpenAppendOrCreateFile(filePath string) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, FileRW)
	if err != nil {
		return nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	return file, nil
}

// OpenAppend opens and appends to a file.
func OpenAppend(filePath string) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, FileRW)
	if err != nil {
		return nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	return file, nil
}

// CreateFileIfNotExists creates a file if it does not exist.
func CreateFileIfNotExists(filePath string) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, FileRW)
	if err != nil {
		return nil, fmt.Errorf("error opening %s file: %v", filePath, err)
	}

	return file, nil
}

// OpenReadWholeFile opens and reads the whole file.
// This method is recommended to be used with known small to-be files.
func OpenReadWholeFile(filePath string) ([]byte, *os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, FileRW)
	if err != nil {
		return nil, nil, errorsx.Wrapf(err, "error opening %s file", filePath)
	}

	bs, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, errorsx.Wrapf(err, "error reading %s file", filePath)
	}

	return bs, file, nil
}

// OpenFile opens a file and it errors out if it does not exist.
func OpenFile(filePath string) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, FileRW)
	if err != nil {
		return nil, errorsx.Wrapf(err, "error opening %s file", filePath)
	}

	return file, nil
}

func IsFileClosed(file *os.File) bool {
	if file == nil {
		return true
	}

	_, err := file.Stat()
	return errors.Is(err, os.ErrClosed)
}
