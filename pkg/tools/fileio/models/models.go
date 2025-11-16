package fileiomodels

import (
	"path/filepath"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
)

const (
	FileExt  = ".ptk"
	BsSep    = "!|ltng|!"
	InnerSep = "-"

	Generic   = "generic"
	Internal  = "internal"
	Temporary = "temporary"

	ActionQueue = "action_queue"
)

const (
	FileQueueBasePath = ".ltngfq" // ".ltng_file_queue"

	FileQueueMmapVersion = "mmapversion"
	FileQueueFileVersion = "fileversion"
	FileQueueV2          = "v2"

	FQ           = "fq"
	FileQueueKey = "file-queue"

	GenericFileQueueFileName = FQ + FileExt
)

var (
	GenericFileQueueFilePath = filepath.Join(Internal, ActionQueue, Generic)
)

func GetFileQueuePath(version, path string) string {
	return filepath.Join(FileQueueBasePath, version, path)
}

func GetFileQueueFilePath(version, path, filename string) string {
	return filepath.Join(FileQueueBasePath, version, path, filename+FileExt)
}

func GetTemporaryFileQueuePath(version, path string) string {
	return filepath.Join(FileQueueBasePath, version, Temporary, path)
}

func GetTemporaryFileQueueFilePath(version, path, filename string) string {
	return filepath.Join(FileQueueBasePath, version, Temporary, path, filename+FileExt)
}

var (
	KeyNotFoundError   = errorsx.New("key not found")
	ItemNotFoundError  = errorsx.New("item not found")
	IndexNotFoundError = errorsx.New("index not found")
)
