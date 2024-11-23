package v1

import (
	"os"
	"sync"

	serializer_models "gitlab.com/pietroski-software-company/devex/golang/serializer/models"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/tools/lock"
)

type (
	LTNGEngine struct {
		opMtx            *lock.EngineLock
		mtx              *sync.Mutex
		serializer       serializer_models.Serializer
		fileStoreMapping map[string]*fileInfo
	}

	fileInfo struct {
		DBInfo     *DBInfo
		File       *os.File
		HeaderSize uint32 `json:"headerSize"`
	}

	DBInfo struct {
		Name         string `json:"name,omitempty"`
		Path         string `json:"path,omitempty"`
		CreatedAt    int64  `json:"createdAt"`
		LastOpenedAt int64  `json:"lastOpenedAt"`
	}

	FileData struct {
		FileStats *DBInfo `json:"fileStats"`
		Data      []byte  `json:"data"`
	}
)

func (dbInfo *DBInfo) IndexInfo() *DBInfo {
	return &DBInfo{
		Name:         dbInfo.Name + suffixSep + dbIndexStoreSuffixName,
		Path:         dbInfo.Path + dbIndexStoreSuffixPath,
		CreatedAt:    dbInfo.CreatedAt,
		LastOpenedAt: dbInfo.LastOpenedAt,
	}
}

func (dbInfo *DBInfo) IndexListInfo() *DBInfo {
	return &DBInfo{
		Name:         dbInfo.Name + suffixSep + dbIndexListStoreSuffixName,
		Path:         dbInfo.Path + dbIndexListStoreSuffixPath,
		CreatedAt:    dbInfo.CreatedAt,
		LastOpenedAt: dbInfo.LastOpenedAt,
	}
}

func (dbInfo *DBInfo) RelationalInfo() *DBInfo {
	return &DBInfo{
		Name:         dbInfo.Name + suffixSep + dbRelationalName,
		Path:         dbInfo.Path + dbRelationalPath,
		CreatedAt:    dbInfo.CreatedAt,
		LastOpenedAt: dbInfo.LastOpenedAt,
	}
}

func (dbInfo *DBInfo) TmpRelationalInfo() *DBInfo {
	return &DBInfo{
		Name:         dbInfo.Name + suffixSep + dbRelationalName + suffixSep + tmp,
		Path:         dbInfo.Path + dbRelationalPath + tmpPath,
		CreatedAt:    dbInfo.CreatedAt,
		LastOpenedAt: dbInfo.LastOpenedAt,
	}
}

func (dbInfo *DBInfo) LockName(key string) string {
	return getFileLockName(dbInfo.Name, key)
}

const (
	dbBasePath    = ".db"
	dbBaseVersion = "/v1"
	dbDataPath    = "/stores"
	dbStatsPath   = "/stores/stats"
	sep           = "/"
	ext           = ".ptk"
	all           = "*"
	allExt        = all + ext
	lineBreak     = "\n"
	lb            = "&#;#&" // "?#\r\n\t#?" // bsSeparator = "&#-#&"

	basePath      = dbBasePath + dbBaseVersion
	baseDataPath  = basePath + dbDataPath
	baseStatsPath = basePath + dbStatsPath

	rubbish           = "/rubbish"
	rubbishBasePath   = basePath + rubbish
	dbTmpDelDataPath  = rubbishBasePath + "/del-tmp-store"
	dbTmpDelStatsPath = rubbishBasePath + "/del-tmp-store-stats"

	suffixSep                  = "-"
	dbIndexStoreSuffixName     = "indexed"
	dbIndexStoreSuffixPath     = "/indexed"
	dbIndexListStoreSuffixName = "indexed-list"
	dbIndexListStoreSuffixPath = "/indexed-list"
	dbRelationalName           = "relational"
	dbRelationalPath           = "/relational"
	tmp                        = "tmp"
	tmpPath                    = "/tmp"

	dbManagerName = "ltng-engine-manager"
	dbManagerPath = "internal/ltng-engine/manager"

	dbFilePerm = 0750
	dbFileOp   = 0644
	dbFileRead = 0444
)

var (
	dbManagerInfo = &DBInfo{
		Name: dbManagerName,
		Path: dbManagerPath,
	}
)

type (
	Item struct {
		Key   []byte
		Value []byte
		Error error
	}

	Items []*Item

	IndexOpts struct {
		HasIdx          bool
		ParentKey       []byte
		IndexingKeys    [][]byte
		IndexProperties IndexProperties
	}

	IndexProperties struct {
		IndexDeletionBehaviour IndexDeletionBehaviour
		IndexSearchPattern     IndexSearchPattern
		ListSearchPattern      ListSearchPattern
	}

	DatabaseMetaInfo struct {
		Name string
		Path string
	}
)

func (dbInfo *DatabaseMetaInfo) LockName(key string) string {
	return getFileLockName(dbInfo.Name, key)
}

func (dbInfo *DatabaseMetaInfo) IndexInfo() *DatabaseMetaInfo {
	return &DatabaseMetaInfo{
		Name: dbInfo.Name + suffixSep + dbIndexStoreSuffixName,
		Path: dbInfo.Path + dbIndexStoreSuffixPath,
	}
}

func (dbInfo *DatabaseMetaInfo) IndexListInfo() *DatabaseMetaInfo {
	return &DatabaseMetaInfo{
		Name: dbInfo.Name + suffixSep + dbIndexListStoreSuffixName,
		Path: dbInfo.Path + dbIndexListStoreSuffixPath,
	}
}

func (dbInfo *DatabaseMetaInfo) RelationalInfo() *DatabaseMetaInfo {
	return &DatabaseMetaInfo{
		Name: dbInfo.Name + suffixSep + dbRelationalName,
		Path: dbInfo.Path + dbRelationalPath,
	}
}

func (dbInfo *DatabaseMetaInfo) TmpRelationalInfo() *DatabaseMetaInfo {
	return &DatabaseMetaInfo{
		Name: dbInfo.Name + suffixSep + dbRelationalName + suffixSep + tmp,
		Path: dbInfo.Path + dbRelationalPath + tmpPath,
	}
}

type IndexDeletionBehaviour int

const (
	None IndexDeletionBehaviour = iota
	Cascade
	IndexOnly
	CascadeByIdx
)

type IndexSearchPattern int

const (
	One IndexSearchPattern = iota
	AndComputational
	OrComputational
	// IndexingList
)

type ListSearchPattern int

const (
	Default ListSearchPattern = iota
	All
)
