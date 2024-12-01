package v1

import (
	"os"
	"sync"

	serializer_models "gitlab.com/pietroski-software-company/devex/golang/serializer/models"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/tools/lock"
)

const (
	dbBasePath    = ".ltngdb"
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
	dbManagerStoreInfo = &StoreInfo{
		Name: dbManagerName,
		Path: dbManagerPath,
	}
)

type (
	LTNGEngine struct {
		opMtx *lock.EngineLock
		mtx   *sync.Mutex

		serializer serializer_models.Serializer

		storeFileMapping map[string]*fileInfo
		itemFileMapping  map[string]*fileInfo
	}

	fileInfo struct {
		File       *os.File
		FileData   *FileData `json:"file_data,omitempty"`
		HeaderSize uint32    `json:"headerSize,omitempty"`
		DataSize   uint32    `json:"dataSize,omitempty"`
	}

	StoreInfo struct {
		Name         string `json:"name,omitempty"`
		Path         string `json:"path,omitempty"`
		CreatedAt    int64  `json:"createdAt,omitempty"`
		LastOpenedAt int64  `json:"lastOpenedAt,omitempty"`
	}

	ItemInfo struct {
		CreatedAt    int64 `json:"createdAt,omitempty"`
		LastOpenedAt int64 `json:"lastOpenedAt,omitempty"`
	}

	Header struct {
		ItemInfo  *ItemInfo  `json:"itemInfo,omitempty"`
		StoreInfo *StoreInfo `json:"storeInfo,omitempty"`
	}

	FileData struct {
		Header *Header `json:"header,omitempty"`
		Data   []byte  `json:"data,omitempty"`
	}
)

func (storeInfo *StoreInfo) IndexInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name + suffixSep + dbIndexStoreSuffixName,
		Path:         storeInfo.Path + dbIndexStoreSuffixPath,
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) IndexListInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name + suffixSep + dbIndexListStoreSuffixName,
		Path:         storeInfo.Path + dbIndexListStoreSuffixPath,
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) RelationalInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name + suffixSep + dbRelationalName,
		Path:         storeInfo.Path + dbRelationalPath,
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) TmpRelationalInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name + suffixSep + dbRelationalName + suffixSep + tmp,
		Path:         storeInfo.Path + dbRelationalPath + tmpPath,
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) ManagerStoreMetaInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: storeInfo.Name,
		Path: storeInfo.Path,
	}
}

func (storeInfo *StoreInfo) LockName(key string) string {
	return getFileLockName(storeInfo.Name, key)
}

type (
	ManagerStoreMetaInfo struct {
		Name string
		Path string
	}
)

// LockName
// msi -> managerStoreInfo
func (msi *ManagerStoreMetaInfo) LockName(key string) string {
	return getFileLockName(msi.Name, key)
}

func (msi *ManagerStoreMetaInfo) IndexInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name + suffixSep + dbIndexStoreSuffixName,
		Path: msi.Path + dbIndexStoreSuffixPath,
	}
}

func (msi *ManagerStoreMetaInfo) IndexListInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name + suffixSep + dbIndexListStoreSuffixName,
		Path: msi.Path + dbIndexListStoreSuffixPath,
	}
}

func (msi *ManagerStoreMetaInfo) RelationalInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name + suffixSep + dbRelationalName,
		Path: msi.Path + dbRelationalPath,
	}
}

func (msi *ManagerStoreMetaInfo) TmpRelationalInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name + suffixSep + dbRelationalName + suffixSep + tmp,
		Path: msi.Path + dbRelationalPath + tmpPath,
	}
}

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
)

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
