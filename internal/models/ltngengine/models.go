package ltngenginemodels

import (
	"context"
	"os"
	"time"
)

const (
	FQBasePath    = ".ltngfq"
	DBBasePath    = ".ltngdb"
	DBBaseVersion = "/v1"
	DBDataPath    = "/stores"
	DBStatsPath   = "/stores/stats"
	Sep           = "/"
	Ext           = ".ptk"
	ALL           = "*"
	ALLExt        = ALL + Ext
	LineBreak     = "\n"
	LB            = "&#!;+|ltngdb|+;#!&"
	BytesSep      = "&#!;+|ltngdb|+;#!&"
	BytesSliceSep = "&#!;+|ltngdb|+;#!&"

	BasePath      = DBBasePath + DBBaseVersion
	BaseDataPath  = BasePath + DBDataPath
	BaseStatsPath = BasePath + DBStatsPath

	Rubbish           = "/rubbish"
	RubbishBasePath   = BasePath + Rubbish
	DBTmpDelDataPath  = RubbishBasePath + "/del-tmp-store"
	DBTmpDelStatsPath = RubbishBasePath + "/del-tmp-store-stats"

	SuffixSep                  = "-"
	DBIndexStoreSuffixName     = "indexed"
	DBIndexStoreSuffixPath     = "/indexed"
	DBIndexListStoreSuffixName = "indexed-list"
	DBIndexListStoreSuffixPath = "/indexed-list"
	DBRelationalName           = "relational"
	DBRelationalPath           = "/relational"
	Tmp                        = "tmp"
	TmpPath                    = "/tmp"

	DBManagerName = "ltng-engine-manager"
	DBManagerPath = "internal/ltng-engine/manager"

	RelationalDataStore     = "relational-data-store"
	RelationalDataStoreFile = RelationalDataStore + Ext
	ListingItemsFromStore   = "listing-items-from-store"

	DBFilePerm = 0750
	DBFileOp   = 0644
	DBFileRead = 0444
)

var (
	DBManagerStoreInfo = &StoreInfo{
		Name: DBManagerName,
		Path: DBManagerPath,
	}
)

type (
	FileInfo struct {
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
		Key    []byte  `json:"key,omitempty"`
		Header *Header `json:"header,omitempty"`
		Data   []byte  `json:"data,omitempty"`
	}

	ItemInfoData struct {
		Ctx        context.Context
		OpType     OpType
		DBMetaInfo *ManagerStoreMetaInfo
		Item       *Item
		Opts       *IndexOpts
		RespSignal chan error
	}

	OpChannels struct {
		QueueChannel                  chan struct{}
		InfoChannel                   chan *ItemInfoData
		ActionItemChannel             chan *ItemInfoData
		RollbackItemChannel           chan *ItemInfoData
		ActionIndexItemChannel        chan *ItemInfoData
		RollbackIndexItemChannel      chan *ItemInfoData
		ActionIndexListItemChannel    chan *ItemInfoData
		RollbackIndexListItemChannel  chan *ItemInfoData
		ActionRelationalItemChannel   chan *ItemInfoData
		RollbackRelationalItemChannel chan *ItemInfoData
	}

	CrudChannels struct {
		OpSagaChannel  *OpChannels
		CreateChannels *OpChannels
		UpsertChannels *OpChannels
		DeleteChannels *OpChannels
	}
)

type OpType string

const (
	OpTypeCreate OpType = "create"
	OpTypeUpsert OpType = "upsert"
	OpTypeDelete OpType = "delete"
	OpTypeGet    OpType = "get"
	OpTypeList   OpType = "list"
)

const ChannelLimit = 1 << 8 // 1 << 15

func MakeOpChannels() *OpChannels {
	return &OpChannels{
		QueueChannel: make(chan struct{}, ChannelLimit),
		InfoChannel:  make(chan *ItemInfoData, ChannelLimit),

		ActionItemChannel:             make(chan *ItemInfoData, ChannelLimit),
		RollbackItemChannel:           make(chan *ItemInfoData, ChannelLimit),
		ActionIndexItemChannel:        make(chan *ItemInfoData, ChannelLimit),
		RollbackIndexItemChannel:      make(chan *ItemInfoData, ChannelLimit),
		ActionIndexListItemChannel:    make(chan *ItemInfoData, ChannelLimit),
		RollbackIndexListItemChannel:  make(chan *ItemInfoData, ChannelLimit),
		ActionRelationalItemChannel:   make(chan *ItemInfoData, ChannelLimit),
		RollbackRelationalItemChannel: make(chan *ItemInfoData, ChannelLimit),
	}
}

func NewFileData(
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
) *FileData {
	timeNow := time.Now().UTC().Unix()
	fileData := &FileData{
		Header: &Header{
			ItemInfo: &ItemInfo{
				CreatedAt:    timeNow,
				LastOpenedAt: timeNow,
			},
			StoreInfo: &StoreInfo{
				Name: dbMetaInfo.Name,
				Path: dbMetaInfo.Path,
			},
		},
		Data: item.Value,
		Key:  item.Key,
	}

	return fileData
}

func (i *ItemInfoData) WithContext(ctx context.Context) *ItemInfoData {
	i.Ctx = ctx
	return i
}

func (i *ItemInfoData) WithRespChan(sigChan chan error) *ItemInfoData {
	return &ItemInfoData{
		Ctx:        i.Ctx,
		DBMetaInfo: i.DBMetaInfo,
		Item:       i.Item,
		Opts:       i.Opts,
		RespSignal: sigChan,
	}
}

func (storeInfo *StoreInfo) IndexInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name + SuffixSep + DBIndexStoreSuffixName,
		Path:         storeInfo.Path + DBIndexStoreSuffixPath,
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) IndexListInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name + SuffixSep + DBIndexListStoreSuffixName,
		Path:         storeInfo.Path + DBIndexListStoreSuffixPath,
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) RelationalInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name + SuffixSep + DBRelationalName,
		Path:         storeInfo.Path + DBRelationalPath,
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) TmpRelationalInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name + SuffixSep + DBRelationalName + SuffixSep + Tmp,
		Path:         storeInfo.Path + DBRelationalPath + TmpPath,
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
	return GetFileLockName(storeInfo.Name, key)
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
	return GetFileLockName(msi.Name, key)
}

func (msi *ManagerStoreMetaInfo) IndexInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name + SuffixSep + DBIndexStoreSuffixName,
		Path: msi.Path + DBIndexStoreSuffixPath,
	}
}

func (msi *ManagerStoreMetaInfo) IndexListInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name + SuffixSep + DBIndexListStoreSuffixName,
		Path: msi.Path + DBIndexListStoreSuffixPath,
	}
}

func (msi *ManagerStoreMetaInfo) RelationalInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name + SuffixSep + DBRelationalName,
		Path: msi.Path + DBRelationalPath,
	}
}

func (msi *ManagerStoreMetaInfo) TmpRelationalInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name + SuffixSep + DBRelationalName + SuffixSep + Tmp,
		Path: msi.Path + DBRelationalPath + TmpPath,
	}
}

type (
	Item struct {
		Key   []byte
		Value []byte
		Error error
	}

	ListItemsResult struct {
		Pagination *Pagination
		Items      []*Item
	}

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
)

type ListSearchPattern int

const (
	Default ListSearchPattern = iota
	All
	IndexingList
)
