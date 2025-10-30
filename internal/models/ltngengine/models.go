package ltngenginemodels

import (
	"context"
	"encoding/hex"
	"os"
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/syncx"
)

const (
	FQBasePath    = ".ltngfq"
	DBBasePath    = ".ltngdb"
	DBBaseVersion = "/v2"
	DBDataPath    = "/stores"
	DBStatsPath   = "/stores/stats"
	Sep           = "/"
	Ext           = ".ptk"
	ALL           = "*"
	ALLExt        = ALL + Ext
	LB            = "\n"
	BsSep         = "!|ltngdb|!" // "&#!;+|ltngdb|+;#!&"
	BytesSep      = "!|ltngdb|!" // "&#!;+|ltngdb|+;#!&"
	BytesSliceSep = "!|ltngdb|!" // "&#!;+|ltngdb|+;#!&"

	BasePath      = DBBasePath + DBBaseVersion
	BaseDataPath  = BasePath + DBDataPath
	BaseStatsPath = BasePath + DBStatsPath

	Rubbish           = "/rubbish"
	RubbishBasePath   = BasePath + Rubbish
	DBTmpDelDataPath  = RubbishBasePath + "/del-tmp-store"
	DBTmpDelStatsPath = RubbishBasePath + "/del-tmp-store-stats"

	InnerSep                   = "-"
	DBIndexStoreSuffixName     = "indexed"
	DBIndexStoreSuffixPath     = "/indexed"
	DBIndexListStoreSuffixName = "indexed-list"
	DBIndexListStoreSuffixPath = "/indexed-list"
	DBRelationalName           = "relational"
	DBRelationalPath           = "/relational"
	Tmp                        = "tmp"
	TmpPath                    = "/tmp"
	TmpPrefix                  = "tmp-"
	TmpSuffix                  = "-tmp"

	DBManagerName = "ltng-engine-manager"
	DBManagerPath = "internal/ltng-engine/manager"

	RelationalDataStore     = "relational-data-store"
	RelationalDataStoreFile = RelationalDataStore + Ext
	ListingItemsFromStore   = "listing-items-from-store"

	DBFilePerm = 0740 // 0750
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
		Ctx          context.Context
		OpNatureType OpNatureType
		OpType       OpType
		DBMetaInfo   *ManagerStoreMetaInfo
		Item         *Item
		Opts         *IndexOpts
		RespSignal   chan error

		IndexKeysToDelete [][]byte
	}

	OpChannels struct {
		QueueChannel                  *syncx.Channel[struct{}]
		InfoChannel                   *syncx.Channel[*ItemInfoData]
		ActionItemChannel             *syncx.Channel[*ItemInfoData]
		RollbackItemChannel           *syncx.Channel[*ItemInfoData]
		ActionIndexItemChannel        *syncx.Channel[*ItemInfoData]
		RollbackIndexItemChannel      *syncx.Channel[*ItemInfoData]
		ActionIndexListItemChannel    *syncx.Channel[*ItemInfoData]
		RollbackIndexListItemChannel  *syncx.Channel[*ItemInfoData]
		ActionRelationalItemChannel   *syncx.Channel[*ItemInfoData]
		RollbackRelationalItemChannel *syncx.Channel[*ItemInfoData]

		CleanUpUpsert *syncx.Channel[*ItemInfoData]
	}

	CrudChannels struct {
		OpSagaChannel  *OpChannels
		CreateChannels *OpChannels
		UpsertChannels *OpChannels
		DeleteChannels *OpChannels
	}
)

func (opChan *OpChannels) Close() {
	opChan.QueueChannel.Close()
	opChan.InfoChannel.Close()
	opChan.ActionItemChannel.Close()
	opChan.RollbackItemChannel.Close()
	opChan.ActionIndexItemChannel.Close()
	opChan.RollbackIndexItemChannel.Close()
	opChan.ActionIndexListItemChannel.Close()
	opChan.RollbackIndexListItemChannel.Close()
	opChan.ActionRelationalItemChannel.Close()
	opChan.RollbackRelationalItemChannel.Close()
	opChan.CleanUpUpsert.Close()
}

type OpNatureType string

const (
	OpNatureTypeStore OpNatureType = "store"
	OpNatureTypeItem  OpNatureType = "item"
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
		QueueChannel: syncx.NewChannel[struct{}](syncx.WithChannelSize[struct{}](ChannelLimit)),
		InfoChannel: syncx.NewChannel[*ItemInfoData](
			syncx.WithChannelSize[*ItemInfoData](ChannelLimit)),

		ActionItemChannel: syncx.NewChannel[*ItemInfoData](
			syncx.WithChannelSize[*ItemInfoData](ChannelLimit)),
		RollbackItemChannel: syncx.NewChannel[*ItemInfoData](
			syncx.WithChannelSize[*ItemInfoData](ChannelLimit)),
		ActionIndexItemChannel: syncx.NewChannel[*ItemInfoData](
			syncx.WithChannelSize[*ItemInfoData](ChannelLimit)),
		RollbackIndexItemChannel: syncx.NewChannel[*ItemInfoData](
			syncx.WithChannelSize[*ItemInfoData](ChannelLimit)),
		ActionIndexListItemChannel: syncx.NewChannel[*ItemInfoData](
			syncx.WithChannelSize[*ItemInfoData](ChannelLimit)),
		RollbackIndexListItemChannel: syncx.NewChannel[*ItemInfoData](
			syncx.WithChannelSize[*ItemInfoData](ChannelLimit)),
		ActionRelationalItemChannel: syncx.NewChannel[*ItemInfoData](
			syncx.WithChannelSize[*ItemInfoData](ChannelLimit)),
		RollbackRelationalItemChannel: syncx.NewChannel[*ItemInfoData](
			syncx.WithChannelSize[*ItemInfoData](ChannelLimit)),

		CleanUpUpsert: syncx.NewChannel[*ItemInfoData](
			syncx.WithChannelSize[*ItemInfoData](ChannelLimit)),
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
		Ctx:               i.Ctx,
		OpNatureType:      i.OpNatureType,
		OpType:            i.OpType,
		DBMetaInfo:        i.DBMetaInfo,
		Item:              i.Item,
		Opts:              i.Opts,
		RespSignal:        sigChan,
		IndexKeysToDelete: i.IndexKeysToDelete,
	}
}

func (storeInfo *StoreInfo) IndexInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name + InnerSep + DBIndexStoreSuffixName,
		Path:         storeInfo.Path + DBIndexStoreSuffixPath,
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) IndexListInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name + InnerSep + DBIndexListStoreSuffixName,
		Path:         storeInfo.Path + DBIndexListStoreSuffixPath,
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) RelationalInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name + InnerSep + DBRelationalName,
		Path:         storeInfo.Path + DBRelationalPath,
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) TmpRelationalInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name + InnerSep + DBRelationalName + InnerSep + Tmp,
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
		Name: msi.Name + InnerSep + DBIndexStoreSuffixName,
		Path: msi.Path + DBIndexStoreSuffixPath,
	}
}

func (msi *ManagerStoreMetaInfo) IndexListInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name + InnerSep + DBIndexListStoreSuffixName,
		Path: msi.Path + DBIndexListStoreSuffixPath,
	}
}

func (msi *ManagerStoreMetaInfo) RelationalInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name + InnerSep + DBRelationalName,
		Path: msi.Path + DBRelationalPath,
	}
}

func (msi *ManagerStoreMetaInfo) TmpRelationalInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name + InnerSep + DBRelationalName + InnerSep + Tmp,
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

	ItemList []*Item

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

func (il *ItemList) GetItemsFromPagination(pagination *Pagination) []*Item {
	if il == nil || len(*il) == 0 || pagination == nil {
		return []*Item{}
	}

	if !pagination.IsValid() {
		return *il
	}

	// If pagination is not valid, return all items
	if !pagination.IsValid() {
		return *il
	}

	// Calculate start and end indices
	startIndex := (pagination.PageID - 1) * pagination.PageSize
	endIndex := startIndex + pagination.PageSize

	// Check if startIndex is beyond the slice length
	if startIndex >= uint64(len(*il)) {
		return []*Item{}
	}

	// Adjust endIndex if it exceeds the slice length
	if endIndex > uint64(len(*il)) {
		endIndex = uint64(len(*il))
	}

	// Return the paginated subset
	return (*il)[startIndex:endIndex]
}

func IndexListToMap(indexingList []*Item) map[string]struct{} {
	indexingMap := map[string]struct{}{}
	for _, item := range indexingList {
		strKey := hex.EncodeToString(item.Key)
		indexingMap[strKey] = struct{}{}
	}

	return indexingMap
}

func IndexListToBytesList(indexingList []*Item) [][]byte {
	bytesList := make([][]byte, len(indexingList))
	for idx, item := range indexingList {
		bytesList[idx] = item.Value
	}

	return bytesList
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
)

type ListSearchPattern int

const (
	Default ListSearchPattern = iota
	All
	IndexingList
)
