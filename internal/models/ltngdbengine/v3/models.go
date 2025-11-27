package ltngdbenginemodelsv3

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
)

const (
	FileExt  = ".ptk"
	BsSep    = "!|ltng|!"
	InnerSep = "-"

	Internal  = "internal"
	Temporary = "temporary"
)

const (
	DBBasePath = ".ltngdb"
	DBName     = "ltngdb"
	Manager    = "manager"

	DBBaseVersion = "v3"

	Stores = "stores"
	Stats  = "stats"
	Data   = "data"

	Indexed     = "indexed"
	IndexedList = "indexed-list"
	Relational  = "relational"

	DBManagerName = "ltngdb-engine-manager"

	RelationalDataStoreKey = "relational-data-store"
)

var (
	DBManagerPath = filepath.Join(Internal, DBName, Manager)

	DBBaseStatsPath                    = filepath.Join(DBBasePath, DBBaseVersion, Stores, Stats)
	DBBaseTemporaryStatsPath           = filepath.Join(DBBasePath, DBBaseVersion, Stores, Stats, Temporary)
	DBBaseRelationalStatsPath          = filepath.Join(DBBasePath, DBBaseVersion, Stores, Stats, Relational)
	DBBaseTemporaryRelationalStatsPath = filepath.Join(DBBasePath, DBBaseVersion, Stores, Stats, Relational, Temporary)

	DBBaseDataPath                     = filepath.Join(DBBasePath, DBBaseVersion, Stores, Data)
	DBBaseTemporaryDataPath            = filepath.Join(DBBasePath, DBBaseVersion, Stores, Data, Temporary)
	DBBaseRelationalDataPath           = filepath.Join(DBBasePath, DBBaseVersion, Stores, Data, Relational)
	DBBaseTemporaryRelationalDataPath  = filepath.Join(DBBasePath, DBBaseVersion, Stores, Data, Relational, Temporary)
	DBBaseIndexedDataPath              = filepath.Join(DBBasePath, DBBaseVersion, Stores, Data, Indexed)
	DBBaseTemporaryIndexedDataPath     = filepath.Join(DBBasePath, DBBaseVersion, Stores, Data, Indexed, Temporary)
	DBBaseIndexedListDataPath          = filepath.Join(DBBasePath, DBBaseVersion, Stores, Data, IndexedList)
	DBBaseTemporaryIndexedListDataPath = filepath.Join(DBBasePath, DBBaseVersion, Stores, Data, IndexedList, Temporary)
)

// stats paths

func GetStatsPath(path string) string {
	return filepath.Join(DBBaseStatsPath, path)
}

func GetStatsFilepath(path, storeName string) string {
	return filepath.Join(DBBaseStatsPath, path, storeName) + FileExt
}

func GetTemporaryStatsPath(path string) string {
	return filepath.Join(DBBaseTemporaryStatsPath, path)
}

func GetTemporaryStatsFilepath(path, storeName string) string {
	return filepath.Join(DBBaseTemporaryStatsPath, path, storeName) + FileExt
}

func GetRelationalStatsPath(path string) string {
	return filepath.Join(DBBaseRelationalStatsPath, path)
}

func GetRelationalStatsFilepath(path, storeName string) string {
	return filepath.Join(DBBaseRelationalStatsPath, path, storeName) + FileExt
}

func GetTemporaryRelationalStatsPath(path string) string {
	return filepath.Join(DBBaseTemporaryRelationalStatsPath, path)
}

func GetTemporaryRelationalStatsFilepath(path, storeName string) string {
	return filepath.Join(DBBaseTemporaryRelationalStatsPath, path, storeName) + FileExt
}

// data paths

func GetDataPath(path string) string {
	return filepath.Join(DBBaseDataPath, path)
}

func GetDataFilepath(path, filename string) string {
	return filepath.Join(DBBaseDataPath, path, filename) + FileExt
}

func GetTemporaryDataPath(path string) string {
	return filepath.Join(DBBaseTemporaryDataPath, path)
}

func GetTemporaryDataFilepath(path, filename string) string {
	return filepath.Join(DBBaseTemporaryDataPath, path, filename) + FileExt
}

func GetRelationalDataPath(path string) string {
	return filepath.Join(DBBaseRelationalDataPath, path)
}

func GetRelationalDataFilepath(path, filename string) string {
	return filepath.Join(DBBaseRelationalDataPath, path, filename) + FileExt
}

func GetTemporaryRelationalDataPath(path string) string {
	return filepath.Join(DBBaseTemporaryRelationalDataPath, path)
}

func GetTemporaryRelationalDataFilepath(path, filename string) string {
	return filepath.Join(DBBaseTemporaryRelationalDataPath, path, filename) + FileExt
}

func GetIndexedDataPath(path string) string {
	return filepath.Join(DBBaseIndexedDataPath, path)
}

func GetIndexedDataFilepath(path, filename string) string {
	return filepath.Join(DBBaseIndexedDataPath, path, filename) + FileExt
}

func GetTemporaryIndexedDataPath(path string) string {
	return filepath.Join(DBBaseTemporaryIndexedDataPath, path)
}

func GetTemporaryIndexedDataFilepath(path, filename string) string {
	return filepath.Join(DBBaseTemporaryIndexedDataPath, path, filename) + FileExt
}

func GetIndexedListDataPath(path string) string {
	return filepath.Join(DBBaseIndexedListDataPath, path)
}

func GetIndexedListDataFilepath(path, filename string) string {
	return filepath.Join(DBBaseIndexedListDataPath, path, filename) + FileExt
}

func GetTemporaryIndexedListDataPath(path string) string {
	return filepath.Join(DBBaseTemporaryIndexedListDataPath, path)
}

func GetTemporaryIndexedListDataFilepath(path, filename string) string {
	return filepath.Join(DBBaseTemporaryIndexedListDataPath, path, filename) + FileExt
}

// general definitions

type (
	DBInfo struct {
		Name      string
		Path      string
		CreatedAt int64
		// LastOpenedAt int64
	}
)

var (
	DBManagerStoreInfo = &StoreInfo{
		Name: DBManagerName,
		Path: DBManagerPath,
	}
)

type (
	FileInfo struct {
		File        *os.File
		FileData    *FileData
		FileManager *mmap.FileManager
	}

	RelationalFileInfo struct {
		File                  *os.File
		FileData              *FileData
		RelationalFileManager *mmap.RelationalFileManager
	}

	StoreInfo struct {
		Name      string
		Path      string
		CreatedAt int64
		// LastOpenedAt would allow to track how long a file is opened to it can be removed from cache.
		// But this is filled and keep track of only in runtime, it is not stored in the file.
		LastOpenedAt int64
	}

	ItemInfo struct {
		CreatedAt int64
	}

	Header struct {
		ItemInfo  *ItemInfo
		StoreInfo *StoreInfo
	}

	FileData struct {
		Header *Header
		Key    []byte
		Data   []byte
	}

	ItemInfoData struct {
		Ctx        context.Context
		RespSignal chan error

		TraceID           string
		OpNatureType      OpNatureType
		OpType            OpType
		DBMetaInfo        *ManagerStoreMetaInfo
		Item              *Item
		Opts              *IndexOpts
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
				CreatedAt: timeNow,
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
	//return &ItemInfoData{
	//	Ctx:        i.Ctx,
	//	RespSignal: sigChan,
	//
	//	OpNatureType:      i.OpNatureType,
	//	OpType:            i.OpType,
	//	DBMetaInfo:        i.DBMetaInfo,
	//	Item:              i.Item,
	//	Opts:              i.Opts,
	//	IndexKeysToDelete: i.IndexKeysToDelete,
	//}
	i.RespSignal = sigChan
	return i
}

// EncodedKey
// return strings.Join([]string{i.DBMetaInfo.Path, i.DBMetaInfo.Name, hex.EncodeToString(i.Item.Key)}, InnerSep)
func (i *ItemInfoData) EncodedKey() string {
	return hex.EncodeToString(i.Item.Key)
}

func (storeInfo *StoreInfo) IndexInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name,
		Path:         filepath.Join(Indexed, storeInfo.Path),
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) IndexListInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name,
		Path:         filepath.Join(IndexedList, storeInfo.Path),
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) RelationalInfo() *StoreInfo {
	return &StoreInfo{
		Name:         RelationalDataStoreKey,
		Path:         filepath.Join(Relational, storeInfo.Path),
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) TemporaryInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name,
		Path:         filepath.Join(Temporary, storeInfo.Path),
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) TemporaryIndexInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name,
		Path:         filepath.Join(Temporary, Indexed, storeInfo.Path),
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) TemporaryIndexListInfo() *StoreInfo {
	return &StoreInfo{
		Name:         storeInfo.Name,
		Path:         filepath.Join(Temporary, IndexedList, storeInfo.Path),
		CreatedAt:    storeInfo.CreatedAt,
		LastOpenedAt: storeInfo.LastOpenedAt,
	}
}

func (storeInfo *StoreInfo) TemporaryRelationalInfo() *StoreInfo {
	return &StoreInfo{
		Name:         RelationalDataStoreKey,
		Path:         filepath.Join(Temporary, Relational, storeInfo.Path),
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

func (storeInfo *StoreInfo) LockStrWithKey(key string) string {
	return strings.Join([]string{storeInfo.Path, storeInfo.Name, key}, InnerSep)
}

func (storeInfo *StoreInfo) LockStr() string {
	return strings.Join([]string{storeInfo.Path, storeInfo.Name}, InnerSep)
}

func (storeInfo *StoreInfo) RelationalLockStr() string {
	return storeInfo.RelationalInfo().LockStr()
}

type (
	ManagerStoreMetaInfo struct {
		Name string
		Path string
	}
)

func (msi *ManagerStoreMetaInfo) LockStrWithKey(key string) string {
	return strings.Join([]string{msi.Path, msi.Name, key}, InnerSep)
}

// LockStr
// msi -> managerStoreInfo
func (msi *ManagerStoreMetaInfo) LockStr() string {
	return strings.Join([]string{msi.Path, msi.Name}, InnerSep)
}

func (msi *ManagerStoreMetaInfo) RelationalLockStr() string {
	return msi.RelationalInfo().LockStr()
}

func (msi *ManagerStoreMetaInfo) IndexInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name,
		Path: filepath.Join(Indexed, msi.Path),
	}
}

func (msi *ManagerStoreMetaInfo) IndexListInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name,
		Path: filepath.Join(IndexedList, msi.Path),
	}
}

func (msi *ManagerStoreMetaInfo) RelationalInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: RelationalDataStoreKey,
		Path: filepath.Join(Relational, msi.Path),
	}
}

func (msi *ManagerStoreMetaInfo) TemporaryInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name,
		Path: filepath.Join(Temporary, msi.Path),
	}
}

func (msi *ManagerStoreMetaInfo) TemporaryIndexInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name,
		Path: filepath.Join(Temporary, Indexed, msi.Path),
	}
}

func (msi *ManagerStoreMetaInfo) TemporaryIndexListInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: msi.Name,
		Path: filepath.Join(Temporary, IndexedList, msi.Path),
	}
}

func (msi *ManagerStoreMetaInfo) TemporaryRelationalInfo() *ManagerStoreMetaInfo {
	return &ManagerStoreMetaInfo{
		Name: RelationalDataStoreKey,
		Path: filepath.Join(Temporary, Relational, msi.Path),
	}
}

type (
	Item struct {
		Key   []byte
		Value []byte
		//Error error
	}

	ListItemsResult struct {
		Pagination *ltngdata.Pagination
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

func (il *ItemList) GetItemsFromPagination(pagination *ltngdata.Pagination) []*Item {
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

// Errors

var (
	ErrStoreAlreadyExists = errorsx.New("store already exists")
)
