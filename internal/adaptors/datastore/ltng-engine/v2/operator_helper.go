package v2

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
)

func (e *LTNGEngine) createRelationalItemStore(
	ctx context.Context,
	info *ltngenginemodels.StoreInfo,
) (*ltngenginemodels.FileInfo, error) {
	file, err := e.fileManager.OpenCreateTruncatedFile(ctx,
		ltngenginemodels.GetDataFilepath(info.RelationalInfo().Path, ltngenginemodels.RelationalDataStore),
	)
	if err != nil {
		return nil, err
	}

	timeNow := time.Now().UTC().Unix()
	fileData := &ltngenginemodels.FileData{
		Key: []byte(ltngenginemodels.RelationalDataStore),
		Header: &ltngenginemodels.Header{
			ItemInfo: &ltngenginemodels.ItemInfo{
				CreatedAt:    timeNow,
				LastOpenedAt: timeNow,
			},
			StoreInfo: info,
		},
		Data: nil,
	}

	bs, err := e.fileManager.WriteToRelationalFile(ctx, file, fileData)
	if err != nil {
		return nil, err
	}

	fi := &ltngenginemodels.FileInfo{
		File:       file,
		FileData:   fileData,
		HeaderSize: uint32(len(bs)),
		DataSize:   uint32(len(fileData.Data)),
	}
	e.itemFileMapping.Set(info.RelationalInfo().LockName(ltngenginemodels.RelationalDataStore), fi)

	return fi, nil
}

// #####################################################################################################################

func (e *LTNGEngine) loadItemFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	updateRelationalData bool,
) (*ltngenginemodels.Item, error) {
	strItemKey := hex.EncodeToString(item.Key)
	fi, ok := e.itemFileMapping.Get(dbMetaInfo.LockName(strItemKey))
	if !ok {
		var err error
		fi, err = e.loadItemFromDisk(ctx, dbMetaInfo, item, updateRelationalData)
		if err != nil {
			return nil, err
		}
		e.itemFileMapping.Set(dbMetaInfo.LockName(strItemKey), fi)

		return &ltngenginemodels.Item{
			Key:   fi.FileData.Key,
			Value: fi.FileData.Data,
		}, nil
	}

	bs, err := e.fileManager.ReadAll(ctx, fi.File)
	if err != nil {
		return nil, err
	}

	var itemFileData ltngenginemodels.FileData
	err = e.serializer.Deserialize(bs, &itemFileData)
	if err != nil {
		return nil, err
	}

	return &ltngenginemodels.Item{
		Key:   itemFileData.Key,
		Value: itemFileData.Data,
	}, nil
}

func (e *LTNGEngine) loadItemFromDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	updateRelationalData bool,
) (*ltngenginemodels.FileInfo, error) {
	strItemKey := hex.EncodeToString(item.Key)
	filepath := ltngenginemodels.GetDataFilepath(dbMetaInfo.Path, strItemKey)
	bs, file, err := e.fileManager.OpenReadWholeFile(ctx, filepath)
	if err != nil {
		return nil, fmt.Errorf("error openning to read whole file %s: %w", filepath, err)
	}

	var fileData ltngenginemodels.FileData
	err = e.serializer.Deserialize(bs, &fileData)
	if err != nil {
		return nil, err
	}
	fileData.Header.ItemInfo.LastOpenedAt = time.Now().UTC().Unix()

	tmpFilepath := ltngenginemodels.GetTmpDataFilepath(dbMetaInfo.Path, strItemKey)
	tmpFile, err := e.fileManager.OpenCreateTruncatedFile(ctx, tmpFilepath)
	if err != nil {
		return nil, fmt.Errorf("error openning temporary file to upsert metadata %s: %w", tmpFilepath, err)
	}

	if _, err = e.fileManager.WriteToFile(ctx, tmpFile, fileData); err != nil {
		return nil, fmt.Errorf("error writing item metadata to file: %w", err)
	}

	if err = os.Rename(tmpFilepath, filepath); err != nil {
		return nil, fmt.Errorf("error renaming temporary file %s to %s: %w", tmpFilepath, filepath, err)
	}

	if updateRelationalData {
		fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
		if err != nil {
			return nil, err
		}

		if err = e.upsertRelationalData(
			ctx, dbMetaInfo, &fileData, fi,
		); err != nil {
			return nil, fmt.Errorf("failed to update store stats manager file: %w", err)
		}
	}

	return &ltngenginemodels.FileInfo{
		File:     file,
		FileData: &fileData,
		DataSize: uint32(len(fileData.Data)),
	}, nil
}

// #####################################################################################################################

func (e *LTNGEngine) loadRelationalItemStoreFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
) (*ltngenginemodels.FileInfo, error) {
	relationalLockKey := dbMetaInfo.RelationalInfo().LockName(ltngenginemodels.RelationalDataStore)
	fi, ok := e.itemFileMapping.Get(relationalLockKey)
	if !ok {
		var err error
		fi, err = e.loadRelationalItemStoreFromDisk(ctx, dbMetaInfo)
		if err != nil {
			return nil, err
		}
		e.itemFileMapping.Set(relationalLockKey, fi)

		return fi, nil
	}

	//if isFileClosed(fi.File) {
	//	return nil, os.ErrClosed
	//}

	return fi, nil
}

func (e *LTNGEngine) loadRelationalItemStoreFromDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
) (*ltngenginemodels.FileInfo, error) {
	f, err := e.fileManager.OpenFile(ctx,
		ltngenginemodels.GetDataFilepath(dbMetaInfo.RelationalInfo().Path, ltngenginemodels.RelationalDataStore))
	if err != nil {
		return nil, err
	}

	fi := &ltngenginemodels.FileInfo{
		File: f,
	}
	reader, err := rw.NewFileReader(ctx, fi, true)
	if err != nil {
		return nil, err
	}

	var fileData ltngenginemodels.FileData
	err = e.serializer.Deserialize(reader.RawHeader, &fileData)
	if err != nil {
		return nil, err
	}

	fileData.Header.ItemInfo.LastOpenedAt = time.Now().UTC().Unix()
	fi.FileData = &fileData
	fi.HeaderSize = uint32(len(reader.RawHeader))
	fi.DataSize = uint32(len(fileData.Data))

	return fi, e.upsertRelationalData(
		ctx, dbMetaInfo, &fileData, fi,
	)
}

// #####################################################################################################################

var itemMarkedAsDeletedErr = errors.New("item marked as deleted")

func (e *LTNGEngine) searchMemoryFirst(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	if item != nil {
		return e.searchMemoryByKeyOrParentKey(ctx, dbMetaInfo, item, opts)
	} else if opts != nil {
		if opts.ParentKey != nil {
			return e.searchMemoryByKeyOrParentKey(ctx, dbMetaInfo, item, opts)
		}

		if opts.HasIdx && len(opts.IndexingKeys) > 0 {
			return e.searchMemoryByIndex(ctx, dbMetaInfo, item, opts)
		}
	}

	return nil, fmt.Errorf("invalid search filter - item and opts are null")
}

func (e *LTNGEngine) searchMemoryByKeyOrParentKey(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	var strItemKey string
	if item != nil {
		strItemKey = hex.EncodeToString(item.Key)
	} else if opts != nil && opts.ParentKey != nil {
		strItemKey = hex.EncodeToString(opts.ParentKey)
	} else {
		return nil, fmt.Errorf("invalid search filter - item and opts are null")
	}

	lockKey := dbMetaInfo.LockName(strItemKey)
	if _, ok := e.markedAsDeletedMapping.Get(lockKey); ok {
		return nil, fmt.Errorf("item %v|%v is marked as deleted: %w", item, opts, itemMarkedAsDeletedErr)
	}

	if i, err := e.memoryStore.LoadItem(
		ctx, dbMetaInfo, item, opts,
	); err == nil && i != nil {
		return i, nil
	}

	return nil, fmt.Errorf("no items in memory")
}

func (e *LTNGEngine) searchMemoryByIndex(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	_ *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	for _, indexItem := range opts.IndexingKeys {
		strItemKey := hex.EncodeToString(indexItem)
		lockKey := dbMetaInfo.IndexInfo().LockName(strItemKey)
		_, ok := e.markedAsDeletedMapping.Get(lockKey)
		if ok {
			return nil, fmt.Errorf("item %s is marked as deleted: %w", indexItem, itemMarkedAsDeletedErr)
		}

		if i, err := e.memoryStore.LoadItem(
			ctx, dbMetaInfo, &ltngenginemodels.Item{
				Key: indexItem,
			}, opts,
		); err == nil && i != nil {
			return i, nil
		}
	}

	return nil, fmt.Errorf("no items in memory")
}

// straightSearch finds the value from the index store with the [0] index list item;
// that found value is the key to the (main) store that holds value that needs to be returned
func (e *LTNGEngine) straightSearch(
	ctx context.Context,
	opts *ltngenginemodels.IndexOpts,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
) (*ltngenginemodels.Item, error) {
	key := opts.ParentKey
	if key == nil && (opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0) {
		return nil, fmt.Errorf("invalid indexing key")
	} else if key == nil {
		key = opts.IndexingKeys[0]
	}

	mainKeyValue, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &ltngenginemodels.Item{
		Key: key,
	}, false)
	if err != nil {
		return nil, err
	}

	item, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &ltngenginemodels.Item{
		Key: mainKeyValue.Value,
	}, true)
	if err != nil {
		return nil, err
	}

	return &ltngenginemodels.Item{
		Key:   item.Key,
		Value: item.Value,
	}, nil
}

func (e *LTNGEngine) andComputationalSearch(
	ctx context.Context,
	opts *ltngenginemodels.IndexOpts,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
) (*ltngenginemodels.Item, error) {
	var parentKey []byte
	for _, key := range opts.IndexingKeys {
		keyValue, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &ltngenginemodels.Item{
			Key: key,
		}, false)
		if err != nil {
			return nil, err
		}

		if !bytes.Equal(keyValue.Value, parentKey) && parentKey != nil {
			return nil, fmt.Errorf("not found")
		}

		parentKey = keyValue.Value
	}

	return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &ltngenginemodels.Item{
		Key: parentKey,
	}, true)
}

func (e *LTNGEngine) orComputationalSearch(
	ctx context.Context,
	opts *ltngenginemodels.IndexOpts,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
) (*ltngenginemodels.Item, error) {
	for _, key := range opts.IndexingKeys {
		parentItem, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &ltngenginemodels.Item{
			Key: key,
		}, false)
		if err != nil {
			continue
		}

		return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &ltngenginemodels.Item{
			Key: parentItem.Value,
		}, true)
	}

	return nil, fmt.Errorf("no keys found")
}

// #####################################################################################################################'

func (e *LTNGEngine) listPaginatedItems(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	pagination *ltngenginemodels.Pagination,
) (*ltngenginemodels.ListItemsResult, error) {
	var matchBox []*ltngenginemodels.Item
	var idx, limit uint64
	if pagination.IsValid() {
		if pagination.PaginationCursor > 0 {
			return e.listPaginatedItemsFromCursor(ctx, dbMetaInfo, pagination)
		}

		idx = (pagination.PageID - 1) * pagination.PageSize
		limit = pagination.PageID * pagination.PageSize

		matchBox = make([]*ltngenginemodels.Item, limit)
	} else {
		return nil, fmt.Errorf("invalid pagination")
	}

	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return nil, err
	}

	reader, err := rw.NewFileReader(ctx, fi, true)
	if err != nil {
		return nil, err
	}

	var count int
	for ; idx < limit; idx++ {
		var bs []byte
		bs, err = reader.Read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		var fileData ltngenginemodels.FileData
		if err = e.serializer.Deserialize(bs, &fileData); err != nil {
			return nil, err
		}

		if _, ok := e.markedAsDeletedMapping.Get(dbMetaInfo.LockName(hex.EncodeToString(fileData.Key))); ok {
			continue
		}

		matchBox[idx] = &ltngenginemodels.Item{
			Key:   fileData.Key,
			Value: fileData.Data,
		}
		count++
	}

	return &ltngenginemodels.ListItemsResult{
		Items: matchBox[:count],
		Pagination: &ltngenginemodels.Pagination{
			PageID:           pagination.PageID + 1,
			PageSize:         pagination.PageSize,
			PaginationCursor: uint64(reader.Cursor),
		},
	}, nil
}

func (e *LTNGEngine) listPaginatedItemsFromCursor(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	pagination *ltngenginemodels.Pagination,
) (*ltngenginemodels.ListItemsResult, error) {
	var matchBox []*ltngenginemodels.Item
	var idx, limit uint64

	if pagination.IsValid() {
		idx = (pagination.PageID - 1) * pagination.PageSize
		limit = idx * pagination.PageSize

		matchBox = make([]*ltngenginemodels.Item, limit)
	} else {
		return nil, fmt.Errorf("invalid pagination")
	}

	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return nil, err
	}

	reader, err := rw.NewFileReader(ctx, fi, false)
	if err != nil {
		return nil, err
	}

	if err = reader.SetCursor(ctx, pagination.PaginationCursor); err != nil {
		return nil, err
	}

	for ; idx < limit; idx++ {
		var bs []byte
		bs, err = reader.Read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		var fileData ltngenginemodels.FileData
		if err = e.serializer.Deserialize(bs, &fileData); err != nil {
			return nil, err
		}

		matchBox[idx] = &ltngenginemodels.Item{
			Key:   fileData.Key,
			Value: fileData.Data,
		}
	}

	return &ltngenginemodels.ListItemsResult{
		Items: matchBox,
		Pagination: &ltngenginemodels.Pagination{
			PageID:           pagination.PageID + 1,
			PageSize:         pagination.PageSize,
			PaginationCursor: uint64(reader.Cursor),
		},
	}, nil
}

func (e *LTNGEngine) listAllItems(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
) (*ltngenginemodels.ListItemsResult, error) {
	var matchBox []*ltngenginemodels.Item

	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return nil, err
	}

	reader, err := rw.NewFileReader(ctx, fi, true)
	if err != nil {
		return nil, err
	}

	for {
		var bs []byte
		bs, err = reader.Read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		var fileData ltngenginemodels.FileData
		if err = e.serializer.Deserialize(bs, &fileData); err != nil {
			return nil, err
		}

		if _, ok := e.markedAsDeletedMapping.Get(dbMetaInfo.LockName(hex.EncodeToString(fileData.Key))); ok {
			continue
		}

		matchBox = append(matchBox, &ltngenginemodels.Item{
			Key:   fileData.Key,
			Value: fileData.Data,
		})
	}

	return &ltngenginemodels.ListItemsResult{Items: matchBox}, nil
}

// loadIndexingList it returns all the indexing list a parent key has
func (e *LTNGEngine) loadIndexingList(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	opts *ltngenginemodels.IndexOpts,
) ([]*ltngenginemodels.Item, error) {
	rawIndexingList, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexListInfo(), &ltngenginemodels.Item{
		Key: opts.ParentKey,
	}, false)
	if err != nil {
		return nil, err
	}

	indexingList := bytes.Split(rawIndexingList.Value, []byte(ltngenginemodels.BytesSep))
	itemList := make([]*ltngenginemodels.Item, len(indexingList))
	for i, item := range indexingList {
		itemList[i] = &ltngenginemodels.Item{
			Key:   opts.ParentKey,
			Value: item,
		}
	}

	return itemList, nil
}

// #####################################################################################################################

func (e *LTNGEngine) createItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
) error {
	strItemKey := hex.EncodeToString(item.Key)
	filePath := ltngenginemodels.GetDataFilepath(dbMetaInfo.Path, strItemKey)

	fileData := ltngenginemodels.NewFileData(dbMetaInfo, item)
	file, err := e.fileManager.OpenCreateTruncatedFile(ctx, filePath)
	if err != nil {
		return fmt.Errorf("error opening/creating a truncated file at %s: %w", filePath, err)
	}

	if _, err = e.fileManager.WriteToFile(ctx, file, fileData); err != nil {
		return err
	}

	return nil
}

func (e *LTNGEngine) createRelationalItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
) error {
	fileData := ltngenginemodels.NewFileData(dbMetaInfo, item)

	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return fmt.Errorf("error creating %s relational store: %w",
			dbMetaInfo.RelationalInfo().Name, err)
	}

	return e.upsertRelationalData(ctx, dbMetaInfo, fileData, fi)
}

func (e *LTNGEngine) upsertItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
) (err error) {
	strItemKey := hex.EncodeToString(item.Key)
	filePath := ltngenginemodels.GetDataFilepath(dbMetaInfo.Path, strItemKey)
	tmpFilePath := ltngenginemodels.GetTmpDataFilepath(dbMetaInfo.Path, strItemKey)

	// TODO: revisit upsert functionality

	//if _, err = execx.MvFileExec(ctx, filePath, tmpFilePath); err != nil {
	//	return err
	//}

	//defer func() {
	//	if err != nil {
	//		_, err = execx.MvFileExec(ctx, tmpFilePath, filePath)
	//	}
	//}()

	fileData := ltngenginemodels.NewFileData(dbMetaInfo, item)
	file, err := e.fileManager.OpenCreateTruncatedFile(ctx, filePath)
	if err != nil {
		return fmt.Errorf("error opening/creating a temporary truncated file at %s: %w", tmpFilePath, err)
	}

	if _, err = e.fileManager.WriteToFile(ctx, file, fileData); err != nil {
		return err
	}

	return nil
}

func (e *LTNGEngine) upsertRelationalItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
) error {
	fileData := ltngenginemodels.NewFileData(dbMetaInfo, item)

	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return fmt.Errorf("error creating %s relational store: %w",
			dbMetaInfo.RelationalInfo().Name, err)
	}

	return e.upsertRelationalData(ctx, dbMetaInfo, fileData, fi)
}

// #####################################################################################################################

type tmpDelPaths struct {
	tmpDelPath           string
	indexTmpDelPath      string
	indexListTmpDelPath  string
	relationalTmpDelPath string
}

func (e *LTNGEngine) createTmpDeletionPaths(
	_ context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
) (*tmpDelPaths, error) {
	tmpDelPath := ltngenginemodels.GetTmpDelDataPathWithSep(dbMetaInfo.Path)
	if err := os.MkdirAll(tmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %w", err)
	}

	indexTmpDelPath := ltngenginemodels.GetTmpDelDataPathWithSep(dbMetaInfo.IndexInfo().Path)
	if err := os.MkdirAll(indexTmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %w", err)
	}

	indexListTmpDelPath := ltngenginemodels.GetTmpDelDataPathWithSep(dbMetaInfo.IndexListInfo().Path)
	if err := os.MkdirAll(indexListTmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %w", err)
	}

	relationalTmpDelPath := ltngenginemodels.GetTmpDelDataPathWithSep(dbMetaInfo.RelationalInfo().Path)
	if err := os.MkdirAll(relationalTmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %w", err)
	}

	return &tmpDelPaths{
		tmpDelPath:           tmpDelPath,
		indexTmpDelPath:      indexTmpDelPath,
		indexListTmpDelPath:  indexListTmpDelPath,
		relationalTmpDelPath: relationalTmpDelPath,
	}, nil
}

// #####################################################################################################################

func (e *LTNGEngine) upsertRelationalData(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	fileData *ltngenginemodels.FileData,
	fi *ltngenginemodels.FileInfo,
) (err error) {
	relationalInfo := fi.FileData.Header.StoreInfo.RelationalInfo()
	relationalPath := relationalInfo.Path
	relationalLockKey := relationalInfo.LockName(ltngenginemodels.RelationalDataStore)

	e.kvLock.Lock(relationalLockKey, struct{}{})
	defer e.kvLock.Unlock(relationalLockKey)

	reader, err := rw.NewFileReader(ctx, fi, true)
	if err != nil {
		return fmt.Errorf("error creating %s file reader: %w",
			fi.File.Name(), err)
	}

	var found bool
	var upTo, from uint32
	for {
		var bs []byte
		bs, err = reader.Read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("error reading %s file: %w", fi.File.Name(), err)
		}

		if bytes.Contains(bs, fileData.Key) {
			found = true
			from = reader.Yield()
			upTo = from - uint32(len(bs)+4)
			break
		}
	}

	if !found {
		if _, err = fi.File.Seek(0, 2); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}
		if _, err = e.fileManager.WriteToRelationalFileWithNoSeek(ctx, fi.File, fileData); err != nil {
			return fmt.Errorf("error writing info into file %s: %w", fi.File.Name(), err)
		}

		return nil
	}

	if _, err = fi.File.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking to file: %w", err)
	}

	var tmpFile *os.File
	{
		relationalTmpPath := ltngenginemodels.GetTmpDelDataPathWithSep(dbMetaInfo.RelationalInfo().Path)
		if err = os.MkdirAll(relationalTmpPath, os.ModePerm); err != nil {
			return fmt.Errorf("error creating tmp delete store item directory: %w", err)
		}

		tmpFile, err = os.OpenFile(
			ltngenginemodels.RawPathWithSepForFile(relationalTmpPath, ltngenginemodels.RelationalDataStore),
			os.O_RDWR|os.O_CREATE|os.O_EXCL, ltngenginemodels.DBFilePerm,
		)
		if err != nil {
			return err
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
			return fmt.Errorf("error copying first part of the file to tmp file: %w", err)
		}

		if _, err = e.fileManager.WriteToRelationalFileWithNoSeek(ctx, tmpFile, fileData); err != nil {
			return fmt.Errorf("error writing info into relational file %s: %w", tmpFile.Name(), err)
		}

		if _, err = fi.File.Seek(int64(from), 0); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}

		if _, err = io.Copy(tmpFile, fi.File); err != nil {
			return fmt.Errorf("error copying second part of the file to tmp file: %w", err)
		}

		if err = tmpFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync file - %s | err: %w", tmpFile.Name(), err)
		}

		if err = tmpFile.Close(); err != nil {
			return fmt.Errorf("failed to close file - %s | err: %w", tmpFile.Name(), err)
		}

		if err = fi.File.Close(); err != nil {
			return fmt.Errorf("failed to close file - %s | err: %w", tmpFile.Name(), err)
		}

		if err = os.Rename(
			ltngenginemodels.RawPathWithSepForFile(relationalTmpPath, ltngenginemodels.RelationalDataStore),
			ltngenginemodels.GetDataFilepath(relationalPath, ltngenginemodels.RelationalDataStore),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		var file *os.File
		file, err = e.fileManager.OpenFile(ctx,
			ltngenginemodels.GetDataFilepath(relationalPath, ltngenginemodels.RelationalDataStore))
		if err != nil {
			return fmt.Errorf("error opening %s file: %w", relationalInfo.Name, err)
		}

		fi.File = file
		e.itemFileMapping.Set(relationalLockKey, fi)
	}

	return
}

func (e *LTNGEngine) deleteRelationalData(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	tmpDelPaths *temporaryDeletionPaths,
) (err error) {
	var fi *ltngenginemodels.FileInfo
	fi, err = e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return err
	}

	relationalInfo := fi.FileData.Header.StoreInfo.RelationalInfo()
	relationalPath := relationalInfo.Path
	relationalLockKey := relationalInfo.LockName(ltngenginemodels.RelationalDataStore)

	e.kvLock.Lock(relationalLockKey, struct{}{})
	defer e.kvLock.Unlock(relationalLockKey)

	reader, err := rw.NewFileReader(ctx, fi, true)
	if err != nil {
		return fmt.Errorf("error creating %s file reader: %w",
			fi.File.Name(), err)
	}

	var deleted bool
	var upTo, from uint32
	for {
		var bs []byte
		bs, err = reader.Read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("error reading %s file: %w", fi.File.Name(), err)
		}

		if bytes.Contains(bs, item.Key) {
			deleted = true
			from = reader.Yield()
			upTo = from - uint32(len(bs)+4)
			break
		}
	}

	if !deleted {
		return fmt.Errorf("key %v not deleted: key not found", item.Key)
	}

	if _, err = fi.File.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking to file: %w", err)
	}

	var tmpFile *os.File
	{
		tmpFilePath := tmpDelPaths.relationalTmpDelPath
		tmpFile, err = os.OpenFile(
			ltngenginemodels.RawPathWithSepForFile(tmpFilePath, ltngenginemodels.RelationalDataStore),
			os.O_RDWR|os.O_CREATE|os.O_EXCL, ltngenginemodels.DBFilePerm,
		)
		if err != nil {
			return err
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
			return fmt.Errorf("error copying first part of the file to tmp file: %w", err)
		}

		if _, err = fi.File.Seek(int64(from), 0); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}

		if _, err = io.Copy(tmpFile, fi.File); err != nil {
			return fmt.Errorf("error copying second part of the file to tmp file: %w", err)
		}

		if err = tmpFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync file - %s | err: %w", tmpFile.Name(), err)
		}

		if err = tmpFile.Close(); err != nil {
			return fmt.Errorf("failed to close file - %s | err: %w", tmpFile.Name(), err)
		}

		if err = os.Rename(
			ltngenginemodels.RawPathWithSepForFile(tmpFilePath, ltngenginemodels.RelationalDataStore),
			ltngenginemodels.GetDataFilepath(relationalPath, ltngenginemodels.RelationalDataStore),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		var file *os.File
		file, err = e.fileManager.OpenFile(ctx,
			ltngenginemodels.GetDataFilepath(relationalPath, ltngenginemodels.RelationalDataStore))
		if err != nil {
			return fmt.Errorf("error opening %s file: %w", relationalInfo.Name, err)
		}

		fi.File = file
		e.itemFileMapping.Set(relationalLockKey, fi)
	}

	return
}

// #####################################################################################################################
