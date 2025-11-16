package ltngdbenginev3

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
	fileiomodels "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/models"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

func (e *LTNGEngine) createRelationalItemStore(
	_ context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) (*ltngdbenginemodelsv3.RelationalFileInfo, error) {
	file, err := osx.CreateFileIfNotExists(ltngdbenginemodelsv3.GetRelationalDataFilepath(info.Path, ltngdbenginemodelsv3.RelationalDataStoreKey))
	if err != nil {
		return nil, err
	}

	timeNow := time.Now().UTC().Unix()
	fileData := &ltngdbenginemodelsv3.FileData{
		Key: []byte(ltngdbenginemodelsv3.RelationalDataStoreKey),
		Header: &ltngdbenginemodelsv3.Header{
			ItemInfo: &ltngdbenginemodelsv3.ItemInfo{
				CreatedAt: timeNow,
			},
			StoreInfo: info,
		},
		Data: nil,
	}

	rfm, err := mmap.NewRelationalFileManagerFromFile(file)
	if err != nil {
		return nil, err
	}

	if _, err = rfm.Write(fileData); err != nil {
		return nil, err
	}

	rfi := &ltngdbenginemodelsv3.RelationalFileInfo{
		File:                  file,
		FileData:              fileData,
		RelationalFileManager: rfm,
	}
	e.relationalItemFileMapping.Set(info.RelationalInfo().LockName(ltngdbenginemodelsv3.RelationalDataStoreKey), rfi)

	return rfi, nil
}

// #####################################################################################################################

func (e *LTNGEngine) loadItemFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	item *ltngdbenginemodelsv3.Item,
) (*ltngdbenginemodelsv3.Item, error) {
	fi, err := e.loadFileInfoFromMemoryOrDisk(ctx, dbMetaInfo, item)
	if err != nil {
		return nil, err
	}

	bs, err := fi.FileManager.Read()
	if err != nil {
		return nil, err
	}

	var itemFileData ltngdbenginemodelsv3.FileData
	err = e.serializer.Deserialize(bs, &itemFileData)
	if err != nil {
		return nil, err
	}

	return &ltngdbenginemodelsv3.Item{
		Key:   itemFileData.Key,
		Value: itemFileData.Data,
	}, nil
}

func (e *LTNGEngine) loadFileInfoFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	item *ltngdbenginemodelsv3.Item,
) (*ltngdbenginemodelsv3.FileInfo, error) {
	strItemKey := hex.EncodeToString(item.Key)
	fi, ok := e.itemFileMapping.Get(dbMetaInfo.LockName(strItemKey))
	if !ok {
		var err error
		fi, err = e.loadFileInfoFromDisk(ctx, dbMetaInfo, item)
		if err != nil {
			return nil, err
		}
		e.itemFileMapping.Set(dbMetaInfo.LockName(strItemKey), fi)

		return fi, nil
	}

	return fi, nil
}

func (e *LTNGEngine) loadFileInfoFromDisk(
	_ context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	item *ltngdbenginemodelsv3.Item,
) (*ltngdbenginemodelsv3.FileInfo, error) {
	strItemKey := hex.EncodeToString(item.Key)
	filepath := ltngdbenginemodelsv3.GetDataFilepath(dbMetaInfo.Path, strItemKey)
	bs, file, err := osx.OpenReadWholeFile(filepath)
	if err != nil {
		return nil, errorsx.Wrapf(err, "error openning to read whole file %s", filepath)
	}

	var fileData ltngdbenginemodelsv3.FileData
	if err = e.serializer.Deserialize(bs, &fileData); err != nil {
		return nil, err
	}

	fm, err := mmap.NewFileManagerFromFile(file)
	if err != nil {
		return nil, err
	}

	return &ltngdbenginemodelsv3.FileInfo{
		File:        file,
		FileData:    &fileData,
		FileManager: fm,
	}, nil
}

// #####################################################################################################################

func (e *LTNGEngine) loadRelationalItemStoreFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
) (*ltngdbenginemodelsv3.RelationalFileInfo, error) {
	relationalLockKey := dbMetaInfo.RelationalInfo().LockName(ltngdbenginemodelsv3.RelationalDataStoreKey)
	rfi, ok := e.relationalItemFileMapping.Get(relationalLockKey)
	if !ok {
		var err error
		rfi, err = e.loadRelationalItemStoreFromDisk(ctx, dbMetaInfo)
		if err != nil {
			return nil, err
		}
		e.relationalItemFileMapping.Set(relationalLockKey, rfi)

		return rfi, nil
	}

	return rfi, nil
}

func (e *LTNGEngine) loadRelationalItemStoreFromDisk(
	_ context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
) (*ltngdbenginemodelsv3.RelationalFileInfo, error) {
	file, err := osx.OpenFile(ltngdbenginemodelsv3.GetRelationalDataFilepath(dbMetaInfo.Path, ltngdbenginemodelsv3.RelationalDataStoreKey))
	if err != nil {
		return nil, err
	}

	rfm, err := mmap.NewRelationalFileManagerFromFile(file)
	if err != nil {
		return nil, err
	}

	bs, err := rfm.Read()
	if err != nil {
		return nil, err
	}

	var fileData ltngdbenginemodelsv3.FileData
	if err = e.serializer.Deserialize(bs, &fileData); err != nil {
		return nil, err
	}

	return &ltngdbenginemodelsv3.RelationalFileInfo{
		File:                  file,
		FileData:              &fileData,
		RelationalFileManager: rfm,
	}, nil
}

// #####################################################################################################################

var itemMarkedAsDeletedErr = errorsx.New("item already marked as deleted")

func (e *LTNGEngine) searchMemoryFirst(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	item *ltngdbenginemodelsv3.Item,
	opts *ltngdbenginemodelsv3.IndexOpts,
) (*ltngdbenginemodelsv3.Item, error) {
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
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	item *ltngdbenginemodelsv3.Item,
	opts *ltngdbenginemodelsv3.IndexOpts,
) (*ltngdbenginemodelsv3.Item, error) {
	var strItemKey string
	if item != nil {
		strItemKey = hex.EncodeToString(item.Key)
	} else if opts != nil && opts.ParentKey != nil {
		strItemKey = hex.EncodeToString(opts.ParentKey)
	} else {
		return nil, errorsx.New("invalid search filter: item and opts are null")
	}

	lockKey := dbMetaInfo.LockName(strItemKey)
	if _, ok := e.markedAsDeletedMapping.Get(lockKey); ok {
		return nil, errorsx.Wrapf(itemMarkedAsDeletedErr, "%+v is marked as deleted", *opts)
	}

	if i, err := e.memoryStore.LoadItem(
		ctx, dbMetaInfo, item, opts,
	); err == nil && i != nil {
		return i, nil
	}

	return nil, errorsx.New("no items in memory")
}

func (e *LTNGEngine) searchMemoryByIndex(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	_ *ltngdbenginemodelsv3.Item,
	opts *ltngdbenginemodelsv3.IndexOpts,
) (*ltngdbenginemodelsv3.Item, error) {
	for _, indexItem := range opts.IndexingKeys {
		strItemKey := hex.EncodeToString(indexItem)
		lockKey := dbMetaInfo.IndexInfo().LockName(strItemKey)
		_, ok := e.markedAsDeletedMapping.Get(lockKey)
		if ok {
			return nil, errorsx.Wrapf(itemMarkedAsDeletedErr, "item %s is marked as deleted", indexItem)
		}

		if i, err := e.memoryStore.LoadItem(
			ctx, dbMetaInfo, &ltngdbenginemodelsv3.Item{
				Key: indexItem,
			}, opts,
		); err == nil && i != nil {
			return i, nil
		}
	}

	return nil, errorsx.New("no items in memory")
}

// straightSearch finds the value from the index store with the [0] index list item;
// that found value is the key to the (main) store that holds value that needs to be returned
func (e *LTNGEngine) straightSearch(
	ctx context.Context,
	opts *ltngdbenginemodelsv3.IndexOpts,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
) (*ltngdbenginemodelsv3.Item, error) {
	key := opts.ParentKey
	if key == nil && (opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0) {
		return nil, errorsx.New("invalid indexing key")
	} else if key == nil {
		key = opts.IndexingKeys[0]
	}

	mainKeyValue, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &ltngdbenginemodelsv3.Item{
		Key: key,
	})
	if err != nil {
		return nil, err
	}

	item, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &ltngdbenginemodelsv3.Item{
		Key: mainKeyValue.Value,
	})
	if err != nil {
		return nil, err
	}

	return &ltngdbenginemodelsv3.Item{
		Key:   item.Key,
		Value: item.Value,
	}, nil
}

func (e *LTNGEngine) andComputationalSearch(
	ctx context.Context,
	opts *ltngdbenginemodelsv3.IndexOpts,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
) (*ltngdbenginemodelsv3.Item, error) {
	var parentKey []byte
	for _, key := range opts.IndexingKeys {
		keyValue, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &ltngdbenginemodelsv3.Item{
			Key: key,
		})
		if err != nil {
			return nil, err
		}

		if !bytes.Equal(keyValue.Value, parentKey) && parentKey != nil {
			return nil, errorsx.New("not found")
		}

		parentKey = keyValue.Value
	}

	return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &ltngdbenginemodelsv3.Item{
		Key: parentKey,
	})
}

func (e *LTNGEngine) orComputationalSearch(
	ctx context.Context,
	opts *ltngdbenginemodelsv3.IndexOpts,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
) (*ltngdbenginemodelsv3.Item, error) {
	for _, key := range opts.IndexingKeys {
		parentItem, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &ltngdbenginemodelsv3.Item{
			Key: key,
		})
		if err != nil {
			continue
		}

		return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &ltngdbenginemodelsv3.Item{
			Key: parentItem.Value,
		})
	}

	return nil, fmt.Errorf("no keys found")
}

// #####################################################################################################################'

func (e *LTNGEngine) listPaginatedItems(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	pagination *ltngdata.Pagination,
) (*ltngdbenginemodelsv3.ListItemsResult, error) {
	var matchBox []*ltngdbenginemodelsv3.Item
	var idx, limit uint64
	if pagination.IsValid() {
		if pagination.PaginationCursor > 0 {
			return e.listPaginatedItemsFromCursor(ctx, dbMetaInfo, pagination)
		}

		idx = (pagination.PageID - 1) * pagination.PageSize
		limit = pagination.PageID * pagination.PageSize

		matchBox = make([]*ltngdbenginemodelsv3.Item, limit)
	} else {
		return nil, errorsx.New("invalid pagination")
	}

	rfi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return nil, err
	}

	if err = rfi.RelationalFileManager.Seek(idx); err != nil {
		return nil, err
	}

	var count uint64
	for ; idx < limit; idx++ {
		var bs []byte

		bs, err = rfi.RelationalFileManager.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		var fileData ltngdbenginemodelsv3.FileData
		if err = e.serializer.Deserialize(bs, &fileData); err != nil {
			return nil, err
		}

		if _, ok := e.markedAsDeletedMapping.Get(dbMetaInfo.LockName(hex.EncodeToString(fileData.Key))); ok {
			continue
		}

		matchBox[idx] = &ltngdbenginemodelsv3.Item{
			Key:   fileData.Key,
			Value: fileData.Data,
		}

		count++
	}

	return &ltngdbenginemodelsv3.ListItemsResult{
		Items: matchBox[:count],
		Pagination: &ltngdata.Pagination{
			PageID:           pagination.PageID + 1,
			PageSize:         pagination.PageSize,
			PaginationCursor: pagination.PaginationCursor + count,
		},
	}, nil
}

func (e *LTNGEngine) listPaginatedItemsFromCursor(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	pagination *ltngdata.Pagination,
) (*ltngdbenginemodelsv3.ListItemsResult, error) {
	var matchBox []*ltngdbenginemodelsv3.Item
	var idx, limit uint64

	if pagination.IsValid() {
		idx = (pagination.PageID - 1) * pagination.PageSize
		limit = idx * pagination.PageSize

		matchBox = make([]*ltngdbenginemodelsv3.Item, limit)
	} else {
		return nil, errorsx.New("invalid pagination")
	}

	rfi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return nil, err
	}

	if err = rfi.RelationalFileManager.Seek(pagination.PaginationCursor); err != nil {
		return nil, err
	}

	var count uint64
	for ; idx < limit; idx++ {
		var bs []byte
		bs, err = rfi.RelationalFileManager.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		var fileData ltngdbenginemodelsv3.FileData
		if err = e.serializer.Deserialize(bs, &fileData); err != nil {
			return nil, err
		}

		matchBox[idx] = &ltngdbenginemodelsv3.Item{
			Key:   fileData.Key,
			Value: fileData.Data,
		}

		count++
	}

	return &ltngdbenginemodelsv3.ListItemsResult{
		Items: matchBox,
		Pagination: &ltngdata.Pagination{
			PageID:           pagination.PageID + 1,
			PageSize:         pagination.PageSize,
			PaginationCursor: pagination.PaginationCursor + count,
		},
	}, nil
}

func (e *LTNGEngine) listAllItems(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
) (*ltngdbenginemodelsv3.ListItemsResult, error) {
	var matchBox []*ltngdbenginemodelsv3.Item

	rfi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return nil, err
	}

	for {
		var bs []byte
		bs, err = rfi.RelationalFileManager.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		var fileData ltngdbenginemodelsv3.FileData
		if err = e.serializer.Deserialize(bs, &fileData); err != nil {
			return nil, err
		}

		if _, ok := e.markedAsDeletedMapping.Get(dbMetaInfo.LockName(hex.EncodeToString(fileData.Key))); ok {
			continue
		}

		matchBox = append(matchBox, &ltngdbenginemodelsv3.Item{
			Key:   fileData.Key,
			Value: fileData.Data,
		})
	}

	return &ltngdbenginemodelsv3.ListItemsResult{Items: matchBox}, nil
}

// loadIndexingList it returns all the indexing list a parent key has
func (e *LTNGEngine) loadIndexingList(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	opts *ltngdbenginemodelsv3.IndexOpts,
) ([]*ltngdbenginemodelsv3.Item, error) {
	rawIndexingList, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexListInfo(), &ltngdbenginemodelsv3.Item{
		Key: opts.ParentKey,
	})
	if err != nil {
		return nil, err
	}

	indexingList := bytes.Split(rawIndexingList.Value, []byte(ltngdbenginemodelsv3.BsSep))
	itemList := make([]*ltngdbenginemodelsv3.Item, len(indexingList))
	for i, item := range indexingList {
		itemList[i] = &ltngdbenginemodelsv3.Item{
			Key:   opts.ParentKey,
			Value: item,
		}
	}

	return itemList, nil
}

// #####################################################################################################################

func (e *LTNGEngine) createItemOnDisk(
	_ context.Context,
	filePath string,
	fileData *ltngdbenginemodelsv3.FileData,
) (*ltngdbenginemodelsv3.FileInfo, error) {
	file, err := osx.CreateFileIfNotExists(filePath)
	if err != nil {
		return nil, errorsx.Wrapf(err, "error opening/creating a file at %s", filePath)
	}

	fm, err := mmap.NewFileManagerFromFile(file)
	if err != nil {
		return nil, errorsx.Wrapf(err, "error creating file manager from file at %s", filePath)
	}

	if _, err = fm.Write(fileData); err != nil {
		return nil, errorsx.Wrapf(err, "error writing to file at %s", filePath)
	}

	return &ltngdbenginemodelsv3.FileInfo{
		File:        file,
		FileData:    fileData,
		FileManager: fm,
	}, nil
}

func (e *LTNGEngine) createRelationalItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	item *ltngdbenginemodelsv3.Item,
) error {
	rfi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return errorsx.Wrapf(err, "error loading %s relational store",
			dbMetaInfo.RelationalInfo().Name)
	}

	fileData := ltngdbenginemodelsv3.NewFileData(dbMetaInfo, item)
	if err = e.upsertRelationalData(ctx, fileData, rfi); err != nil {
		return errorsx.Wrapf(err, "error upserting relational data to %s store",
			dbMetaInfo.RelationalInfo().Name)
	}

	return nil
}

func (e *LTNGEngine) upsertRelationalItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	item *ltngdbenginemodelsv3.Item,
) error {
	rfi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return errorsx.Wrapf(err, "error loading %s relational store", dbMetaInfo.Name)
	}

	fileData := ltngdbenginemodelsv3.NewFileData(dbMetaInfo, item)
	return e.upsertRelationalData(ctx, fileData, rfi)
}

func (e *LTNGEngine) upsertItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngdbenginemodelsv3.ManagerStoreMetaInfo,
	item *ltngdbenginemodelsv3.Item,
) (err error) {
	fi, err := e.loadFileInfoFromMemoryOrDisk(ctx, dbMetaInfo, item)
	if err != nil {
		return err
	}

	fileData := ltngdbenginemodelsv3.NewFileData(dbMetaInfo, item)
	if _, err = fi.FileManager.Rewrite(fileData); err != nil {
		return err
	}

	return nil
}

// #####################################################################################################################

func (e *LTNGEngine) upsertRelationalData(
	ctx context.Context,
	fileData *ltngdbenginemodelsv3.FileData,
	rfi *ltngdbenginemodelsv3.RelationalFileInfo,
) error {
	info := rfi.FileData.Header.StoreInfo.RelationalInfo()
	lockKey := info.LockName(ltngdbenginemodelsv3.RelationalDataStoreKey)

	e.kvLock.Lock(lockKey, struct{}{})
	defer e.kvLock.Unlock(lockKey)

	if _, err := rfi.RelationalFileManager.Find(ctx, fileData.Key); err != nil {
		if errorsx.Is(err, fileiomodels.KeyNotFoundError) {
			if _, err = rfi.RelationalFileManager.Write(fileData); err != nil {
				return errorsx.Wrapf(err, "error writing info into file %s", rfi.File.Name())
			}

			return nil
		}

		return errorsx.Wrapf(err, "error finding %s in file", fileData.Key)
	}

	return nil
}

func (e *LTNGEngine) deleteRelationalData(
	ctx context.Context,
	item *ltngdbenginemodelsv3.Item,
	rfi *ltngdbenginemodelsv3.RelationalFileInfo,
) (err error) {
	info := rfi.FileData.Header.StoreInfo.RelationalInfo()
	lockKey := info.LockName(ltngdbenginemodelsv3.RelationalDataStoreKey)

	e.kvLock.Lock(lockKey, struct{}{})
	defer e.kvLock.Unlock(lockKey)

	if _, err = rfi.RelationalFileManager.DeleteByKey(ctx, item.Key); err != nil {
		return errorsx.Wrapf(err, "error deleting %s from file", item.Key)
	}

	return
}

// #####################################################################################################################
