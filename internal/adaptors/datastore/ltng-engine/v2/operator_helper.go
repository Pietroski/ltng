package v2

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
)

func (e *LTNGEngine) createRelationalItemStore(
	ctx context.Context,
	info *ltngdata.StoreInfo,
) (*ltngdata.FileInfo, error) {
	file, err := e.fileManager.CreateFileIfNotExists(ctx,
		ltngdata.GetRelationalDataFilepath(info.Path, ltngdata.RelationalDataStoreKey),
	)
	if err != nil {
		return nil, err
	}

	timeNow := time.Now().UTC().Unix()
	fileData := &ltngdata.FileData{
		Key: []byte(ltngdata.RelationalDataStoreKey),
		Header: &ltngdata.Header{
			ItemInfo: &ltngdata.ItemInfo{
				CreatedAt: timeNow,
			},
			StoreInfo: info,
		},
		Data: nil,
	}

	bs, err := e.fileManager.WriteToRelationalFile(ctx, file, fileData)
	if err != nil {
		return nil, err
	}

	fi := &ltngdata.FileInfo{
		File:       file,
		FileData:   fileData,
		HeaderSize: uint32(len(bs)),
		DataSize:   uint32(len(bs)),
	}
	e.itemFileMapping.Set(info.RelationalInfo().LockName(ltngdata.RelationalDataStoreKey), fi)

	return fi, nil
}

// #####################################################################################################################

func (e *LTNGEngine) loadItemFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
) (*ltngdata.Item, error) {
	strItemKey := hex.EncodeToString(item.Key)
	fi, ok := e.itemFileMapping.Get(dbMetaInfo.LockName(strItemKey))
	if !ok {
		var err error
		fi, err = e.loadItemFromDisk(ctx, dbMetaInfo, item)
		if err != nil {
			return nil, err
		}
		e.itemFileMapping.Set(dbMetaInfo.LockName(strItemKey), fi)

		return &ltngdata.Item{
			Key:   fi.FileData.Key,
			Value: fi.FileData.Data,
		}, nil
	}

	//if fi.FileData != nil {
	//	return &ltngdata.Item{
	//		Key:   fi.FileData.Key,
	//		Value: fi.FileData.Data,
	//	}, nil
	//}

	bs, err := e.fileManager.ReadAll(ctx, fi.File)
	if err != nil {
		return nil, err
	}

	var itemFileData ltngdata.FileData
	err = e.serializer.Deserialize(bs, &itemFileData)
	if err != nil {
		return nil, err
	}

	//fi.FileData = &itemFileData
	//e.itemFileMapping.Set(dbMetaInfo.LockName(strItemKey), fi)

	return &ltngdata.Item{
		Key:   itemFileData.Key,
		Value: itemFileData.Data,
	}, nil
}

func (e *LTNGEngine) loadItemFromDisk(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
) (*ltngdata.FileInfo, error) {
	strItemKey := hex.EncodeToString(item.Key)
	filepath := ltngdata.GetDataFilepath(dbMetaInfo.Path, strItemKey)
	bs, file, err := e.fileManager.OpenReadWholeFile(ctx, filepath)
	if err != nil {
		return nil, errorsx.Wrapf(err, "error openning to read whole file %s", filepath)
	}

	var fileData ltngdata.FileData
	err = e.serializer.Deserialize(bs, &fileData)
	if err != nil {
		return nil, err
	}

	return &ltngdata.FileInfo{
		File:     file,
		FileData: &fileData,
		DataSize: uint32(len(fileData.Data)),
	}, nil
}

// #####################################################################################################################

func (e *LTNGEngine) loadRelationalItemStoreFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
) (*ltngdata.FileInfo, error) {
	relationalLockKey := dbMetaInfo.RelationalInfo().LockName(ltngdata.RelationalDataStoreKey)
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

	return fi, nil
}

func (e *LTNGEngine) loadRelationalItemStoreFromDisk(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
) (*ltngdata.FileInfo, error) {
	f, err := e.fileManager.OpenFile(ctx,
		ltngdata.GetRelationalDataFilepath(dbMetaInfo.Path, ltngdata.RelationalDataStoreKey))
	if err != nil {
		return nil, err
	}

	fi := &ltngdata.FileInfo{
		File: f,
	}

	// TODO: consider using sync pool here?
	reader, err := rw.NewFileReader(ctx, fi, true)
	if err != nil {
		return nil, err
	}

	var fileData ltngdata.FileData
	err = e.serializer.Deserialize(reader.RawHeader, &fileData)
	if err != nil {
		return nil, err
	}

	fi.FileData = &fileData
	fi.HeaderSize = uint32(len(reader.RawHeader))
	fi.DataSize = uint32(len(fileData.Data))

	return fi, nil
}

// #####################################################################################################################

var itemMarkedAsDeletedErr = errorsx.New("item already marked as deleted")

func (e *LTNGEngine) searchMemoryFirst(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
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
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
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
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	_ *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	for _, indexItem := range opts.IndexingKeys {
		strItemKey := hex.EncodeToString(indexItem)
		lockKey := dbMetaInfo.IndexInfo().LockName(strItemKey)
		_, ok := e.markedAsDeletedMapping.Get(lockKey)
		if ok {
			return nil, errorsx.Wrapf(itemMarkedAsDeletedErr, "item %s is marked as deleted", indexItem)
		}

		if i, err := e.memoryStore.LoadItem(
			ctx, dbMetaInfo, &ltngdata.Item{
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
	opts *ltngdata.IndexOpts,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
) (*ltngdata.Item, error) {
	key := opts.ParentKey
	if key == nil && (opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0) {
		return nil, errorsx.New("invalid indexing key")
	} else if key == nil {
		key = opts.IndexingKeys[0]
	}

	mainKeyValue, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &ltngdata.Item{
		Key: key,
	})
	if err != nil {
		return nil, err
	}

	item, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &ltngdata.Item{
		Key: mainKeyValue.Value,
	})
	if err != nil {
		return nil, err
	}

	return &ltngdata.Item{
		Key:   item.Key,
		Value: item.Value,
	}, nil
}

func (e *LTNGEngine) andComputationalSearch(
	ctx context.Context,
	opts *ltngdata.IndexOpts,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
) (*ltngdata.Item, error) {
	var parentKey []byte
	for _, key := range opts.IndexingKeys {
		keyValue, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &ltngdata.Item{
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

	return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &ltngdata.Item{
		Key: parentKey,
	})
}

func (e *LTNGEngine) orComputationalSearch(
	ctx context.Context,
	opts *ltngdata.IndexOpts,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
) (*ltngdata.Item, error) {
	for _, key := range opts.IndexingKeys {
		parentItem, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &ltngdata.Item{
			Key: key,
		})
		if err != nil {
			continue
		}

		return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &ltngdata.Item{
			Key: parentItem.Value,
		})
	}

	return nil, fmt.Errorf("no keys found")
}

// #####################################################################################################################'

func (e *LTNGEngine) listPaginatedItems(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	pagination *ltngdata.Pagination,
) (*ltngdata.ListItemsResult, error) {
	var matchBox []*ltngdata.Item
	var idx, limit uint64
	if pagination.IsValid() {
		if pagination.PaginationCursor > 0 {
			return e.listPaginatedItemsFromCursor(ctx, dbMetaInfo, pagination)
		}

		idx = (pagination.PageID - 1) * pagination.PageSize
		limit = pagination.PageID * pagination.PageSize

		matchBox = make([]*ltngdata.Item, limit)
	} else {
		return nil, errorsx.New("invalid pagination")
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

		var fileData ltngdata.FileData
		if err = e.serializer.Deserialize(bs, &fileData); err != nil {
			return nil, err
		}

		if _, ok := e.markedAsDeletedMapping.Get(dbMetaInfo.LockName(hex.EncodeToString(fileData.Key))); ok {
			continue
		}

		matchBox[idx] = &ltngdata.Item{
			Key:   fileData.Key,
			Value: fileData.Data,
		}
		count++
	}

	return &ltngdata.ListItemsResult{
		Items: matchBox[:count],
		Pagination: &ltngdata.Pagination{
			PageID:           pagination.PageID + 1,
			PageSize:         pagination.PageSize,
			PaginationCursor: uint64(reader.Cursor),
		},
	}, nil
}

func (e *LTNGEngine) listPaginatedItemsFromCursor(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	pagination *ltngdata.Pagination,
) (*ltngdata.ListItemsResult, error) {
	var matchBox []*ltngdata.Item
	var idx, limit uint64

	if pagination.IsValid() {
		idx = (pagination.PageID - 1) * pagination.PageSize
		limit = idx * pagination.PageSize

		matchBox = make([]*ltngdata.Item, limit)
	} else {
		return nil, errorsx.New("invalid pagination")
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

		var fileData ltngdata.FileData
		if err = e.serializer.Deserialize(bs, &fileData); err != nil {
			return nil, err
		}

		matchBox[idx] = &ltngdata.Item{
			Key:   fileData.Key,
			Value: fileData.Data,
		}
	}

	return &ltngdata.ListItemsResult{
		Items: matchBox,
		Pagination: &ltngdata.Pagination{
			PageID:           pagination.PageID + 1,
			PageSize:         pagination.PageSize,
			PaginationCursor: uint64(reader.Cursor),
		},
	}, nil
}

func (e *LTNGEngine) listAllItems(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
) (*ltngdata.ListItemsResult, error) {
	var matchBox []*ltngdata.Item

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

		var fileData ltngdata.FileData
		if err = e.serializer.Deserialize(bs, &fileData); err != nil {
			return nil, err
		}

		if _, ok := e.markedAsDeletedMapping.Get(dbMetaInfo.LockName(hex.EncodeToString(fileData.Key))); ok {
			continue
		}

		matchBox = append(matchBox, &ltngdata.Item{
			Key:   fileData.Key,
			Value: fileData.Data,
		})
	}

	return &ltngdata.ListItemsResult{Items: matchBox}, nil
}

// loadIndexingList it returns all the indexing list a parent key has
func (e *LTNGEngine) loadIndexingList(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	opts *ltngdata.IndexOpts,
) ([]*ltngdata.Item, error) {
	rawIndexingList, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexListInfo(), &ltngdata.Item{
		Key: opts.ParentKey,
	})
	if err != nil {
		return nil, err
	}

	indexingList := bytes.Split(rawIndexingList.Value, []byte(ltngdata.BsSep))
	itemList := make([]*ltngdata.Item, len(indexingList))
	for i, item := range indexingList {
		itemList[i] = &ltngdata.Item{
			Key:   opts.ParentKey,
			Value: item,
		}
	}

	return itemList, nil
}

// #####################################################################################################################

func (e *LTNGEngine) createItemOnDisk(
	ctx context.Context,
	filePath string,
	fileData interface{},
) (*os.File, error) {
	file, err := e.fileManager.CreateFileIfNotExists(ctx, filePath)
	if err != nil {
		return nil, errorsx.Wrapf(err, "error opening/creating a file at %s", filePath)
	}

	if _, err = e.fileManager.WriteToFile(ctx, file, fileData); err != nil {
		return nil, errorsx.Wrapf(err, "error writing to file at %s", filePath)
	}

	return file, nil
}

func (e *LTNGEngine) createRelationalItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
) error {
	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return errorsx.Wrapf(err, "error creating %s relational store",
			dbMetaInfo.RelationalInfo().Name)
	}

	fileData := ltngdata.NewFileData(dbMetaInfo, item)
	if err = e.upsertRelationalData(ctx, fileData, fi); err != nil {
		return errorsx.Wrapf(err, "error upserting relational data to %s store",
			dbMetaInfo.RelationalInfo().Name)
	}

	return nil
}

func (e *LTNGEngine) upsertItemOnDisk(
	ctx context.Context,
	filePath, tmpFilePath string,
	fileData interface{},
) (err error) {
	file, err := e.fileManager.CreateFileIfNotExists(ctx, tmpFilePath)
	if err != nil {
		return errorsx.Wrapf(err, "error opening/creating a temporary truncated file at %s", tmpFilePath)
	}

	if _, err = e.fileManager.WriteToFile(ctx, file, fileData); err != nil {
		return errorsx.Wrapf(err, "error writing to file at %s", tmpFilePath)
	}

	if err = file.Close(); err != nil {
		return errorsx.Wrapf(err, "error closing file at %s", tmpFilePath)
	}

	if err = os.Rename(tmpFilePath, filePath); err != nil {
		return errorsx.Wrapf(err, "error renaming file to %s", filePath)
	}

	return nil
}

func (e *LTNGEngine) upsertRelationalItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
) error {
	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return errorsx.Wrapf(err, "error creating %s relational store", dbMetaInfo.Name)
	}

	fileData := ltngdata.NewFileData(dbMetaInfo, item)
	return e.upsertRelationalData(ctx, fileData, fi)
}

// #####################################################################################################################

func (e *LTNGEngine) upsertRelationalData(
	ctx context.Context,
	fileData *ltngdata.FileData,
	fi *ltngdata.FileInfo,
) (err error) {
	info := fi.FileData.Header.StoreInfo.RelationalInfo()
	lockKey := info.LockName(ltngdata.RelationalDataStoreKey)

	e.kvLock.Lock(lockKey, struct{}{})
	defer e.kvLock.Unlock(lockKey)

	reader, err := rw.NewFileReader(ctx, fi, true)
	if err != nil {
		return errorsx.Wrapf(err, "error creating %s file reader", fi.File.Name())
	}

	upTo, from, err := reader.FindInFile(ctx, fileData.Key)
	if err != nil {
		if errorsx.Is(err, rw.KeyNotFoundError) {
			if _, err = fi.File.Seek(0, 2); err != nil {
				return errorsx.Wrap(err, "error seeking to file")
			}

			if _, err = e.fileManager.WriteToRelationalFile(ctx, fi.File, fileData); err != nil {
				return errorsx.Wrapf(err, "error writing info into file %s", fi.File.Name())
			}

			return nil
		}

		return errorsx.Wrapf(err, "error finding %s in file", fileData.Key)
	}

	if err = reader.SetCursor(ctx, 0); err != nil {
		return errorsx.Wrap(err, "error seeking to file")
	}

	{
		tmpFile, err := e.fileManager.OpenCreateFile(ctx, ltngdata.GetTemporaryRelationalDataFilepath(
			info.Path, ltngdata.RelationalDataStoreKey))
		if err != nil {
			return err
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
			return errorsx.Wrap(err, "error copying first part of the file to tmp file")
		}

		if _, err = e.fileManager.WriteToRelationalFile(ctx, tmpFile, fileData); err != nil {
			return errorsx.Wrapf(err, "error writing info into relational file %s", tmpFile.Name())
		}

		if _, err = fi.File.Seek(int64(from), 0); err != nil {
			return errorsx.Wrap(err, "error seeking to file")
		}

		if _, err = io.Copy(tmpFile, fi.File); err != nil {
			return errorsx.Wrap(err, "error copying second part of the file to tmp file")
		}

		if err = tmpFile.Sync(); err != nil {
			return errorsx.Wrapf(err, "failed to sync file - %s", tmpFile.Name())
		}

		if err = tmpFile.Close(); err != nil {
			return errorsx.Wrapf(err, "failed to close tmp file %s", tmpFile.Name())
		}

		if err = fi.File.Close(); err != nil {
			return errorsx.Wrapf(err, "failed to close file %s", fi.File.Name())
		}

		if err = osx.MvFile(ctx, tmpFile.Name(), ltngdata.GetRelationalDataFilepath(
			info.Path, ltngdata.RelationalDataStoreKey)); err != nil {
			return errorsx.Wrap(err, "error moving tmp file")
		}

		file, err := e.fileManager.OpenCreateFile(ctx, ltngdata.GetRelationalDataFilepath(
			info.Path, ltngdata.RelationalDataStoreKey))
		if err != nil {
			return errorsx.Wrapf(err, "error opening %s relational file", fi.File.Name())
		}

		fi.File = file
		e.itemFileMapping.Set(lockKey, fi)
	}

	return
}

func (e *LTNGEngine) deleteRelationalData(
	ctx context.Context,
	item *ltngdata.Item,
	fi *ltngdata.FileInfo,
) (err error) {
	info := fi.FileData.Header.StoreInfo.RelationalInfo()
	lockKey := info.LockName(ltngdata.RelationalDataStoreKey)

	e.kvLock.Lock(lockKey, struct{}{})
	defer e.kvLock.Unlock(lockKey)

	reader, err := rw.NewFileReader(ctx, fi, true)
	if err != nil {
		return fmt.Errorf("error creating %s file reader: %w",
			fi.File.Name(), err)
	}

	upTo, from, err := reader.FindInFile(ctx, item.Key)
	if err != nil {
		return errorsx.Wrapf(err, "error finding key: '%s' in file %s", item.Key, fi.File.Name())
	}

	if err = reader.SetCursor(ctx, 0); err != nil {
		return errorsx.Wrap(err, "error setting file cursor")
	}

	{
		tmpFile, err := e.fileManager.OpenCreateFile(ctx,
			ltngdata.GetTemporaryRelationalDataFilepath(info.Path, ltngdata.RelationalDataStoreKey))
		if err != nil {
			return err
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
			return errorsx.Wrap(err, "error copying first part of the file to tmp file")
		}

		//if _, err = e.fileManager.WriteToRelationalFile(ctx, tmpFile, info); err != nil {
		//	return errorsx.Wrapf(err, "error updating info into tmp file %s", tmpFile.Name())
		//}

		if _, err = fi.File.Seek(int64(from), 0); err != nil {
			return errorsx.Wrap(err, "error seeking to file")
		}

		if _, err = io.Copy(tmpFile, fi.File); err != nil {
			return errorsx.Wrap(err, "error copying second part of the file to tmp file")
		}

		if err = tmpFile.Close(); err != nil {
			return errorsx.Wrapf(err, "failed to close tmp file %s", tmpFile.Name())
		}

		if err = fi.File.Close(); err != nil {
			return errorsx.Wrapf(err, "failed to close file %s", fi.File.Name())
		}

		newFilePath := ltngdata.GetRelationalDataFilepath(
			info.Path, ltngdata.RelationalDataStoreKey)
		if err = osx.MvFile(ctx, tmpFile.Name(), newFilePath); err != nil {
			return errorsx.Wrap(err, "error moving tmp file")
		}

		file, err := e.fileManager.OpenCreateFile(ctx, newFilePath)
		if err != nil {
			return errorsx.Wrapf(err, "error opening %s relational file", fi.File.Name())
		}

		fi.File = file
		e.itemFileMapping.Set(lockKey, fi)
	}

	return
}

// #####################################################################################################################
