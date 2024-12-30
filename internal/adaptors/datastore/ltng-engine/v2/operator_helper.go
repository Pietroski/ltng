package v2

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/execx"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
	"io"
	"os"
	"time"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	lo "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/list-operator"
)

func (e *LTNGEngine) loadItemFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	updateRelationalData bool,
) (*ltngenginemodels.Item, error) {
	strItemKey := hex.EncodeToString(item.Key)
	fi, ok := e.itemFileMapping[dbMetaInfo.LockName(strItemKey)]
	if !ok {
		return e.loadItemFromDisk(ctx, dbMetaInfo, item, updateRelationalData)
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
) (*ltngenginemodels.Item, error) {
	strItemKey := hex.EncodeToString(item.Key)
	filepath := ltngenginemodels.GetDataFilepath(dbMetaInfo.Path, strItemKey)
	bs, file, err := e.fileManager.OpenReadWholeFile(ctx, filepath)
	if err != nil {
		return nil, fmt.Errorf("error openning to read whole file %s: %w", filepath, err)
	}

	var itemFileData ltngenginemodels.FileData
	err = e.serializer.Deserialize(bs, &itemFileData)
	if err != nil {
		return nil, err
	}
	itemFileData.Header.ItemInfo.LastOpenedAt = time.Now().UTC().Unix()

	// TODO: write to tmp file and rename and delete

	err = file.Truncate(0)
	if err != nil {
		return nil, fmt.Errorf("failed to truncate store stats file: %v", err)
	}

	if _, err = e.fileManager.WriteToFile(ctx, file, itemFileData); err != nil {
		return nil, fmt.Errorf("error writing item data to file: %v", err)
	}

	fi := &ltngenginemodels.FileInfo{
		File:     file,
		FileData: &itemFileData,
		DataSize: uint32(len(itemFileData.Data)),
	}

	if updateRelationalData {
		if err = e.updateRelationalDataFile(
			ctx, dbMetaInfo, &itemFileData,
		); err != nil {
			return nil, fmt.Errorf("failed to update store stats manager file: %v", err)
		}
	}

	e.itemFileMapping[dbMetaInfo.LockName(strItemKey)] = fi

	return &ltngenginemodels.Item{
		Key:   itemFileData.Key,
		Value: itemFileData.Data,
	}, nil
}

// #####################################################################################################################

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
	e.itemFileMapping[info.RelationalInfo().LockName(ltngenginemodels.RelationalDataStore)] = fi

	return fi, nil
}

func (e *LTNGEngine) loadRelationalItemStoreFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
) (*ltngenginemodels.FileInfo, error) {
	relationalLockKey := dbMetaInfo.RelationalInfo().LockName(ltngenginemodels.RelationalDataStore)
	fi, ok := e.itemFileMapping[relationalLockKey]
	if !ok {
		var err error
		fi, err = e.loadRelationalItemStoreFromDisk(ctx, dbMetaInfo)
		if err != nil {
			return nil, err
		}
		e.itemFileMapping[relationalLockKey] = fi

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

	return fi, e.updateRelationalData(
		ctx, dbMetaInfo, fi, &fileData,
	)
}

func (e *LTNGEngine) updateRelationalDataFile(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	fileData *ltngenginemodels.FileData,
) error {
	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return err
	}

	if err = e.updateRelationalData(
		ctx, dbMetaInfo, fi, fileData,
	); err != nil {
		return fmt.Errorf("failed to update store stats manager file: %v", err)
	}

	return nil
}

// #####################################################################################################################

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

	// TODO: sometimes dbMetaInfo others dbMetaInfo.IndexInfo
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
			return nil, fmt.Errorf("")
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
	if _, err := os.Stat(ltngenginemodels.GetDataPath(dbMetaInfo.Path)); os.IsNotExist(err) {
		return fmt.Errorf("store does not exist: %v", err)
	}

	return e.upsertItemOnDisk(ctx, dbMetaInfo, item)
}

func (e *LTNGEngine) createIndexItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
) error {
	if _, err := os.Stat(ltngenginemodels.GetDataPath(dbMetaInfo.Path)); os.IsNotExist(err) {
		return fmt.Errorf("store does not exist: %v", err)
	}

	return e.upsertIndexItemOnDisk(ctx, dbMetaInfo, item)
}

func (e *LTNGEngine) createRelationalItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
) error {
	if _, err := os.Stat(ltngenginemodels.GetDataPath(dbMetaInfo.Path)); os.IsNotExist(err) {
		return fmt.Errorf("store does not exist: %v", err)
	}

	return e.upsertRelationalItemOnDisk(ctx, dbMetaInfo, item)
}

func (e *LTNGEngine) upsertItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
) error {
	strItemKey := hex.EncodeToString(item.Key)
	filePath := ltngenginemodels.GetDataFilepath(dbMetaInfo.Path, strItemKey)

	fileData := ltngenginemodels.NewFileData(dbMetaInfo, item)
	file, err := e.fileManager.OpenCreateTruncatedFile(ctx, filePath)
	if err != nil {
		return fmt.Errorf("error opening/creating a truncated file at %s: %v", filePath, err)
	}

	if _, err = e.fileManager.WriteToFile(ctx, file, fileData); err != nil {
		return err
	}

	return nil
}

func (e *LTNGEngine) upsertIndexItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
) error {
	strItemKey := hex.EncodeToString(item.Key)
	filePath := ltngenginemodels.GetDataFilepath(dbMetaInfo.Path, strItemKey)

	// TODO: write to tmp file and then delete main and rename tmp and then delete tmp
	fileData := ltngenginemodels.NewFileData(dbMetaInfo, item)
	file, err := e.fileManager.OpenCreateTruncatedFile(ctx, filePath)
	if err != nil {
		return fmt.Errorf("error opening/creating a truncated indexed file at %s: %v", filePath, err)
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
	return e.upsertRelationalData(ctx, dbMetaInfo, fileData)
}

// #####################################################################################################################

func (e *LTNGEngine) deleteCascade(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	key []byte,
) error {
	strItemKey := hex.EncodeToString(key)
	filePath := ltngenginemodels.GetDataFilepath(dbMetaInfo.Path, strItemKey)

	delPaths, err := e.createTmpDeletionPaths(ctx, dbMetaInfo)
	if err != nil {
		return err
	}

	moveItemForDeletion := func() error {
		if _, err = execx.MvFileExec(ctx, filePath,
			ltngenginemodels.RawPathWithSepForFile(delPaths.tmpDelPath, strItemKey),
		); err != nil {
			return err
		}

		fileStats, ok := e.itemFileMapping[dbMetaInfo.LockName(strItemKey)]
		if ok {
			_ = fileStats.File.Close()
		}
		delete(e.itemFileMapping, dbMetaInfo.LockName(strItemKey))

		return nil
	}
	recreateDeletedItem := func() error {
		if _, err = execx.MvFileExec(ctx,
			ltngenginemodels.RawPathWithSepForFile(delPaths.tmpDelPath, strItemKey), filePath,
		); err != nil {
			return err
		}

		return nil
	}

	itemList, err := e.loadIndexingList(ctx, dbMetaInfo, &ltngenginemodels.IndexOpts{ParentKey: key})
	if err != nil {
		return err
	}
	moveIndexesToTmpFile := func() error {
		for _, item := range itemList {
			strItemKey := hex.EncodeToString(item.Value)

			if _, err = execx.MvFileExec(ctx,
				ltngenginemodels.GetDataFilepath(dbMetaInfo.IndexInfo().Path, strItemKey),
				ltngenginemodels.RawPathWithSepForFile(delPaths.indexTmpDelPath, strItemKey),
			); err != nil {
				return err
			}

			fileStats, ok := e.itemFileMapping[dbMetaInfo.IndexInfo().LockName(strItemKey)]
			if ok {
				_ = fileStats.File.Close()
			}
			delete(e.itemFileMapping, dbMetaInfo.IndexInfo().LockName(strItemKey))
		}

		return nil
	}
	recreateIndexes := func() error {
		for _, item := range itemList {
			if err = e.createIndexItemOnDisk(ctx, dbMetaInfo.IndexInfo(), &ltngenginemodels.Item{
				Key:   item.Value,
				Value: key,
			}); err != nil {
				return err
			}
		}

		return nil
	}

	moveIndexListToTmpFile := func() error {
		if _, err = execx.MvFileExec(ctx,
			ltngenginemodels.GetDataFilepath(dbMetaInfo.IndexListInfo().Path, strItemKey),
			ltngenginemodels.RawPathWithSepForFile(delPaths.indexListTmpDelPath, strItemKey),
		); err != nil {
			return err
		}

		fileStats, ok := e.itemFileMapping[dbMetaInfo.IndexListInfo().LockName(strItemKey)]
		if ok {
			_ = fileStats.File.Close()
		}
		delete(e.itemFileMapping, dbMetaInfo.IndexListInfo().LockName(strItemKey))

		return nil
	}
	recreateIndexListFromTmpFile := func() error {
		indexList := make([][]byte, len(itemList))
		for i, item := range itemList {
			indexList[i] = item.Value
		}
		idxList := bytes.Join(indexList, []byte(ltngenginemodels.BytesSep))
		return e.createIndexItemOnDisk(ctx, dbMetaInfo.IndexListInfo(), &ltngenginemodels.Item{
			Key:   key,
			Value: idxList,
		})
	}

	deleteFromRelationalData := func() error {
		return e.deleteRelationalData(ctx, dbMetaInfo, key)
	}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         moveItemForDeletion,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         moveIndexesToTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: recreateDeletedItem,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         moveIndexListToTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: recreateIndexes,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         deleteFromRelationalData,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: recreateIndexListFromTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}
	if err = lo.New(operations...).Operate(); err != nil {
		return err
	}

	// deleteTmpFiles
	if _, err = execx.DelDataStoreRawDirsExec(ctx, delPaths.tmpDelPath); err != nil {
		return err
	}

	return nil
}

func (e *LTNGEngine) deleteIndexOnly(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	key []byte,
) error {
	strItemKey := hex.EncodeToString(key)

	delPaths, err := e.createTmpDeletionPaths(ctx, dbMetaInfo)
	if err != nil {
		return err
	}

	item, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &ltngenginemodels.Item{Key: key}, true)
	if err != nil {
		return err
	}

	var fileData ltngenginemodels.FileData
	if err = e.serializer.Deserialize(item.Value, &fileData); err != nil {
		return err
	}

	itemList, err := e.loadIndexingList(ctx, dbMetaInfo, &ltngenginemodels.IndexOpts{ParentKey: fileData.Data})
	if err != nil {
		return err
	}

	moveIndexesToTmpFile := func() error {
		if _, err = execx.MvFileExec(ctx,
			ltngenginemodels.GetDataFilepath(dbMetaInfo.IndexInfo().Path, strItemKey),
			ltngenginemodels.GetTmpDelDataFilePath(delPaths.indexTmpDelPath, strItemKey),
		); err != nil {
			return err
		}

		return nil
	}
	recreateIndexes := func() error {
		return e.createIndexItemOnDisk(ctx, dbMetaInfo, &ltngenginemodels.Item{
			Key:   fileData.Data,
			Value: key,
		})
	}

	updateIndexList := func() error {
		var newIndexList [][]byte
		for _, item = range itemList {
			if bytes.Equal(item.Value, key) {
				continue
			}

			newIndexList = append(newIndexList, item.Value)
		}

		newIndexListBs := bytes.Join(newIndexList, []byte(ltngenginemodels.BytesSep))

		return e.upsertIndexItemOnDisk(ctx, dbMetaInfo.IndexListInfo(), &ltngenginemodels.Item{
			Key:   key,
			Value: newIndexListBs,
		})
	}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         moveIndexesToTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         updateIndexList,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: recreateIndexes,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}
	if err = lo.New(operations...).Operate(); err != nil {
		return err
	}

	// deleteTmpFiles
	if _, err = execx.DelStoreDirsExec(ctx,
		ltngenginemodels.DBTmpDelDataPath+ltngenginemodels.Sep+delPaths.tmpDelPath,
	); err != nil {
		return err
	}

	return nil
}

func (e *LTNGEngine) deleteCascadeByIdx(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	key []byte,
) error {
	item, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &ltngenginemodels.Item{Key: key}, true)
	if err != nil {
		return err
	}

	var fileData ltngenginemodels.FileData
	if err = e.serializer.Deserialize(item.Value, &fileData); err != nil {
		return err
	}

	return e.deleteCascade(ctx, dbMetaInfo, fileData.Data)
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
		return nil, fmt.Errorf("error creating tmp delete store item directory: %v", err)
	}

	indexTmpDelPath := ltngenginemodels.GetTmpDelDataPathWithSep(dbMetaInfo.IndexInfo().Path)
	if err := os.MkdirAll(indexTmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %v", err)
	}

	indexListTmpDelPath := ltngenginemodels.GetTmpDelDataPathWithSep(dbMetaInfo.IndexListInfo().Path)
	if err := os.MkdirAll(indexListTmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %v", err)
	}

	relationalTmpDelPath := ltngenginemodels.GetTmpDelDataPathWithSep(dbMetaInfo.RelationalInfo().Path)
	if err := os.MkdirAll(relationalTmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %v", err)
	}

	return &tmpDelPaths{
		tmpDelPath:           tmpDelPath,
		indexTmpDelPath:      indexTmpDelPath,
		indexListTmpDelPath:  indexListTmpDelPath,
		relationalTmpDelPath: relationalTmpDelPath,
	}, nil
}

// #####################################################################################################################

func (e *LTNGEngine) insertRelationalData(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	fileData *ltngenginemodels.FileData,
) (err error) {
	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return fmt.Errorf("error creating %s relational store: %w",
			dbMetaInfo.RelationalInfo().Name, err)
	}

	if _, err = fi.File.Seek(0, 2); err != nil {
		return fmt.Errorf("error seeking to file: %w", err)
	}

	if _, err = e.fileManager.WriteToRelationalFileWithNoSeek(ctx, fi.File, fileData); err != nil {
		return fmt.Errorf("error updating info into file %s: %w", fi.File.Name(), err)
	}

	return nil
}

func (e *LTNGEngine) updateRelationalData(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	fi *ltngenginemodels.FileInfo,
	fileData *ltngenginemodels.FileData,
) (err error) {
	strKey := hex.EncodeToString(fileData.Key)
	relationalInfo := fi.FileData.Header.StoreInfo.RelationalInfo()
	relationalPath := relationalInfo.Path
	tmpFileInfo := fi.FileData.Header.StoreInfo.TmpRelationalInfo()
	tmpFilePath := tmpFileInfo.Path
	relationalLockKey := relationalInfo.LockName(ltngenginemodels.RelationalDataStore)

	reader, err := rw.NewFileReader(ctx, fi, false)
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
		return fmt.Errorf("could not find key %s in item relational file %s", fileData.Key, fi.File.Name())
	}

	if _, err = fi.File.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking to file: %w", err)
	}

	var tmpFile *os.File
	copyToTmpFile := func() error {
		if err = os.MkdirAll(ltngenginemodels.GetDataPathWithSep(tmpFilePath), os.ModePerm); err != nil {
			return fmt.Errorf("error creating tmp file directory: %w", err)
		}

		tmpFile, err = os.OpenFile(
			ltngenginemodels.GetDataFilepath(tmpFilePath, ltngenginemodels.RelationalDataStore),
			os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, ltngenginemodels.DBFilePerm,
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

		if err = tmpFile.Close(); err != nil {
			return fmt.Errorf("failed to sync file - %s | err: %v", tmpFile.Name(), err)
		}

		return nil
	}
	discardTmpFile := func() error {
		if _, err = fi.File.Seek(0, 0); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}

		if err = os.Remove(ltngenginemodels.GetDataFilepath(tmpFilePath, strKey)); err != nil {
			return fmt.Errorf("error removing unecessary tmp %s relational file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		return nil
	}

	removeMainFile := func() error {
		if err = os.Remove(
			ltngenginemodels.GetDataFilepath(relationalPath, ltngenginemodels.RelationalDataStore),
		); err != nil {
			return fmt.Errorf("error removing %s relational item's file: %w", relationalPath, err)
		}

		return nil
	}
	reopenMainFile := func() error {
		//fi, err = e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
		//if err != nil {
		//	return fmt.Errorf(
		//		"error re-opening %s item relational store: %w",
		//		relationalLockKey, err)
		//}
		//
		//e.itemFileMapping[relationalLockKey] = fi

		return nil
	}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         copyToTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: discardTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         removeMainFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: reopenMainFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}
	if err = lo.New(operations...).Operate(); err != nil {
		return err
	}

	{ // renaming tmp file
		if err = os.Rename(
			ltngenginemodels.GetDataFilepath(tmpFilePath, ltngenginemodels.RelationalDataStore),
			ltngenginemodels.GetDataFilepath(relationalPath, ltngenginemodels.RelationalDataStore),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		// TODO: log error
		_ = os.RemoveAll(ltngenginemodels.GetDataPathWithSep(tmpFilePath))

		var file *os.File
		file, err = e.fileManager.OpenFile(ctx,
			ltngenginemodels.GetDataFilepath(relationalPath, ltngenginemodels.RelationalDataStore))
		if err != nil {
			return fmt.Errorf("error opening %s file: %w", relationalInfo.Name, err)
		}

		if _, ok := e.itemFileMapping[relationalLockKey]; !ok {
			e.itemFileMapping[relationalLockKey] = fi
		}
		fi.File = file
		e.itemFileMapping[relationalLockKey].File = file
	}

	return
}

func (e *LTNGEngine) upsertRelationalData(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	fileData *ltngenginemodels.FileData,
) (err error) {
	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return fmt.Errorf("error creating %s relational store: %w",
			dbMetaInfo.RelationalInfo().Name, err)
	}

	strKey := hex.EncodeToString(fileData.Key)
	relationalInfo := fi.FileData.Header.StoreInfo.RelationalInfo()
	relationalPath := relationalInfo.Path
	tmpFileInfo := fi.FileData.Header.StoreInfo.TmpRelationalInfo()
	tmpFilePath := tmpFileInfo.Path
	relationalLockKey := relationalInfo.LockName(ltngenginemodels.RelationalDataStore)

	e.opMtx.Lock(relationalLockKey, struct{}{})
	defer e.opMtx.Unlock(relationalLockKey)

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
		if _, err = e.fileManager.WriteToRelationalFileWithNoSeek(ctx, fi.File, fileData); err != nil {
			return fmt.Errorf("error writing info into file %s: %w", fi.File.Name(), err)
		}

		return nil
	}

	if _, err = fi.File.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking to file: %w", err)
	}

	var tmpFile *os.File
	copyToTmpFile := func() error {
		if err = os.MkdirAll(ltngenginemodels.GetDataPathWithSep(tmpFilePath), os.ModePerm); err != nil {
			return fmt.Errorf("error creating tmp file directory: %w", err)
		}

		tmpFile, err = os.OpenFile(
			ltngenginemodels.GetDataFilepath(tmpFilePath, ltngenginemodels.RelationalDataStore),
			os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, ltngenginemodels.DBFilePerm,
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

		if err = tmpFile.Close(); err != nil {
			return fmt.Errorf("failed to sync file - %s | err: %v", tmpFile.Name(), err)
		}

		return nil
	}
	discardTmpFile := func() error {
		if _, err = fi.File.Seek(0, 0); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}

		if err = os.Remove(ltngenginemodels.GetDataFilepath(tmpFilePath, strKey)); err != nil {
			return fmt.Errorf("error removing unecessary tmp %s relational file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		return nil
	}

	removeMainFile := func() error {
		if err = os.Remove(ltngenginemodels.GetDataFilepath(relationalPath, strKey)); err != nil {
			return fmt.Errorf("error removing %s relational item's file: %w", relationalPath, err)
		}

		return nil
	}
	reopenMainFile := func() error {
		fi, err = e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
		if err != nil {
			return fmt.Errorf(
				"error re-opening %s item relational store: %w",
				relationalPath, err)
		}

		e.itemFileMapping[relationalLockKey] = fi

		return nil
	}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         copyToTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: discardTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         removeMainFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: reopenMainFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}
	if err = lo.New(operations...).Operate(); err != nil {
		return err
	}

	{ // renaming tmp file
		if err = os.Rename(
			ltngenginemodels.GetDataFilepath(tmpFilePath, ltngenginemodels.RelationalDataStore),
			ltngenginemodels.GetDataFilepath(relationalPath, ltngenginemodels.RelationalDataStore),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		// TODO: log error
		_ = os.RemoveAll(ltngenginemodels.GetDataPathWithSep(tmpFilePath))

		var file *os.File
		file, err = e.fileManager.OpenFile(ctx, ltngenginemodels.GetDataFilepath(relationalPath, ltngenginemodels.RelationalDataStore))
		if err != nil {
			return fmt.Errorf("error opening %s file: %w", relationalInfo.Name, err)
		}

		if _, ok := e.itemFileMapping[relationalLockKey]; !ok {
			e.itemFileMapping[relationalLockKey] = fi
		}
		fi.File = file
		e.itemFileMapping[relationalLockKey].File = file
	}

	return
}

func (e *LTNGEngine) deleteRelationalData(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	key []byte,
) (err error) {
	var fi *ltngenginemodels.FileInfo
	fi, err = e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return err
	}

	strKey := hex.EncodeToString(key)
	relationalInfo := fi.FileData.Header.StoreInfo.RelationalInfo()
	relationalPath := relationalInfo.Path
	relationalFilePath := relationalPath + ltngenginemodels.Sep + strKey + ltngenginemodels.Ext
	tmpFileInfo := fi.FileData.Header.StoreInfo.TmpRelationalInfo()
	tmpFilePath := tmpFileInfo.Path
	relationalLockKey := relationalInfo.LockName(ltngenginemodels.RelationalDataStore)

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

		if bytes.Contains(bs, key) {
			deleted = true
			from = reader.Yield()
			upTo = from - uint32(len(bs)+4)
			break
		}
	}

	if !deleted {
		return fmt.Errorf("key %v not deleted: key not found", key)
	}

	if _, err = fi.File.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking to file: %w", err)
	}

	var tmpFile *os.File
	copyToTmpFile := func() error {
		if err = os.MkdirAll(ltngenginemodels.GetDataPathWithSep(tmpFilePath), os.ModePerm); err != nil {
			return fmt.Errorf("error creating tmp file directory: %w", err)
		}

		tmpFile, err = os.OpenFile(
			ltngenginemodels.GetDataFilepath(tmpFilePath, ltngenginemodels.RelationalDataStore),
			os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, ltngenginemodels.DBFilePerm,
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

		if err = tmpFile.Close(); err != nil {
			return fmt.Errorf("failed to sync file - %s | err: %v", tmpFile.Name(), err)
		}

		return nil
	}
	discardTmpFile := func() error {
		if _, err = fi.File.Seek(0, 0); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}

		if err = os.Remove(ltngenginemodels.GetDataFilepath(tmpFilePath, strKey)); err != nil {
			return fmt.Errorf(
				"error removing unecessary tmp %s item file: %w",
				relationalFilePath, err)
		}

		return nil
	}

	removeMainFile := func() error {
		if err = os.Remove(ltngenginemodels.GetDataFilepath(relationalPath, ltngenginemodels.RelationalDataStore)); err != nil {
			return fmt.Errorf("error removing %s relational item's file: %w", relationalPath, err)
		}

		return nil
	}
	reopenMainFile := func() error {
		fi, err = e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
		if err != nil {
			return fmt.Errorf(
				"error re-opening %s item relational store: %w",
				relationalFilePath, err)
		}

		e.itemFileMapping[relationalLockKey] = fi

		return nil
	}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         copyToTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: discardTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         removeMainFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: reopenMainFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}
	if err = lo.New(operations...).Operate(); err != nil {
		return err
	}

	{ // renaming tmp file
		if err = os.Rename(
			ltngenginemodels.GetDataFilepath(tmpFilePath, ltngenginemodels.RelationalDataStore),
			ltngenginemodels.GetDataFilepath(relationalPath, ltngenginemodels.RelationalDataStore),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		// TODO: log error
		_ = os.RemoveAll(ltngenginemodels.GetDataPathWithSep(tmpFilePath))

		var file *os.File
		file, err = e.fileManager.OpenFile(ctx, ltngenginemodels.GetDataFilepath(relationalPath, ltngenginemodels.RelationalDataStore))
		if err != nil {
			return fmt.Errorf("error opening %s file: %w", relationalInfo.Name, err)
		}

		if _, ok := e.itemFileMapping[relationalLockKey]; !ok {
			e.itemFileMapping[relationalLockKey] = fi
		}
		fi.File = file
		e.itemFileMapping[relationalLockKey].File = file
	}

	return
}
