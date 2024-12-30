package v1

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesx"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"time"

	ltng_engine_models "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	lo "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/list-operator"
)

func (e *LTNGEngine) loadItemFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	updateRelationalData bool,
) (*Item, error) {
	strItemKey := hex.EncodeToString(item.Key)
	fi, ok := e.itemFileMapping[dbMetaInfo.LockName(strItemKey)]
	if !ok {
		return e.loadItemFromDisk(ctx, dbMetaInfo, item, updateRelationalData)
	}

	bs, err := e.readAll(ctx, fi.File)
	if err != nil {
		return nil, err
	}

	var itemFileData FileData
	err = e.serializer.Deserialize(bs, &itemFileData)
	if err != nil {
		return nil, err
	}

	return &Item{
		Key:   itemFileData.Key,
		Value: itemFileData.Data,
	}, nil
}

func (e *LTNGEngine) loadItemFromDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	updateRelationalData bool,
) (*Item, error) {
	strItemKey := hex.EncodeToString(item.Key)
	filepath := getDataFilepath(dbMetaInfo.Path, strItemKey)
	bs, file, err := e.openReadWholeFile(ctx, filepath)
	if err != nil {
		return nil, fmt.Errorf("error openning to read whole file %s: %w", filepath, err)
	}

	var itemFileData FileData
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

	if _, err = e.writeToFile(ctx, file, itemFileData); err != nil {
		return nil, fmt.Errorf("error writing item data to file: %v", err)
	}

	fi := &fileInfo{
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

	return &Item{
		Key:   itemFileData.Key,
		Value: itemFileData.Data,
	}, nil
}

// #####################################################################################################################

func (e *LTNGEngine) createRelationalItemStore(
	ctx context.Context,
	info *StoreInfo,
) (*fileInfo, error) {
	file, err := e.openCreateTruncatedFile(ctx, getDataFilepath(info.RelationalInfo().Path, relationalDataStore))
	if err != nil {
		return nil, err
	}

	timeNow := time.Now().UTC().Unix()
	fileData := &FileData{
		Key: []byte(relationalDataStore),
		Header: &Header{
			ItemInfo: &ItemInfo{
				CreatedAt:    timeNow,
				LastOpenedAt: timeNow,
			},
			StoreInfo: info,
		},
		Data: nil,
	}

	bs, err := e.writeToRelationalFile(ctx, file, fileData)
	if err != nil {
		return nil, err
	}

	fi := &fileInfo{
		File:       file,
		FileData:   fileData,
		HeaderSize: uint32(len(bs)),
		DataSize:   uint32(len(fileData.Data)),
	}
	e.itemFileMapping[info.RelationalInfo().LockName(relationalDataStore)] = fi

	return fi, nil
}

func (e *LTNGEngine) loadRelationalItemStoreFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
) (*fileInfo, error) {
	relationalLockKey := dbMetaInfo.LockName(relationalDataStore)
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
	dbMetaInfo *ManagerStoreMetaInfo,
) (*fileInfo, error) {
	f, err := e.openFile(ctx,
		getDataFilepath(dbMetaInfo.RelationalInfo().Path, relationalDataStore))
	if err != nil {
		return nil, err
	}

	fi := &fileInfo{
		File: f,
	}
	reader, err := newFileReader(ctx, fi, true)
	if err != nil {
		return nil, err
	}

	var fileData FileData
	err = e.serializer.Deserialize(reader.rawHeader, &fileData)
	if err != nil {
		return nil, err
	}

	fileData.Header.ItemInfo.LastOpenedAt = time.Now().UTC().Unix()
	fi.FileData = &fileData
	fi.HeaderSize = uint32(len(reader.rawHeader))
	fi.DataSize = uint32(len(fileData.Data))

	return fi, e.updateRelationalData(
		ctx, dbMetaInfo, fi, &fileData,
	)
}

func (e *LTNGEngine) updateRelationalDataFile(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	fileData *FileData,
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
	opts *IndexOpts,
	dbMetaInfo *ManagerStoreMetaInfo,
) (*Item, error) {
	key := opts.ParentKey
	if key == nil && (opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0) {
		return nil, fmt.Errorf("invalid indexing key")
	} else if key == nil {
		key = opts.IndexingKeys[0]
	}

	// TODO: sometimes dbMetaInfo others dbMetaInfo.IndexInfo
	mainKeyValue, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &Item{
		Key: key,
	}, false)
	if err != nil {
		return nil, err
	}

	item, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &Item{
		Key: mainKeyValue.Value,
	}, true)
	if err != nil {
		return nil, err
	}

	return &Item{
		Key:   item.Key,
		Value: item.Value,
	}, nil
}

func (e *LTNGEngine) andComputationalSearch(
	ctx context.Context,
	opts *IndexOpts,
	dbMetaInfo *ManagerStoreMetaInfo,
) (*Item, error) {
	var parentKey []byte
	for _, key := range opts.IndexingKeys {
		keyValue, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &Item{
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

	return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &Item{
		Key: parentKey,
	}, true)
}

func (e *LTNGEngine) orComputationalSearch(
	ctx context.Context,
	opts *IndexOpts,
	dbMetaInfo *ManagerStoreMetaInfo,
) (*Item, error) {
	for _, key := range opts.IndexingKeys {
		parentItem, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &Item{
			Key: key,
		}, false)
		if err != nil {
			continue
		}

		return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &Item{
			Key: parentItem.Value,
		}, true)
	}

	return nil, fmt.Errorf("no keys found")
}

// #####################################################################################################################'

func (e *LTNGEngine) listPaginatedItems(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	pagination *ltng_engine_models.Pagination,
) (*ListItemsResult, error) {
	var matchBox []*Item
	var idx, limit uint64
	if pagination.IsValid() {
		if pagination.PaginationCursor > 0 {
			return e.listPaginatedItemsFromCursor(ctx, dbMetaInfo, pagination)
		}

		idx = (pagination.PageID - 1) * pagination.PageSize
		limit = pagination.PageID * pagination.PageSize

		matchBox = make([]*Item, limit)
	} else {
		return nil, fmt.Errorf("invalid pagination")
	}

	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return nil, err
	}

	reader, err := newFileReader(ctx, fi, true)
	if err != nil {
		return nil, err
	}

	var count int
	for ; idx < limit; idx++ {
		var bs []byte
		bs, err = reader.read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		var fileData FileData
		if err = e.serializer.Deserialize(bs, &fileData); err != nil {
			return nil, err
		}

		matchBox[idx] = &Item{
			Key:   fileData.Key,
			Value: fileData.Data,
		}
		count++
	}

	return &ListItemsResult{
		Items: matchBox[:count],
		Pagination: &ltng_engine_models.Pagination{
			PageID:           pagination.PageID + 1,
			PageSize:         pagination.PageSize,
			PaginationCursor: uint64(reader.cursor),
		},
	}, nil
}

func (e *LTNGEngine) listPaginatedItemsFromCursor(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	pagination *ltng_engine_models.Pagination,
) (*ListItemsResult, error) {
	var matchBox []*Item
	var idx, limit uint64

	if pagination.IsValid() {
		idx = (pagination.PageID - 1) * pagination.PageSize
		limit = idx * pagination.PageSize

		matchBox = make([]*Item, limit)
	} else {
		return nil, fmt.Errorf("invalid pagination")
	}

	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return nil, err
	}

	reader, err := newFileReader(ctx, fi, false)
	if err != nil {
		return nil, err
	}

	if err = reader.setCursor(ctx, pagination.PaginationCursor); err != nil {
		return nil, err
	}

	for ; idx < limit; idx++ {
		var bs []byte
		bs, err = reader.read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		var fileData FileData
		if err = e.serializer.Deserialize(bs, &fileData); err != nil {
			return nil, err
		}

		matchBox[idx] = &Item{
			Key:   fileData.Key,
			Value: fileData.Data,
		}
	}

	return &ListItemsResult{
		Items: matchBox,
		Pagination: &ltng_engine_models.Pagination{
			PageID:           pagination.PageID + 1,
			PageSize:         pagination.PageSize,
			PaginationCursor: uint64(reader.cursor),
		},
	}, nil
}

func (e *LTNGEngine) listAllItems(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
) (*ListItemsResult, error) {
	var matchBox []*Item

	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return nil, err
	}

	reader, err := newFileReader(ctx, fi, true)
	if err != nil {
		return nil, err
	}

	for {
		var bs []byte
		bs, err = reader.read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		var fileData FileData
		if err = e.serializer.Deserialize(bs, &fileData); err != nil {
			return nil, err
		}

		matchBox = append(matchBox, &Item{
			Key:   fileData.Key,
			Value: fileData.Data,
		})
	}

	return &ListItemsResult{Items: matchBox}, nil
}

// loadIndexingList it returns all the indexing list a parent key has
func (e *LTNGEngine) loadIndexingList(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	opts *IndexOpts,
) ([]*Item, error) {
	rawIndexingList, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexListInfo(), &Item{
		Key: opts.ParentKey,
	}, false)
	if err != nil {
		return nil, err
	}

	indexingList := bytes.Split(rawIndexingList.Value, []byte(bytesSep))
	itemList := make([]*Item, len(indexingList))
	for i, item := range indexingList {
		itemList[i] = &Item{
			Key:   opts.ParentKey,
			Value: item,
		}
	}

	return itemList, nil
}

// #####################################################################################################################

func (e *LTNGEngine) createItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
) error {
	if _, err := os.Stat(getDataPath(dbMetaInfo.Path)); os.IsNotExist(err) {
		return fmt.Errorf("store does not exist: %v", err)
	}

	return e.upsertItemOnDisk(ctx, dbMetaInfo, item)
}

func (e *LTNGEngine) createIndexItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
) error {
	if _, err := os.Stat(getDataPath(dbMetaInfo.Path)); os.IsNotExist(err) {
		return fmt.Errorf("store does not exist: %v", err)
	}

	return e.upsertIndexItemOnDisk(ctx, dbMetaInfo, item)
}

func (e *LTNGEngine) upsertItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
) error {
	strItemKey := hex.EncodeToString(item.Key)
	filePath := getDataFilepath(dbMetaInfo.Path, strItemKey)

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

	createFileItem := func() error {
		file, err := e.openCreateTruncatedFile(ctx, filePath)
		if err != nil {
			return fmt.Errorf("error opening/creating a truncated file at %s: %v", filePath, err)
		}

		if _, err = e.writeToFile(ctx, file, fileData); err != nil {
			return err
		}

		return nil
	}
	deleteFileItem := func() error {
		return os.Remove(filePath)
	}

	insertFileDataIntoRelationalStore := func() error {
		return e.upsertRelationalData(ctx, dbMetaInfo, fileData)
	}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         createFileItem,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteFileItem,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         insertFileDataIntoRelationalStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}
	if err := lo.New(operations...).Operate(); err != nil {
		return err
	}

	return nil
}

func (e *LTNGEngine) upsertIndexItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
) error {
	strItemKey := hex.EncodeToString(item.Key)
	filePath := getDataFilepath(dbMetaInfo.Path, strItemKey)

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

	// TODO: write to tmp file and then delete main and rename tmp and then delete tmp

	file, err := e.openCreateTruncatedFile(ctx, filePath)
	if err != nil {
		return fmt.Errorf("error opening/creating a truncated indexed file at %s: %v", filePath, err)
	}

	if _, err = e.writeToFile(ctx, file, fileData); err != nil {
		return err
	}

	return nil
}

// #####################################################################################################################

func (e *LTNGEngine) deleteCascade(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	key []byte,
) error {
	strItemKey := hex.EncodeToString(key)
	filePath := getDataFilepath(dbMetaInfo.Path, strItemKey)

	delPaths, err := e.createTmpDeletionPaths(ctx, dbMetaInfo)
	if err != nil {
		return err
	}

	moveItemForDeletion := func() error {
		if _, err = mvFileExec(ctx, filePath, rawPathWithSepForFile(delPaths.tmpDelPath, strItemKey)); err != nil {
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
		if _, err = mvFileExec(ctx, rawPathWithSepForFile(delPaths.tmpDelPath, strItemKey), filePath); err != nil {
			return err
		}

		return nil
	}

	itemList, err := e.loadIndexingList(ctx, dbMetaInfo, &IndexOpts{ParentKey: key})
	if err != nil {
		return err
	}
	moveIndexesToTmpFile := func() error {
		for _, item := range itemList {
			strItemKey := hex.EncodeToString(item.Value)

			if _, err = mvFileExec(ctx,
				getDataFilepath(dbMetaInfo.IndexInfo().Path, strItemKey),
				rawPathWithSepForFile(delPaths.indexTmpDelPath, strItemKey),
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
			if err = e.createIndexItemOnDisk(ctx, dbMetaInfo.IndexInfo(), &Item{
				Key:   item.Value,
				Value: key,
			}); err != nil {
				return err
			}
		}

		return nil
	}

	moveIndexListToTmpFile := func() error {
		if _, err = mvFileExec(ctx,
			getDataFilepath(dbMetaInfo.IndexListInfo().Path, strItemKey),
			rawPathWithSepForFile(delPaths.indexListTmpDelPath, strItemKey),
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
		idxList := bytes.Join(indexList, []byte(bytesSep))
		return e.createIndexItemOnDisk(ctx, dbMetaInfo.IndexListInfo(), &Item{
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
	if _, err = delDataStoreRawDirsExec(ctx, delPaths.tmpDelPath); err != nil {
		return err
	}

	return nil
}

func (e *LTNGEngine) deleteIndexOnly(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	key []byte,
) error {
	strItemKey := hex.EncodeToString(key)

	delPaths, err := e.createTmpDeletionPaths(ctx, dbMetaInfo)
	if err != nil {
		return err
	}

	item, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &Item{Key: key}, true)
	if err != nil {
		return err
	}

	var fileData FileData
	if err = e.serializer.Deserialize(item.Value, &fileData); err != nil {
		return err
	}

	itemList, err := e.loadIndexingList(ctx, dbMetaInfo, &IndexOpts{ParentKey: fileData.Data})
	if err != nil {
		return err
	}

	moveIndexesToTmpFile := func() error {
		if _, err = mvFileExec(ctx,
			getDataFilepath(dbMetaInfo.IndexInfo().Path, strItemKey),
			getTmpDelDataFilePath(delPaths.indexTmpDelPath, strItemKey),
		); err != nil {
			return err
		}

		return nil
	}
	recreateIndexes := func() error {
		return e.createIndexItemOnDisk(ctx, dbMetaInfo, &Item{
			Key:   fileData.Data,
			Value: key,
		})
	}

	updateIndexList := func() error {
		var newIndexList [][]byte
		for _, item := range itemList {
			if bytes.Equal(item.Value, key) {
				continue
			}

			newIndexList = append(newIndexList, item.Value)
		}

		newIndexListBs := bytes.Join(newIndexList, []byte(bytesSep))

		return e.upsertIndexItemOnDisk(ctx, dbMetaInfo.IndexListInfo(), &Item{
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
	if _, err = delStoreDirsExec(ctx, dbTmpDelDataPath+sep+delPaths.tmpDelPath); err != nil {
		return err
	}

	return nil
}

func (e *LTNGEngine) deleteCascadeByIdx(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	key []byte,
) error {
	item, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &Item{Key: key}, true)
	if err != nil {
		return err
	}

	var fileData FileData
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
	dbMetaInfo *ManagerStoreMetaInfo,
) (*tmpDelPaths, error) {
	tmpDelPath := getTmpDelDataPathWithSep(dbMetaInfo.Path)
	if err := os.MkdirAll(tmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %v", err)
	}

	indexTmpDelPath := getTmpDelDataPathWithSep(dbMetaInfo.IndexInfo().Path)
	if err := os.MkdirAll(indexTmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %v", err)
	}

	indexListTmpDelPath := getTmpDelDataPathWithSep(dbMetaInfo.IndexListInfo().Path)
	if err := os.MkdirAll(indexListTmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %v", err)
	}

	relationalTmpDelPath := getTmpDelDataPathWithSep(dbMetaInfo.RelationalInfo().Path)
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
	dbMetaInfo *ManagerStoreMetaInfo,
	fileData *FileData,
) (err error) {
	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return fmt.Errorf("error creating %s relational store: %w",
			dbMetaInfo.RelationalInfo().Name, err)
	}

	if _, err = fi.File.Seek(0, 2); err != nil {
		return fmt.Errorf("error seeking to file: %w", err)
	}

	if _, err = e.writeToRelationalFileWithNoSeek(ctx, fi.File, fileData); err != nil {
		return fmt.Errorf("error updating info into file %s: %w", fi.File.Name(), err)
	}

	return nil
}

func (e *LTNGEngine) updateRelationalData(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	fi *fileInfo,
	fileData *FileData,
) (err error) {
	//strKey := hex.EncodeToString(fileData.Key)
	relationalInfo := fi.FileData.Header.StoreInfo.RelationalInfo()
	relationalPath := relationalInfo.Path
	tmpFileInfo := fi.FileData.Header.StoreInfo.TmpRelationalInfo()
	tmpFilePath := tmpFileInfo.Path
	//relationalLockKey := relationalInfo.LockName(relationalDataStore)

	reader, err := newFileReader(ctx, fi, false)
	if err != nil {
		return fmt.Errorf("error creating %s file reader: %w",
			fi.File.Name(), err)
	}

	var found bool
	var upTo, from uint32
	for {
		var bs []byte
		bs, err = reader.read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("error reading %s file: %w", fi.File.Name(), err)
		}

		if bytes.Contains(bs, fileData.Key) {
			found = true
			from = reader.yield()
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

	//var tmpFile *os.File
	//copyToTmpFile := func() error {
	//	if err = os.MkdirAll(getDataPathWithSep(tmpFilePath), os.ModePerm); err != nil {
	//		return fmt.Errorf("error creating tmp file directory: %w", err)
	//	}
	//
	//	tmpFile, err = os.OpenFile(
	//		getDataFilepath(tmpFilePath, relationalDataStore),
	//		os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, dbFilePerm,
	//	)
	//	if err != nil {
	//		return err
	//	}
	//
	//	// example pair (upTo - from) | 102 - 154
	//	if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
	//		return fmt.Errorf("error copying first part of the file to tmp file: %w", err)
	//	}
	//
	//	if _, err = e.writeToRelationalFileWithNoSeek(ctx, tmpFile, fileData); err != nil {
	//		return fmt.Errorf("error writing info into relational file %s: %w", tmpFile.Name(), err)
	//	}
	//
	//	if _, err = fi.File.Seek(int64(from), 0); err != nil {
	//		return fmt.Errorf("error seeking to file: %w", err)
	//	}
	//
	//	if _, err = io.Copy(tmpFile, fi.File); err != nil {
	//		return fmt.Errorf("error copying second part of the file to tmp file: %w", err)
	//	}
	//
	//	if err = tmpFile.Close(); err != nil {
	//		return fmt.Errorf("failed to sync file - %s | err: %v", tmpFile.Name(), err)
	//	}
	//
	//	return nil
	//}
	//discardTmpFile := func() error {
	//	if _, err = fi.File.Seek(0, 0); err != nil {
	//		return fmt.Errorf("error seeking to file: %w", err)
	//	}
	//
	//	if err = os.Remove(getDataFilepath(tmpFilePath, strKey)); err != nil {
	//		return fmt.Errorf("error removing unecessary tmp %s relational file: %w", fi.FileData.Header.StoreInfo.Name, err)
	//	}
	//
	//	return nil
	//}
	//
	//removeMainFile := func() error {
	//	if err = os.Remove(getDataFilepath(relationalPath, relationalDataStore)); err != nil {
	//		return fmt.Errorf("error removing %s relational item's file: %w", relationalPath, err)
	//	}
	//
	//	return nil
	//}
	//reopenMainFile := func() error {
	//	//fi, err = e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	//	//if err != nil {
	//	//	return fmt.Errorf(
	//	//		"error re-opening %s item relational store: %w",
	//	//		relationalLockKey, err)
	//	//}
	//	//
	//	//e.itemFileMapping[relationalLockKey] = fi
	//
	//	return nil
	//}
	//
	//operations := []*lo.Operation{
	//	{
	//		Action: &lo.Action{
	//			Act:         copyToTmpFile,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//		Rollback: &lo.RollbackAction{
	//			RollbackAct: discardTmpFile,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//	},
	//	{
	//		Action: &lo.Action{
	//			Act:         removeMainFile,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//		Rollback: &lo.RollbackAction{
	//			RollbackAct: reopenMainFile,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//	},
	//}
	//if err = lo.New(operations...).Operate(); err != nil {
	//	return err
	//}
	//
	//{ // renaming tmp file
	//	if err = os.Rename(
	//		getDataFilepath(tmpFilePath, relationalDataStore),
	//		getDataFilepath(relationalPath, relationalDataStore),
	//	); err != nil {
	//		return fmt.Errorf("error renaming tmp file: %w", err)
	//	}
	//
	//	// TODO: log error
	//	_ = os.RemoveAll(getDataPathWithSep(tmpFilePath))
	//
	//	var file *os.File
	//	file, err = e.openFile(ctx, getDataFilepath(relationalPath, relationalDataStore))
	//	if err != nil {
	//		return fmt.Errorf("error opening %s file: %w", relationalInfo.Name, err)
	//	}
	//
	//	if _, ok := e.itemFileMapping[relationalLockKey]; !ok {
	//		e.itemFileMapping[relationalLockKey] = fi
	//	}
	//	fi.File = file
	//	e.itemFileMapping[relationalLockKey].File = file
	//}

	//{
	//	bs, err := e.serializer.Serialize(fileData)
	//	if err != nil {
	//		return fmt.Errorf("failed to serialize data - %s | err: %v", fi.File.Name(), err)
	//	}
	//	bsLen := bytesx.AddUint32(uint32(len(bs)))
	//
	//	bsToWrite := make([]byte, len(bs)+4)
	//	copy(bsToWrite[:4], bsLen)
	//	copy(bsToWrite[4:], bs)
	//
	//	fileStats, err := fi.File.Stat()
	//	if err != nil {
	//		return fmt.Errorf("error getting file stats: %w", err)
	//	}
	//
	//	// Memory map the file
	//	data, err := unix.Mmap(
	//		int(fi.File.Fd()),
	//		0,
	//		int(fileStats.Size()),
	//		unix.PROT_READ|unix.PROT_WRITE,
	//		unix.MAP_SHARED,
	//	)
	//	if err != nil {
	//		return err
	//	}
	//
	//	// Copy the data we need
	//	firstPart := make([]byte, upTo)
	//	copy(firstPart, data[:upTo])
	//
	//	lastPart := make([]byte, len(data)-int(from))
	//	copy(lastPart, data[from:])
	//
	//	// Calculate and set new file size
	//	newSize := int(upTo) + len(bsToWrite) + len(lastPart)
	//
	//	dataToWrite := make([]byte, newSize)
	//	copy(dataToWrite[:upTo], firstPart)
	//	copy(dataToWrite[upTo:], bsToWrite)
	//	copy(dataToWrite[upTo+uint32(len(bsToWrite)):], lastPart)
	//
	//	if err = unix.Munmap(data); err != nil {
	//		return fmt.Errorf("error unmapping original file: %w", err)
	//	}
	//	if err = fi.File.Truncate(int64(newSize)); err != nil {
	//		return fmt.Errorf("error resizing file: %w", err)
	//	}
	//
	//	// Create new mapping with new size
	//	newData, err := unix.Mmap(
	//		int(fi.File.Fd()),
	//		0,
	//		newSize,
	//		unix.PROT_READ|unix.PROT_WRITE,
	//		unix.MAP_SHARED,
	//	)
	//	if err != nil {
	//		return fmt.Errorf("error mapping resized file: %w", err)
	//	}
	//	copy(newData, dataToWrite)
	//
	//	// Sync changes to disk
	//	if err = unix.Msync(newData, unix.MS_SYNC); err != nil {
	//		return fmt.Errorf("error syncing mapped memory: %w", err)
	//	}
	//	if err := unix.Munmap(newData); err != nil {
	//		return fmt.Errorf("error unmapping original file: %w", err)
	//	}
	//}

	{
		bs, err := e.serializer.Serialize(fileData)
		if err != nil {
			return fmt.Errorf("failed to serialize data - %s | err: %v", fi.File.Name(), err)
		}
		bsLen := bytesx.AddUint32(uint32(len(bs)))

		bsToWrite := make([]byte, len(bs)+4)
		copy(bsToWrite[:4], bsLen)
		copy(bsToWrite[4:], bs)

		fileStats, err := fi.File.Stat()
		if err != nil {
			return fmt.Errorf("error getting file stats: %w", err)
		}

		// Memory map the file
		data, err := unix.Mmap(
			int(fi.File.Fd()),
			0,
			int(fileStats.Size()),
			unix.PROT_READ|unix.PROT_WRITE,
			unix.MAP_SHARED,
		)
		if err != nil {
			return err
		}

		// Copy the data we need
		firstPart := make([]byte, upTo)
		copy(firstPart, data[:upTo])

		lastPart := make([]byte, len(data)-int(from))
		copy(lastPart, data[from:])

		// Calculate and set new file size
		newSize := int(upTo) + len(bsToWrite) + len(lastPart)

		dataToWrite := make([]byte, newSize)
		copy(dataToWrite[:upTo], firstPart)
		copy(dataToWrite[upTo:], bsToWrite)
		copy(dataToWrite[upTo+uint32(len(bsToWrite)):], lastPart)

		if err = unix.Munmap(data); err != nil {
			return fmt.Errorf("error unmapping original file: %w", err)
		}
		//if err = fi.File.Truncate(int64(newSize)); err != nil {
		//	return fmt.Errorf("error resizing file: %w", err)
		//}
		//
		//// Create new mapping with new size
		//newData, err := unix.Mmap(
		//	int(fi.File.Fd()),
		//	0,
		//	newSize,
		//	unix.PROT_READ|unix.PROT_WRITE,
		//	unix.MAP_SHARED,
		//)
		//if err != nil {
		//	return fmt.Errorf("error mapping resized file: %w", err)
		//}
		//copy(newData, dataToWrite)

		tmpFile, err := os.OpenFile(
			getDataFilepath(tmpFilePath, tmp+suffixSep+relationalDataStore),
			os.O_RDWR|os.O_CREATE|os.O_EXCL, dbFilePerm)
		if err != nil {
			return err
		}

		writer := bufio.NewWriter(tmpFile)
		if _, err = writer.Write(dataToWrite); err != nil {
			return err
		}
		if err = writer.Flush(); err != nil {
			return err
		}

		if err = os.Rename(
			getDataFilepath(tmpFilePath, tmp+suffixSep+relationalDataStore),
			getDataFilepath(relationalPath, relationalDataStore)); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		//// Sync changes to disk
		//if err = unix.Msync(newData, unix.MS_SYNC); err != nil {
		//	return fmt.Errorf("error syncing mapped memory: %w", err)
		//}
		//if err = unix.Munmap(newData); err != nil {
		//	return fmt.Errorf("error unmapping original file: %w", err)
		//}
	}

	return
}

func (e *LTNGEngine) upsertRelationalData(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	fileData *FileData,
) (err error) {
	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return fmt.Errorf("error creating %s relational store: %w",
			dbMetaInfo.RelationalInfo().Name, err)
	}

	//strKey := hex.EncodeToString(fileData.Key)
	relationalInfo := fi.FileData.Header.StoreInfo.RelationalInfo()
	relationalPath := relationalInfo.Path
	tmpFileInfo := fi.FileData.Header.StoreInfo.TmpRelationalInfo()
	tmpFilePath := tmpFileInfo.Path
	//relationalLockKey := relationalInfo.LockName(relationalDataStore)

	reader, err := newFileReader(ctx, fi, true)
	if err != nil {
		return fmt.Errorf("error creating %s file reader: %w",
			fi.File.Name(), err)
	}

	var found bool
	var upTo, from uint32
	for {
		var bs []byte
		bs, err = reader.read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("error reading %s file: %w", fi.File.Name(), err)
		}

		if bytes.Contains(bs, fileData.Key) {
			found = true
			from = reader.yield()
			upTo = from - uint32(len(bs)+4)
			break
		}
	}

	if !found {
		if _, err = e.writeToRelationalFileWithNoSeek(ctx, fi.File, fileData); err != nil {
			return fmt.Errorf("error writing info into file %s: %w", fi.File.Name(), err)
		}

		return nil
	}

	if _, err = fi.File.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking to file: %w", err)
	}

	//var tmpFile *os.File
	//copyToTmpFile := func() error {
	//	if err = os.MkdirAll(getDataPathWithSep(tmpFilePath), os.ModePerm); err != nil {
	//		return fmt.Errorf("error creating tmp file directory: %w", err)
	//	}
	//
	//	tmpFile, err = os.OpenFile(
	//		getDataFilepath(tmpFilePath, relationalDataStore),
	//		os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, dbFilePerm,
	//	)
	//	if err != nil {
	//		return err
	//	}
	//
	//	// example pair (upTo - from) | 102 - 154
	//	if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
	//		return fmt.Errorf("error copying first part of the file to tmp file: %w", err)
	//	}
	//
	//	if _, err = e.writeToRelationalFileWithNoSeek(ctx, tmpFile, fileData); err != nil {
	//		return fmt.Errorf("error writing info into relational file %s: %w", tmpFile.Name(), err)
	//	}
	//
	//	if _, err = fi.File.Seek(int64(from), 0); err != nil {
	//		return fmt.Errorf("error seeking to file: %w", err)
	//	}
	//
	//	if _, err = io.Copy(tmpFile, fi.File); err != nil {
	//		return fmt.Errorf("error copying second part of the file to tmp file: %w", err)
	//	}
	//
	//	if err = tmpFile.Close(); err != nil {
	//		return fmt.Errorf("failed to sync file - %s | err: %v", tmpFile.Name(), err)
	//	}
	//
	//	return nil
	//}
	//discardTmpFile := func() error {
	//	if _, err = fi.File.Seek(0, 0); err != nil {
	//		return fmt.Errorf("error seeking to file: %w", err)
	//	}
	//
	//	if err = os.Remove(getDataFilepath(tmpFilePath, strKey)); err != nil {
	//		return fmt.Errorf("error removing unecessary tmp %s relational file: %w", fi.FileData.Header.StoreInfo.Name, err)
	//	}
	//
	//	return nil
	//}
	//
	//removeMainFile := func() error {
	//	if err = os.Remove(getDataFilepath(relationalPath, strKey)); err != nil {
	//		return fmt.Errorf("error removing %s relational item's file: %w", relationalPath, err)
	//	}
	//
	//	return nil
	//}
	//reopenMainFile := func() error {
	//	fi, err = e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	//	if err != nil {
	//		return fmt.Errorf(
	//			"error re-opening %s item relational store: %w",
	//			relationalPath, err)
	//	}
	//
	//	e.itemFileMapping[relationalLockKey] = fi
	//
	//	return nil
	//}
	//
	//operations := []*lo.Operation{
	//	{
	//		Action: &lo.Action{
	//			Act:         copyToTmpFile,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//		Rollback: &lo.RollbackAction{
	//			RollbackAct: discardTmpFile,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//	},
	//	{
	//		Action: &lo.Action{
	//			Act:         removeMainFile,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//		Rollback: &lo.RollbackAction{
	//			RollbackAct: reopenMainFile,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//	},
	//}
	//if err = lo.New(operations...).Operate(); err != nil {
	//	return err
	//}
	//
	//{ // renaming tmp file
	//	if err = os.Rename(
	//		getDataFilepath(tmpFilePath, relationalDataStore),
	//		getDataFilepath(relationalPath, relationalDataStore),
	//	); err != nil {
	//		return fmt.Errorf("error renaming tmp file: %w", err)
	//	}
	//
	//	// TODO: log error
	//	_ = os.RemoveAll(getDataPathWithSep(tmpFilePath))
	//
	//	var file *os.File
	//	file, err = e.openFile(ctx, getDataFilepath(relationalPath, relationalDataStore))
	//	if err != nil {
	//		return fmt.Errorf("error opening %s file: %w", relationalInfo.Name, err)
	//	}
	//
	//	if _, ok := e.itemFileMapping[relationalLockKey]; !ok {
	//		e.itemFileMapping[relationalLockKey] = fi
	//	}
	//	fi.File = file
	//	e.itemFileMapping[relationalLockKey].File = file
	//}

	{
		bs, err := e.serializer.Serialize(fileData)
		if err != nil {
			return fmt.Errorf("failed to serialize data - %s | err: %v", fi.File.Name(), err)
		}
		bsLen := bytesx.AddUint32(uint32(len(bs)))

		bsToWrite := make([]byte, len(bs)+4)
		copy(bsToWrite[:4], bsLen)
		copy(bsToWrite[4:], bs)

		fileStats, err := fi.File.Stat()
		if err != nil {
			return fmt.Errorf("error getting file stats: %w", err)
		}

		// Memory map the file
		data, err := unix.Mmap(
			int(fi.File.Fd()),
			0,
			int(fileStats.Size()),
			unix.PROT_READ|unix.PROT_WRITE,
			unix.MAP_SHARED,
		)
		if err != nil {
			return err
		}

		// Copy the data we need
		firstPart := make([]byte, upTo)
		copy(firstPart, data[:upTo])

		lastPart := make([]byte, len(data)-int(from))
		copy(lastPart, data[from:])

		// Calculate and set new file size
		newSize := int(upTo) + len(bsToWrite) + len(lastPart)

		dataToWrite := make([]byte, newSize)
		copy(dataToWrite[:upTo], firstPart)
		copy(dataToWrite[upTo:], bsToWrite)
		copy(dataToWrite[upTo+uint32(len(bsToWrite)):], lastPart)

		if err = unix.Munmap(data); err != nil {
			return fmt.Errorf("error unmapping original file: %w", err)
		}
		//if err = fi.File.Truncate(int64(newSize)); err != nil {
		//	return fmt.Errorf("error resizing file: %w", err)
		//}
		//
		//// Create new mapping with new size
		//newData, err := unix.Mmap(
		//	int(fi.File.Fd()),
		//	0,
		//	newSize,
		//	unix.PROT_READ|unix.PROT_WRITE,
		//	unix.MAP_SHARED,
		//)
		//if err != nil {
		//	return fmt.Errorf("error mapping resized file: %w", err)
		//}
		//copy(newData, dataToWrite)

		tmpFile, err := os.OpenFile(
			getDataFilepath(tmpFilePath, tmp+suffixSep+relationalDataStore),
			os.O_RDWR|os.O_CREATE|os.O_EXCL, dbFilePerm)
		if err != nil {
			return err
		}

		writer := bufio.NewWriter(tmpFile)
		if _, err = writer.Write(dataToWrite); err != nil {
			return err
		}
		if err = writer.Flush(); err != nil {
			return err
		}

		if err = os.Rename(
			getDataFilepath(tmpFilePath, tmp+suffixSep+relationalDataStore),
			getDataFilepath(relationalPath, relationalDataStore)); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		//// Sync changes to disk
		//if err = unix.Msync(newData, unix.MS_SYNC); err != nil {
		//	return fmt.Errorf("error syncing mapped memory: %w", err)
		//}
		//if err = unix.Munmap(newData); err != nil {
		//	return fmt.Errorf("error unmapping original file: %w", err)
		//}
	}

	return
}

func (e *LTNGEngine) deleteRelationalData(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	key []byte,
) (err error) {
	var fi *fileInfo
	fi, err = e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return err
	}

	strKey := hex.EncodeToString(key)
	relationalInfo := fi.FileData.Header.StoreInfo.RelationalInfo()
	relationalPath := relationalInfo.Path
	relationalFilePath := relationalPath + sep + strKey + ext
	tmpFileInfo := fi.FileData.Header.StoreInfo.TmpRelationalInfo()
	tmpFilePath := tmpFileInfo.Path
	relationalLockKey := relationalInfo.LockName(relationalDataStore)

	reader, err := newFileReader(ctx, fi, true)
	if err != nil {
		return fmt.Errorf("error creating %s file reader: %w",
			fi.File.Name(), err)
	}

	var deleted bool
	var upTo, from uint32
	for {
		var bs []byte
		bs, err = reader.read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("error reading %s file: %w", fi.File.Name(), err)
		}

		if bytes.Contains(bs, key) {
			deleted = true
			from = reader.yield()
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
		//if err = os.MkdirAll(getDataPathWithSep(tmpFilePath), os.ModePerm); err != nil {
		//	return fmt.Errorf("error creating tmp file directory: %w", err)
		//}

		tmpFile, err = os.OpenFile(
			getDataFilepath(tmpFilePath, tmp+suffixSep+relationalDataStore),
			os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, dbFilePerm,
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

		if err = os.Remove(getDataFilepath(tmpFilePath, strKey)); err != nil {
			return fmt.Errorf(
				"error removing unecessary tmp %s item file: %w",
				relationalFilePath, err)
		}

		return nil
	}

	//removeMainFile := func() error {
	//	if err = os.Remove(getDataFilepath(relationalPath, relationalDataStore)); err != nil {
	//		return fmt.Errorf("error removing %s relational item's file: %w", relationalPath, err)
	//	}
	//
	//	return nil
	//}
	//reopenMainFile := func() error {
	//	fi, err = e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	//	if err != nil {
	//		return fmt.Errorf(
	//			"error re-opening %s item relational store: %w",
	//			relationalFilePath, err)
	//	}
	//
	//	e.itemFileMapping[relationalLockKey] = fi
	//
	//	return nil
	//}

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
		//{
		//	Action: &lo.Action{
		//		Act:         removeMainFile,
		//		RetrialOpts: lo.DefaultRetrialOps,
		//	},
		//	Rollback: &lo.RollbackAction{
		//		RollbackAct: reopenMainFile,
		//		RetrialOpts: lo.DefaultRetrialOps,
		//	},
		//},
	}
	if err = lo.New(operations...).Operate(); err != nil {
		return err
	}

	{ // renaming tmp file
		if err = os.Rename(
			getDataFilepath(tmpFilePath, tmp+suffixSep+relationalDataStore),
			getDataFilepath(relationalPath, relationalDataStore),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		//// TODO: log error
		//_ = os.Remove(getDataFilepath(tmpFilePath, tmp+suffixSep+relationalDataStore))

		var file *os.File
		file, err = e.openFile(ctx, getDataFilepath(relationalPath, relationalDataStore))
		if err != nil {
			return fmt.Errorf("error opening %s file: %w", relationalInfo.Name, err)
		}

		if _, ok := e.itemFileMapping[relationalLockKey]; !ok {
			e.itemFileMapping[relationalLockKey] = fi
		}
		fi.File = file
		e.itemFileMapping[relationalLockKey].File = file
	}

	//{
	//	fileStats, err := fi.File.Stat()
	//	if err != nil {
	//		return fmt.Errorf("error getting file stats: %w", err)
	//	}
	//
	//	// Memory map the file
	//	mapped, err := unix.Mmap(
	//		int(fi.File.Fd()),
	//		0,
	//		int(fileStats.Size()),
	//		unix.PROT_READ|unix.PROT_WRITE,
	//		unix.MAP_SHARED,
	//	)
	//	if err != nil {
	//		return err
	//	}
	//
	//	// Calculate how many bytes need to be moved
	//	moveLen := uint32(len(mapped)) - from
	//
	//	// Move data from 'from' position to 'upTo' position
	//	copy(mapped[upTo:], mapped[from:from+moveLen])
	//
	//	// Sync changes to disk
	//	if err := unix.Msync(mapped, syscall.MS_SYNC); err != nil {
	//		return fmt.Errorf("error syncing mapped memory: %w", err)
	//	}
	//
	//	// Truncate the file to remove the extra data at the end
	//	newSize := fileStats.Size() - int64(from-upTo)
	//	if err = fi.File.Truncate(newSize); err != nil {
	//		return fmt.Errorf("error truncating file: %w", err)
	//	}
	//}

	return
}
