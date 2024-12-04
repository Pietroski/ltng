package v1

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	lo "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/list-operator"
)

func (e *LTNGEngine) loadItemFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
) ([]byte, error) {
	strItemKey := string(item.Key)
	fi, ok := e.itemFileMapping[dbMetaInfo.LockName(strItemKey)]
	if !ok {
		return e.loadItemFromDisk(ctx, dbMetaInfo, item)
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

	return itemFileData.Data, nil
}

func (e *LTNGEngine) loadItemFromDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
) ([]byte, error) {
	strItemKey := string(item.Key)
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

	err = file.Truncate(0)
	if err != nil {
		return nil, fmt.Errorf("failed to truncate store stats file: %v", err)
	}

	if _, err = e.writeToFile(ctx, file, itemFileData); err != nil {
		return nil, fmt.Errorf("error writing item data to file: %v", err)
	}

	if err = e.updateRelationalData(
		ctx, dbMetaInfo, &itemFileData,
	); err != nil {
		return nil, fmt.Errorf("failed to update store stats manager file: %v", err)
	}

	e.itemFileMapping[dbMetaInfo.LockName(strItemKey)] = &fileInfo{
		File:     file,
		FileData: &itemFileData,
		DataSize: uint32(len(itemFileData.Data)),
	}

	return itemFileData.Data, nil
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

	return fi, nil
}

func (e *LTNGEngine) loadRelationalItemStoreFromDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
) (*fileInfo, error) {
	f, err := e.openFile(ctx,
		getDataFilepath(dbMetaInfo.RelationalInfo().Path, relationalDataStoreFile))
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
		ctx, dbMetaInfo, &fileData,
	)
}

func (e *LTNGEngine) updateRelationalDataFile(
	ctx context.Context, dbMetaInfo *ManagerStoreMetaInfo, fileData *FileData,
) error {
	_, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return err
	}

	if err = e.updateRelationalData(
		ctx, dbMetaInfo, fileData,
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
) ([]byte, error) {
	mainKeyValue, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &Item{
		Key: opts.IndexingKeys[0],
	})
	if err != nil {
		return []byte{}, err
	}

	value, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &Item{
		Key: mainKeyValue,
	})
	if err != nil {
		return []byte{}, err
	}

	return value, nil
}

func (e *LTNGEngine) andComputationalSearch(
	ctx context.Context,
	opts *IndexOpts,
	dbMetaInfo *ManagerStoreMetaInfo,
) ([]byte, error) {
	var parentKey []byte
	for _, key := range opts.IndexingKeys {
		keyValue, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &Item{
			Key: key,
		})
		if err != nil {
			return nil, err
		}

		if !bytes.Equal(keyValue, parentKey) && parentKey != nil {
			return nil, fmt.Errorf("")
		}

		parentKey = keyValue
	}

	return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &Item{
		Key: parentKey,
	})
}

func (e *LTNGEngine) orComputationalSearch(
	ctx context.Context,
	opts *IndexOpts,
	dbMetaInfo *ManagerStoreMetaInfo,
) ([]byte, error) {
	for _, key := range opts.IndexingKeys {
		parentKey, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexInfo(), &Item{
			Key: key,
		})
		if err != nil {
			continue
		}

		return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &Item{
			Key: parentKey,
		})
	}

	return nil, fmt.Errorf("no keys found")
}

// loadIndexingList it returns all the indexing list a parent key has
func (e *LTNGEngine) loadIndexingList(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	opts *IndexOpts,
) ([][]byte, error) {
	rawIndexingList, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexListInfo(), &Item{
		Key: opts.ParentKey,
	})
	if err != nil {
		return nil, err
	}

	return bytes.Split(rawIndexingList, []byte(bytesSep)), nil
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
	hex.EncodeToString(item.Key[:])
	strItemKey := string(item.Key)
	//strItemKey := hex.EncodeToString(item.Key[:])
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

	fmt.Println(strItemKey)

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
	strItemKey := string(item.Key)
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

	file, err := e.openCreateTruncatedFile(ctx, getDataPath(filePath))
	if err != nil {
		return fmt.Errorf("error opening/creating a truncated indexed file at %s: %v", filePath, err)
	}

	if _, err = e.writeToFile(ctx, file, fileData); err != nil {
		return err
	}

	return nil
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
	fileData *FileData,
) (err error) {
	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return fmt.Errorf("error creating %s relational store: %w",
			dbMetaInfo.RelationalInfo().Name, err)
	}

	strKey := string(fileData.Key)
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
		return fmt.Errorf("could not find key %s in item relational file %s", fileData.Key, relationalFilePath)
	}

	if _, err = fi.File.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking to file: %w", err)
	}

	var tmpFile *os.File
	copyToTmpFile := func() error {
		tmpFile, err = os.OpenFile(
			getDataFilepath(tmpFilePath, strKey),
			os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, dbFilePerm,
		)
		if err != nil {
			return err
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
			return fmt.Errorf("error copying first part of the file to tmp file: %w", err)
		}

		if _, err = e.writeToRelationalFileWithNoSeek(ctx, tmpFile, fileData); err != nil {
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

		if err = os.Remove(getDataFilepath(tmpFilePath, strKey)); err != nil {
			return fmt.Errorf("error removing unecessary tmp %s relational file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		return nil
	}

	removeMainFile := func() error {
		if err = fi.File.Close(); err != nil {
			return fmt.Errorf("error closing original file: %w", err)
		}

		if err = os.Remove(getDataFilepath(relationalPath, strKey)); err != nil {
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
			getDataFilepath(tmpFilePath, strKey),
			getDataFilepath(relationalPath, strKey),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		var file *os.File
		file, err = e.openFile(ctx, getDataFilepath(relationalPath, strKey))
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
	dbMetaInfo *ManagerStoreMetaInfo,
	fileData *FileData,
) (err error) {
	fi, err := e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return fmt.Errorf("error creating %s relational store: %w",
			dbMetaInfo.RelationalInfo().Name, err)
	}

	strKey := string(fileData.Key)
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

	var tmpFile *os.File
	copyToTmpFile := func() error {
		tmpFile, err = os.OpenFile(
			getDataFilepath(tmpFilePath, strKey),
			os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, dbFilePerm,
		)
		if err != nil {
			return err
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
			return fmt.Errorf("error copying first part of the file to tmp file: %w", err)
		}

		if _, err = e.writeToRelationalFileWithNoSeek(ctx, tmpFile, fileData); err != nil {
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

		if err = os.Remove(getDataFilepath(tmpFilePath, strKey)); err != nil {
			return fmt.Errorf("error removing unecessary tmp %s relational file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		return nil
	}

	removeMainFile := func() error {
		if err = fi.File.Close(); err != nil {
			return fmt.Errorf("error closing original file: %w", err)
		}

		if err = os.Remove(getDataFilepath(relationalPath, strKey)); err != nil {
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
			getDataFilepath(tmpFilePath, strKey),
			getDataFilepath(relationalPath, strKey),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		var file *os.File
		file, err = e.openFile(ctx, getDataFilepath(relationalPath, strKey))
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
	dbMetaInfo *ManagerStoreMetaInfo,
	key []byte,
) (err error) {
	var fi *fileInfo
	fi, err = e.loadRelationalItemStoreFromMemoryOrDisk(ctx, dbMetaInfo)
	if err != nil {
		return err
	}

	strKey := string(key)
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
		tmpFile, err = os.OpenFile(
			getDataFilepath(tmpFilePath, strKey),
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

	removeMainFile := func() error {
		if err = fi.File.Close(); err != nil {
			return fmt.Errorf("error closing original file: %w", err)
		}

		if err = os.Remove(getDataFilepath(relationalPath, strKey)); err != nil {
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
			getDataFilepath(tmpFilePath, strKey),
			getDataFilepath(relationalPath, strKey),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		var file *os.File
		file, err = e.openFile(ctx, getDataFilepath(relationalPath, strKey))
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
