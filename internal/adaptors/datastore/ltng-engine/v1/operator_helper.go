package v1

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"
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
	bs, file, err := e.openReadWholeFile(ctx, getDataFilepath(dbMetaInfo.Path, strItemKey))
	if err != nil {
		return nil, err
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

	fi, err := e.writeStatsStoreToFile(ctx, file, &itemFileData)
	if err != nil {
		return nil, fmt.Errorf("error writing store stats file: %v", err)
	}

	e.itemFileMapping[dbMetaInfo.LockName(strItemKey)] = fi

	if _, err = e.updateRelationalStatsFile(ctx, fi, &itemFileData); err != nil {
		return nil, fmt.Errorf("error updateRelationalStatsFile: %v", err)
	}

	return itemFileData.Data, nil
}

// ################################################################################

func (e *LTNGEngine) loadRelationalItemStoreFromMemoryOrDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
) ([]byte, error) {
	strItemKey := string(item.Key)
	fi, ok := e.itemFileMapping[dbMetaInfo.LockName(strItemKey)]
	if !ok {
		return e.loadRelationalItemStoreFromDisk(ctx, dbMetaInfo, item)
	}

	bs, err := e.readRelationalRow(ctx, fi.File)
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

func (e *LTNGEngine) loadRelationalItemStoreFromDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
) ([]byte, error) {
	strItemKey := string(item.Key)
	bs, f, err := e.openReadWholeFile(ctx, getDataFilepath(dbMetaInfo.Path, strItemKey))
	if err != nil {
		return nil, err
	}

	var itemFileData FileData
	err = e.serializer.Deserialize(bs, &itemFileData)
	if err != nil {
		return nil, err
	}

	{ // update file stats with last opened at
		itemFileData.Header.ItemInfo.LastOpenedAt = time.Now().UTC().Unix()
		e.itemFileMapping[dbMetaInfo.LockName(strItemKey)] = &fileInfo{
			//Header: &Header{
			//	ItemInfo: &ItemInfo{
			//		CreatedAt:    itemFileData.Header.ItemInfo.CreatedAt,
			//		LastOpenedAt: itemFileData.Header.ItemInfo.LastOpenedAt,
			//	},
			//	StoreInfo: itemFileData.Header.StoreInfo,
			//},
			File: f,
		}
	}

	return itemFileData.Data, nil
}

// ################################################################################

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
	opts *IndexOpts,
	dbMetaInfo *ManagerStoreMetaInfo,
) ([][]byte, error) {
	rawIndexingList, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo.IndexListInfo(), &Item{
		Key: opts.ParentKey,
	})
	if err != nil {
		return nil, err
	}

	var indexingList [][]byte
	err = e.serializer.Deserialize(rawIndexingList, &indexingList)
	if err != nil {
		return nil, fmt.Errorf("error deserializing indexing list: %v", err)
	}

	return indexingList, nil
}

// ################################################################################

func (e *LTNGEngine) createItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) error {
	if err := os.MkdirAll(
		getDataPath(dbMetaInfo.Path), dbFilePerm,
	); err != nil {
		return err
	}

	filePath := dbMetaInfo.Path + sep + string(item.Key) + ext
	file, err := e.openCreateTruncatedFile(ctx, getDataPath(filePath))
	if err != nil {
		return fmt.Errorf("error opening creating a file at %s: %v", filePath, err)
	}

	timeNow := time.Now().UTC().Unix()
	itemFileData := &FileData{
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
	}

	bs, err := e.serializer.Serialize(itemFileData)
	if err != nil {
		return fmt.Errorf("error serializing item: %v", err)
	}

	if _, err = file.Write(bs); err != nil {
		return fmt.Errorf("error writing item file data: %v", err)
	}

	//relationalStore, ok := e.itemFileMapping[dbMetaInfo.RelationalInfo().Name]
	//if !ok {
	//	e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, &Item{})
	//}

	return nil
}

func (e *LTNGEngine) createItemOnDiskAndMemory(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) error {
	_, err := e.loadItem(ctx, dbMetaInfo, item, opts)
	if err == nil {
		return fmt.Errorf("error key already exists")
	}

	if err = os.MkdirAll(
		getDataPath(dbMetaInfo.Path), dbFilePerm,
	); err != nil {
		return err
	}

	return nil
}

// ################################################################################
