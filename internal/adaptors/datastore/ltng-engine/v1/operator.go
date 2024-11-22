package v1

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"
)

func (e *LTNGEngine) loadItem(
	ctx context.Context,
	dbMetaInfo *DatabaseMetaInfo,
	item *Item,
	opts *IndexOpts,
) ([]byte, error) {
	lockKey := dbMetaInfo.LockName(string(item.Key))
	e.opMtx.Lock(lockKey, struct{}{})
	defer e.opMtx.Unlock(lockKey)

	if opts == nil || len(opts.IndexingKeys) != 1 {
		err := fmt.Errorf("load item requires index key list with length of 1")
		return []byte{}, fmt.Errorf("invalid index payload size for giving option: %w", err)
	}

	if !opts.HasIdx {
		return e.loadItemFromMemoryOrDisk(ctx, item, dbMetaInfo)
	}

	switch opts.IndexProperties.IndexSearchPattern {
	case AndComputational:
		return e.andComputationalSearch(ctx, opts, dbMetaInfo)
	case OrComputational:
		return e.orComputationalSearch(ctx, opts, dbMetaInfo)
	case One:
		fallthrough
	default:
		return e.straightSearch(ctx, opts, dbMetaInfo)
	}
}

func (e *LTNGEngine) loadItemFromMemoryOrDisk(
	ctx context.Context,
	item *Item,
	dbMetaInfo *DatabaseMetaInfo,
) ([]byte, error) {
	strItemKey := string(item.Key)
	fsm, ok := e.fileStoreMapping[dbMetaInfo.LockName(strItemKey)]
	if !ok {
		return e.loadItemFromDisk(ctx, item, dbMetaInfo)
	}

	bs, err := e.readFile(ctx, fsm.File)
	if err != nil {
		return nil, err
	}

	var fileData FileData
	err = e.serializer.Deserialize(bs, &fileData)
	if err != nil {
		return nil, err
	}

	return fileData.Data, nil
}

func (e *LTNGEngine) loadItemFromDisk(
	ctx context.Context,
	item *Item,
	dbMetaInfo *DatabaseMetaInfo,
) ([]byte, error) {
	strItemKey := string(item.Key)
	bs, f, err := e.openReadFile(ctx, getDataFilepath(dbMetaInfo.Path, strItemKey))
	if err != nil {
		return nil, err
	}

	var fileData FileData
	err = e.serializer.Deserialize(bs, &fileData)
	if err != nil {
		return nil, err
	}

	{ // update file stats with last opened at
		fileData.FileStats.LastOpenedAt = time.Now().Unix()
		e.fileStoreMapping[dbMetaInfo.LockName(strItemKey)] = &fileInfo{
			DBInfo: fileData.FileStats,
			File:   f,
		}
	}

	return fileData.Data, nil
}

// straightSearch finds the value from the index store with the [0] index list item;
// that found value is the key to the (main) store that holds value that needs to be returned
func (e *LTNGEngine) straightSearch(
	ctx context.Context,
	opts *IndexOpts,
	dbMetaInfo *DatabaseMetaInfo,
) ([]byte, error) {
	mainKeyValue, err := e.loadItemFromMemoryOrDisk(ctx, &Item{
		Key: opts.IndexingKeys[0],
	}, dbMetaInfo.IndexInfo())
	if err != nil {
		return []byte{}, err
	}

	value, err := e.loadItemFromMemoryOrDisk(ctx, &Item{
		Key: mainKeyValue,
	}, dbMetaInfo)
	if err != nil {
		return []byte{}, err
	}

	return value, nil
}

func (e *LTNGEngine) andComputationalSearch(
	ctx context.Context,
	opts *IndexOpts,
	dbMetaInfo *DatabaseMetaInfo,
) ([]byte, error) {
	var parentKey []byte
	for _, key := range opts.IndexingKeys {
		keyValue, err := e.loadItemFromMemoryOrDisk(ctx, &Item{
			Key: key,
		}, dbMetaInfo.IndexInfo())
		if err != nil {
			return nil, err
		}

		if !bytes.Equal(keyValue, parentKey) && parentKey != nil {
			return nil, fmt.Errorf("")
		}

		parentKey = keyValue
	}

	return e.loadItemFromMemoryOrDisk(ctx, &Item{
		Key: parentKey,
	}, dbMetaInfo)
}

func (e *LTNGEngine) orComputationalSearch(
	ctx context.Context,
	opts *IndexOpts,
	dbMetaInfo *DatabaseMetaInfo,
) ([]byte, error) {
	for _, key := range opts.IndexingKeys {
		parentKey, err := e.loadItemFromMemoryOrDisk(ctx, &Item{
			Key: key,
		}, dbMetaInfo.IndexInfo())
		if err != nil {
			continue
		}

		return e.loadItemFromMemoryOrDisk(ctx, &Item{
			Key: parentKey,
		}, dbMetaInfo)
	}

	return nil, fmt.Errorf("no keys found")
}

// loadIndexingList it returns all the indexing list a parent key has
func (e *LTNGEngine) loadIndexingList(
	ctx context.Context,
	opts *IndexOpts,
	dbMetaInfo *DatabaseMetaInfo,
) ([][]byte, error) {
	rawIndexingList, err := e.loadItemFromMemoryOrDisk(ctx, &Item{
		Key: opts.ParentKey,
	}, dbMetaInfo.IndexListInfo())
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

func (e *LTNGEngine) createItem(
	ctx context.Context,
	dbMetaInfo *DatabaseMetaInfo,
	item *Item,
	opts *IndexOpts,
) ([]byte, error) {
	lockKey := dbMetaInfo.LockName(string(item.Key))
	e.opMtx.Lock(lockKey, struct{}{})
	defer e.opMtx.Unlock(lockKey)

	if opts == nil || len(opts.IndexingKeys) != 1 {
		err := fmt.Errorf("straightSearch requires index key list with length of 1")
		return []byte{}, fmt.Errorf("invalid index payload size for giving option: %v", err)
	}

	if bs, err := e.loadItemFromMemoryOrDisk(ctx, item, dbMetaInfo); err == nil {
		return bs, nil
		//return nil, fmt.Errorf("error item already exists")
	}

	if !opts.HasIdx {
		return e.loadItemFromMemoryOrDisk(ctx, item, dbMetaInfo)
	}

	{
	}

	return nil, nil
}

func (e *LTNGEngine) createItemOnDisk(
	ctx context.Context,
	dbMetaInfo *DatabaseMetaInfo,
	item *Item,
	opts *IndexOpts,
) error {
	if err := os.MkdirAll(
		getDataPath(dbMetaInfo.Path), dbFilePerm,
	); err != nil {
		return err
	}

	return nil
}
