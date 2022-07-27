package indexed_operations

import (
	"bytes"
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v3"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	indexed_management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management/indexed"
	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
	indexed_operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation/indexed"
)

const (
	bsSeparator = "&#-#&"
)

func (idxOps *BadgerIndexedOperator) getIndexedDDMemoryInfo(
	ctx context.Context,
) (*management_models.DBMemoryInfo, error) {
	indexedDBInfo :=
		indexed_management_models.NewIndexedDBInfoFromDBInfo(idxOps.dbInfo.MemoryInfoToInfo())

	indexedMemoryDBInfo, err := idxOps.manager.GetDBMemoryInfo(ctx, indexedDBInfo.Name)
	if err != nil {
		err = fmt.Errorf("failed to get memory info from indexed db: %v", err)
		err = fmt.Errorf("%v - indexedMemoryDBInfo: %v", err, indexedMemoryDBInfo)
		return nil, err
	}

	return indexedMemoryDBInfo, nil
}

func (idxOps *BadgerIndexedOperator) createIndexRelation(
	ctx context.Context,
	item *operation_models.OpItem,
	index *indexed_operation_models.IdxOpsIndex,
) (err error) {
	indexedMemoryDBInfo, err := idxOps.getIndexedDDMemoryInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get indexed memory info: %v", err)
	}

	operator := idxOps.getIndexedOperator(indexedMemoryDBInfo)

	for _, key := range index.IndexKeys {
		if err = operator.Create(key, index.ParentKey); err != nil {
			err = fmt.Errorf("failed to create index for given key: %v", err)
			err = fmt.Errorf("%v - key: %v; parent key: %v", err, key, index.ParentKey)
			return err
		}
	}

	indexKeys := bytes.Join(index.IndexKeys, []byte(bsSeparator))
	err = operator.Create(index.ParentKey, indexKeys)
	if err != nil {
		err = fmt.Errorf("failed to create main INDEX key-value pair relation: %v", err)
		err = fmt.Errorf("%v - key: %s; value: %s", err, item.Key, item.Value)
		return err
	}

	return err
}

func (idxOps *BadgerIndexedOperator) updateIndexRelation(
	ctx context.Context,
	index *indexed_operation_models.IdxOpsIndex,
) (err error) {
	indexedMemoryDBInfo, err := idxOps.getIndexedDDMemoryInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get indexed memory info: %v", err)
	}

	operator := idxOps.getIndexedOperator(indexedMemoryDBInfo)

	for _, key := range index.IndexKeys {
		if key == nil || len(key) == 0 {
			continue
		}

		if err = operator.Update(key, index.ParentKey); err != nil {
			err = fmt.Errorf("failed to create index for given key: %v", err)
			err = fmt.Errorf("%v - key: %v; parent key: %v", err, key, index.ParentKey)
			return err
		}
	}

	indexKeys := bytes.Join(index.IndexKeys, []byte(bsSeparator))
	err = operator.Update(index.ParentKey, indexKeys)
	if err != nil {
		err = fmt.Errorf("failed to update main INDEX key-value pair relation: %v", err)
		err = fmt.Errorf("%v - key: %s; value: %s", err, index.ParentKey, indexKeys)
		return err
	}

	return err
}

func (idxOps *BadgerIndexedOperator) deleteCascade(
	ctx context.Context,
	item *operation_models.OpItem,
) error {
	indexedMemoryDBInfo, err := idxOps.getIndexedDDMemoryInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get indexed memory info: %v", err)
	}

	if err = idxOps.dbInfo.DB.Update(func(txn *badger.Txn) error {
		if err = idxOps.getOperator().Delete(item.Key); err != nil {
			return fmt.Errorf("failed to delete main key-value pair: %v", err)
		}

		operator := idxOps.getIndexedOperator(indexedMemoryDBInfo)

		rawIndexedKeys, err := operator.Load(item.Key)
		if err != nil {
			return fmt.Errorf("failed to load parent key-indexed-keys relation: %v", err)
		}

		if err = operator.Delete(item.Key); err != nil {
			return fmt.Errorf("failed to delete parent key-indexed-keys relation: %v", err)
		}

		indexedKeys := bytes.Split(rawIndexedKeys, []byte(bsSeparator))
		for _, key := range indexedKeys {
			if err = operator.Delete(key); err != nil {
				return fmt.Errorf("failed to delete indexed-keys-to-key relation: %v", err)
			}
		}

		return nil
	}); err != nil {
		err = fmt.Errorf("failed to deleteCascade indexed key relation: %v", err)
	}

	return err
}

func (idxOps *BadgerIndexedOperator) deleteIdxOnly(
	ctx context.Context,
	index *indexed_operation_models.IdxOpsIndex,
) error {
	if index == nil || len(index.IndexKeys) != 1 {
		err := fmt.Errorf("deleteIdxOnly requires index key list with len of 1")
		return fmt.Errorf("invalid index payload size for giving option: %v", err)
	}

	indexedMemoryDBInfo, err := idxOps.getIndexedDDMemoryInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get indexed memory info: %v", err)
	}

	if err = idxOps.dbInfo.DB.Update(func(txn *badger.Txn) error {
		operator := idxOps.getIndexedOperator(indexedMemoryDBInfo)

		var objKey, objValue []byte
		if objKey, err = andComputationalSearchFn(index.IndexKeys, operator); err != nil {
			return fmt.Errorf("failed to retrieve main index key to proceed deletion: %v", err)
		}

		err = operator.Delete(index.IndexKeys[0])
		if err != nil {
			return fmt.Errorf("failed to delete index key: %v", err)
		}

		objValue, err = operator.Load(objKey)
		if err != nil {
			return fmt.Errorf("failed to load indexed keys relation: %v", err)
		}

		index.IndexKeys = removeBytes(objValue, index.IndexKeys[0])
		index.ParentKey = objKey
		err = idxOps.updateIndexRelation(ctx, index)
		if err != nil {
			err = fmt.Errorf("failed to update indexed keys relation: %v", err)
		}

		return err
	}); err != nil {
		err = fmt.Errorf("failed to deleteIdxOnly: %v", err)
	}

	return err
}

var (
	andComputationalSearchFn = func(
		indexedKeys [][]byte, operator operations.Operator,
	) ([]byte, error) {
		var objKey []byte
		for _, key := range indexedKeys {
			keyValue, err := operator.Load(key)
			if err != nil {
				return []byte{}, fmt.Errorf("inconsistent database err: %v", err)
			}

			if !bytes.Equal(objKey, keyValue) && objKey != nil {
				err = fmt.Errorf("inconsistent key-value indexing: %v", err)
				return []byte{}, fmt.Errorf("inconsistent database err: %v", err)
			}

			objKey = keyValue
		}

		return objKey, nil
	}

	orComputationalSearchFn = func(
		indexedKeys [][]byte, operator operations.Operator,
	) ([]byte, error) {
		var objKey []byte
		for _, key := range indexedKeys {
			keyValue, err := operator.Load(key)
			if err != nil {
				continue
			}

			if !bytes.Equal(objKey, keyValue) && objKey != nil {
				err = fmt.Errorf("inconsistent key-value indexing: %v", err)
				return []byte{}, fmt.Errorf("inconsistent database err: %v", err)
			}

			objKey = keyValue
		}

		return objKey, nil
	}
)

func (idxOps *BadgerIndexedOperator) andComputationalSearch(
	ctx context.Context,
	index *indexed_operation_models.IdxOpsIndex,
) ([]byte, error) {
	objValue, err := idxOps.computationalSearch(ctx, index, andComputationalSearchFn)
	if err != nil {
		err = fmt.Errorf("failed to retrieve data on AND computational search: %v", err)
	}

	return objValue, err
}

func (idxOps *BadgerIndexedOperator) orComputationalSearch(
	ctx context.Context,
	index *indexed_operation_models.IdxOpsIndex,
) ([]byte, error) {
	objValue, err := idxOps.computationalSearch(ctx, index, orComputationalSearchFn)
	if err != nil {
		err = fmt.Errorf("failed to retrieve data on OR computational search: %v", err)
	}

	return objValue, err
}

func (idxOps *BadgerIndexedOperator) computationalSearch(
	ctx context.Context,
	index *indexed_operation_models.IdxOpsIndex,
	fn func(
		indexedKeys [][]byte,
		operator operations.Operator,
	) ([]byte, error),
) ([]byte, error) {
	indexedMemoryDBInfo, err := idxOps.getIndexedDDMemoryInfo(ctx)
	if err != nil {
		return []byte{}, fmt.Errorf("failed to get indexed memory info: %v", err)
	}

	var objKey, objValue []byte
	if err = idxOps.dbInfo.DB.View(func(txn *badger.Txn) error {
		operator := idxOps.getIndexedOperator(indexedMemoryDBInfo)

		if objKey, err = fn(index.IndexKeys, operator); err != nil {
			return fmt.Errorf("failed on computational search pattern: %v", err)
		}

		item, err := txn.Get(objKey)
		if err != nil {
			return fmt.Errorf("failed to get item value from key[%v]: %v", objKey, err)
		}

		if objValue, err = item.ValueCopy(nil); err != nil {
			return fmt.Errorf("failed to copy item value: %v", err)
		}

		return err
	}); err != nil {
		err = fmt.Errorf("failed to retrieve data on computational search: %v", err)
	}

	return objValue, err
}

func (idxOps *BadgerIndexedOperator) straightSearch(
	ctx context.Context,
	index *indexed_operation_models.IdxOpsIndex,
) ([]byte, error) {
	if index == nil || len(index.IndexKeys) != 1 {
		err := fmt.Errorf("straightSearch requires index key list with len of 1")
		return []byte{}, fmt.Errorf("invalid index payload size for giving option: %v", err)
	}

	indexedMemoryDBInfo, err := idxOps.getIndexedDDMemoryInfo(ctx)
	if err != nil {
		return []byte{}, fmt.Errorf("failed to get indexed memory info: %v", err)
	}

	var objKey, objValue []byte
	if err = idxOps.dbInfo.DB.View(func(txn *badger.Txn) error {
		if objKey, err = idxOps.getIndexedOperator(indexedMemoryDBInfo).
			Load(index.IndexKeys[0]); err != nil {
			return fmt.Errorf("failed to load from key - %v: %v", index.IndexKeys[0], err)
		}

		item, err := txn.Get(objKey)
		if err != nil {
			return fmt.Errorf("failed to get object value from object key[%v]: %v", objKey, err)
		}

		objValue, err = item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to copy object value from object key[%v]: %v", objKey, err)
		}

		return err
	}); err != nil {
		err = fmt.Errorf("failed to straightSearch: %v", err)
	}

	return objValue, err
}

func removeBytes(obj, index []byte) [][]byte {
	splitObj := bytes.Split(obj, []byte(bsSeparator))
	var newObj [][]byte
	for _, bs := range splitObj {
		if bytes.Equal(bs, index) {
			continue
		}

		newObj = append(newObj, bs)
	}

	return newObj
}

func (idxOps *BadgerIndexedOperator) getOperator() operations.Operator {
	return idxOps.operator.Operate(idxOps.dbInfo)
}

func (idxOps *BadgerIndexedOperator) getIndexedOperator(
	dbInfo *management_models.DBMemoryInfo,
) operations.Operator {
	return idxOps.operator.Operate(dbInfo)
}
