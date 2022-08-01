package indexed_operations

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v3"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	indexed_manager "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager/indexed"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/transactions/operations"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
	indexed_operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation/indexed"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
)

type (
	IndexedOperator interface {
		Operate(dbInfo *management_models.DBMemoryInfo) IndexedOperator

		CreateIndexed(
			ctx context.Context,
			item *operation_models.OpItem,
			index *indexed_operation_models.IdxOpsIndex,
		) error
		UpdateIndexed(
			ctx context.Context,
			item *operation_models.OpItem,
			index *indexed_operation_models.IdxOpsIndex,
		) error

		LoadIndexed(
			ctx context.Context,
			item *operation_models.OpItem,
			index *indexed_operation_models.IdxOpsIndex,
		) (bs []byte, err error)
		DeleteIndexed(
			ctx context.Context,
			item *operation_models.OpItem,
			index *indexed_operation_models.IdxOpsIndex,
		) error

		ListIndexed(
			ctx context.Context,
			index *indexed_operation_models.IdxOpsIndex,
			pagination *management_models.PaginationRequest,
		) (operation_models.OpList, error)
	}

	BadgerIndexedOperator struct {
		manager    manager.Manager
		idxManager indexed_manager.IndexerManager
		operator   operations.Operator
		dbInfo     *management_models.DBMemoryInfo
		serializer go_serializer.Serializer
	}
)

func NewBadgerIndexedOperator(
	manager manager.Manager,
	idxManager indexed_manager.IndexerManager,
	operator operations.Operator,
	serializer go_serializer.Serializer,
) IndexedOperator {
	idxOp := &BadgerIndexedOperator{
		manager:    manager,
		idxManager: idxManager,
		operator:   operator,
		serializer: serializer,
	}

	return idxOp
}

// Operate operates in the given database.
func (idxOps *BadgerIndexedOperator) Operate(
	dbInfo *management_models.DBMemoryInfo,
) IndexedOperator {
	idxOp := &BadgerIndexedOperator{
		manager:    idxOps.manager,
		operator:   idxOps.operator,
		dbInfo:     dbInfo,
		serializer: idxOps.serializer,
	}
	idxOp.operator.Operate(dbInfo)

	return idxOp
}

// CreateIndexed checks if the key exists, if not, it stores the item.
func (idxOps *BadgerIndexedOperator) CreateIndexed(
	ctx context.Context,
	item *operation_models.OpItem,
	index *indexed_operation_models.IdxOpsIndex,
) error {
	if !index.ShallIndex {
		return idxOps.getOperator().Create(item.Key, item.Value)
	}

	err := idxOps.dbInfo.DB.Update(func(txn *badger.Txn) error {
		if err := idxOps.createIndexRelation(ctx, item, index); err != nil {
			return fmt.Errorf("failed to create index relation: %v", err)
		}

		err := idxOps.getOperator().Create(item.Key, item.Value)
		if err != nil {
			err = fmt.Errorf("failed to store key-value pair: %v", err)
			err = fmt.Errorf("%v - key: %s; value: %s", err, item.Key, item.Value)
		}

		return err
	})

	return err
}

// UpdateIndexed updates or creates the key value and updates indexes no matter if the keys already exist or not.
func (idxOps *BadgerIndexedOperator) UpdateIndexed(
	ctx context.Context,
	item *operation_models.OpItem,
	index *indexed_operation_models.IdxOpsIndex,
) error {
	if !index.ShallIndex {
		return idxOps.getOperator().Update(item.Key, item.Value)
	}

	err := idxOps.dbInfo.DB.Update(func(txn *badger.Txn) error {
		if err := idxOps.updateIndexRelation(ctx, index); err != nil {
			return fmt.Errorf("failed to create index relation: %v", err)
		}

		err := txn.Set(item.Key, item.Value)
		if err != nil {
			err = fmt.Errorf("failed to store key-value pair: %v", err)
			err = fmt.Errorf("%v - key: %s; value: %s", err, item.Key, item.Value)
		}

		return err
	})

	return err
}

// DeleteIndexed deletes the given key entry if present.
func (idxOps *BadgerIndexedOperator) DeleteIndexed(
	ctx context.Context,
	item *operation_models.OpItem,
	index *indexed_operation_models.IdxOpsIndex,
) (err error) {
	if !index.ShallIndex {
		return idxOps.getOperator().Delete(item.Key)
	}

	switch index.IndexProperties.IndexDeletionBehaviour {
	case indexed_operation_models.Cascade:
		err = idxOps.deleteCascade(ctx, item)
	case indexed_operation_models.IndexOnly:
		err = idxOps.deleteIdxOnly(ctx, index)
	case indexed_operation_models.None:
		fallthrough
	default:
		err = idxOps.getOperator().Delete(item.Key)
	}

	return err
}

// LoadIndexed checks the given key and returns the serialized item's value whether it exists.
func (idxOps *BadgerIndexedOperator) LoadIndexed(
	ctx context.Context,
	item *operation_models.OpItem,
	index *indexed_operation_models.IdxOpsIndex,
) (bs []byte, err error) {
	if !index.ShallIndex {
		return idxOps.getOperator().Load(item.Key)
	}

	switch index.IndexProperties.IndexSearchPattern {
	case indexed_operation_models.AndComputational:
		bs, err = idxOps.andComputationalSearch(ctx, index)
	case indexed_operation_models.OrComputational:
		bs, err = idxOps.orComputationalSearch(ctx, index)
	case indexed_operation_models.One:
		fallthrough
	default:
		bs, err = idxOps.straightSearch(ctx, index)
	}

	return
}

func (idxOps *BadgerIndexedOperator) listAllIndexed(
	ctx context.Context,
	index *indexed_operation_models.IdxOpsIndex,
) (operation_models.OpList, error) {
	indexedMemoryDBInfo, err := idxOps.getIndexedDBMemoryInfo(ctx)
	if err != nil {
		return operation_models.OpList{}, fmt.Errorf("failed to get indexed memory info: %v", err)
	}

	var objectList operation_models.OpList
	err = idxOps.dbInfo.DB.View(func(txn *badger.Txn) error {
		operator := idxOps.getIndexedOperator(indexedMemoryDBInfo)

		for _, key := range index.IndexKeys {
			objKey, err := operator.Load(key)
			if err != nil {
				continue
			}

			objValue, err := idxOps.getOperator().Load(objKey)
			if err != nil {
				continue
			}

			objectList = append(objectList, &operation_models.OpItem{
				Key:   objKey,
				Value: objValue,
			})
		}

		return nil
	})
	if err != nil {
		err = fmt.Errorf("failed to listAllIndexed")
	}

	return objectList, err
}

func (idxOps *BadgerIndexedOperator) listPaginatedIndexed(
	ctx context.Context,
	index *indexed_operation_models.IdxOpsIndex,
	pagination *management_models.PaginationRequest,
) (operation_models.OpList, error) {
	indexedMemoryDBInfo, err := idxOps.getIndexedDBMemoryInfo(ctx)
	if err != nil {
		return operation_models.OpList{}, fmt.Errorf("failed to get indexed memory info: %v", err)
	}

	page := int(pagination.PageID)
	size := int(pagination.PageSize)
	limit := size
	offset := (page - 1) * size

	if limit+offset > len(index.IndexKeys) {
		return operation_models.OpList{}, fmt.Errorf("pagination grater than index key list")
	}

	paginatedIndexedKeys := index.IndexKeys[offset:limit]

	objectList := make(operation_models.OpList, size)
	if err := idxOps.dbInfo.DB.View(func(txn *badger.Txn) error {
		operator := idxOps.getIndexedOperator(indexedMemoryDBInfo)

		for idx, key := range paginatedIndexedKeys {
			objKey, err := operator.Load(key)
			if err != nil {
				continue
			}

			objValue, err := idxOps.getOperator().Load(objKey)
			if err != nil {
				continue
			}

			objItem := &operation_models.OpItem{
				Key:   objKey,
				Value: objValue,
			}
			objectList[idx] = objItem
		}

		return nil
	}); err != nil {
		err = fmt.Errorf("failed to listPaginatedIndexed")
	}

	return objectList, nil
}

func (idxOps *BadgerIndexedOperator) ListIndexed(
	ctx context.Context,
	index *indexed_operation_models.IdxOpsIndex,
	pagination *management_models.PaginationRequest,
) (operation_models.OpList, error) {
	pageID := int(pagination.PageID)
	pageSize := int(pagination.PageSize)
	if ok, err := idxOps.manager.ValidatePagination(pageSize, pageID); !ok {
		if err != nil {
			return operation_models.OpList{}, fmt.Errorf("failed to validate pagination: %v", err)
		}

		if !index.ShallIndex {
			return idxOps.getOperator().ListAll()
		}

		return idxOps.listAllIndexed(ctx, index)
	}

	if !index.ShallIndex {
		return idxOps.getOperator().ListPaginated(pagination)
	}

	return idxOps.listPaginatedIndexed(ctx, index, pagination)
}
