package operations

import (
	"bytes"
	"context"
	"fmt"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
	co "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
)

const (
	bsSeparator = "&#-#&"

	ErrKeyAlreadyExist = "key already exist"
)

func (o *BadgerOperator) indexedStoreOperator(
	ctx context.Context,
) (*BadgerOperator, error) {
	indexedName := o.dbInfo.Name + manager.IndexedSuffixName
	idxMemoryInfo, err := o.manager.GetDBMemoryInfo(ctx, indexedName)
	if err != nil {
		return o, err
	}

	idxOp := o.operate(idxMemoryInfo)

	return idxOp, nil
}

func (o *BadgerOperator) indexedListStoreOperator(
	ctx context.Context,
) (*BadgerOperator, error) {
	indexedListName := o.dbInfo.Name + manager.IndexedListSuffixName
	idxListMemoryInfo, err := o.manager.GetDBMemoryInfo(ctx, indexedListName)
	if err != nil {
		return o, err
	}

	idxListOp := o.operate(idxListMemoryInfo)

	return idxListOp, nil
}

func (o *BadgerOperator) deleteCascade(
	ctx context.Context,
	item *operation_models.Item,
	opts *operation_models.IndexOpts,
	retrialOpts *co.RetrialOpts,
) error {
	txn := o.dbInfo.DB.NewTransaction(true)
	var deleteFn = func() error {
		return txn.Delete(item.Key)
	}
	var deleteRollbackFn = func() error {
		txn.Discard()
		return nil
	}

	idxListOp, err := o.indexedListStoreOperator(ctx)
	if err != nil {
		return err
	}
	idxListTxn := idxListOp.dbInfo.DB.NewTransaction(true)
	bsIdxs, err := idxListOp.load(item.Key)
	if err != nil {
		return err
	}
	idxs := bytes.Split(bsIdxs, []byte(bsSeparator))
	idxOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return err
	}
	idxTxn := idxOp.dbInfo.DB.NewTransaction(true)

	var deleteIdxFn = func() error {
		for _, idx := range idxs {
			opsErr := idxTxn.Delete(idx)
			if opsErr != nil {
				return opsErr
			}
		}

		return nil
	}
	var deleteIdxRollbackFn = func() error {
		idxTxn.Discard()

		return nil
	}
	var createIdxFn = func() error {
		for _, idx := range idxs {
			opsErr := idxOp.upsert(idx, item.Key)
			if opsErr != nil {
				continue
			}
		}

		return nil
	}

	var deleteIdxListFn = func() error {
		return idxListTxn.Delete(item.Key)
	}
	var deleteIdxListRollbackFn = func() error {
		idxListTxn.Discard()
		return nil
	}
	var createIdxListFn = func() error {
		return o.create(item.Key, bsIdxs)
	}

	var commitFn = func() error {
		if err := idxListTxn.Commit(); err != nil {
			return err
		}
		if err := idxTxn.Commit(); err != nil {
			return err
		}
		if err := txn.Commit(); err != nil {
			return err
		}

		return nil
	}

	commitOps := &co.Ops{
		Action: &co.Action{
			Act:         commitFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: createIdxFn,
			RetrialOpts: retrialOpts,
			Next: &co.Ops{
				Action: nil,
				RollbackAction: &co.RollbackAction{
					RollbackAct: createIdxListFn,
					RetrialOpts: retrialOpts,
					Next:        nil,
				},
			},
		},
	}

	deleteIdxListOps := &co.Ops{
		Action: &co.Action{
			Act:         deleteIdxListFn,
			RetrialOpts: retrialOpts,
			Next:        commitOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: deleteIdxListRollbackFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
	}

	deleteIdxOps := &co.Ops{
		Action: &co.Action{
			Act:         deleteIdxFn,
			RetrialOpts: retrialOpts,
			Next:        deleteIdxListOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: deleteIdxRollbackFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
	}
	deleteIdxListOps.RollbackAction.Next = deleteIdxOps

	deleteOps := &co.Ops{
		Action: &co.Action{
			Act:         deleteFn,
			RetrialOpts: retrialOpts,
			Next:        deleteIdxOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: deleteRollbackFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
	}
	deleteIdxOps.RollbackAction.Next = deleteOps

	err = o.chainedOperator.Operate(deleteOps)
	return err
}

func (o *BadgerOperator) deleteIdxOnly(
	ctx context.Context,
	item *operation_models.Item,
	opts *operation_models.IndexOpts,
	retrialOpts *co.RetrialOpts,
) error {
	idxOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return err
	}
	idxTxn := idxOp.dbInfo.DB.NewTransaction(true)

	mainIdx, err := idxOp.load(opts.IndexingKeys[0])
	if err != nil {
		return err
	}

	var deleteIdxFn = func() error {
		for _, deleteIdx := range opts.IndexingKeys {
			opErr := idxTxn.Delete(deleteIdx)
			if opErr != nil {
				return opErr
			}
		}

		return nil
	}
	var deleteIdxRollbackFn = func() error {
		idxTxn.Discard()

		return nil
	}
	var createIdxFn = func() error {
		for _, deleteIdx := range opts.IndexingKeys {
			opErr := idxOp.upsert(deleteIdx, mainIdx)
			if opErr != nil {
				return opErr
			}
		}

		return nil
	}

	idxListOp, err := o.indexedListStoreOperator(ctx)
	if err != nil {
		return err
	}
	idxListTxn := idxListOp.dbInfo.DB.NewTransaction(true)
	var updateIdxListRelationFn = func() error {
		bsIdxs, err := idxListOp.load(mainIdx)
		if err != nil {
			return err
		}

		newBsIdxs := removeBytesBytes(bsIdxs, opts.IndexingKeys)

		return idxListTxn.Set(mainIdx, newBsIdxs)
	}
	var updateIdxListRelationRollbackFn = func() error {
		idxListTxn.Discard()
		return nil
	}

	var commitFn = func() error {
		if err := idxTxn.Commit(); err != nil {
			return err
		}
		if err := idxListTxn.Commit(); err != nil {
			return err
		}

		return nil
	}

	commitOps := &co.Ops{
		Action: &co.Action{
			Act:         commitFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: createIdxFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
	}

	updateIdxListRelationFnOps := &co.Ops{
		Action: &co.Action{
			Act:         updateIdxListRelationFn,
			RetrialOpts: retrialOpts,
			Next:        commitOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: updateIdxListRelationRollbackFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
	}

	deleteIdxOps := &co.Ops{
		Action: &co.Action{
			Act:         deleteIdxFn,
			RetrialOpts: retrialOpts,
			Next:        updateIdxListRelationFnOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: deleteIdxRollbackFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
	}
	updateIdxListRelationFnOps.RollbackAction.Next = deleteIdxOps

	err = o.chainedOperator.Operate(deleteIdxOps)
	return err
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

func removeBytesBytes(obj []byte, index [][]byte) []byte {
	splitObj := bytes.Split(obj, []byte(bsSeparator))

	var newObj [][]byte

	for _, bsObj := range splitObj {
		for _, bsIdx := range index {
			if bytes.Equal(bsObj, bsIdx) {
				continue
			}

			newObj = append(newObj, bsObj)
		}
	}

	newBsObj := bytes.Join(newObj, []byte(bsSeparator))

	return newBsObj
}

func (o *BadgerOperator) andComputationalSearchFn(
	indexedKeys [][]byte,
) ([]byte, error) {
	var objKey []byte
	for _, key := range indexedKeys {
		keyValue, err := o.load(key)
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

func (o *BadgerOperator) orComputationalSearchFn(
	indexedKeys [][]byte,
) ([]byte, error) {
	var objKey []byte
	for _, key := range indexedKeys {
		keyValue, err := o.load(key)
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

func (o *BadgerOperator) andComputationalSearch(
	ctx context.Context,
	opts *operation_models.IndexOpts,
) (objValue []byte, err error) {
	indexedOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return []byte{}, fmt.Errorf("failed to get indexed memory info: %v", err)
	}

	objValue, err = o.computationalSearch(ctx, opts, indexedOp.andComputationalSearchFn)
	if err != nil {
		err = fmt.Errorf("failed to retrieve data on AND computational search: %v", err)
	}

	return
}

func (o *BadgerOperator) orComputationalSearch(
	ctx context.Context,
	opts *operation_models.IndexOpts,
) ([]byte, error) {
	indexedOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return []byte{}, fmt.Errorf("failed to get indexed memory info: %v", err)
	}

	objValue, err := o.computationalSearch(ctx, opts, indexedOp.orComputationalSearchFn)
	if err != nil {
		err = fmt.Errorf("failed to retrieve data on OR computational search: %v", err)
	}

	return objValue, err
}

func (o *BadgerOperator) computationalSearch(
	ctx context.Context,
	opts *operation_models.IndexOpts,
	fn func(
	indexedKeys [][]byte,
) ([]byte, error),
) ([]byte, error) {
	objKey, err := fn(opts.IndexingKeys)
	if err != nil {
		return []byte{}, fmt.Errorf("failed on computational search pattern: %v", err)
	}

	return o.load(objKey)
}

func (o *BadgerOperator) straightSearch(
	ctx context.Context,
	opts *operation_models.IndexOpts,
) ([]byte, error) {
	if opts == nil || len(opts.IndexingKeys) != 1 {
		err := fmt.Errorf("straightSearch requires index key list with length of 1")
		return []byte{}, fmt.Errorf("invalid index payload size for giving option: %v", err)
	}

	idxOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return []byte{}, fmt.Errorf("failed to get indexed memory info: %v", err)
	}

	var objKey, objValue []byte
	if objKey, err = idxOp.load(opts.IndexingKeys[0]); err != nil {
		return []byte{}, fmt.Errorf("failed to load from key - %v: %v", opts.IndexingKeys[0], err)
	}

	if objValue, err = o.load(objKey); err != nil {
		return []byte{}, fmt.Errorf("failed to load from key - %v: %v", string(objKey), err)
	}

	return objValue, err
}

func (o *BadgerOperator) indexingList(
	ctx context.Context,
	opts *operation_models.IndexOpts,
) (idxList []byte, err error) {
	idxListOp, err := o.indexedListStoreOperator(ctx)
	if err != nil {
		return idxList, err
	}

	if opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0 {
		return idxListOp.load(opts.ParentKey)
	} else if len(opts.IndexingKeys) > 1 {
		return idxList, fmt.Errorf("to many indexing keys")
	}

	idxOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return idxList, err
	}
	objKey, err := idxOp.load(opts.IndexingKeys[0])
	if err != nil {
		return idxList, err
	}

	return idxListOp.load(objKey)
}
