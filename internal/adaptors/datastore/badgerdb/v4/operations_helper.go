package v4

import (
	"bytes"
	"context"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/saga"

	badgerdb_operation_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/operation"
)

const (
	bsSeparator = "&#-#&"

	ErrKeyAlreadyExist = "key already exist"
)

func (o *BadgerOperatorV4) indexedStoreOperator(
	ctx context.Context,
) (*BadgerOperatorV4, error) {
	indexedName := o.dbInfo.Name + IndexedSuffixName
	idxMemoryInfo, err := o.manager.GetDBMemoryInfo(ctx, indexedName)
	if err != nil {
		return o, err
	}

	idxOp := o.operate(idxMemoryInfo)

	return idxOp, nil
}

func (o *BadgerOperatorV4) indexedListStoreOperator(
	ctx context.Context,
) (*BadgerOperatorV4, error) {
	indexedListName := o.dbInfo.Name + IndexedListSuffixName
	idxListMemoryInfo, err := o.manager.GetDBMemoryInfo(ctx, indexedListName)
	if err != nil {
		return o, err
	}

	idxListOp := o.operate(idxListMemoryInfo)

	return idxListOp, nil
}

func (o *BadgerOperatorV4) deleteCascadeByIdx(
	ctx context.Context,
	item *badgerdb_operation_models_v4.Item,
	opts *badgerdb_operation_models_v4.IndexOpts,
	retrialOpts *saga.RetrialOpts,
) error {
	if opts != nil && opts.HasIdx && len(opts.IndexingKeys) == 1 {
		idxOp, err := o.indexedStoreOperator(ctx)
		if err != nil {
			return errorsx.Wrap(err, "failed to get indexed memory info on deleteCascadeByIdx")
		}

		var objKey []byte
		if objKey, err = idxOp.load(opts.IndexingKeys[0]); err != nil {
			return errorsx.Wrapf(err, "deleteCascadeByIdx - failed to load from key - %v", opts.IndexingKeys[0])
		}

		item.Key = objKey
	}

	return o.deleteCascade(ctx, item, opts, retrialOpts)
}

func (o *BadgerOperatorV4) deleteCascade(
	ctx context.Context,
	item *badgerdb_operation_models_v4.Item,
	opts *badgerdb_operation_models_v4.IndexOpts,
	retrialOpts *saga.RetrialOpts,
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
	var createIdxListFn = func() error {
		return o.create(item.Key, bsIdxs)
	}

	var commitFn = func() error {
		if err = idxListTxn.Commit(); err != nil {
			return err
		}
		if err = idxTxn.Commit(); err != nil {
			return err
		}
		if err = txn.Commit(); err != nil {
			return err
		}

		return nil
	}

	operations := []*saga.Operation{
		{
			Action: &saga.Action{
				Do:          deleteFn,
				RetrialOpts: retrialOpts,
			},
		},
		{
			Action: &saga.Action{
				Do:          deleteIdxFn,
				RetrialOpts: retrialOpts,
			},
			Rollback: &saga.Rollback{
				Do:          deleteRollbackFn,
				RetrialOpts: retrialOpts,
			},
		},
		{
			Action: &saga.Action{
				Do:          deleteIdxListFn,
				RetrialOpts: retrialOpts,
			},
			Rollback: &saga.Rollback{
				Do:          createIdxFn,
				RetrialOpts: retrialOpts,
			},
		},
		{
			Action: &saga.Action{
				Do:          commitFn,
				RetrialOpts: retrialOpts,
			},
			Rollback: &saga.Rollback{
				Do:          createIdxListFn,
				RetrialOpts: retrialOpts,
			},
		},
	}

	return saga.NewListOperator(operations...).Operate()
}

func (o *BadgerOperatorV4) deleteIdxOnly(
	ctx context.Context,
	item *badgerdb_operation_models_v4.Item,
	opts *badgerdb_operation_models_v4.IndexOpts,
	retrialOpts *saga.RetrialOpts,
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
		if err = idxTxn.Commit(); err != nil {
			return err
		}
		if err = idxListTxn.Commit(); err != nil {
			return err
		}

		return nil
	}

	operations := []*saga.Operation{
		{
			Action: &saga.Action{
				Do:          deleteIdxFn,
				RetrialOpts: retrialOpts,
			},
			Rollback: &saga.Rollback{
				Do:          deleteIdxRollbackFn,
				RetrialOpts: retrialOpts,
			},
		},
		{
			Action: &saga.Action{
				Do:          updateIdxListRelationFn,
				RetrialOpts: retrialOpts,
			},
			Rollback: &saga.Rollback{
				Do:          createIdxFn,
				RetrialOpts: retrialOpts,
			},
		},
		{
			Action: &saga.Action{
				Do:          commitFn,
				RetrialOpts: retrialOpts,
			},
			Rollback: &saga.Rollback{
				Do:          updateIdxListRelationRollbackFn,
				RetrialOpts: retrialOpts,
			},
		},
	}

	return saga.NewListOperator(operations...).Operate()
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

func (o *BadgerOperatorV4) andComputationalSearchFn(
	indexedKeys [][]byte,
) ([]byte, error) {
	var objKey []byte
	for _, key := range indexedKeys {
		keyValue, err := o.load(key)
		if err != nil {
			return []byte{}, errorsx.Wrap(err, "inconsistent database err")
		}

		if !bytes.Equal(objKey, keyValue) && objKey != nil {
			err = errorsx.Wrap(err, "inconsistent key-value indexing")
			return []byte{}, errorsx.Wrap(err, "inconsistent database err")
		}

		objKey = keyValue
	}

	return objKey, nil
}

func (o *BadgerOperatorV4) orComputationalSearchFn(
	indexedKeys [][]byte,
) ([]byte, error) {
	var objKey []byte
	for _, key := range indexedKeys {
		keyValue, err := o.load(key)
		if err != nil {
			continue
		}

		if !bytes.Equal(objKey, keyValue) && objKey != nil {
			err = errorsx.Wrap(err, "inconsistent key-value indexing")
			return []byte{}, errorsx.Wrap(err, "inconsistent database err")
		}

		objKey = keyValue
	}

	return objKey, nil
}

func (o *BadgerOperatorV4) andComputationalSearch(
	ctx context.Context,
	opts *badgerdb_operation_models_v4.IndexOpts,
) (objValue []byte, err error) {
	indexedOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return []byte{}, errorsx.Wrap(err, "failed to get indexed memory info")
	}

	objValue, err = o.computationalSearch(ctx, opts, indexedOp.andComputationalSearchFn)
	if err != nil {
		err = errorsx.Wrap(err, "failed to retrieve data on AND computational search")
	}

	return
}

func (o *BadgerOperatorV4) orComputationalSearch(
	ctx context.Context,
	opts *badgerdb_operation_models_v4.IndexOpts,
) ([]byte, error) {
	indexedOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return []byte{}, errorsx.Wrap(err, "failed to get indexed memory info")
	}

	objValue, err := o.computationalSearch(ctx, opts, indexedOp.orComputationalSearchFn)
	if err != nil {
		err = errorsx.Wrap(err, "failed to retrieve data on OR computational search")
	}

	return objValue, err
}

func (o *BadgerOperatorV4) computationalSearch(
	ctx context.Context,
	opts *badgerdb_operation_models_v4.IndexOpts,
	fn func(
		indexedKeys [][]byte,
	) ([]byte, error),
) ([]byte, error) {
	objKey, err := fn(opts.IndexingKeys)
	if err != nil {
		return []byte{}, errorsx.Wrap(err, "failed on computational search pattern")
	}

	return o.load(objKey)
}

func (o *BadgerOperatorV4) straightSearch(
	ctx context.Context,
	opts *badgerdb_operation_models_v4.IndexOpts,
) ([]byte, error) {
	if opts == nil || len(opts.IndexingKeys) != 1 {
		err := errorsx.New("straightSearch requires index key list with length of 1")
		return []byte{}, errorsx.Wrap(err, "invalid index payload size for giving option")
	}

	idxOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return []byte{}, errorsx.Wrap(err, "failed to get indexed memory info")
	}

	var objKey, objValue []byte
	if objKey, err = idxOp.load(opts.IndexingKeys[0]); err != nil {
		return []byte{}, errorsx.Wrapf(err, "failed to load from key - %v", opts.IndexingKeys[0])
	}

	if objValue, err = o.load(objKey); err != nil {
		return []byte{}, errorsx.Wrapf(err, "failed to load from key - %v", string(objKey))
	}

	return objValue, err
}

func (o *BadgerOperatorV4) indexingList(
	ctx context.Context,
	opts *badgerdb_operation_models_v4.IndexOpts,
) (idxList []byte, err error) {
	idxListOp, err := o.indexedListStoreOperator(ctx)
	if err != nil {
		return idxList, err
	}

	if opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0 {
		return idxListOp.load(opts.ParentKey)
	} else if len(opts.IndexingKeys) > 1 {
		return idxList, errorsx.New("too many indexing keys")
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
