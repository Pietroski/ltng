package badgerdb_operations_adaptor_v4

import (
	"bytes"
	"context"
	"fmt"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	serializer_models "gitlab.com/pietroski-software-company/devex/golang/serializer/models"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	badgerdb_manager_adaptor_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v4/manager"
	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/management"
	badgerdb_operation_models_v4 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v4/operation"
	co "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
	lo "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/list-operator"
)

type (
	Operator interface {
		Operate(dbInfo *badgerdb_management_models_v4.DBMemoryInfo) Operator

		Create(
			ctx context.Context,
			item *badgerdb_operation_models_v4.Item,
			opts *badgerdb_operation_models_v4.IndexOpts,
			retrialOpts *lo.RetrialOpts,
		) error
		Upsert(
			ctx context.Context,
			item *badgerdb_operation_models_v4.Item,
			opts *badgerdb_operation_models_v4.IndexOpts,
			retrialOpts *lo.RetrialOpts,
		) error
		Delete(
			ctx context.Context,
			item *badgerdb_operation_models_v4.Item,
			opts *badgerdb_operation_models_v4.IndexOpts,
			retrialOpts *lo.RetrialOpts,
		) error
		Load(
			ctx context.Context,
			item *badgerdb_operation_models_v4.Item,
			opts *badgerdb_operation_models_v4.IndexOpts,
		) ([]byte, error)

		List(
			ctx context.Context,
			//item *badgerdb_operation_models_v4.Item,
			opts *badgerdb_operation_models_v4.IndexOpts,
			pagination *badgerdb_management_models_v4.Pagination,
		) (badgerdb_operation_models_v4.Items, error)
		ListValuesFromIndexingKeys(
			ctx context.Context,
			opts *badgerdb_operation_models_v4.IndexOpts,
		) (badgerdb_operation_models_v4.Items, error)
	}

	BadgerOperatorV4Params struct {
		Manager    badgerdb_manager_adaptor_v4.Manager
		Serializer serializer_models.Serializer
	}

	BadgerOperatorV4 struct {
		manager    badgerdb_manager_adaptor_v4.Manager
		dbInfo     *badgerdb_management_models_v4.DBMemoryInfo
		serializer serializer_models.Serializer

		chainedOperator *co.ChainOperator
		listOperator    *lo.ListOperator

		// TODO: add a context global retrial opts
	}
)

func NewBadgerOperatorV4(
	ctx context.Context, opts ...options.Option,
) (*BadgerOperatorV4, error) {
	o := &BadgerOperatorV4{
		serializer:      serializer.NewJsonSerializer(),
		chainedOperator: co.NewChainOperator(),
	}
	options.ApplyOptions(o, opts...)

	return o, nil
}

// Operate operates in the given database.
func (o *BadgerOperatorV4) Operate(dbInfo *badgerdb_management_models_v4.DBMemoryInfo) Operator {
	no := &BadgerOperatorV4{
		manager:    o.manager,
		dbInfo:     dbInfo,
		serializer: o.serializer,
	}

	return no
}

// operateInternally operates in the given database.
func (o *BadgerOperatorV4) operate(dbInfo *badgerdb_management_models_v4.DBMemoryInfo) *BadgerOperatorV4 {
	no := &BadgerOperatorV4{
		manager:    o.manager,
		dbInfo:     dbInfo,
		serializer: o.serializer,
	}

	return no
}

// Create checks if the key exists, if not, it stores the item.
func (o *BadgerOperatorV4) Create(
	ctx context.Context,
	item *badgerdb_operation_models_v4.Item,
	opts *badgerdb_operation_models_v4.IndexOpts,
	retrialOpts *lo.RetrialOpts,
) error {
	txn := o.dbInfo.DB.NewTransaction(true)
	var createFn = func() error {
		return o.createWithTxn(txn, item.Key, item.Value)
	}

	// If the op does not have an idx, we can skip the rest
	if !opts.HasIdx {
		var commitCreateStage = func() error {
			if err := txn.Commit(); err != nil {
				return err
			}

			return nil
		}

		operations := []*lo.Operation{
			{
				Action: &lo.Action{
					Act:         createFn,
					RetrialOpts: retrialOpts,
				},
			},
			{
				Action: &lo.Action{
					Act:         commitCreateStage,
					RetrialOpts: retrialOpts,
				},
			},
		}

		return lo.New(operations...).Operate()
	}

	idxOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return err
	}
	idxTxn := idxOp.dbInfo.DB.NewTransaction(true)
	var createIdxsFn = func() (err error) {
		for _, idx := range opts.IndexingKeys {
			err = idxOp.createWithTxn(idxTxn, idx, opts.ParentKey)
			if err != nil {
				return
			}
		}

		return
	}
	var deleteIdxsRollbackFn = func() error {
		for _, idx := range opts.IndexingKeys {
			err = idxOp.delete(idx)
			if err != nil {
				continue
			}
		}

		return nil
	}

	idxListOp, err := o.indexedListStoreOperator(ctx)
	if err != nil {
		return err
	}
	idxListTxn := idxListOp.dbInfo.DB.NewTransaction(true)
	var createIdxListFn = func() error {
		idxList := bytes.Join(opts.IndexingKeys, []byte(bsSeparator))
		return idxListOp.createWithTxn(idxListTxn, opts.ParentKey, idxList)
	}
	var deleteIdxListRollbackFn = func() error {
		idxList := bytes.Join(opts.IndexingKeys, []byte(bsSeparator))
		return idxListOp.delete(idxList)
	}

	var commitStage = func() error {
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

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         createFn,
				RetrialOpts: retrialOpts,
			},
		},
		{
			Action: &lo.Action{
				Act:         createIdxsFn,
				RetrialOpts: retrialOpts,
			},
		},
		{
			Action: &lo.Action{
				Act:         createIdxListFn,
				RetrialOpts: retrialOpts,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteIdxsRollbackFn,
				RetrialOpts: retrialOpts,
			},
		},
		{
			Action: &lo.Action{
				Act:         commitStage,
				RetrialOpts: retrialOpts,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteIdxListRollbackFn,
				RetrialOpts: retrialOpts,
			},
		},
	}

	return lo.New(operations...).Operate()
}

// Upsert updates or creates the key value no matter if the key already exists or not.
func (o *BadgerOperatorV4) Upsert(
	ctx context.Context,
	item *badgerdb_operation_models_v4.Item,
	opts *badgerdb_operation_models_v4.IndexOpts,
	retrialOpts *lo.RetrialOpts,
) error {
	txn := o.dbInfo.DB.NewTransaction(true)
	var upsertFn = func() error {
		return o.upsertWithTxn(txn, item.Key, item.Value)
	}

	if !opts.HasIdx {
		var commitCreateStage = func() error {
			if err := txn.Commit(); err != nil {
				return err
			}

			return nil
		}

		operations := []*lo.Operation{
			{
				Action: &lo.Action{
					Act:         upsertFn,
					RetrialOpts: retrialOpts,
				},
			},
			{
				Action: &lo.Action{
					Act:         commitCreateStage,
					RetrialOpts: retrialOpts,
				},
			},
		}

		return lo.New(operations...).Operate()
	}

	// TODO: Take all the indexes, traverse, compare, delete or/and add it
	idxListOp, err := o.indexedListStoreOperator(ctx)
	if err != nil {
		return err
	}
	idxListTxn := idxListOp.dbInfo.DB.NewTransaction(true)

	idxOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return err
	}
	idxTxn := idxOp.dbInfo.DB.NewTransaction(true)

	newIdxList := make([][]byte, 0)
	var upsertIdxsFn = func() (err error) {
		indexListItem, err := idxListTxn.Get(opts.ParentKey)
		if err != nil {
			for _, idx := range opts.IndexingKeys {
				err = idxOp.upsertWithTxn(idxTxn, idx, opts.ParentKey)
				if err != nil {
					return
				}

				newIdxList = append(newIdxList, idx)
			}

			return nil
		}

		var rawIndexList []byte
		rawIndexList, err = indexListItem.ValueCopy(nil)
		if err != nil {
			return
		}

		indexList := bytes.Split(rawIndexList, []byte(bsSeparator))
		for _, optsIdxKey := range opts.IndexingKeys {
			var isThereIdx bool
			for _, idxKey := range indexList {
				if bytes.Equal(optsIdxKey, idxKey) {
					isThereIdx = true
					newIdxList = append(newIdxList, optsIdxKey)
				}
			}

			if !isThereIdx {
				err = idxOp.upsertWithTxn(idxTxn, optsIdxKey, opts.ParentKey)
				if err != nil {
					return
				}
			}
		}

		for _, idxKey := range indexList {
			if idxKey == nil {
				continue
			}

			var isThereIdx bool
			for _, optsIdxKey := range opts.IndexingKeys {
				if bytes.Equal(idxKey, optsIdxKey) {
					isThereIdx = true
					newIdxList = append(newIdxList, optsIdxKey)
				}
			}

			if !isThereIdx {
				err = idxTxn.Delete(idxKey)
				if err != nil {
					return
				}
			}
		}

		return nil
	}

	var deleteIdxsRollbackFn = func() error {
		for _, idx := range opts.IndexingKeys {
			err = idxOp.delete(idx)
			if err != nil {
				continue
			}
		}

		return nil
	}

	var upsertIdxListFn = func() error {
		idxList := bytes.Join(newIdxList, []byte(bsSeparator)) // opts.IndexingKeys newIdxList
		return idxListOp.upsertWithTxn(idxListTxn, opts.ParentKey, idxList)
	}
	var deleteIdxListRollbackFn = func() error {
		idxList := bytes.Join(newIdxList, []byte(bsSeparator)) // opts.IndexingKeys newIdxList
		return idxListOp.delete(idxList)
	}

	var commitStage = func() error {
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

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         upsertFn,
				RetrialOpts: retrialOpts,
			},
		},
		{
			Action: &lo.Action{
				Act:         upsertIdxsFn,
				RetrialOpts: retrialOpts,
			},
		},
		{
			Action: &lo.Action{
				Act:         upsertIdxListFn,
				RetrialOpts: retrialOpts,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteIdxsRollbackFn,
				RetrialOpts: retrialOpts,
			},
		},
		{
			Action: &lo.Action{
				Act:         commitStage,
				RetrialOpts: retrialOpts,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteIdxListRollbackFn,
				RetrialOpts: retrialOpts,
			},
		},
	}

	return lo.New(operations...).Operate()
}

// Delete deletes the given key entry if present.
func (o *BadgerOperatorV4) Delete(
	ctx context.Context,
	item *badgerdb_operation_models_v4.Item,
	opts *badgerdb_operation_models_v4.IndexOpts,
	retrialOpts *lo.RetrialOpts,
) error {
	if !opts.HasIdx {
		return o.delete(item.Key)
	}

	switch opts.IndexProperties.IndexDeletionBehaviour {
	case badgerdb_operation_models_v4.IndexOnly:
		return o.deleteIdxOnly(ctx, item, opts, retrialOpts)
	case badgerdb_operation_models_v4.Cascade:
		return o.deleteCascade(ctx, item, opts, retrialOpts)
	case badgerdb_operation_models_v4.CascadeByIdx:
		return o.deleteCascadeByIdx(ctx, item, opts, retrialOpts)
	case badgerdb_operation_models_v4.None:
		fallthrough
	default:
		return fmt.Errorf("delete was not called - invalid behaviour")
	}
}

// Load checks the given key and returns the serialized item's value whether it exists.
func (o *BadgerOperatorV4) Load(
	ctx context.Context,
	item *badgerdb_operation_models_v4.Item,
	opts *badgerdb_operation_models_v4.IndexOpts,
) (bs []byte, err error) {
	if !opts.HasIdx {
		return o.load(item.Key)
	}

	switch opts.IndexProperties.IndexSearchPattern {
	case badgerdb_operation_models_v4.IndexingList:
		bs, err = o.indexingList(ctx, opts)
	case badgerdb_operation_models_v4.AndComputational:
		bs, err = o.andComputationalSearch(ctx, opts)
	case badgerdb_operation_models_v4.OrComputational:
		bs, err = o.orComputationalSearch(ctx, opts)
	case badgerdb_operation_models_v4.One:
		fallthrough
	default:
		bs, err = o.straightSearch(ctx, opts)
	}

	return bs, err
}

func (o *BadgerOperatorV4) List(
	_ context.Context,
	//item *badgerdb_operation_models_v4.Item,
	opts *badgerdb_operation_models_v4.IndexOpts,
	pagination *badgerdb_management_models_v4.Pagination,
) (items badgerdb_operation_models_v4.Items, err error) {
	ok, err := o.manager.ValidatePagination(int(pagination.PageSize), int(pagination.PageID))
	if err != nil {
		return items, fmt.Errorf("invalid pagination: %v", err)
	}
	if ok {
		return o.listPaginated(pagination)
	}

	switch opts.IndexProperties.ListSearchPattern {
	case badgerdb_operation_models_v4.All:
		return o.listAll()
	case badgerdb_operation_models_v4.Default:
		fallthrough
	default:
		pagination = &badgerdb_management_models_v4.Pagination{
			PageID:   1,
			PageSize: 20,
		}
		return o.listPaginated(pagination)
	}
}

func (o *BadgerOperatorV4) ListValuesFromIndexingKeys(
	ctx context.Context,
	opts *badgerdb_operation_models_v4.IndexOpts,
) (badgerdb_operation_models_v4.Items, error) {
	idxOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return badgerdb_operation_models_v4.Items{}, nil
	}

	items := make(badgerdb_operation_models_v4.Items, len(opts.IndexingKeys))
	for idx, idxKey := range opts.IndexingKeys {
		objKey, err := idxOp.load(idxKey)
		if err != nil {
			item := &badgerdb_operation_models_v4.Item{
				Error: fmt.Errorf("error searching from idx key: %s", idxKey),
			}
			items[idx] = item

			continue
		}

		objValue, err := o.load(objKey)
		if err != nil {
			item := &badgerdb_operation_models_v4.Item{
				Error: fmt.Errorf("error searching from main key: %s", objKey),
			}
			items[idx] = item

			continue
		}

		item := &badgerdb_operation_models_v4.Item{
			Key:   idxKey,
			Value: objValue,
		}
		items[idx] = item
	}

	return items, nil
}
