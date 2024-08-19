package badgerdb_operations_adaptor_v3

import (
	"bytes"
	"context"
	"fmt"

	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"

	badgerdb_manager_adaptor_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/v3/manager"
	badgerdb_management_models_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v3/management"
	badgerdb_operation_models_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v3/operation"
	co "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
)

type (
	Operator interface {
		Operate(dbInfo *badgerdb_management_models_v3.DBMemoryInfo) Operator

		Create(
			ctx context.Context,
			item *badgerdb_operation_models_v3.Item,
			opts *badgerdb_operation_models_v3.IndexOpts,
			retrialOpts *co.RetrialOpts,
		) error
		Upsert(
			ctx context.Context,
			item *badgerdb_operation_models_v3.Item,
			opts *badgerdb_operation_models_v3.IndexOpts,
			retrialOpts *co.RetrialOpts,
		) error
		Delete(
			ctx context.Context,
			item *badgerdb_operation_models_v3.Item,
			opts *badgerdb_operation_models_v3.IndexOpts,
			retrialOpts *co.RetrialOpts,
		) error
		Load(
			ctx context.Context,
			item *badgerdb_operation_models_v3.Item,
			opts *badgerdb_operation_models_v3.IndexOpts,
		) ([]byte, error)

		List(
			ctx context.Context,
			//item *badgerdb_operation_models_v3.Item,
			opts *badgerdb_operation_models_v3.IndexOpts,
			pagination *badgerdb_management_models_v3.Pagination,
		) (badgerdb_operation_models_v3.Items, error)
		ListValuesFromIndexingKeys(
			ctx context.Context,
			opts *badgerdb_operation_models_v3.IndexOpts,
		) (badgerdb_operation_models_v3.Items, error)
	}

	BadgerOperatorV3Params struct {
		Manager         badgerdb_manager_adaptor_v3.Manager
		Serializer      go_serializer.Serializer
		ChainedOperator *co.ChainOperator
	}

	BadgerOperatorV3 struct {
		manager    badgerdb_manager_adaptor_v3.Manager
		dbInfo     *badgerdb_management_models_v3.DBMemoryInfo
		serializer go_serializer.Serializer

		chainedOperator *co.ChainOperator
		// TODO: add a context global retrial opts
	}
)

func NewBadgerOperatorV3(
	params *BadgerOperatorV3Params,
) (*BadgerOperatorV3, error) {
	o := &BadgerOperatorV3{
		manager:         params.Manager,
		serializer:      params.Serializer,
		chainedOperator: params.ChainedOperator,
	}

	return o, nil
}

// Operate operates in the given database.
func (o *BadgerOperatorV3) Operate(dbInfo *badgerdb_management_models_v3.DBMemoryInfo) Operator {
	no := &BadgerOperatorV3{
		manager:    o.manager,
		dbInfo:     dbInfo,
		serializer: o.serializer,
	}

	return no
}

// operateInternally operates in the given database.
func (o *BadgerOperatorV3) operate(dbInfo *badgerdb_management_models_v3.DBMemoryInfo) *BadgerOperatorV3 {
	no := &BadgerOperatorV3{
		manager:    o.manager,
		dbInfo:     dbInfo,
		serializer: o.serializer,
	}

	return no
}

// Create checks if the key exists, if not, it stores the item.
func (o *BadgerOperatorV3) Create(
	ctx context.Context,
	item *badgerdb_operation_models_v3.Item,
	opts *badgerdb_operation_models_v3.IndexOpts,
	retrialOpts *co.RetrialOpts,
) error {
	txn := o.dbInfo.DB.NewTransaction(true)
	var createFn = func() error {
		return o.createWithTxn(txn, item.Key, item.Value)
	}
	var createRollbackFn = func() error {
		txn.Discard()
		return nil
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
	var createIdxsRollbackFn = func() error {
		idxTxn.Discard()
		return nil
	}
	var deleteIdxsRollbackFn = func() error {
		idxTxn.Discard()
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
	var createIdxListRollbackFn = func() error {
		idxListTxn.Discard()
		return nil
	}
	var deleteIdxListRollbackFn = func() error {
		idxList := bytes.Join(opts.IndexingKeys, []byte(bsSeparator))
		idxListTxn.Discard()
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

	commitStageOps := &co.Ops{
		Action: &co.Action{
			Act:         commitStage,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: nil,
			RetrialOpts: retrialOpts,
			Next: &co.Ops{
				Action: nil,
				RollbackAction: &co.RollbackAction{
					RollbackAct: deleteIdxListRollbackFn,
					RetrialOpts: retrialOpts,
					Next: &co.Ops{
						Action: nil,
						RollbackAction: &co.RollbackAction{
							RollbackAct: deleteIdxsRollbackFn,
							RetrialOpts: retrialOpts,
							Next:        nil,
						},
					},
				},
			},
		},
	}

	createIdxListOps := &co.Ops{
		Action: &co.Action{
			Act:         createIdxListFn,
			RetrialOpts: retrialOpts,
			Next:        commitStageOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: createIdxListRollbackFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
	}

	createIdxsOps := &co.Ops{
		Action: &co.Action{
			Act:         createIdxsFn,
			RetrialOpts: retrialOpts,
			Next:        createIdxListOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: createIdxsRollbackFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
	}
	createIdxListOps.RollbackAction.Next = createIdxsOps

	createOps := &co.Ops{
		Action: &co.Action{
			Act:         createFn,
			RetrialOpts: retrialOpts,
			Next:        createIdxsOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: createRollbackFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
	}
	createIdxsOps.RollbackAction.Next = createOps

	if !opts.HasIdx {
		var commitCreateStage = func() error {
			if err = txn.Commit(); err != nil {
				return err
			}

			return nil
		}
		var commitCreateRollbackStage = func() error {
			txn.Discard()
			return nil
		}

		createOps.Action.Next = &co.Ops{
			Action: &co.Action{
				Act:         commitCreateStage,
				RetrialOpts: retrialOpts,
				Next:        nil,
			},
			RollbackAction: &co.RollbackAction{
				RollbackAct: commitCreateRollbackStage,
				RetrialOpts: retrialOpts,
				Next:        nil,
			},
		}

		return o.chainedOperator.Operate(createOps)
	}

	return o.chainedOperator.Operate(createOps)
}

// Upsert updates or creates the key value no matter if the key already exists or not.
func (o *BadgerOperatorV3) Upsert(
	ctx context.Context,
	item *badgerdb_operation_models_v3.Item,
	opts *badgerdb_operation_models_v3.IndexOpts,
	retrialOpts *co.RetrialOpts,
) error {
	txn := o.dbInfo.DB.NewTransaction(true)
	var upsertFn = func() error {
		return o.upsertWithTxn(txn, item.Key, item.Value)
	}
	var upsertRollbackFn = func() error {
		txn.Discard()
		return nil
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
	var upsertIdxsRollbackFn = func() error {
		idxTxn.Discard()
		return nil
	}
	var deleteIdxsRollbackFn = func() error {
		idxTxn.Discard()
		for _, idx := range opts.IndexingKeys {
			err = idxOp.delete(idx)
			if err != nil {
				continue
			}
		}

		return nil
	}

	// TODO: remove after test and validation
	//idxOp, err := o.indexedStoreOperator(ctx)
	//if err != nil {
	//	return err
	//}
	//idxTxn := idxOp.dbInfo.DB.NewTransaction(true)
	//var upsertIdxsFn = func() (err error) {
	//	for _, idx := range opts.IndexingKeys {
	//		err = idxOp.upsertWithTxn(idxTxn, idx, opts.ParentKey)
	//		if err != nil {
	//			return
	//		}
	//	}
	//
	//	return
	//}
	//var upsertIdxsRollbackFn = func() error {
	//	idxTxn.Discard()
	//	return nil
	//}
	//var deleteIdxsRollbackFn = func() error {
	//	idxTxn.Discard()
	//	for _, idx := range opts.IndexingKeys {
	//		err = idxOp.delete(idx)
	//		if err != nil {
	//			continue
	//		}
	//	}
	//
	//	return nil
	//}

	// TODO: remove after test and validation
	//idxListOp, err := o.indexedListStoreOperator(ctx)
	//if err != nil {
	//	return err
	//}
	//idxListTxn := idxListOp.dbInfo.DB.NewTransaction(true)
	var upsertIdxListFn = func() error {
		idxList := bytes.Join(newIdxList, []byte(bsSeparator)) // opts.IndexingKeys newIdxList
		return idxListOp.upsertWithTxn(idxListTxn, opts.ParentKey, idxList)
	}
	var upsertIdxListRollbackFn = func() error {
		idxListTxn.Discard()
		return nil
	}
	var deleteIdxListRollbackFn = func() error {
		idxList := bytes.Join(newIdxList, []byte(bsSeparator)) // opts.IndexingKeys newIdxList
		idxListTxn.Discard()
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

	commitStageOps := &co.Ops{
		Action: &co.Action{
			Act:         commitStage,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: nil,
			RetrialOpts: retrialOpts,
			Next: &co.Ops{
				Action: nil,
				RollbackAction: &co.RollbackAction{
					RollbackAct: deleteIdxListRollbackFn,
					RetrialOpts: retrialOpts,
					Next: &co.Ops{
						Action: nil,
						RollbackAction: &co.RollbackAction{
							RollbackAct: deleteIdxsRollbackFn,
							RetrialOpts: retrialOpts,
							Next:        nil,
						},
					},
				},
			},
		},
	}

	upsertIdxListOps := &co.Ops{
		Action: &co.Action{
			Act:         upsertIdxListFn,
			RetrialOpts: retrialOpts,
			Next:        commitStageOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: upsertIdxListRollbackFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
	}

	upsertIdxsOps := &co.Ops{
		Action: &co.Action{
			Act:         upsertIdxsFn,
			RetrialOpts: retrialOpts,
			Next:        upsertIdxListOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: upsertIdxsRollbackFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
	}
	upsertIdxListOps.RollbackAction.Next = upsertIdxsOps

	upsertOps := &co.Ops{
		Action: &co.Action{
			Act:         upsertFn,
			RetrialOpts: retrialOpts,
			Next:        upsertIdxsOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: upsertRollbackFn,
			RetrialOpts: retrialOpts,
			Next:        nil,
		},
	}
	upsertIdxsOps.RollbackAction.Next = upsertOps

	if !opts.HasIdx {
		var commitCreateStage = func() error {
			if err = txn.Commit(); err != nil {
				return err
			}

			return nil
		}
		var commitCreateRollbackStage = func() error {
			txn.Discard()
			return nil
		}

		upsertOps.Action.Next = &co.Ops{
			Action: &co.Action{
				Act:         commitCreateStage,
				RetrialOpts: retrialOpts,
				Next:        nil,
			},
			RollbackAction: &co.RollbackAction{
				RollbackAct: commitCreateRollbackStage,
				RetrialOpts: retrialOpts,
				Next:        nil,
			},
		}

		return o.chainedOperator.Operate(upsertOps)
	}

	return o.chainedOperator.Operate(upsertOps)
}

// Delete deletes the given key entry if present.
func (o *BadgerOperatorV3) Delete(
	ctx context.Context,
	item *badgerdb_operation_models_v3.Item,
	opts *badgerdb_operation_models_v3.IndexOpts,
	retrialOpts *co.RetrialOpts,
) error {
	if !opts.HasIdx {
		return o.delete(item.Key)
	}

	switch opts.IndexProperties.IndexDeletionBehaviour {
	case badgerdb_operation_models_v3.IndexOnly:
		return o.deleteIdxOnly(ctx, item, opts, retrialOpts)
	case badgerdb_operation_models_v3.Cascade:
		return o.deleteCascade(ctx, item, opts, retrialOpts)
	case badgerdb_operation_models_v3.CascadeByIdx:
		return o.deleteCascadeByIdx(ctx, item, opts, retrialOpts)
	case badgerdb_operation_models_v3.None:
		fallthrough
	default:
		return fmt.Errorf("delete was not called - invalid behaviour")
	}
}

// Load checks the given key and returns the serialized item's value whether it exists.
func (o *BadgerOperatorV3) Load(
	ctx context.Context,
	item *badgerdb_operation_models_v3.Item,
	opts *badgerdb_operation_models_v3.IndexOpts,
) (bs []byte, err error) {
	if !opts.HasIdx {
		return o.load(item.Key)
	}

	switch opts.IndexProperties.IndexSearchPattern {
	case badgerdb_operation_models_v3.IndexingList:
		bs, err = o.indexingList(ctx, opts)
	case badgerdb_operation_models_v3.AndComputational:
		bs, err = o.andComputationalSearch(ctx, opts)
	case badgerdb_operation_models_v3.OrComputational:
		bs, err = o.orComputationalSearch(ctx, opts)
	case badgerdb_operation_models_v3.One:
		fallthrough
	default:
		bs, err = o.straightSearch(ctx, opts)
	}

	return bs, err
}

func (o *BadgerOperatorV3) List(
	_ context.Context,
	//item *badgerdb_operation_models_v3.Item,
	opts *badgerdb_operation_models_v3.IndexOpts,
	pagination *badgerdb_management_models_v3.Pagination,
) (items badgerdb_operation_models_v3.Items, err error) {
	ok, err := o.manager.ValidatePagination(int(pagination.PageSize), int(pagination.PageID))
	if err != nil {
		return items, fmt.Errorf("invalid pagination: %v", err)
	}
	if ok {
		return o.listPaginated(pagination)
	}

	switch opts.IndexProperties.ListSearchPattern {
	case badgerdb_operation_models_v3.All:
		return o.listAll()
	case badgerdb_operation_models_v3.Default:
		fallthrough
	default:
		pagination = &badgerdb_management_models_v3.Pagination{
			PageID:   1,
			PageSize: 20,
		}
		return o.listPaginated(pagination)
	}
}

func (o *BadgerOperatorV3) ListValuesFromIndexingKeys(
	ctx context.Context,
	opts *badgerdb_operation_models_v3.IndexOpts,
) (badgerdb_operation_models_v3.Items, error) {
	idxOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return badgerdb_operation_models_v3.Items{}, nil
	}

	items := make(badgerdb_operation_models_v3.Items, len(opts.IndexingKeys))
	for idx, idxKey := range opts.IndexingKeys {
		objKey, err := idxOp.load(idxKey)
		if err != nil {
			item := &badgerdb_operation_models_v3.Item{
				Error: fmt.Errorf("error searching from idx key: %s", idxKey),
			}
			items[idx] = item

			continue
		}

		objValue, err := o.load(objKey)
		if err != nil {
			item := &badgerdb_operation_models_v3.Item{
				Error: fmt.Errorf("error searching from main key: %s", objKey),
			}
			items[idx] = item

			continue
		}

		item := &badgerdb_operation_models_v3.Item{
			Key:   idxKey,
			Value: objValue,
		}
		items[idx] = item
	}

	return items, nil
}
