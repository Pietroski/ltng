package operations

import (
	"bytes"
	"context"
	"fmt"

	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	operation_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/operation"
	co "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
)

type (
	Operator interface {
		Operate(dbInfo *management_models.DBMemoryInfo) Operator

		Create(
			ctx context.Context,
			item *operation_models.Item,
			opts *operation_models.IndexOpts,
			retrialOpts *co.RetrialOpts,
		) error
		Upsert(
			ctx context.Context,
			item *operation_models.Item,
			opts *operation_models.IndexOpts,
			retrialOpts *co.RetrialOpts,
		) error
		Delete(
			ctx context.Context,
			item *operation_models.Item,
			opts *operation_models.IndexOpts,
			retrialOpts *co.RetrialOpts,
		) error
		Load(
			ctx context.Context,
			item *operation_models.Item,
			opts *operation_models.IndexOpts,
		) ([]byte, error)
		//SoftDelete(
		//	ctx context.Context,
		//	item *operation_models.Item,
		//	opts *operation_models.IndexOpts,
		//	retrialOpts *co.RetrialOpts,
		//) error

		List(
			//ctx context.Context,
			//item *operation_models.Item,
			opts *operation_models.IndexOpts,
			pagination *management_models.Pagination,
		) (operation_models.Items, error)
		ListValuesFromIndexingKeys(
			ctx context.Context,
			opts *operation_models.IndexOpts,
		) (operation_models.Items, error)
	}

	BadgerOperator struct {
		manager    manager.Manager
		dbInfo     *management_models.DBMemoryInfo
		serializer go_serializer.Serializer

		chainedOperator *co.ChainOperator
		// TODO: add a context global retrial opts
	}
)

func NewBadgerOperator(
	manager manager.Manager,
	serializer go_serializer.Serializer,
	chainedOperator *co.ChainOperator,
) Operator {
	o := &BadgerOperator{
		manager:         manager,
		serializer:      serializer,
		chainedOperator: chainedOperator,
	}

	return o
}

// Operate operates in the given database.
func (o *BadgerOperator) Operate(dbInfo *management_models.DBMemoryInfo) Operator {
	no := &BadgerOperator{
		manager:    o.manager,
		dbInfo:     dbInfo,
		serializer: o.serializer,
	}

	return no
}

// operateInternally operates in the given database.
func (o *BadgerOperator) operate(dbInfo *management_models.DBMemoryInfo) *BadgerOperator {
	no := &BadgerOperator{
		manager:    o.manager,
		dbInfo:     dbInfo,
		serializer: o.serializer,
	}

	return no
}

// Create checks if the key exists, if not, it stores the item.
func (o *BadgerOperator) Create(
	ctx context.Context,
	item *operation_models.Item,
	opts *operation_models.IndexOpts,
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
func (o *BadgerOperator) Upsert(
	ctx context.Context,
	item *operation_models.Item,
	opts *operation_models.IndexOpts,
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

	idxOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return err
	}
	idxTxn := idxOp.dbInfo.DB.NewTransaction(true)
	var upsertIdxsFn = func() (err error) {
		for _, idx := range opts.IndexingKeys {
			err = idxOp.upsertWithTxn(idxTxn, idx, opts.ParentKey)
			if err != nil {
				return
			}
		}

		return
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

	idxListOp, err := o.indexedListStoreOperator(ctx)
	if err != nil {
		return err
	}
	idxListTxn := idxListOp.dbInfo.DB.NewTransaction(true)
	var upsertIdxListFn = func() error {
		idxList := bytes.Join(opts.IndexingKeys, []byte(bsSeparator))
		return idxListOp.upsertWithTxn(idxListTxn, opts.ParentKey, idxList)
	}
	var upsertIdxListRollbackFn = func() error {
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
func (o *BadgerOperator) Delete(
	ctx context.Context,
	item *operation_models.Item,
	opts *operation_models.IndexOpts,
	retrialOpts *co.RetrialOpts,
) error {
	if !opts.HasIdx {
		return o.delete(item.Key)
	}

	switch opts.IndexProperties.IndexDeletionBehaviour {
	case operation_models.IndexOnly:
		return o.deleteIdxOnly(ctx, item, opts, retrialOpts)
	case operation_models.Cascade:
		return o.deleteCascade(ctx, item, opts, retrialOpts)
	// case operation_models.IndexOnCascade:
	// return o.deleteByIndexOnCascade(ctx, item, opts, retrialOpts)
	case operation_models.None:
		fallthrough
	default:
		return fmt.Errorf("delete was not call - invalid behaviour")
	}
}

// Load checks the given key and returns the serialized item's value whether it exists.
func (o *BadgerOperator) Load(
	ctx context.Context,
	item *operation_models.Item,
	opts *operation_models.IndexOpts,
) (bs []byte, err error) {
	if !opts.HasIdx {
		return o.load(item.Key)
	}

	switch opts.IndexProperties.IndexSearchPattern {
	case operation_models.IndexingList:
		bs, err = o.indexingList(ctx, opts)
	case operation_models.AndComputational:
		bs, err = o.andComputationalSearch(ctx, opts)
	case operation_models.OrComputational:
		bs, err = o.orComputationalSearch(ctx, opts)
	case operation_models.One:
		fallthrough
	default:
		bs, err = o.straightSearch(ctx, opts)
	}

	return bs, err
}

func (o *BadgerOperator) List(
	//ctx context.Context,
	//item *operation_models.Item,
	opts *operation_models.IndexOpts,
	pagination *management_models.Pagination,
) (items operation_models.Items, err error) {
	ok, err := o.manager.ValidatePagination(int(pagination.PageSize), int(pagination.PageID))
	if err != nil {
		return items, fmt.Errorf("invalid pagination: %v", err)
	}
	if ok {
		return o.listPaginated(pagination)
	}

	switch opts.IndexProperties.ListSearchPattern {
	case operation_models.All:
		return o.listAll()
	case operation_models.Default:
		fallthrough
	default:
		pagination = &management_models.Pagination{
			PageID:   1,
			PageSize: 20,
		}
		return o.listPaginated(pagination)
	}
}

func (o *BadgerOperator) ListValuesFromIndexingKeys(
	ctx context.Context,
	opts *operation_models.IndexOpts,
) (operation_models.Items, error) {
	idxOp, err := o.indexedStoreOperator(ctx)
	if err != nil {
		return operation_models.Items{}, nil
	}

	items := make(operation_models.Items, len(opts.IndexingKeys))
	for idx, idxKey := range opts.IndexingKeys {
		objKey, err := idxOp.load(idxKey)
		if err != nil {
			item := &operation_models.Item{
				Error: fmt.Errorf("error searching from idx key: %s", idxKey),
			}
			items[idx] = item

			continue
		}

		objValue, err := o.load(objKey)
		if err != nil {
			item := &operation_models.Item{
				Error: fmt.Errorf("error searching from main key: %s", objKey),
			}
			items[idx] = item

			continue
		}

		item := &operation_models.Item{
			Key:   idxKey,
			Value: objValue,
		}
		items[idx] = item
	}

	return items, nil
}
