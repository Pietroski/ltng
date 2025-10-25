package v2

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/concurrent"
	"gitlab.com/pietroski-software-company/golang/devex/errorsx"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/ctx/ctxrunner"
)

type createSaga struct {
	opSaga *opSaga
	cancel context.CancelFunc
}

func newCreateSaga(ctx context.Context, opSaga *opSaga) *createSaga {
	ctx, cancel := context.WithCancel(ctx)
	cs := &createSaga{
		opSaga: opSaga,
		cancel: cancel,
	}

	cs.opSaga.offThread.Op(func() {
		cs.createItemOnDiskOnThread(ctx)
	})
	cs.opSaga.offThread.Op(func() {
		cs.createIndexItemOnDiskOnThread(ctx)
	})
	cs.opSaga.offThread.Op(func() {
		cs.createIndexListItemOnDiskOnThread(ctx)
	})
	cs.opSaga.offThread.Op(func() {
		cs.createRelationalItemOnDiskOnThread(ctx)
	})
	cs.opSaga.offThread.Op(func() {
		cs.deleteItemOnDiskOnThread(ctx)
	})
	cs.opSaga.offThread.Op(func() {
		cs.deleteIndexItemFromDiskOnThread(ctx)
	})
	cs.opSaga.offThread.Op(func() {
		cs.deleteIndexListItemFromDiskOnThread(ctx)
	})

	cs.opSaga.offThread.Op(func() {
		cs.ListenAndTrigger(ctx)
	})

	return cs
}

func (s *createSaga) ListenAndTrigger(ctx context.Context) {
	ctx = context.WithValue(ctx, "thread", "operator_create_saga-ListenAndTrigger")
	ctxrunner.WithCancellation(ctx,
		s.opSaga.crudChannels.CreateChannels.InfoChannel,
		func(itemInfoData *ltngenginemodels.ItemInfoData) {
			if !itemInfoData.Opts.HasIdx {
				s.noIndexTrigger(ctx, itemInfoData)
			} else {
				s.indexTrigger(ctx, itemInfoData)
			}
		},
	)
	s.cancel()
}

func (s *createSaga) noIndexTrigger(
	ctx context.Context, itemInfoData *ltngenginemodels.ItemInfoData,
) {
	createItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateItemOnDisk := itemInfoData.WithRespChan(createItemOnDiskRespSignal)
	s.opSaga.crudChannels.CreateChannels.ActionItemChannel <- itemInfoDataForCreateItemOnDisk
	err := <-createItemOnDiskRespSignal
	if err != nil {
		s.opSaga.e.logger.Error(ctx, "error on triggering create action itemInfoData",
			"item_info_data", itemInfoData, "err", err)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	createRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateRelationalItemOnDisk := itemInfoData.WithRespChan(createRelationalItemOnDiskRespSignal)
	s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk
	err = <-createRelationalItemOnDiskRespSignal
	if err != nil {
		s.opSaga.e.logger.Error(ctx, "error on trigger create action itemInfoData relational",
			"item_info_data", itemInfoData, "err", err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	itemInfoData.RespSignal <- nil
	close(itemInfoData.RespSignal)
}

func (s *createSaga) indexTrigger(
	ctx context.Context, itemInfoData *ltngenginemodels.ItemInfoData,
) {
	createItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateItemOnDisk := itemInfoData.WithRespChan(createItemOnDiskRespSignal)
	createIndexItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateIndexItemOnDisk := itemInfoData.WithRespChan(createIndexItemOnDiskRespSignal)
	createIndexItemListOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateIndexItemListOnDisk := itemInfoData.WithRespChan(createIndexItemListOnDiskRespSignal)

	s.opSaga.crudChannels.CreateChannels.ActionItemChannel <- itemInfoDataForCreateItemOnDisk
	s.opSaga.crudChannels.CreateChannels.ActionIndexItemChannel <- itemInfoDataForCreateIndexItemOnDisk
	s.opSaga.crudChannels.CreateChannels.ActionIndexListItemChannel <- itemInfoDataForCreateIndexItemListOnDisk

	if err := ResponseAccumulator(
		createItemOnDiskRespSignal,
		createIndexItemOnDiskRespSignal,
		createIndexItemListOnDiskRespSignal,
	); err != nil {
		s.opSaga.e.logger.Error(ctx, "error on trigger create indexed action item info data",
			"item_info_data", itemInfoData, "err", err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	createRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateRelationalItemOnDisk := itemInfoData.WithRespChan(createRelationalItemOnDiskRespSignal)
	s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk
	err := <-createRelationalItemOnDiskRespSignal
	if err != nil {
		s.opSaga.e.logger.Error(ctx, "error on trigger create indexed action item info data relational",
			"item_info_data", itemInfoData, "err", err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	itemInfoData.RespSignal <- nil
	close(itemInfoData.RespSignal)
}

func (s *createSaga) RollbackTrigger(ctx context.Context, itemInfoData *ltngenginemodels.ItemInfoData) {
	if !itemInfoData.Opts.HasIdx {
		s.noIndexRollback(ctx, itemInfoData)
		return
	}

	s.indexRollback(ctx, itemInfoData)
}

func (s *createSaga) noIndexRollback(
	ctx context.Context, itemInfoData *ltngenginemodels.ItemInfoData,
) {
	deleteItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteItemOnDisk := itemInfoData.WithRespChan(deleteItemOnDiskRespSignal)
	s.opSaga.crudChannels.CreateChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	err := <-deleteItemOnDiskRespSignal
	if err != nil {
		s.opSaga.e.logger.Error(ctx, "error rolling back trigger for item info data",
			"item_info_data", itemInfoData, "err", err)
	}
}

func (s *createSaga) indexRollback(
	ctx context.Context, itemInfoData *ltngenginemodels.ItemInfoData,
) {
	deleteItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteItemOnDisk := itemInfoData.WithRespChan(deleteItemOnDiskRespSignal)
	deleteIndexItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteIndexItemOnDisk := itemInfoData.WithRespChan(deleteIndexItemOnDiskRespSignal)
	deleteIndexItemListOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteIndexItemListOnDisk := itemInfoData.WithRespChan(deleteIndexItemListOnDiskRespSignal)

	s.opSaga.crudChannels.CreateChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	s.opSaga.crudChannels.CreateChannels.RollbackIndexItemChannel <- itemInfoDataForDeleteIndexItemOnDisk
	s.opSaga.crudChannels.CreateChannels.RollbackIndexListItemChannel <- itemInfoDataForDeleteIndexItemListOnDisk

	if err := ResponseAccumulator(
		deleteItemOnDiskRespSignal,
		deleteIndexItemOnDiskRespSignal,
		deleteIndexItemListOnDiskRespSignal,
	); err != nil {
		s.opSaga.e.logger.Error(ctx, "error rolling back trigger for item info data",
			"item_info_data", itemInfoData, "err", err)
	}
}

// createItemOnDiskOnThread stands for createItemOnDisk on thread.
func (s *createSaga) createItemOnDiskOnThread(
	ctx context.Context,
) {
	ctx = context.WithValue(ctx, "thread", "createItemOnDiskOnThread")
	ctxrunner.WithCancellation(ctx,
		s.opSaga.crudChannels.CreateChannels.ActionItemChannel,
		func(v *ltngenginemodels.ItemInfoData) {
			err := s.opSaga.e.createItemOnDisk(v.Ctx, v.DBMetaInfo, v.Item)
			v.RespSignal <- err
			close(v.RespSignal)
		},
	)
}

func (s *createSaga) deleteItemOnDiskOnThread(
	ctx context.Context,
) {
	ctx = context.WithValue(ctx, "thread", "deleteItemOnDiskOnThread")
	ctxrunner.WithCancellation(ctx,
		s.opSaga.crudChannels.CreateChannels.RollbackItemChannel,
		func(v *ltngenginemodels.ItemInfoData) {
			strItemKey := hex.EncodeToString(v.Item.Key)
			filePath := ltngenginemodels.GetDataFilepath(v.DBMetaInfo.Path, strItemKey)
			err := os.Remove(filePath)
			v.RespSignal <- err
			close(v.RespSignal)
		},
	)
}

func (s *createSaga) createIndexItemOnDiskOnThread(
	ctx context.Context,
) {
	ctx = context.WithValue(ctx, "thread", "createIndexItemOnDiskOnThread")
	ctxrunner.WithCancellation(ctx,
		s.opSaga.crudChannels.CreateChannels.ActionIndexItemChannel,
		func(v *ltngenginemodels.ItemInfoData) {
			op := concurrent.New("createIndexItemOnDisk")
			for _, indexKey := range v.Opts.IndexingKeys {
				op.OpX(func() (any, error) {
					if err := s.opSaga.e.createItemOnDisk(v.Ctx,
						v.DBMetaInfo.IndexInfo(),
						&ltngenginemodels.Item{
							Key:   indexKey,
							Value: v.Opts.ParentKey,
						},
					); err != nil {
						return nil, err
					}

					return nil, nil
				})
			}
			err := op.WaitAndWrapErr()
			v.RespSignal <- err
			close(v.RespSignal)
		},
	)
}

func (s *createSaga) deleteIndexItemFromDiskOnThread(
	ctx context.Context,
) {
	ctx = context.WithValue(ctx, "thread", "deleteIndexItemFromDiskOnThread")
	ctxrunner.WithCancellation(ctx,
		s.opSaga.crudChannels.CreateChannels.RollbackIndexItemChannel,
		func(v *ltngenginemodels.ItemInfoData) {
			var errAcc error
			for _, indexKey := range v.Opts.IndexingKeys {
				strItemKey := hex.EncodeToString(indexKey)
				filePath := ltngenginemodels.GetDataFilepath(v.DBMetaInfo.IndexInfo().Path, strItemKey)
				if err := os.Remove(filePath); err != nil {
					if errAcc == nil {
						errAcc = err
					} else {
						err = errorsx.Wrapf(err, "%v", errAcc)
						errAcc = errorsx.Wrap(err, "error deleting item on database")
					}
				}
			}
			v.RespSignal <- errAcc
			close(v.RespSignal)
		},
	)
}

func (s *createSaga) createIndexListItemOnDiskOnThread(
	ctx context.Context,
) {
	ctx = context.WithValue(ctx, "thread", "createIndexListItemOnDiskOnThread")
	ctxrunner.WithCancellation(ctx,
		s.opSaga.crudChannels.CreateChannels.ActionIndexListItemChannel,
		func(v *ltngenginemodels.ItemInfoData) {
			err := s.opSaga.e.createItemOnDisk(ctx,
				v.DBMetaInfo.IndexListInfo(),
				&ltngenginemodels.Item{
					Key:   v.Opts.ParentKey,
					Value: bytes.Join(v.Opts.IndexingKeys, []byte(ltngenginemodels.BytesSep)),
				},
			)
			v.RespSignal <- err
			close(v.RespSignal)
		},
	)
}

func (s *createSaga) deleteIndexListItemFromDiskOnThread(
	ctx context.Context,
) {
	ctx = context.WithValue(ctx, "thread", "deleteIndexListItemFromDiskOnThread")
	ctxrunner.WithCancellation(ctx,
		s.opSaga.crudChannels.CreateChannels.RollbackIndexListItemChannel,
		func(v *ltngenginemodels.ItemInfoData) {
			strItemKey := hex.EncodeToString(v.Item.Key)
			filePath := ltngenginemodels.GetDataFilepath(v.DBMetaInfo.IndexListInfo().Path, strItemKey)
			err := os.Remove(filePath)
			v.RespSignal <- err
			close(v.RespSignal)
		},
	)
}

func (s *createSaga) createRelationalItemOnDiskOnThread(
	ctx context.Context,
) {
	ctx = context.WithValue(ctx, "thread", "createRelationalItemOnDiskOnThread")
	ctxrunner.WithCancellation(ctx,
		s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel,
		func(v *ltngenginemodels.ItemInfoData) {
			err := s.opSaga.e.createRelationalItemOnDisk(v.Ctx, v.DBMetaInfo, v.Item)
			v.RespSignal <- err
			close(v.RespSignal)
		},
	)
}
