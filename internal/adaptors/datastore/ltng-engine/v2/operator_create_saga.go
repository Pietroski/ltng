package v2

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/loop"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
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
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.CreateChannels.InfoChannel.Ch,
		func(itemInfoData *ltngdata.ItemInfoData) {
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
	ctx context.Context, itemInfoData *ltngdata.ItemInfoData,
) {
	itemRespSignal := make(chan error, 1)
	itemInfoDataWithChan := itemInfoData.WithRespChan(itemRespSignal)
	s.opSaga.crudChannels.CreateChannels.ActionItemChannel.Send(itemInfoDataWithChan)
	if err := <-itemRespSignal; err != nil {
		s.opSaga.e.logger.Error(ctx, "error on triggering create action itemInfoData",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)

		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)

		return
	}

	relationalItemRespSignal := make(chan error, 1)
	relationalItemInfoDataWithChan := itemInfoData.WithRespChan(relationalItemRespSignal)
	s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel.Send(relationalItemInfoDataWithChan)
	if err := <-relationalItemRespSignal; err != nil {
		s.opSaga.e.logger.Error(ctx, "error on trigger create action itemInfoData relational",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)

		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		return
	}

	itemInfoData.RespSignal <- nil
	close(itemInfoData.RespSignal)
}

func (s *createSaga) indexTrigger(
	ctx context.Context, itemInfoData *ltngdata.ItemInfoData,
) {
	itemRespSignal := make(chan error, 1)
	itemInfoDataWithChan := itemInfoData.WithRespChan(itemRespSignal)
	indexedItemRespSignal := make(chan error, 1)
	indexedItemInfoDataWithChan := itemInfoData.WithRespChan(indexedItemRespSignal)
	indexedListItemRespSignal := make(chan error, 1)
	indexedListItemInfoDataWithChan := itemInfoData.WithRespChan(indexedListItemRespSignal)

	s.opSaga.crudChannels.CreateChannels.ActionItemChannel.Send(itemInfoDataWithChan)
	s.opSaga.crudChannels.CreateChannels.ActionIndexItemChannel.Send(indexedItemInfoDataWithChan)
	s.opSaga.crudChannels.CreateChannels.ActionIndexListItemChannel.Send(indexedListItemInfoDataWithChan)

	if err := ResponseAccumulator(
		itemRespSignal,
		indexedItemRespSignal,
		indexedListItemRespSignal,
	); err != nil {
		s.opSaga.e.logger.Error(ctx, "error on trigger create indexed action item info data",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)

		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		return
	}

	relationalItemRespSignal := make(chan error, 1)
	relationalItemInfoDataWithChan := itemInfoData.WithRespChan(relationalItemRespSignal)
	s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel.Send(relationalItemInfoDataWithChan)
	err := <-relationalItemRespSignal
	if err != nil {
		s.opSaga.e.logger.Error(ctx, "error on trigger create indexed action item info data relational",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)

		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		return
	}

	itemInfoData.RespSignal <- nil
	close(itemInfoData.RespSignal)
}

func (s *createSaga) RollbackTrigger(ctx context.Context, itemInfoData *ltngdata.ItemInfoData) {
	if !itemInfoData.Opts.HasIdx {
		s.noIndexRollback(ctx, itemInfoData)
		return
	}

	s.indexRollback(ctx, itemInfoData)
}

func (s *createSaga) noIndexRollback(
	ctx context.Context, itemInfoData *ltngdata.ItemInfoData,
) {
	itemRespSignal := make(chan error, 1)
	itemInfoDataWithChan := itemInfoData.WithRespChan(itemRespSignal)
	s.opSaga.crudChannels.CreateChannels.RollbackItemChannel.Send(itemInfoDataWithChan)
	err := <-itemRespSignal
	if err != nil {
		s.opSaga.e.logger.Error(ctx, "error rolling back trigger for item info data",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)
	}
}

func (s *createSaga) indexRollback(
	ctx context.Context, itemInfoData *ltngdata.ItemInfoData,
) {
	itemRespSignal := make(chan error, 1)
	itemInfoDataWithChan := itemInfoData.WithRespChan(itemRespSignal)
	indexedItemRespSignal := make(chan error, 1)
	indexedItemInfoDataWithChan := itemInfoData.WithRespChan(indexedItemRespSignal)
	indexedListItemRespSignal := make(chan error, 1)
	indexedListItemInfoDataWithChan := itemInfoData.WithRespChan(indexedListItemRespSignal)

	s.opSaga.crudChannels.CreateChannels.RollbackItemChannel.Send(itemInfoDataWithChan)
	s.opSaga.crudChannels.CreateChannels.RollbackIndexItemChannel.Send(indexedItemInfoDataWithChan)
	s.opSaga.crudChannels.CreateChannels.RollbackIndexListItemChannel.Send(indexedListItemInfoDataWithChan)

	if err := ResponseAccumulator(
		itemRespSignal,
		indexedItemRespSignal,
		indexedListItemRespSignal,
	); err != nil {
		s.opSaga.e.logger.Error(ctx, "error rolling back trigger for item info data",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)
	}
}

// createItemOnDiskOnThread stands for createItemOnDisk on thread.
func (s *createSaga) createItemOnDiskOnThread(
	ctx context.Context,
) {
	ctx = context.WithValue(ctx, "thread", "createItemOnDiskOnThread")
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.CreateChannels.ActionItemChannel.Ch,
		func(itemInfoData *ltngdata.ItemInfoData) {
			strItemKey := hex.EncodeToString(itemInfoData.Item.Key)
			filePath := ltngdata.GetDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey)
			fileData := ltngdata.NewFileData(itemInfoData.DBMetaInfo.IndexInfo(), &ltngdata.Item{
				Key:   itemInfoData.Item.Key,
				Value: itemInfoData.Opts.ParentKey,
			})

			file, err := s.opSaga.e.createItemOnDisk(itemInfoData.Ctx, filePath, fileData)
			if err != nil {
				s.opSaga.e.logger.Error(ctx, "error creating item on disk",
					"item_info_data", *itemInfoData.DBMetaInfo, "err", err)
				itemInfoData.RespSignal <- err
				close(itemInfoData.RespSignal)
				return
			}

			s.opSaga.e.itemFileMapping.Set(itemInfoData.DBMetaInfo.LockName(strItemKey),
				&ltngdata.FileInfo{
					File:     file,
					FileData: fileData,
				})

			itemInfoData.RespSignal <- nil
			close(itemInfoData.RespSignal)
		},
	)
}

func (s *createSaga) deleteItemOnDiskOnThread(
	ctx context.Context,
) {
	ctx = context.WithValue(ctx, "thread", "deleteItemOnDiskOnThread")
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.CreateChannels.RollbackItemChannel.Ch,
		func(v *ltngdata.ItemInfoData) {
			strItemKey := hex.EncodeToString(v.Item.Key)
			filePath := ltngdata.GetDataFilepath(v.DBMetaInfo.Path, strItemKey)
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
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.CreateChannels.ActionIndexItemChannel.Ch,
		func(itemInfoData *ltngdata.ItemInfoData) {
			op := syncx.NewThreadOperator("createIndexItemOnDisk")
			for _, indexKey := range itemInfoData.Opts.IndexingKeys {
				op.OpX(func() (any, error) {
					strItemKey := hex.EncodeToString(indexKey)
					filePath := ltngdata.GetIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey)
					fileData := ltngdata.NewFileData(itemInfoData.DBMetaInfo.IndexInfo(),
						&ltngdata.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey,
						})

					file, err := s.opSaga.e.createItemOnDisk(itemInfoData.Ctx, filePath, fileData)
					if err != nil {
						return nil, err
					}

					s.opSaga.e.itemFileMapping.Set(itemInfoData.DBMetaInfo.LockName(strItemKey),
						&ltngdata.FileInfo{
							File:     file,
							FileData: fileData,
						})

					return nil, nil
				})
			}
			err := op.WaitAndWrapErr()

			itemInfoData.RespSignal <- err
			close(itemInfoData.RespSignal)
		},
	)
}

func (s *createSaga) deleteIndexItemFromDiskOnThread(
	ctx context.Context,
) {
	ctx = context.WithValue(ctx, "thread", "deleteIndexItemFromDiskOnThread")
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.CreateChannels.RollbackIndexItemChannel.Ch,
		func(v *ltngdata.ItemInfoData) {
			var errAcc error
			for _, indexKey := range v.Opts.IndexingKeys {
				strItemKey := hex.EncodeToString(indexKey)
				filePath := ltngdata.GetIndexedDataFilepath(v.DBMetaInfo.Path, strItemKey)
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
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.CreateChannels.ActionIndexListItemChannel.Ch,
		func(v *ltngdata.ItemInfoData) {
			strItemKey := hex.EncodeToString(v.Item.Key)
			filePath := ltngdata.GetIndexedListDataFilepath(v.DBMetaInfo.Path, strItemKey)
			fileData := ltngdata.NewFileData(v.DBMetaInfo.IndexInfo(), &ltngdata.Item{
				Key:   v.Opts.ParentKey,
				Value: bytes.Join(v.Opts.IndexingKeys, []byte(ltngdata.BsSep)),
			})

			file, err := s.opSaga.e.createItemOnDisk(ctx, filePath, fileData)
			if err != nil {
				v.RespSignal <- err
				close(v.RespSignal)
				return
			}

			s.opSaga.e.itemFileMapping.Set(v.DBMetaInfo.LockName(strItemKey),
				&ltngdata.FileInfo{
					File:     file,
					FileData: fileData,
				})

			v.RespSignal <- nil
			close(v.RespSignal)
		},
	)
}

func (s *createSaga) deleteIndexListItemFromDiskOnThread(
	ctx context.Context,
) {
	ctx = context.WithValue(ctx, "thread", "deleteIndexListItemFromDiskOnThread")
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.CreateChannels.RollbackIndexListItemChannel.Ch,
		func(v *ltngdata.ItemInfoData) {
			strItemKey := hex.EncodeToString(v.Item.Key)
			filePath := ltngdata.GetDataFilepath(v.DBMetaInfo.IndexListInfo().Path, strItemKey)
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
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel.Ch,
		func(v *ltngdata.ItemInfoData) {
			err := s.opSaga.e.createRelationalItemOnDisk(v.Ctx, v.DBMetaInfo, v.Item)
			v.RespSignal <- err
			close(v.RespSignal)
		},
	)
}
