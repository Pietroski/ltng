package ltngdbenginev3

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/loop"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesop"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

type upsertSaga struct {
	opSaga *opSaga
	cancel context.CancelFunc
}

func newUpsertSaga(ctx context.Context, opSaga *opSaga) *upsertSaga {
	ctx, cancel := context.WithCancel(ctx)
	us := &upsertSaga{
		opSaga: opSaga,
		cancel: cancel,
	}

	us.opSaga.offThread.Op(func() {
		us.upsertItemOnDiskOnThread(ctx)
	})
	us.opSaga.offThread.Op(func() {
		us.upsertIndexItemOnDiskOnThread(ctx)
	})
	us.opSaga.offThread.Op(func() {
		us.upsertIndexListItemOnDiskOnThread(ctx)
	})
	us.opSaga.offThread.Op(func() {
		us.upsertRelationalItemOnDiskOnThread(ctx)
	})
	us.opSaga.offThread.Op(func() {
		us.cleanUpUpsert(ctx)
	})
	us.opSaga.offThread.Op(func() {
		us.deleteItemOnDiskOnThread(ctx)
	})
	us.opSaga.offThread.Op(func() {
		us.deleteIndexItemFromDiskOnThread(ctx)
	})
	us.opSaga.offThread.Op(func() {
		us.deleteIndexListItemFromDiskOnThread(ctx)
	})

	us.opSaga.offThread.Op(func() {
		us.ListenAndTrigger(ctx)
	})

	return us
}

func (s *upsertSaga) ListenAndTrigger(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.UpsertChannels.InfoChannel.Ch,
		func(itemInfoData *ltngdbenginemodelsv3.ItemInfoData) {
			if !itemInfoData.Opts.HasIdx {
				s.noIndexTrigger(ctx, itemInfoData)
			} else {
				s.indexTrigger(ctx, itemInfoData)
			}
		},
	)
	s.cancel()
}

func (s *upsertSaga) noIndexTrigger(
	_ context.Context, itemInfoData *ltngdbenginemodelsv3.ItemInfoData,
) {
	upsertItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForUpsertItemOnDisk := itemInfoData.WithRespChan(upsertItemOnDiskRespSignal)
	s.opSaga.crudChannels.UpsertChannels.ActionItemChannel.Send(itemInfoDataForUpsertItemOnDisk)
	err := <-upsertItemOnDiskRespSignal
	if err != nil {
		s.opSaga.e.logger.Error(itemInfoData.Ctx, "error on trigger upsert action item info data",
			"item_info_data", itemInfoData, "error", err)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	upsertRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForUpsertRelationalItemOnDisk := itemInfoData.WithRespChan(upsertRelationalItemOnDiskRespSignal)
	s.opSaga.crudChannels.UpsertChannels.ActionRelationalItemChannel.Send(itemInfoDataForUpsertRelationalItemOnDisk)
	err = <-upsertRelationalItemOnDiskRespSignal
	if err != nil {
		s.opSaga.e.logger.Error(itemInfoData.Ctx, "error on trigger upsert action item info data relational",
			"item_info_data", itemInfoData, "error", err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	cleanUpUpsertItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCleanUpUpsertItemOnDisk := itemInfoData.WithRespChan(cleanUpUpsertItemOnDiskRespSignal)
	s.opSaga.crudChannels.UpsertChannels.CleanUpUpsert.Send(itemInfoDataForCleanUpUpsertItemOnDisk)
	err = <-cleanUpUpsertItemOnDiskRespSignal
	if err != nil {
		s.opSaga.e.logger.Error(itemInfoData.Ctx, "error on trigger upsert action item info data cleanup",
			"item_info_data", itemInfoData, "error", err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	itemInfoData.RespSignal <- nil
	close(itemInfoData.RespSignal)
}

func (s *upsertSaga) indexTrigger(
	_ context.Context, itemInfoData *ltngdbenginemodelsv3.ItemInfoData,
) {
	upsertItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForUpsertItemOnDisk := itemInfoData.WithRespChan(upsertItemOnDiskRespSignal)
	upsertIndexItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForUpsertIndexItemOnDisk := itemInfoData.WithRespChan(upsertIndexItemOnDiskRespSignal)
	upsertIndexItemListOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForUpsertIndexItemListOnDisk := itemInfoData.WithRespChan(upsertIndexItemListOnDiskRespSignal)

	s.opSaga.crudChannels.UpsertChannels.ActionItemChannel.Send(itemInfoDataForUpsertItemOnDisk)
	s.opSaga.crudChannels.UpsertChannels.ActionIndexItemChannel.Send(itemInfoDataForUpsertIndexItemOnDisk)
	s.opSaga.crudChannels.UpsertChannels.ActionIndexListItemChannel.Send(itemInfoDataForUpsertIndexItemListOnDisk)

	if err := ResponseAccumulator(
		upsertItemOnDiskRespSignal,
		upsertIndexItemOnDiskRespSignal,
		upsertIndexItemListOnDiskRespSignal,
	); err != nil {
		s.opSaga.e.logger.Error(itemInfoData.Ctx, "error on trigger upsert indexed action item info data",
			"item_info_data", itemInfoData.DBMetaInfo, "error", err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	upsertRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForUpsertRelationalItemOnDisk := itemInfoData.WithRespChan(upsertRelationalItemOnDiskRespSignal)
	s.opSaga.crudChannels.UpsertChannels.ActionRelationalItemChannel.Send(itemInfoDataForUpsertRelationalItemOnDisk)
	err := <-upsertRelationalItemOnDiskRespSignal
	if err != nil {
		s.opSaga.e.logger.Error(itemInfoData.Ctx,
			"error on trigger upsert indexed action item info data relational",
			"item_info_data", itemInfoData.DBMetaInfo, "error", err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	cleanUpUpsertItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCleanUpUpsertItemOnDisk := itemInfoData.WithRespChan(cleanUpUpsertItemOnDiskRespSignal)
	s.opSaga.crudChannels.UpsertChannels.CleanUpUpsert.Send(itemInfoDataForCleanUpUpsertItemOnDisk)
	err = <-cleanUpUpsertItemOnDiskRespSignal
	if err != nil {
		s.opSaga.e.logger.Error(itemInfoData.Ctx, "error on trigger upsert action item info data cleanup",
			"item_info_data", itemInfoData.DBMetaInfo, "error", err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	itemInfoData.RespSignal <- nil
	close(itemInfoData.RespSignal)
}

func (s *upsertSaga) RollbackTrigger(ctx context.Context, itemInfoData *ltngdbenginemodelsv3.ItemInfoData) {
	if !itemInfoData.Opts.HasIdx {
		s.noIndexRollback(ctx, itemInfoData)
		return
	}

	s.indexRollback(ctx, itemInfoData)
}

func (s *upsertSaga) noIndexRollback(
	_ context.Context, itemInfoData *ltngdbenginemodelsv3.ItemInfoData,
) {
	deleteItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteItemOnDisk := itemInfoData.WithRespChan(deleteItemOnDiskRespSignal)
	s.opSaga.crudChannels.UpsertChannels.RollbackItemChannel.Send(itemInfoDataForDeleteItemOnDisk)
	err := <-deleteItemOnDiskRespSignal
	if err != nil {
		s.opSaga.e.logger.Error(itemInfoData.Ctx, "error rolling back trigger for upsert item info data",
			"item_info_data", itemInfoData.DBMetaInfo, "error", err)
	}
}

func (s *upsertSaga) indexRollback(
	_ context.Context, itemInfoData *ltngdbenginemodelsv3.ItemInfoData,
) {
	deleteItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteItemOnDisk := itemInfoData.WithRespChan(deleteItemOnDiskRespSignal)
	deleteIndexItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteIndexItemOnDisk := itemInfoData.WithRespChan(deleteIndexItemOnDiskRespSignal)
	deleteIndexItemListOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteIndexItemListOnDisk := itemInfoData.WithRespChan(deleteIndexItemListOnDiskRespSignal)

	s.opSaga.crudChannels.UpsertChannels.RollbackItemChannel.Send(itemInfoDataForDeleteItemOnDisk)
	s.opSaga.crudChannels.UpsertChannels.RollbackIndexItemChannel.Send(itemInfoDataForDeleteIndexItemOnDisk)
	s.opSaga.crudChannels.UpsertChannels.RollbackIndexListItemChannel.Send(itemInfoDataForDeleteIndexItemListOnDisk)

	if err := ResponseAccumulator(
		deleteItemOnDiskRespSignal,
		deleteIndexItemOnDiskRespSignal,
		deleteIndexItemListOnDiskRespSignal,
	); err != nil {
		s.opSaga.e.logger.Error(itemInfoData.Ctx, "error rolling back trigger for indexed upsert item info data",
			"item_info_data", itemInfoData.DBMetaInfo, "error", err)
	}
}

// upsertItemOnDiskOnThread stands for upsertItemOnDisk on thread.
func (s *upsertSaga) upsertItemOnDiskOnThread(
	ctx context.Context,
) {
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.UpsertChannels.ActionItemChannel.Ch,
		func(v *ltngdbenginemodelsv3.ItemInfoData) {
			//strItemKey := hex.EncodeToString(v.Item.Key)
			//filePath := GetDataFilepath(v.DBMetaInfo.Path, strItemKey)
			//tmpFilePath := GetTemporaryDataFilepath(v.DBMetaInfo.Path, strItemKey)
			//fileData := NewFileData(v.DBMetaInfo, v.Item)
			//
			//err := s.opSaga.e.upsertItemOnDisk(v.Ctx, filePath, tmpFilePath, fileData)

			err := s.opSaga.e.upsertItemOnDisk(v.Ctx, v.DBMetaInfo, v.Item)
			v.RespSignal <- err
			close(v.RespSignal)
		},
	)
}

func (s *upsertSaga) deleteItemOnDiskOnThread(
	ctx context.Context,
) {
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.UpsertChannels.RollbackItemChannel.Ch,
		func(v *ltngdbenginemodelsv3.ItemInfoData) {
			strItemKey := hex.EncodeToString(v.Item.Key)
			filePath := ltngdbenginemodelsv3.GetDataFilepath(v.DBMetaInfo.Path, strItemKey)
			tmpFilePath := ltngdbenginemodelsv3.GetTemporaryDataFilepath(v.DBMetaInfo.Path, strItemKey)

			if err := osx.MvFile(ctx, tmpFilePath, filePath); err != nil {
				v.RespSignal <- err
				close(v.RespSignal)
				return
			}

			v.RespSignal <- nil
			close(v.RespSignal)
		},
	)
}

func (s *upsertSaga) upsertIndexItemOnDiskOnThread(
	ctx context.Context,
) {
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.UpsertChannels.ActionIndexItemChannel.Ch,
		func(v *ltngdbenginemodelsv3.ItemInfoData) {
			indexingList, err := s.opSaga.e.loadIndexingList(v.Ctx, v.DBMetaInfo, v.Opts)
			if err != nil {
				v.RespSignal <- err
				close(v.RespSignal)
				return
			}

			op := syncx.NewThreadOperator("upsertIndexItemOnDisk")
			op.OpX(func() (any, error) {
				keysToSave := bytesop.CalRightDiff(
					ltngdbenginemodelsv3.IndexListToBytesList(indexingList),
					v.Opts.IndexingKeys)

				for _, indexKey := range keysToSave {
					//strKey := hex.EncodeToString(v.Item.Key)
					//filePath := GetIndexedDataFilepath(v.DBMetaInfo.Path, strKey)
					//tmpFilePath := GetTemporaryIndexedDataFilepath(v.DBMetaInfo.Path, strKey)
					//fileData := NewFileData(v.DBMetaInfo.IndexInfo(), &Item{
					//	Key:   indexKey,
					//	Value: v.Opts.ParentKey,
					//})

					//if err := s.opSaga.e.upsertItemOnDisk(v.Ctx, filePath, tmpFilePath, fileData); err != nil {
					//	return nil, err
					//}

					if err := s.opSaga.e.upsertItemOnDisk(v.Ctx, v.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key:   indexKey,
						Value: v.Opts.ParentKey,
					}); err != nil {
						v.RespSignal <- err
					}
				}

				return nil, nil
			})
			op.OpX(func() (any, error) {
				keysToDelete := bytesop.CalRightDiff(
					v.Opts.IndexingKeys,
					ltngdbenginemodelsv3.IndexListToBytesList(indexingList))
				v.IndexKeysToDelete = keysToDelete

				for _, indexKey := range keysToDelete {
					strItemKey := hex.EncodeToString(indexKey)
					filePath := ltngdbenginemodelsv3.GetIndexedDataFilepath(v.DBMetaInfo.Path, strItemKey)
					tmpFilePath := ltngdbenginemodelsv3.GetTemporaryIndexedDataFilepath(v.DBMetaInfo.Path, strItemKey)

					if err := osx.MvFile(ctx, filePath, tmpFilePath); err != nil {
						return nil, err
					}
				}

				return nil, nil
			})

			err = op.WaitAndWrapErr()
			v.RespSignal <- err
			close(v.RespSignal)
		},
	)
}

func (s *upsertSaga) deleteIndexItemFromDiskOnThread(
	ctx context.Context,
) {
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.UpsertChannels.RollbackIndexItemChannel.Ch,
		func(v *ltngdbenginemodelsv3.ItemInfoData) {
			indexingList, err := s.opSaga.e.loadIndexingList(v.Ctx, v.DBMetaInfo, v.Opts)
			if err != nil {
				v.RespSignal <- err
				close(v.RespSignal)
				return
			}

			op := syncx.NewThreadOperator("upsertIndexItemOnDisk")
			op.OpX(func() (any, error) {
				keysToSave := bytesop.CalRightDiff(
					v.Opts.IndexingKeys,
					ltngdbenginemodelsv3.IndexListToBytesList(indexingList))

				for _, indexKey := range keysToSave {
					//strItemKey := hex.EncodeToString(indexKey)
					//filePath := GetIndexedDataFilepath(v.DBMetaInfo.Path, strItemKey)
					//tmpFilePath := GetTemporaryIndexedDataFilepath(v.DBMetaInfo.Path, strItemKey)
					//fileData := NewFileData(v.DBMetaInfo.IndexInfo(), &Item{
					//	Key:   indexKey,
					//	Value: v.Opts.ParentKey,
					//})
					//
					//if err := s.opSaga.e.upsertItemOnDisk(v.Ctx, filePath, tmpFilePath, fileData); err != nil {
					//	return nil, err
					//}

					if err := s.opSaga.e.upsertItemOnDisk(v.Ctx, v.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key:   indexKey,
						Value: v.Opts.ParentKey,
					}); err != nil {
						return nil, err
					}
				}

				return nil, nil
			})
			op.OpX(func() (any, error) {
				keysToDelete := bytesop.CalRightDiff(
					ltngdbenginemodelsv3.IndexListToBytesList(indexingList),
					v.Opts.IndexingKeys)

				var errAcc error
				for _, indexKey := range keysToDelete {
					strItemKey := hex.EncodeToString(indexKey)
					filePath := ltngdbenginemodelsv3.GetDataFilepath(v.DBMetaInfo.IndexInfo().Path, strItemKey)

					if err := os.Remove(filePath); err != nil {
						if errAcc == nil {
							errAcc = err
						} else {
							err = fmt.Errorf("%s: %w", errAcc, err)
							errAcc = fmt.Errorf("error deleting item on database: %w", err)
						}
					}
				}

				return nil, nil
			})

			err = op.WaitAndWrapErr()
			v.RespSignal <- err
			close(v.RespSignal)
		},
	)
}

func (s *upsertSaga) upsertIndexListItemOnDiskOnThread(
	ctx context.Context,
) {
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.UpsertChannels.ActionIndexListItemChannel.Ch,
		func(v *ltngdbenginemodelsv3.ItemInfoData) {
			//strItemKey := hex.EncodeToString(v.Opts.ParentKey)
			//filePath := GetIndexedListDataFilepath(v.DBMetaInfo.Path, strItemKey)
			//tmpFilePath := GetTemporaryIndexedListDataFilepath(v.DBMetaInfo.Path, strItemKey)
			//fileData := NewFileData(v.DBMetaInfo.IndexListInfo(), &Item{
			//	Key:   v.Opts.ParentKey,
			//	Value: bytes.Join(v.Opts.IndexingKeys, []byte(BsSep)),
			//})
			//
			//err := s.opSaga.e.upsertItemOnDisk(ctx, filePath, tmpFilePath, fileData)

			err := s.opSaga.e.upsertItemOnDisk(ctx, v.DBMetaInfo, &ltngdbenginemodelsv3.Item{
				Key:   v.Opts.ParentKey,
				Value: bytes.Join(v.Opts.IndexingKeys, []byte(ltngdbenginemodelsv3.BsSep)),
			})
			v.RespSignal <- err
			close(v.RespSignal)
		},
	)
}

func (s *upsertSaga) deleteIndexListItemFromDiskOnThread(
	ctx context.Context,
) {
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.UpsertChannels.RollbackIndexListItemChannel.Ch,
		func(v *ltngdbenginemodelsv3.ItemInfoData) {
			strItemKey := hex.EncodeToString(v.Item.Key)
			filePath := ltngdbenginemodelsv3.GetDataFilepath(v.DBMetaInfo.Path, strItemKey)
			tmpFilePath := ltngdbenginemodelsv3.GetTemporaryIndexedListDataFilepath(v.DBMetaInfo.Path, strItemKey)

			if err := osx.MvFile(ctx, tmpFilePath, filePath); err != nil {
				v.RespSignal <- err
				close(v.RespSignal)
				return
			}

			v.RespSignal <- nil
			close(v.RespSignal)
		},
	)
}

func (s *upsertSaga) upsertRelationalItemOnDiskOnThread(
	ctx context.Context,
) {
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.UpsertChannels.ActionRelationalItemChannel.Ch,
		func(v *ltngdbenginemodelsv3.ItemInfoData) {
			err := s.opSaga.e.upsertRelationalItemOnDisk(v.Ctx, v.DBMetaInfo, v.Item)
			v.RespSignal <- err
			close(v.RespSignal)
		},
	)
}

func (s *upsertSaga) cleanUpUpsert(
	ctx context.Context,
) {
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.UpsertChannels.CleanUpUpsert.Ch,
		func(v *ltngdbenginemodelsv3.ItemInfoData) {
			{
				strItemKey := hex.EncodeToString(v.Item.Key)
				tmpFilePath := ltngdbenginemodelsv3.GetTemporaryDataFilepath(v.DBMetaInfo.Path, strItemKey)
				_ = os.Remove(tmpFilePath)
				if !v.Opts.HasIdx {
					v.RespSignal <- nil
					close(v.RespSignal)
					return
				}
			}

			{
				for _, indexKey := range v.IndexKeysToDelete {
					strItemKey := hex.EncodeToString(indexKey)
					tmpFilePath := ltngdbenginemodelsv3.GetTemporaryIndexedDataFilepath(v.DBMetaInfo.Path, strItemKey)
					_ = os.Remove(tmpFilePath)
				}
			}

			{
				strItemKey := hex.EncodeToString(v.Item.Key)
				tmpFilePath := ltngdbenginemodelsv3.GetTemporaryIndexedListDataFilepath(v.DBMetaInfo.Path, strItemKey)
				_ = os.Remove(tmpFilePath)
			}

			v.RespSignal <- nil
			close(v.RespSignal)
		},
	)
}
