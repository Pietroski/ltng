package v2

import (
	"bytes"
	"context"
	"encoding/hex"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/loop"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
)

type (
	deletionChannels struct {
		QueueChannel                  *syncx.Channel[struct{}]
		InfoChannel                   *syncx.Channel[*deleteItemInfoData]
		ActionItemChannel             *syncx.Channel[*deleteItemInfoData]
		RollbackItemChannel           *syncx.Channel[*deleteItemInfoData]
		ActionIndexItemChannel        *syncx.Channel[*deleteItemInfoData]
		RollbackIndexItemChannel      *syncx.Channel[*deleteItemInfoData]
		ActionIndexListItemChannel    *syncx.Channel[*deleteItemInfoData]
		RollbackIndexListItemChannel  *syncx.Channel[*deleteItemInfoData]
		ActionRelationalItemChannel   *syncx.Channel[*deleteItemInfoData]
		RollbackRelationalItemChannel *syncx.Channel[*deleteItemInfoData]

		ActionDelTmpFiles   *syncx.Channel[*deleteItemInfoData]
		RollbackDelTmpFiles *syncx.Channel[*deleteItemInfoData]
	}

	deleteChannels struct {
		deleteCascadeChannel   *deletionChannels
		deleteCascadeByIndex   *deletionChannels
		deleteIndexOnlyChannel *deletionChannels
	}

	deleteItemInfoData struct {
		*ltngdata.ItemInfoData
		IndexList []*ltngdata.Item
	}
)

func makeDeleteChannels() *deleteChannels {
	return &deleteChannels{
		deleteCascadeByIndex:   makeDeletionChannels(),
		deleteIndexOnlyChannel: makeDeletionChannels(),
		deleteCascadeChannel:   makeDeletionChannels(),
	}
}

func makeDeletionChannels() *deletionChannels {
	return &deletionChannels{
		QueueChannel: syncx.NewChannel[struct{}](syncx.WithChannelSize[struct{}](ltngdata.ChannelLimit)),
		InfoChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdata.ChannelLimit)),

		ActionItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdata.ChannelLimit)),
		RollbackItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdata.ChannelLimit)),
		ActionIndexItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdata.ChannelLimit)),
		RollbackIndexItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdata.ChannelLimit)),
		ActionIndexListItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdata.ChannelLimit)),
		RollbackIndexListItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdata.ChannelLimit)),
		ActionRelationalItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdata.ChannelLimit)),
		RollbackRelationalItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdata.ChannelLimit)),

		ActionDelTmpFiles: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdata.ChannelLimit)),
		RollbackDelTmpFiles: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdata.ChannelLimit)),
	}
}

func (i *deleteItemInfoData) withRespChan(sigChan chan error) *deleteItemInfoData {
	return &deleteItemInfoData{
		ItemInfoData: &ltngdata.ItemInfoData{
			Ctx:        i.Ctx,
			DBMetaInfo: i.DBMetaInfo,
			Item:       i.Item,
			Opts:       i.Opts,
			RespSignal: sigChan,
		},
		IndexList: i.IndexList,
	}
}

// #####################################################################################################################

type (
	deleteSaga struct {
		opSaga         *opSaga
		deleteChannels *deleteChannels
		offThread      *syncx.OffThread
		cancel         context.CancelFunc
	}
)

func newDeleteSaga(ctx context.Context, opSaga *opSaga) *deleteSaga {
	ctx, cancel := context.WithCancel(ctx)
	ds := &deleteSaga{
		opSaga:         opSaga,
		deleteChannels: makeDeleteChannels(),
		offThread:      syncx.NewThreadOperator("DeleteSaga", syncx.WithThreadLimit(threadLimit)),
		cancel:         cancel,
	}

	ds.offThread.Op(func() {
		ds.ListenAndTrigger(ctx)
	})

	newDeleteCascadeSaga(ctx, ds)
	newDeleteCascadeByIdxSaga(ctx, ds)
	newDeleteIdxOnlySaga(ctx, ds)

	return ds
}

func (s *deleteSaga) ListenAndTrigger(ctx context.Context) {
	ctx = context.WithValue(ctx, "thread", "operator_delete_saga-ListenAndTrigger")
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.DeleteChannels.InfoChannel.Ch,
		func(itemInfoData *ltngdata.ItemInfoData) {
			switch itemInfoData.Opts.IndexProperties.IndexDeletionBehaviour {
			case ltngdata.IndexOnly:
				s.deleteChannels.deleteIndexOnlyChannel.InfoChannel.Send(&deleteItemInfoData{ItemInfoData: itemInfoData})
			case ltngdata.CascadeByIdx:
				s.deleteChannels.deleteCascadeByIndex.InfoChannel.Send(&deleteItemInfoData{ItemInfoData: itemInfoData})
			case ltngdata.Cascade:
				s.deleteChannels.deleteCascadeChannel.InfoChannel.Send(&deleteItemInfoData{ItemInfoData: itemInfoData})
			case ltngdata.None:
				fallthrough
			default:
				itemInfoData.RespSignal <- errorsx.New("invalid index deletion behaviour")
			}
		},
	)
	s.cancel()
}

// #####################################################################################################################

type deleteCascadeSaga struct {
	deleteSaga *deleteSaga
	cancel     context.CancelFunc
}

func newDeleteCascadeSaga(ctx context.Context, deleteSaga *deleteSaga) *deleteCascadeSaga {
	ctx, cancel := context.WithCancel(ctx)
	dcs := &deleteCascadeSaga{
		deleteSaga: deleteSaga,
		cancel:     cancel,
	}

	dcs.deleteSaga.offThread.Op(func() {
		dcs.deleteItemFromDiskOnThread(ctx)
	})
	dcs.deleteSaga.offThread.Op(func() {
		dcs.deleteIndexItemFromDiskOnThread(ctx)
	})
	dcs.deleteSaga.offThread.Op(func() {
		dcs.deleteIndexingListItemFromDiskOnThread(ctx)
	})
	dcs.deleteSaga.offThread.Op(func() {
		dcs.deleteRelationalItemFromDiskOnThread(ctx)
	})
	dcs.deleteSaga.offThread.Op(func() {
		dcs.deleteTemporaryRecords(ctx)
	})
	dcs.deleteSaga.offThread.Op(func() {
		dcs.recreateItemOnDiskOnThread(ctx)
	})
	dcs.deleteSaga.offThread.Op(func() {
		dcs.recreateIndexItemOnDiskOnThread(ctx)
	})
	dcs.deleteSaga.offThread.Op(func() {
		dcs.recreateIndexListItemOnDiskOnThread(ctx)
	})

	dcs.deleteSaga.offThread.Op(func() {
		dcs.ListenAndTrigger(ctx)
	})

	return dcs
}

func (s *deleteCascadeSaga) ListenAndTrigger(ctx context.Context) {
	ctx = context.WithValue(ctx, "thread", "operator_delete_cascade_saga-ListenAndTrigger")
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteCascadeChannel.InfoChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			if !itemInfoData.Opts.HasIdx {
				s.noIndexTrigger(ctx, itemInfoData)
			} else {
				s.indexTrigger(ctx, itemInfoData)
			}
		},
	)
	s.cancel()
}

func (s *deleteCascadeSaga) noIndexTrigger(
	ctx context.Context, itemInfoData *deleteItemInfoData,
) {
	itemRespSignal := make(chan error, 1)
	itemInfoDataWithChan := itemInfoData.
		withRespChan(itemRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionItemChannel.Send(itemInfoDataWithChan)
	if err := <-itemRespSignal; err != nil {
		s.deleteSaga.opSaga.e.logger.Error(ctx, "error on triggering delete action item info data",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	relationalItemRespSignal := make(chan error, 1)
	relationalItemInfoDataWithChan := itemInfoData.
		withRespChan(relationalItemRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionRelationalItemChannel.Send(relationalItemInfoDataWithChan)
	if err := <-relationalItemRespSignal; err != nil {
		s.deleteSaga.opSaga.e.logger.Error(ctx, "error on triggering delete action relational item info data",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)

		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		return
	}

	temporaryItemRespSignal := make(chan error, 1)
	temporaryItemInfoDataWithChan := itemInfoData.
		withRespChan(temporaryItemRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionDelTmpFiles.Send(temporaryItemInfoDataWithChan)
	if err := <-temporaryItemRespSignal; err != nil {
		s.deleteSaga.opSaga.e.logger.Error(ctx,
			"error on triggering delete action item info data delete temporary records",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)

		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		return
	}

	itemInfoData.RespSignal <- nil
	close(itemInfoData.RespSignal)
}

func (s *deleteCascadeSaga) indexTrigger(
	ctx context.Context, itemInfoData *deleteItemInfoData,
) {
	indexItemList, err := s.deleteSaga.opSaga.e.loadIndexingList(
		itemInfoData.Ctx,
		itemInfoData.DBMetaInfo,
		&ltngdata.IndexOpts{ParentKey: itemInfoData.Item.Key},
	)
	if err != nil {
		s.deleteSaga.opSaga.e.logger.Error(ctx, "error loading indexing list before deleting",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)

		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)

		return
	}
	itemInfoData.IndexList = indexItemList

	itemRespSignal := make(chan error, 1)
	itemInfoDataWithChan := itemInfoData.withRespChan(itemRespSignal)
	indexedItemRespSignal := make(chan error, len(indexItemList))
	indexedItemInfoDataWithChan := itemInfoData.withRespChan(indexedItemRespSignal)
	indexedListItemRespSignal := make(chan error, 1)
	IndexedListItemInfoDataWithChan := itemInfoData.withRespChan(indexedListItemRespSignal)

	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionItemChannel.Send(itemInfoDataWithChan)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionIndexItemChannel.Send(indexedItemInfoDataWithChan)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionIndexListItemChannel.Send(IndexedListItemInfoDataWithChan)

	if err = ResponseAccumulator(
		itemRespSignal,
		indexedItemRespSignal,
		indexedListItemRespSignal,
	); err != nil {
		s.deleteSaga.opSaga.e.logger.Error(ctx, "error on index trigger",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)

		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		return
	}

	relationalItemRespSignal := make(chan error, 1)
	relationalItemInfoDataWithChan := itemInfoData.withRespChan(relationalItemRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionRelationalItemChannel.Send(relationalItemInfoDataWithChan)
	if err = <-relationalItemRespSignal; err != nil {
		s.deleteSaga.opSaga.e.logger.Error(ctx, "error on relational index trigger",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)

		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		return
	}

	temporaryItemRespSignal := make(chan error, 1)
	itemInfoDataForActionDelTmpFiles := itemInfoData.
		withRespChan(temporaryItemRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionDelTmpFiles.Send(itemInfoDataForActionDelTmpFiles)
	if err = <-temporaryItemRespSignal; err != nil {
		s.deleteSaga.opSaga.e.logger.Error(ctx,
			"error on temporary index trigger",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)

		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		return
	}

	itemInfoData.RespSignal <- nil
	close(itemInfoData.RespSignal)
}

func (s *deleteCascadeSaga) RollbackTrigger(ctx context.Context, itemInfoData *deleteItemInfoData) {
	if !itemInfoData.Opts.HasIdx {
		s.noIndexRollback(ctx, itemInfoData)
		return
	}

	s.indexRollback(ctx, itemInfoData)
}

func (s *deleteCascadeSaga) noIndexRollback(
	ctx context.Context, itemInfoData *deleteItemInfoData,
) {
	itemRespSignal := make(chan error, 1)
	itemInfoDataWithChan := itemInfoData.withRespChan(itemRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		RollbackItemChannel.Send(itemInfoDataWithChan)
	if err := <-itemRespSignal; err != nil {
		s.deleteSaga.opSaga.e.logger.Error(ctx,
			"error on rolling back trigger for item info data",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)
	}

	temporaryItemRespSignal := make(chan error, 1)
	temporaryItemInfoDataWithChan := itemInfoData.
		withRespChan(temporaryItemRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionDelTmpFiles.Send(temporaryItemInfoDataWithChan)
	if err := <-temporaryItemRespSignal; err != nil {
		s.deleteSaga.opSaga.e.logger.Error(ctx,
			"error on rolling back trigger for temporary info data",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)
	}
}

func (s *deleteCascadeSaga) indexRollback(
	ctx context.Context, itemInfoData *deleteItemInfoData,
) {
	itemRespSignal := make(chan error, 1)
	itemInfoDataWithChan := itemInfoData.
		withRespChan(itemRespSignal)
	indexedItemRespSignal := make(chan error, 1)
	indexedItemInfoDataWithChan := itemInfoData.
		withRespChan(indexedItemRespSignal)
	indexedListItemRespSignal := make(chan error, 1)
	indexedListItemInfoDataWithChan := itemInfoData.
		withRespChan(indexedListItemRespSignal)

	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		RollbackItemChannel.Send(itemInfoDataWithChan)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		RollbackIndexItemChannel.Send(indexedItemInfoDataWithChan)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		RollbackIndexListItemChannel.Send(indexedListItemInfoDataWithChan)

	if err := ResponseAccumulator(
		itemRespSignal,
		indexedItemRespSignal,
		indexedListItemRespSignal,
	); err != nil {
		s.deleteSaga.opSaga.e.logger.Error(ctx,
			"error on rolling back trigger for item info data",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)
	}

	temporaryItemRespSignal := make(chan error, 1)
	temporaryItemInfoDataWithChan := itemInfoData.
		withRespChan(temporaryItemRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionDelTmpFiles.Send(temporaryItemInfoDataWithChan)
	if err := <-temporaryItemRespSignal; err != nil {
		s.deleteSaga.opSaga.e.logger.Error(ctx,
			"error on triggering item info data delete temporary records action",
			"item_info_data", itemInfoData.DBMetaInfo, "err", err)
	}
}

func (s *deleteCascadeSaga) deleteItemFromDiskOnThread(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteCascadeChannel.ActionItemChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			strItemKey := hex.EncodeToString(itemInfoData.Item.Key)
			if err := osx.MvFile(itemInfoData.Ctx,
				ltngdata.GetDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
				ltngdata.GetTemporaryDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
			); err != nil {
				s.deleteSaga.opSaga.e.logger.Debug(ctx, "error on deleting item from disk",
					"item_info_data", *itemInfoData.DBMetaInfo, "err", err)

				itemInfoData.RespSignal <- errorsx.Wrap(err, "error on deleting item from disk")
				close(itemInfoData.RespSignal)

				return
			}

			lockStrKey := itemInfoData.DBMetaInfo.LockName(strItemKey)
			fileStats, ok := s.deleteSaga.opSaga.e.itemFileMapping.Get(lockStrKey)
			if ok {
				if !rw.IsFileClosed(fileStats.File) {
					_ = fileStats.File.Close()
				}
			}
			s.deleteSaga.opSaga.e.itemFileMapping.Delete(lockStrKey)

			itemInfoData.RespSignal <- nil
			close(itemInfoData.RespSignal)
		},
	)
}

func (s *deleteCascadeSaga) recreateItemOnDiskOnThread(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteCascadeChannel.RollbackItemChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			strItemKey := hex.EncodeToString(itemInfoData.Item.Key)

			err := osx.MvFile(itemInfoData.Ctx,
				ltngdata.GetTemporaryDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
				ltngdata.GetDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey))

			itemInfoData.RespSignal <- err
			close(itemInfoData.RespSignal)
		},
	)
}

func (s *deleteCascadeSaga) deleteIndexItemFromDiskOnThread(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteCascadeChannel.ActionIndexItemChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			for _, item := range itemInfoData.IndexList {
				strItemKey := hex.EncodeToString(item.Value)

				if err := osx.MvFile(itemInfoData.Ctx,
					ltngdata.GetIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
					ltngdata.GetTemporaryIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
				); err != nil {
					s.deleteSaga.opSaga.e.logger.Debug(ctx, "error on deleting indexed item from disk",
						"item_info_data", *itemInfoData.DBMetaInfo, "err", err)

					itemInfoData.RespSignal <- errorsx.Wrap(err, "error on deleting indexed item from disk")

					continue
				}

				lockStrKey := itemInfoData.DBMetaInfo.IndexInfo().LockName(strItemKey)
				fileStats, ok := s.deleteSaga.opSaga.e.itemFileMapping.Get(lockStrKey)
				if ok {
					if !rw.IsFileClosed(fileStats.File) {
						_ = fileStats.File.Close()
					}
				}
				s.deleteSaga.opSaga.e.itemFileMapping.Delete(lockStrKey)
			}

			itemInfoData.RespSignal <- nil
			close(itemInfoData.RespSignal)
		},
	)
}

func (s *deleteCascadeSaga) recreateIndexItemOnDiskOnThread(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteCascadeChannel.RollbackIndexItemChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			for _, item := range itemInfoData.IndexList {
				strItemKey := hex.EncodeToString(item.Value)

				if err := osx.MvFile(itemInfoData.Ctx,
					ltngdata.GetTemporaryIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
					ltngdata.GetIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
				); err != nil {
					// TODO: debug log
					itemInfoData.RespSignal <- err
					continue
				}
			}

			itemInfoData.RespSignal <- nil
			//close(itemInfoData.RespSignal)
		},
	)
}

func (s *deleteCascadeSaga) deleteIndexingListItemFromDiskOnThread(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteCascadeChannel.ActionIndexListItemChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			strItemKey := hex.EncodeToString(itemInfoData.Item.Key)
			if err := osx.MvFile(itemInfoData.Ctx,
				ltngdata.GetIndexedListDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
				ltngdata.GetTemporaryIndexedListDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
			); err != nil {
				// TODO: debug log
				itemInfoData.RespSignal <- err
				//close(itemInfoData.RespSignal)
				return
			}

			fileStats, ok := s.deleteSaga.opSaga.e.
				itemFileMapping.Get(itemInfoData.DBMetaInfo.IndexListInfo().LockName(strItemKey))
			if ok {
				if !rw.IsFileClosed(fileStats.File) {
					_ = fileStats.File.Close()
				}
			}
			s.deleteSaga.opSaga.e.itemFileMapping.Delete(itemInfoData.DBMetaInfo.IndexListInfo().LockName(strItemKey))

			itemInfoData.RespSignal <- nil
			//close(itemInfoData.RespSignal)
		},
	)
}

func (s *deleteCascadeSaga) recreateIndexListItemOnDiskOnThread(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteCascadeChannel.RollbackIndexListItemChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			strItemKey := hex.EncodeToString(itemInfoData.Item.Key)
			if err := osx.MvFile(itemInfoData.Ctx,
				ltngdata.GetTemporaryIndexedListDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
				ltngdata.GetIndexedListDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
			); err != nil {
				// TODO: debug log
				itemInfoData.RespSignal <- err
				//close(itemInfoData.RespSignal)
				return
			}

			itemInfoData.RespSignal <- nil
			//close(itemInfoData.RespSignal)
		},
	)
}

func (s *deleteCascadeSaga) deleteRelationalItemFromDiskOnThread(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteCascadeChannel.ActionRelationalItemChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			fi, err := s.deleteSaga.opSaga.e.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
			if err != nil {
				itemInfoData.RespSignal <- err
				close(itemInfoData.RespSignal)
				return
			}

			err = s.deleteSaga.opSaga.e.deleteRelationalData(
				itemInfoData.Ctx, itemInfoData.Item, fi)
			if err != nil {
				s.deleteSaga.opSaga.e.logger.Error(itemInfoData.Ctx,
					"error on relational index trigger",
					"item_info_data", itemInfoData.DBMetaInfo, "error", err)
				itemInfoData.RespSignal <- err
				close(itemInfoData.RespSignal)
				return
			}

			itemInfoData.RespSignal <- nil
			close(itemInfoData.RespSignal)
		},
	)
}

func (s *deleteCascadeSaga) deleteTemporaryRecords(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteCascadeChannel.ActionDelTmpFiles.Ch,
		func(itemInfoData *deleteItemInfoData) {
			if _, err := osx.DelOnlyFilesFromDirAsync(itemInfoData.Ctx,
				ltngdata.GetTemporaryDataPath(itemInfoData.DBMetaInfo.Path)); err != nil {
				itemInfoData.RespSignal <- err
				//close(itemInfoData.RespSignal)
				return
			}

			strItemKey := hex.EncodeToString(itemInfoData.Item.Key)
			lockKey := itemInfoData.DBMetaInfo.LockName(strItemKey)
			s.deleteSaga.opSaga.e.markedAsDeletedMapping.Delete(lockKey)

			itemInfoData.RespSignal <- nil
			//close(itemInfoData.RespSignal)
		},
	)
}

// #####################################################################################################################

type deleteCascadeByIdxSaga struct {
	deleteSaga *deleteSaga
	cancel     context.CancelFunc
}

func newDeleteCascadeByIdxSaga(ctx context.Context, deleteSaga *deleteSaga) *deleteCascadeByIdxSaga {
	ctx, cancel := context.WithCancel(ctx)
	dcs := &deleteCascadeByIdxSaga{
		deleteSaga: deleteSaga,
		cancel:     cancel,
	}

	dcs.deleteSaga.offThread.Op(func() {
		dcs.ListenAndTrigger(ctx)
	})

	return dcs
}

func (s *deleteCascadeByIdxSaga) ListenAndTrigger(ctx context.Context) {
	ctx = context.WithValue(ctx, "thread", "operator_delete_cascade_by_index_saga-ListenAndTrigger")
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteCascadeByIndex.InfoChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			item, err := s.deleteSaga.opSaga.e.loadItem(context.Background(),
				itemInfoData.DBMetaInfo, itemInfoData.Item, itemInfoData.Opts)
			if err != nil {
				// TODO: log error
				return
			}

			itemInfoData.Item = item
			s.deleteSaga.deleteChannels.deleteCascadeChannel.InfoChannel.Send(itemInfoData)
		},
	)
	s.cancel()
}

// #####################################################################################################################

type deleteIdxOnlySaga struct {
	deleteSaga *deleteSaga
	cancel     context.CancelFunc
}

func newDeleteIdxOnlySaga(ctx context.Context, deleteSaga *deleteSaga) *deleteIdxOnlySaga {
	ctx, cancel := context.WithCancel(ctx)
	dcs := &deleteIdxOnlySaga{
		deleteSaga: deleteSaga,
		cancel:     cancel,
	}

	dcs.deleteSaga.offThread.Op(func() {
		dcs.deleteIndexItemFromDiskOnThread(ctx)
	})
	dcs.deleteSaga.offThread.Op(func() {
		dcs.updateIndexingListItemFromDiskOnThread(ctx)
	})
	dcs.deleteSaga.offThread.Op(func() {
		dcs.deleteTemporaryRecords(ctx)
	})
	dcs.deleteSaga.offThread.Op(func() {
		dcs.recreateIndexItemOnDiskOnThread(ctx)
	})
	dcs.deleteSaga.offThread.Op(func() {
		dcs.rollbackIndexListItemOnDiskOnThread(ctx)
	})

	dcs.deleteSaga.offThread.Op(func() {
		dcs.ListenAndTrigger(ctx)
	})

	return dcs
}

func (s *deleteIdxOnlySaga) ListenAndTrigger(ctx context.Context) {
	ctx = context.WithValue(ctx, "thread", "operator_delete_index_only_saga-ListenAndTrigger")
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.InfoChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			indexItemList, err := s.deleteSaga.opSaga.e.loadIndexingList(
				itemInfoData.Ctx,
				itemInfoData.DBMetaInfo,
				itemInfoData.Opts, // &ltngdata.IndexOpts{ParentKey: itemInfoData.Item.Key},
			)
			if err != nil {
				// TODO: log
				itemInfoData.RespSignal <- err
				//close(itemInfoData.RespSignal)
				return
			}
			itemInfoData.IndexList = indexItemList

			s.indexTrigger(ctx, itemInfoData)
		},
	)
	s.cancel()
}

func (s *deleteIdxOnlySaga) indexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	indexItemList, err := s.deleteSaga.opSaga.e.loadIndexingList(
		itemInfoData.Ctx,
		itemInfoData.DBMetaInfo,
		&ltngdata.IndexOpts{ParentKey: itemInfoData.Item.Key},
	)
	if err != nil {
		// TODO: log
		itemInfoData.RespSignal <- err
		//close(itemInfoData.RespSignal)
		return
	}
	itemInfoData.IndexList = indexItemList

	deleteIndexItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionIndexItemChannel := itemInfoData.
		withRespChan(deleteIndexItemFromDiskOnThreadRespSignal)
	updateIndexingListItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionIndexListItemChannel := itemInfoData.
		withRespChan(updateIndexingListItemFromDiskOnThreadRespSignal)

	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		ActionIndexItemChannel.Send(itemInfoDataForActionIndexItemChannel)
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		ActionIndexListItemChannel.Send(itemInfoDataForActionIndexListItemChannel)

	if err = ResponseAccumulator(
		deleteIndexItemFromDiskOnThreadRespSignal,
		updateIndexingListItemFromDiskOnThreadRespSignal,
	); err != nil {
		s.deleteSaga.opSaga.e.logger.Error(itemInfoData.Ctx, "error on trigger action itemInfoData",
			"item_info_data", itemInfoData.DBMetaInfo, "error", err)
		s.indexRollback(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		return
	}

	deleteTemporaryRecordsFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionDelTmpFiles := itemInfoData.
		withRespChan(deleteTemporaryRecordsFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		ActionDelTmpFiles.Send(itemInfoDataForActionDelTmpFiles)
	err = <-deleteTemporaryRecordsFromDiskOnThreadRespSignal
	if err != nil {
		s.deleteSaga.opSaga.e.logger.Error(itemInfoData.Ctx,
			"error on trigger action itemInfoData delete temporary data",
			"item_info_data", itemInfoData.DBMetaInfo, "error", err)
		s.indexRollback(itemInfoData.Ctx, itemInfoData)
	}

	itemInfoData.RespSignal <- err
}

func (s *deleteIdxOnlySaga) indexRollback(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	recreateIndexItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackIndexItemChannel := itemInfoData.
		withRespChan(recreateIndexItemOnDiskRespSignal)
	rollbackIndexListItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackIndexListItemChannel := itemInfoData.
		withRespChan(rollbackIndexListItemOnDiskRespSignal)

	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		RollbackIndexItemChannel.Send(itemInfoDataForRollbackIndexItemChannel)
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		RollbackIndexListItemChannel.Send(itemInfoDataForRollbackIndexListItemChannel)

	if err := ResponseAccumulator(
		recreateIndexItemOnDiskRespSignal,
		rollbackIndexListItemOnDiskRespSignal,
	); err != nil {
		s.deleteSaga.opSaga.e.logger.Error(itemInfoData.Ctx,
			"error rolling back trigger for itemInfoData",
			"item_info_data", itemInfoData.DBMetaInfo, "error", err)
	}
}

func (s *deleteIdxOnlySaga) deleteIndexItemFromDiskOnThread(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.ActionIndexItemChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			for _, indexKey := range itemInfoData.Opts.IndexingKeys {
				strItemKey := hex.EncodeToString(indexKey)
				lockStrKey := itemInfoData.DBMetaInfo.IndexInfo().LockName(strItemKey)

				fileStats, ok := s.deleteSaga.opSaga.e.itemFileMapping.Get(lockStrKey)
				if ok {
					if !rw.IsFileClosed(fileStats.File) {
						_ = fileStats.File.Close()
					}
				}

				if err := osx.MvFile(itemInfoData.Ctx,
					ltngdata.GetIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
					ltngdata.GetTemporaryIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
				); err != nil {
					// TODO: log
					itemInfoData.RespSignal <- err
					//close(itemInfoData.RespSignal)
					continue
				}

				s.deleteSaga.opSaga.e.itemFileMapping.Delete(lockStrKey)
			}

			itemInfoData.RespSignal <- nil
			//close(itemInfoData.RespSignal)
		},
	)
}

func (s *deleteIdxOnlySaga) recreateIndexItemOnDiskOnThread(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.RollbackIndexItemChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			for _, item := range itemInfoData.IndexList {
				strItemKey := hex.EncodeToString(item.Value)

				if err := osx.MvFile(itemInfoData.Ctx,
					ltngdata.GetTemporaryIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
					ltngdata.GetIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
				); err != nil {
					// TODO: log
					itemInfoData.RespSignal <- err
					continue
				}
			}

			itemInfoData.RespSignal <- nil
			//close(itemInfoData.RespSignal)
		},
	)
}

func (s *deleteIdxOnlySaga) updateIndexingListItemFromDiskOnThread(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.ActionIndexListItemChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			key := itemInfoData.Opts.ParentKey
			var newIndexList [][]byte
			for _, item := range itemInfoData.IndexList {
				// TODO: convert the list to a map search
				addToIndexingList := true
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					if bytes.Equal(item.Value, indexKey) {
						addToIndexingList = false
					}
				}

				if !addToIndexingList {
					continue
				}
				newIndexList = append(newIndexList, item.Value)
			}

			newIndexListBs := bytes.Join(newIndexList, []byte(ltngdata.BsSep))

			strItemKey := hex.EncodeToString(key)
			filePath := ltngdata.GetIndexedListDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey)
			tmpFilePath := ltngdata.GetTemporaryIndexedListDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey)
			fileData := ltngdata.NewFileData(itemInfoData.DBMetaInfo.IndexListInfo(), &ltngdata.Item{
				Key:   key,
				Value: newIndexListBs,
			})

			err := s.deleteSaga.opSaga.e.upsertItemOnDisk(ctx, filePath, tmpFilePath, fileData)
			if err != nil {
				itemInfoData.RespSignal <- err
				//close(itemInfoData.RespSignal)
				return
			}

			itemInfoData.RespSignal <- nil
			//close(itemInfoData.RespSignal)
		},
	)
}

func (s *deleteIdxOnlySaga) rollbackIndexListItemOnDiskOnThread(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.RollbackIndexListItemChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			var indexList [][]byte
			for _, item := range itemInfoData.IndexList {
				indexList = append(indexList, item.Value)
			}

			indexListBs := bytes.Join(indexList, []byte(ltngdata.BsSep))
			strItemKey := hex.EncodeToString(itemInfoData.Opts.ParentKey)
			filePath := ltngdata.GetIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey)
			tmpFilePath := ltngdata.GetTemporaryIndexedListDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey)
			fileData := ltngdata.NewFileData(itemInfoData.DBMetaInfo.IndexListInfo(), &ltngdata.Item{
				Key:   itemInfoData.Opts.ParentKey,
				Value: indexListBs,
			})

			if err := s.deleteSaga.opSaga.e.upsertItemOnDisk(ctx, filePath, tmpFilePath, fileData); err != nil {
				itemInfoData.RespSignal <- err
				//close(itemInfoData.RespSignal)
				return
			}

			itemInfoData.RespSignal <- nil
			//close(itemInfoData.RespSignal)
		},
	)
}

func (s *deleteIdxOnlySaga) deleteTemporaryRecords(ctx context.Context) {
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.ActionDelTmpFiles.Ch,
		func(itemInfoData *deleteItemInfoData) {
			if err := osx.CleanupDirs(ctx,
				ltngdata.GetIndexedDataPath(itemInfoData.DBMetaInfo.Path),
			); err != nil {
				itemInfoData.RespSignal <- errorsx.Wrap(err, "error deleting tmp files")
				//close(itemInfoData.RespSignal)
				return
			}

			for _, item := range itemInfoData.IndexList {
				strItemKey := hex.EncodeToString(item.Key)
				lockKey := itemInfoData.DBMetaInfo.IndexInfo().LockName(strItemKey)
				s.deleteSaga.opSaga.e.markedAsDeletedMapping.Delete(lockKey)
			}

			itemInfoData.RespSignal <- nil
			//close(itemInfoData.RespSignal)
		},
	)
}

// #####################################################################################################################
