package ltngdbenginev3

import (
	"context"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/loop"
	"gitlab.com/pietroski-software-company/golang/devex/saga"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
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
		*ltngdbenginemodelsv3.ItemInfoData
		IndexList []*ltngdbenginemodelsv3.Item
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
		QueueChannel: syncx.NewChannel[struct{}](syncx.WithChannelSize[struct{}](ltngdbenginemodelsv3.ChannelLimit)),
		InfoChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdbenginemodelsv3.ChannelLimit)),

		ActionItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdbenginemodelsv3.ChannelLimit)),
		RollbackItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdbenginemodelsv3.ChannelLimit)),
		ActionIndexItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdbenginemodelsv3.ChannelLimit)),
		RollbackIndexItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdbenginemodelsv3.ChannelLimit)),
		ActionIndexListItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdbenginemodelsv3.ChannelLimit)),
		RollbackIndexListItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdbenginemodelsv3.ChannelLimit)),
		ActionRelationalItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdbenginemodelsv3.ChannelLimit)),
		RollbackRelationalItemChannel: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdbenginemodelsv3.ChannelLimit)),

		ActionDelTmpFiles: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdbenginemodelsv3.ChannelLimit)),
		RollbackDelTmpFiles: syncx.NewChannel[*deleteItemInfoData](
			syncx.WithChannelSize[*deleteItemInfoData](ltngdbenginemodelsv3.ChannelLimit)),
	}
}

func (i *deleteItemInfoData) withRespChan(sigChan chan error) *deleteItemInfoData {
	return &deleteItemInfoData{
		ItemInfoData: &ltngdbenginemodelsv3.ItemInfoData{
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
	//newDeleteIdxOnlySaga(ctx, ds)

	return ds
}

func (s *deleteSaga) ListenAndTrigger(ctx context.Context) {
	ctx = context.WithValue(ctx, "thread", "operator_delete_saga-ListenAndTrigger")
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.DeleteChannels.InfoChannel.Ch,
		func(itemInfoData *ltngdbenginemodelsv3.ItemInfoData) {
			switch itemInfoData.Opts.IndexProperties.IndexDeletionBehaviour {
			case ltngdbenginemodelsv3.IndexOnly:
				itemInfoData.RespSignal <- errorsx.New("unimplemented index only deletion behaviour")
				close(itemInfoData.RespSignal)
				return
				//s.deleteChannels.deleteIndexOnlyChannel.InfoChannel.Send(&deleteItemInfoData{ItemInfoData: itemInfoData})
			case ltngdbenginemodelsv3.CascadeByIdx:
				s.deleteChannels.deleteCascadeByIndex.InfoChannel.Send(&deleteItemInfoData{ItemInfoData: itemInfoData})
			case ltngdbenginemodelsv3.Cascade:
				s.deleteChannels.deleteCascadeChannel.InfoChannel.Send(&deleteItemInfoData{ItemInfoData: itemInfoData})
			case ltngdbenginemodelsv3.None:
				fallthrough
			default:
				itemInfoData.RespSignal <- errorsx.New("invalid index deletion behaviour")
				close(itemInfoData.RespSignal)
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
		dcs.ListenAndTrigger(ctx)
	})

	return dcs
}

func (s *deleteCascadeSaga) ListenAndTrigger(ctx context.Context) {
	ctx = context.WithValue(ctx, "thread", "operator_delete_cascade_saga-ListenAndTrigger")
	loop.RunFromChannel(ctx,
		s.deleteSaga.deleteChannels.deleteCascadeChannel.InfoChannel.Ch,
		func(itemInfoData *deleteItemInfoData) {
			if _, err := s.deleteItemInfoData(itemInfoData.Ctx, itemInfoData); err != nil {
				itemInfoData.RespSignal <- errorsx.Wrap(err, "error creating item info data on disk")
				close(itemInfoData.RespSignal)

				return
			}

			itemInfoData.RespSignal <- nil
			close(itemInfoData.RespSignal)
		},
	)
	s.cancel()
}

func (s *deleteCascadeSaga) deleteItemInfoData(
	ctx context.Context,
	itemInfoData *deleteItemInfoData,
) (*deleteItemInfoData, error) {
	return itemInfoData, saga.NewListOperator(s.buildDeleteItemInfoData(ctx, itemInfoData)...).Operate()
}

func (s *deleteCascadeSaga) buildDeleteItemInfoData(
	_ context.Context,
	itemInfoData *deleteItemInfoData,
) []*saga.Operation {
	encodedStr := itemInfoData.EncodedKey()
	itemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.Path, encodedStr)
	tmpItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.TemporaryInfo().Path, encodedStr)

	deleteItemFromDisk := func() error {
		if err := osx.MvFile(itemInfoData.Ctx, itemDataFilePath, tmpItemDataFilePath); err != nil {
			return err
		}

		return nil
	}
	recreateItemOnDisk := func() error {
		if err := osx.MvFile(itemInfoData.Ctx, tmpItemDataFilePath, itemDataFilePath); err != nil {
			return err
		}

		return nil
	}

	deleteRelationalData := func() error {
		rfi, err := s.deleteSaga.opSaga.e.
			loadRelationalItemStoreFromMemoryOrDisk(
				itemInfoData.Ctx, itemInfoData.DBMetaInfo)
		if err != nil {
			return errorsx.Wrapf(err,
				"error loading relational data store - %s",
				itemInfoData.DBMetaInfo.Name)
		}

		if err = s.deleteSaga.opSaga.e.deleteRelationalData(itemInfoData.Ctx,
			itemInfoData.Item, rfi); err != nil {
			return errorsx.Wrapf(err,
				"error deleting item %s from relational data store %s",
				itemInfoData.Item.Key, itemInfoData.DBMetaInfo.Name)
		}

		return nil
	}

	nonIndexedCleanup := func() error {
		_ = osx.CleanupDirs(itemInfoData.Ctx, itemInfoData.DBMetaInfo.Path)
		return nil
	}

	if !itemInfoData.Opts.HasIdx {
		return []*saga.Operation{
			{
				Action: &saga.Action{
					Name:        "deleteItemFromDisk",
					Do:          deleteItemFromDisk,
					RetrialOpts: saga.DefaultRetrialOps,
				},
				Rollback: &saga.Rollback{
					Name:        "recreateItemOnDisk",
					Do:          recreateItemOnDisk,
					RetrialOpts: saga.DefaultRetrialOps,
				},
			},
			{
				Action: &saga.Action{
					Name:        "deleteRelationalData",
					Do:          deleteRelationalData,
					RetrialOpts: saga.DefaultRetrialOps,
				},
			},
			{
				Action: &saga.Action{
					Name:        "nonIndexedCleanup",
					Do:          nonIndexedCleanup,
					RetrialOpts: saga.DefaultRetrialOps,
				},
			},
		}
	}

	indexedItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.IndexInfo().Path, encodedStr)
	tmpIndexedItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.TemporaryIndexInfo().Path, encodedStr)

	indexedListItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.IndexListInfo().Path, encodedStr)
	tmpIndexedListItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.TemporaryIndexListInfo().Path, encodedStr)

	deleteIndexedItemsFromDisk := func() error {
		if _, err := osx.MvOnlyFilesFromDir(itemInfoData.Ctx,
			indexedItemDataFilePath,
			tmpIndexedItemDataFilePath,
		); err != nil {
			return err
		}

		return nil
	}
	recreateIndexedItemsOnDisk := func() error {
		if _, err := osx.MvOnlyFilesFromDir(itemInfoData.Ctx,
			tmpIndexedItemDataFilePath,
			indexedItemDataFilePath,
		); err != nil {
			return err
		}

		return nil
	}

	deleteIndexedListItemFromDisk := func() error {
		if err := osx.MvFile(itemInfoData.Ctx,
			indexedListItemDataFilePath,
			tmpIndexedListItemDataFilePath,
		); err != nil {
			return err
		}

		return nil
	}
	recreateIndexedListItemOnDisk := func() error {
		if err := osx.MvFile(itemInfoData.Ctx,
			tmpIndexedListItemDataFilePath,
			indexedListItemDataFilePath,
		); err != nil {
			return err
		}

		return nil
	}

	indexedCleanup := func() error {
		_ = osx.CleanupDirs(itemInfoData.Ctx, itemInfoData.DBMetaInfo.Path)
		_ = osx.CleanupDirs(itemInfoData.Ctx, itemInfoData.DBMetaInfo.IndexInfo().Path)
		_ = osx.CleanupDirs(itemInfoData.Ctx, itemInfoData.DBMetaInfo.IndexListInfo().Path)
		return nil
	}

	return []*saga.Operation{
		{
			Action: &saga.Action{
				Name:        "deleteItemFromDisk",
				Do:          deleteItemFromDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "recreateItemOnDisk",
				Do:          recreateItemOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "deleteIndexedItemsFromDisk",
				Do:          deleteIndexedItemsFromDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "recreateIndexedItemsOnDisk",
				Do:          recreateIndexedItemsOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "deleteIndexedListItemFromDisk",
				Do:          deleteIndexedListItemFromDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "recreateIndexedListItemOnDisk",
				Do:          recreateIndexedListItemOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "deleteRelationalData",
				Do:          deleteRelationalData,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "indexedCleanup",
				Do:          indexedCleanup,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
	}
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
			item, err := s.deleteSaga.opSaga.e.loadItem(itemInfoData.Ctx,
				itemInfoData.DBMetaInfo, itemInfoData.Item, itemInfoData.Opts)
			if err != nil {
				s.deleteSaga.opSaga.e.logger.Error(itemInfoData.Ctx, "loadItem failed", "error", err)
				return
			}

			itemInfoData.Item = item
			s.deleteSaga.deleteChannels.deleteCascadeChannel.InfoChannel.Send(itemInfoData)
		},
	)
	s.cancel()
}

// #####################################################################################################################

//type deleteIdxOnlySaga struct {
//	deleteSaga *deleteSaga
//	cancel     context.CancelFunc
//}
//
//func newDeleteIdxOnlySaga(ctx context.Context, deleteSaga *deleteSaga) *deleteIdxOnlySaga {
//	ctx, cancel := context.WithCancel(ctx)
//	dcs := &deleteIdxOnlySaga{
//		deleteSaga: deleteSaga,
//		cancel:     cancel,
//	}
//
//	dcs.deleteSaga.offThread.Op(func() {
//		dcs.deleteIndexItemFromDiskOnThread(ctx)
//	})
//	dcs.deleteSaga.offThread.Op(func() {
//		dcs.updateIndexingListItemFromDiskOnThread(ctx)
//	})
//	dcs.deleteSaga.offThread.Op(func() {
//		dcs.deleteTemporaryRecords(ctx)
//	})
//	dcs.deleteSaga.offThread.Op(func() {
//		dcs.recreateIndexItemOnDiskOnThread(ctx)
//	})
//	dcs.deleteSaga.offThread.Op(func() {
//		dcs.rollbackIndexListItemOnDiskOnThread(ctx)
//	})
//
//	dcs.deleteSaga.offThread.Op(func() {
//		dcs.ListenAndTrigger(ctx)
//	})
//
//	return dcs
//}
//
//func (s *deleteIdxOnlySaga) ListenAndTrigger(ctx context.Context) {
//	ctx = context.WithValue(ctx, "thread", "operator_delete_index_only_saga-ListenAndTrigger")
//	loop.RunFromChannel(ctx,
//		s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.InfoChannel.Ch,
//		func(itemInfoData *deleteItemInfoData) {
//			indexItemList, err := s.deleteSaga.opSaga.e.loadIndexingList(
//				itemInfoData.Ctx,
//				itemInfoData.DBMetaInfo,
//				itemInfoData.Opts, // &IndexOpts{ParentKey: itemInfoData.Item.Key},
//			)
//			if err != nil {
//				// TODO: log
//				itemInfoData.RespSignal <- err
//				//close(itemInfoData.RespSignal)
//				return
//			}
//			itemInfoData.IndexList = indexItemList
//
//			s.indexTrigger(ctx, itemInfoData)
//		},
//	)
//	s.cancel()
//}
//
//func (s *deleteIdxOnlySaga) indexTrigger(
//	_ context.Context, itemInfoData *deleteItemInfoData,
//) {
//	indexItemList, err := s.deleteSaga.opSaga.e.loadIndexingList(
//		itemInfoData.Ctx,
//		itemInfoData.DBMetaInfo,
//		&ltngdbenginemodelsv3.IndexOpts{ParentKey: itemInfoData.Item.Key},
//	)
//	if err != nil {
//		// TODO: log
//		itemInfoData.RespSignal <- err
//		//close(itemInfoData.RespSignal)
//		return
//	}
//	itemInfoData.IndexList = indexItemList
//
//	deleteIndexItemFromDiskOnThreadRespSignal := make(chan error, 1)
//	itemInfoDataForActionIndexItemChannel := itemInfoData.
//		withRespChan(deleteIndexItemFromDiskOnThreadRespSignal)
//	updateIndexingListItemFromDiskOnThreadRespSignal := make(chan error, 1)
//	itemInfoDataForActionIndexListItemChannel := itemInfoData.
//		withRespChan(updateIndexingListItemFromDiskOnThreadRespSignal)
//
//	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
//		ActionIndexItemChannel.Send(itemInfoDataForActionIndexItemChannel)
//	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
//		ActionIndexListItemChannel.Send(itemInfoDataForActionIndexListItemChannel)
//
//	if err = ResponseAccumulator(
//		deleteIndexItemFromDiskOnThreadRespSignal,
//		updateIndexingListItemFromDiskOnThreadRespSignal,
//	); err != nil {
//		s.deleteSaga.opSaga.e.logger.Error(itemInfoData.Ctx, "error on trigger action itemInfoData",
//			"item_info_data", itemInfoData.DBMetaInfo, "error", err)
//		s.indexRollback(itemInfoData.Ctx, itemInfoData)
//		itemInfoData.RespSignal <- err
//		return
//	}
//
//	deleteTemporaryRecordsFromDiskOnThreadRespSignal := make(chan error, 1)
//	itemInfoDataForActionDelTmpFiles := itemInfoData.
//		withRespChan(deleteTemporaryRecordsFromDiskOnThreadRespSignal)
//	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
//		ActionDelTmpFiles.Send(itemInfoDataForActionDelTmpFiles)
//	err = <-deleteTemporaryRecordsFromDiskOnThreadRespSignal
//	if err != nil {
//		s.deleteSaga.opSaga.e.logger.Error(itemInfoData.Ctx,
//			"error on trigger action itemInfoData delete temporary data",
//			"item_info_data", itemInfoData.DBMetaInfo, "error", err)
//		s.indexRollback(itemInfoData.Ctx, itemInfoData)
//	}
//
//	itemInfoData.RespSignal <- err
//}
//
//func (s *deleteIdxOnlySaga) indexRollback(
//	_ context.Context, itemInfoData *deleteItemInfoData,
//) {
//	recreateIndexItemOnDiskRespSignal := make(chan error, 1)
//	itemInfoDataForRollbackIndexItemChannel := itemInfoData.
//		withRespChan(recreateIndexItemOnDiskRespSignal)
//	rollbackIndexListItemOnDiskRespSignal := make(chan error, 1)
//	itemInfoDataForRollbackIndexListItemChannel := itemInfoData.
//		withRespChan(rollbackIndexListItemOnDiskRespSignal)
//
//	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
//		RollbackIndexItemChannel.Send(itemInfoDataForRollbackIndexItemChannel)
//	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
//		RollbackIndexListItemChannel.Send(itemInfoDataForRollbackIndexListItemChannel)
//
//	if err := ResponseAccumulator(
//		recreateIndexItemOnDiskRespSignal,
//		rollbackIndexListItemOnDiskRespSignal,
//	); err != nil {
//		s.deleteSaga.opSaga.e.logger.Error(itemInfoData.Ctx,
//			"error rolling back trigger for itemInfoData",
//			"item_info_data", itemInfoData.DBMetaInfo, "error", err)
//	}
//}
//
//func (s *deleteIdxOnlySaga) deleteIndexItemFromDiskOnThread(ctx context.Context) {
//	loop.RunFromChannel(ctx,
//		s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.ActionIndexItemChannel.Ch,
//		func(itemInfoData *deleteItemInfoData) {
//			for _, indexKey := range itemInfoData.Opts.IndexingKeys {
//				strItemKey := hex.EncodeToString(indexKey)
//				lockStrKey := itemInfoData.DBMetaInfo.IndexInfo().LockName(strItemKey)
//
//				fileStats, ok := s.deleteSaga.opSaga.e.itemFileMapping.Get(lockStrKey)
//				if ok {
//					if !osx.IsFileClosed(fileStats.File) {
//						_ = fileStats.File.Close()
//					}
//				}
//
//				if err := osx.MvFile(itemInfoData.Ctx,
//					ltngdbenginemodelsv3.GetIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
//					ltngdbenginemodelsv3.GetTemporaryIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
//				); err != nil {
//					// TODO: log
//					itemInfoData.RespSignal <- err
//					//close(itemInfoData.RespSignal)
//					continue
//				}
//
//				s.deleteSaga.opSaga.e.itemFileMapping.Delete(lockStrKey)
//			}
//
//			itemInfoData.RespSignal <- nil
//			//close(itemInfoData.RespSignal)
//		},
//	)
//}
//
//func (s *deleteIdxOnlySaga) recreateIndexItemOnDiskOnThread(ctx context.Context) {
//	loop.RunFromChannel(ctx,
//		s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.RollbackIndexItemChannel.Ch,
//		func(itemInfoData *deleteItemInfoData) {
//			for _, item := range itemInfoData.IndexList {
//				strItemKey := hex.EncodeToString(item.Value)
//
//				if err := osx.MvFile(itemInfoData.Ctx,
//					ltngdbenginemodelsv3.GetTemporaryIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
//					ltngdbenginemodelsv3.GetIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
//				); err != nil {
//					// TODO: log
//					itemInfoData.RespSignal <- err
//					continue
//				}
//			}
//
//			itemInfoData.RespSignal <- nil
//			//close(itemInfoData.RespSignal)
//		},
//	)
//}
//
//func (s *deleteIdxOnlySaga) updateIndexingListItemFromDiskOnThread(ctx context.Context) {
//	loop.RunFromChannel(ctx,
//		s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.ActionIndexListItemChannel.Ch,
//		func(itemInfoData *deleteItemInfoData) {
//			var newIndexList [][]byte
//			for _, item := range itemInfoData.IndexList {
//				// TODO: convert the list to a map search
//				addToIndexingList := true
//				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
//					if bytes.Equal(item.Value, indexKey) {
//						addToIndexingList = false
//					}
//				}
//
//				if !addToIndexingList {
//					continue
//				}
//				newIndexList = append(newIndexList, item.Value)
//			}
//
//			indexListBs := bytes.Join(newIndexList, []byte(ltngdbenginemodelsv3.BsSep))
//			fileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo.IndexListInfo(),
//				&ltngdbenginemodelsv3.Item{
//					Key:   itemInfoData.Opts.ParentKey,
//					Value: indexListBs,
//				})
//
//			itemInfoData.RespSignal <- s.deleteSaga.opSaga.e.upsertItemOnDisk(
//				ctx, itemInfoData.DBMetaInfo, fileData)
//			close(itemInfoData.RespSignal)
//		},
//	)
//}
//
//func (s *deleteIdxOnlySaga) rollbackIndexListItemOnDiskOnThread(ctx context.Context) {
//	loop.RunFromChannel(ctx,
//		s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.RollbackIndexListItemChannel.Ch,
//		func(itemInfoData *deleteItemInfoData) {
//			var indexList [][]byte
//			for _, item := range itemInfoData.IndexList {
//				indexList = append(indexList, item.Value)
//			}
//
//			//strItemKey := hex.EncodeToString(itemInfoData.Opts.ParentKey)
//			//filePath := GetIndexedDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey)
//			//tmpFilePath := GetTemporaryIndexedListDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey)
//			indexListBs := bytes.Join(indexList, []byte(ltngdbenginemodelsv3.BsSep))
//			fileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo.IndexListInfo(),
//				&ltngdbenginemodelsv3.Item{
//					Key:   itemInfoData.Opts.ParentKey,
//					Value: indexListBs,
//				})
//
//			itemInfoData.RespSignal <- s.deleteSaga.opSaga.e.upsertItemOnDisk(
//				ctx, itemInfoData.DBMetaInfo, fileData)
//			close(itemInfoData.RespSignal)
//		},
//	)
//}
//
//func (s *deleteIdxOnlySaga) deleteTemporaryRecords(ctx context.Context) {
//	loop.RunFromChannel(ctx,
//		s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.ActionDelTmpFiles.Ch,
//		func(itemInfoData *deleteItemInfoData) {
//			if err := osx.CleanupDirs(ctx,
//				ltngdbenginemodelsv3.GetIndexedDataPath(itemInfoData.DBMetaInfo.Path),
//			); err != nil {
//				itemInfoData.RespSignal <- errorsx.Wrap(err, "error deleting tmp files")
//				//close(itemInfoData.RespSignal)
//				return
//			}
//
//			for _, item := range itemInfoData.IndexList {
//				strItemKey := hex.EncodeToString(item.Key)
//				lockKey := itemInfoData.DBMetaInfo.IndexInfo().LockName(strItemKey)
//				s.deleteSaga.opSaga.e.markedAsDeletedMapping.Delete(lockKey)
//			}
//
//			itemInfoData.RespSignal <- nil
//			//close(itemInfoData.RespSignal)
//		},
//	)
//}

// #####################################################################################################################
