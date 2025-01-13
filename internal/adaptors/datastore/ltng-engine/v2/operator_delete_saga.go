package v2

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/execx"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
)

type (
	deletionChannels struct {
		QueueChannel                  chan struct{}
		InfoChannel                   chan *deleteItemInfoData
		ActionItemChannel             chan *deleteItemInfoData
		RollbackItemChannel           chan *deleteItemInfoData
		ActionIndexItemChannel        chan *deleteItemInfoData
		RollbackIndexItemChannel      chan *deleteItemInfoData
		ActionIndexListItemChannel    chan *deleteItemInfoData
		RollbackIndexListItemChannel  chan *deleteItemInfoData
		ActionRelationalItemChannel   chan *deleteItemInfoData
		RollbackRelationalItemChannel chan *deleteItemInfoData

		ActionDelTmpFiles   chan *deleteItemInfoData
		RollbackDelTmpFiles chan *deleteItemInfoData
	}

	deleteChannels struct {
		deleteCascadeChannel   *deletionChannels
		deleteCascadeByIndex   *deletionChannels
		deleteIndexOnlyChannel *deletionChannels
	}

	temporaryDeletionPaths struct {
		tmpDelPath           string
		indexTmpDelPath      string
		indexListTmpDelPath  string
		relationalTmpDelPath string
	}

	deleteItemInfoData struct {
		*ltngenginemodels.ItemInfoData
		IndexList   []*ltngenginemodels.Item
		TmpDelPaths *temporaryDeletionPaths
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
		QueueChannel: make(chan struct{}, ltngenginemodels.ChannelLimit),
		InfoChannel:  make(chan *deleteItemInfoData, ltngenginemodels.ChannelLimit),

		ActionItemChannel:             make(chan *deleteItemInfoData, ltngenginemodels.ChannelLimit),
		RollbackItemChannel:           make(chan *deleteItemInfoData, ltngenginemodels.ChannelLimit),
		ActionIndexItemChannel:        make(chan *deleteItemInfoData, ltngenginemodels.ChannelLimit),
		RollbackIndexItemChannel:      make(chan *deleteItemInfoData, ltngenginemodels.ChannelLimit),
		ActionIndexListItemChannel:    make(chan *deleteItemInfoData, ltngenginemodels.ChannelLimit),
		RollbackIndexListItemChannel:  make(chan *deleteItemInfoData, ltngenginemodels.ChannelLimit),
		ActionRelationalItemChannel:   make(chan *deleteItemInfoData, ltngenginemodels.ChannelLimit),
		RollbackRelationalItemChannel: make(chan *deleteItemInfoData, ltngenginemodels.ChannelLimit),

		ActionDelTmpFiles:   make(chan *deleteItemInfoData, ltngenginemodels.ChannelLimit),
		RollbackDelTmpFiles: make(chan *deleteItemInfoData, ltngenginemodels.ChannelLimit),
	}
}

func (i *deleteItemInfoData) withRespChan(sigChan chan error) *deleteItemInfoData {
	return &deleteItemInfoData{
		ItemInfoData: &ltngenginemodels.ItemInfoData{
			Ctx:        i.Ctx,
			DBMetaInfo: i.DBMetaInfo,
			Item:       i.Item,
			Opts:       i.Opts,
			RespSignal: sigChan,
		},
		IndexList:   i.IndexList,
		TmpDelPaths: i.TmpDelPaths,
	}
}

// #####################################################################################################################

type (
	deleteSaga struct {
		opSaga         *opSaga
		deleteChannels *deleteChannels
	}
)

func newDeleteSaga(ctx context.Context, opSaga *opSaga) *deleteSaga {
	ds := &deleteSaga{
		opSaga:         opSaga,
		deleteChannels: makeDeleteChannels(),
	}

	newDeleteCascadeSaga(ctx, ds)
	newDeleteCascadeByIdxSaga(ctx, ds)
	newDeleteIdxOnlySaga(ctx, ds)

	go ds.ListenAndTrigger(ctx)

	return ds
}

func (s *deleteSaga) ListenAndTrigger(_ context.Context) {
	for itemInfoData := range s.opSaga.crudChannels.DeleteChannels.InfoChannel {
		switch itemInfoData.Opts.IndexProperties.IndexDeletionBehaviour {
		case ltngenginemodels.IndexOnly:
			s.deleteChannels.deleteIndexOnlyChannel.InfoChannel <- &deleteItemInfoData{ItemInfoData: itemInfoData}
		case ltngenginemodels.CascadeByIdx:
			s.deleteChannels.deleteCascadeByIndex.InfoChannel <- &deleteItemInfoData{ItemInfoData: itemInfoData}
		case ltngenginemodels.Cascade:
			s.deleteChannels.deleteCascadeChannel.InfoChannel <- &deleteItemInfoData{ItemInfoData: itemInfoData}
		case ltngenginemodels.None:
			fallthrough
		default:
			itemInfoData.RespSignal <- fmt.Errorf("invalid index deletion behaviour")
		}
	}
}

func (s *deleteSaga) createTmpDeletionPaths(
	_ context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
) (*temporaryDeletionPaths, error) {
	tmpDelPath := ltngenginemodels.GetTmpDelDataPathWithSep(dbMetaInfo.Path)
	if err := os.MkdirAll(tmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %v", err)
	}

	indexTmpDelPath := ltngenginemodels.GetTmpDelDataPathWithSep(dbMetaInfo.IndexInfo().Path)
	if err := os.MkdirAll(indexTmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %v", err)
	}

	indexListTmpDelPath := ltngenginemodels.GetTmpDelDataPathWithSep(dbMetaInfo.IndexListInfo().Path)
	if err := os.MkdirAll(indexListTmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %v", err)
	}

	relationalTmpDelPath := ltngenginemodels.GetTmpDelDataPathWithSep(dbMetaInfo.RelationalInfo().Path)
	if err := os.MkdirAll(relationalTmpDelPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating tmp delete store item directory: %v", err)
	}

	return &temporaryDeletionPaths{
		tmpDelPath:           tmpDelPath,
		indexTmpDelPath:      indexTmpDelPath,
		indexListTmpDelPath:  indexListTmpDelPath,
		relationalTmpDelPath: relationalTmpDelPath,
	}, nil
}

// #####################################################################################################################

type deleteCascadeSaga struct {
	deleteSaga *deleteSaga
}

func newDeleteCascadeSaga(ctx context.Context, deleteSaga *deleteSaga) *deleteCascadeSaga {
	dcs := &deleteCascadeSaga{
		deleteSaga: deleteSaga,
	}

	go dcs.deleteItemFromDiskOnThread(ctx)
	go dcs.deleteIndexItemFromDiskOnThread(ctx)
	go dcs.deleteIndexingListItemFromDiskOnThread(ctx)

	go dcs.deleteRelationalItemFromDiskOnThread(ctx)
	go dcs.deleteTemporaryRecords(ctx)

	go dcs.recreateItemOnDiskOnThread(ctx)
	go dcs.recreateIndexItemOnDiskOnThread(ctx)
	go dcs.recreateIndexListItemOnDiskOnThread(ctx)

	go dcs.ListenAndTrigger(ctx)

	return dcs
}

func (s *deleteCascadeSaga) ListenAndTrigger(ctx context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeChannel.InfoChannel {
		strItemKey := hex.EncodeToString(itemInfoData.Item.Key)
		if _, err := s.deleteSaga.opSaga.e.memoryStore.LoadItem(ctx,
			itemInfoData.DBMetaInfo, itemInfoData.Item, itemInfoData.Opts,
		); err != nil {
			if _, err = os.Stat(ltngenginemodels.GetDataFilepath(
				itemInfoData.DBMetaInfo.Path, strItemKey),
			); os.IsNotExist(err) {
				// TODO: log fmt.Errorf("file does not exist: %s: %v", itemInfoData.DBMetaInfo.Path, err)
				itemInfoData.RespSignal <- err
				close(itemInfoData.RespSignal)
				continue
			}
		}

		temporaryDelPaths, err := s.deleteSaga.createTmpDeletionPaths(itemInfoData.Ctx, itemInfoData.DBMetaInfo)
		if err != nil {
			itemInfoData.RespSignal <- err
			close(itemInfoData.RespSignal)
			continue
		}
		itemInfoData.TmpDelPaths = temporaryDelPaths

		if !itemInfoData.Opts.HasIdx {
			s.noIndexTrigger(ctx, itemInfoData)
		} else {
			s.indexTrigger(ctx, itemInfoData)
		}
	}
}

func (s *deleteCascadeSaga) noIndexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	deleteItemFromDiskRespSignal := make(chan error, 1)
	itemInfoDataActionItemChannel := itemInfoData.
		withRespChan(deleteItemFromDiskRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionItemChannel <- itemInfoDataActionItemChannel
	err := <-deleteItemFromDiskRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	deleteRelationalItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionRelationalItemChannel := itemInfoData.
		withRespChan(deleteRelationalItemFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionRelationalItemChannel <- itemInfoDataForActionRelationalItemChannel
	err = <-deleteRelationalItemFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	deleteTemporaryRecordsFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionDelTmpFiles := itemInfoData.
		withRespChan(deleteTemporaryRecordsFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionDelTmpFiles <- itemInfoDataForActionDelTmpFiles
	err = <-deleteTemporaryRecordsFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData delete temporary data: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	itemInfoData.RespSignal <- nil
	close(itemInfoData.RespSignal)
}

func (s *deleteCascadeSaga) indexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	indexItemList, err := s.deleteSaga.opSaga.e.loadIndexingList(
		itemInfoData.Ctx,
		itemInfoData.DBMetaInfo,
		&ltngenginemodels.IndexOpts{ParentKey: itemInfoData.Item.Key},
	)
	if err != nil {
		// TODO: log
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}
	itemInfoData.IndexList = indexItemList

	deleteItemFromDiskRespSignal := make(chan error, 1)
	itemInfoDataActionItemChannel := itemInfoData.
		withRespChan(deleteItemFromDiskRespSignal)
	deleteIndexItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionIndexItemChannel := itemInfoData.
		withRespChan(deleteIndexItemFromDiskOnThreadRespSignal)
	deleteIndexingListItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionIndexListItemChannel := itemInfoData.
		withRespChan(deleteIndexingListItemFromDiskOnThreadRespSignal)

	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionItemChannel <- itemInfoDataActionItemChannel
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionIndexItemChannel <- itemInfoDataForActionIndexItemChannel
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionIndexListItemChannel <- itemInfoDataForActionIndexListItemChannel

	if err = ResponseAccumulator(
		deleteItemFromDiskRespSignal,
		deleteIndexItemFromDiskOnThreadRespSignal,
		deleteIndexingListItemFromDiskOnThreadRespSignal,
	); err != nil {
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	deleteRelationalItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionRelationalItemChannel := itemInfoData.
		withRespChan(deleteRelationalItemFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionRelationalItemChannel <- itemInfoDataForActionRelationalItemChannel
	err = <-deleteRelationalItemFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	deleteTemporaryRecordsFromDiskOnThreadRespSignal := make(chan error, 1)
	defer close(deleteTemporaryRecordsFromDiskOnThreadRespSignal)
	itemInfoDataForActionDelTmpFiles := itemInfoData.
		withRespChan(deleteTemporaryRecordsFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionDelTmpFiles <- itemInfoDataForActionDelTmpFiles
	err = <-deleteTemporaryRecordsFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData delete temporary data: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	itemInfoData.RespSignal <- err
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
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	recreateItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackItemChannel := itemInfoData.withRespChan(recreateItemOnDiskRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.RollbackItemChannel <- itemInfoDataForRollbackItemChannel
	err := <-recreateItemOnDiskRespSignal
	if err != nil {
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}

	deleteTemporaryRecordsFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionDelTmpFiles := itemInfoData.
		withRespChan(deleteTemporaryRecordsFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionDelTmpFiles <- itemInfoDataForActionDelTmpFiles
	err = <-deleteTemporaryRecordsFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData delete temporary data: %v: %v\n", itemInfoData, err)
	}
}

func (s *deleteCascadeSaga) indexRollback(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	recreateItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackItemChannel := itemInfoData.
		withRespChan(recreateItemOnDiskRespSignal)
	recreateIndexItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackIndexItemChannel := itemInfoData.
		withRespChan(recreateIndexItemOnDiskRespSignal)
	recreateIndexListItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackIndexListItemChannel := itemInfoData.
		withRespChan(recreateIndexListItemOnDiskRespSignal)

	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		RollbackItemChannel <- itemInfoDataForRollbackItemChannel
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		RollbackIndexItemChannel <- itemInfoDataForRollbackIndexItemChannel
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		RollbackIndexListItemChannel <- itemInfoDataForRollbackIndexListItemChannel

	if err := ResponseAccumulator(
		recreateItemOnDiskRespSignal,
		recreateIndexItemOnDiskRespSignal,
		recreateIndexListItemOnDiskRespSignal,
	); err != nil {
		log.Printf("error rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}

	deleteTemporaryRecordsFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionDelTmpFiles := itemInfoData.
		withRespChan(deleteTemporaryRecordsFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeChannel.
		ActionDelTmpFiles <- itemInfoDataForActionDelTmpFiles
	err := <-deleteTemporaryRecordsFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData delete temporary data: %v: %v\n", itemInfoData, err)
	}
}

func (s *deleteCascadeSaga) deleteItemFromDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeChannel.ActionItemChannel {
		strItemKey := hex.EncodeToString(itemInfoData.Item.Key)
		lockStrKey := itemInfoData.DBMetaInfo.LockName(strItemKey)
		if _, err := execx.MvFileExec(itemInfoData.Ctx,
			ltngenginemodels.GetDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey),
			ltngenginemodels.RawPathWithSepForFile(itemInfoData.TmpDelPaths.tmpDelPath, strItemKey),
		); err != nil {
			// log
			itemInfoData.RespSignal <- err
			close(itemInfoData.RespSignal)
			continue
		}

		fileStats, ok := s.deleteSaga.opSaga.e.itemFileMapping.Get(lockStrKey)
		if ok {
			if !rw.IsFileClosed(fileStats.File) {
				_ = fileStats.File.Close()
			}
		}
		s.deleteSaga.opSaga.e.itemFileMapping.Delete(lockStrKey)

		itemInfoData.RespSignal <- nil
		close(itemInfoData.RespSignal)
	}
}

func (s *deleteCascadeSaga) recreateItemOnDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeChannel.RollbackItemChannel {
		strItemKey := hex.EncodeToString(itemInfoData.Item.Key)
		filePath := ltngenginemodels.GetDataFilepath(itemInfoData.DBMetaInfo.Path, strItemKey)

		if _, err := execx.MvFileExec(itemInfoData.Ctx, ltngenginemodels.RawPathWithSepForFile(
			itemInfoData.TmpDelPaths.tmpDelPath, strItemKey), filePath,
		); err != nil {
			// log
			itemInfoData.RespSignal <- err
			close(itemInfoData.RespSignal)
			continue
		}

		itemInfoData.RespSignal <- nil
		close(itemInfoData.RespSignal)
	}
}

func (s *deleteCascadeSaga) deleteIndexItemFromDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeChannel.ActionIndexItemChannel {
		for _, item := range itemInfoData.IndexList {
			strItemKey := hex.EncodeToString(item.Value)
			lockStrKey := itemInfoData.DBMetaInfo.IndexInfo().LockName(strItemKey)

			fileStats, ok := s.deleteSaga.opSaga.e.itemFileMapping.Get(lockStrKey)
			if ok {
				if !rw.IsFileClosed(fileStats.File) {
					_ = fileStats.File.Close()
				}
			}

			if _, err := execx.MvFileExec(itemInfoData.Ctx,
				ltngenginemodels.GetDataFilepath(itemInfoData.DBMetaInfo.IndexInfo().Path, strItemKey),
				ltngenginemodels.RawPathWithSepForFile(itemInfoData.TmpDelPaths.indexTmpDelPath, strItemKey),
			); err != nil {
				// TODO: log
				itemInfoData.RespSignal <- err
				continue
			}

			s.deleteSaga.opSaga.e.itemFileMapping.Delete(lockStrKey)
		}

		itemInfoData.RespSignal <- nil
		close(itemInfoData.RespSignal)
	}
}

func (s *deleteCascadeSaga) recreateIndexItemOnDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeChannel.RollbackIndexItemChannel {
		for _, item := range itemInfoData.IndexList {
			strItemKey := hex.EncodeToString(item.Value)

			if _, err := execx.MvFileExec(itemInfoData.Ctx,
				ltngenginemodels.RawPathWithSepForFile(itemInfoData.TmpDelPaths.indexTmpDelPath, strItemKey),
				ltngenginemodels.GetDataFilepath(itemInfoData.DBMetaInfo.IndexInfo().Path, strItemKey),
			); err != nil {
				// TODO: log
				itemInfoData.RespSignal <- err
				continue
			}
		}

		itemInfoData.RespSignal <- nil
		close(itemInfoData.RespSignal)
	}
}

func (s *deleteCascadeSaga) deleteIndexingListItemFromDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeChannel.ActionIndexListItemChannel {
		strItemKey := hex.EncodeToString(itemInfoData.Item.Key)
		if _, err := execx.MvFileExec(itemInfoData.Ctx,
			ltngenginemodels.GetDataFilepath(itemInfoData.DBMetaInfo.IndexListInfo().Path, strItemKey),
			ltngenginemodels.RawPathWithSepForFile(itemInfoData.TmpDelPaths.indexListTmpDelPath, strItemKey),
		); err != nil {
			// TODO: log
			itemInfoData.RespSignal <- err
			close(itemInfoData.RespSignal)
			continue
		}

		fileStats, ok := s.deleteSaga.opSaga.e.
			itemFileMapping.Get(itemInfoData.DBMetaInfo.IndexListInfo().LockName(strItemKey))
		if ok {
			// TODO:  isFileClosed?
			_ = fileStats.File.Close()
		}
		s.deleteSaga.opSaga.e.itemFileMapping.Delete(itemInfoData.DBMetaInfo.IndexListInfo().LockName(strItemKey))

		itemInfoData.RespSignal <- nil
		close(itemInfoData.RespSignal)
	}
}

func (s *deleteCascadeSaga) recreateIndexListItemOnDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeChannel.RollbackIndexListItemChannel {
		strItemKey := hex.EncodeToString(itemInfoData.Item.Key)
		if _, err := execx.MvFileExec(itemInfoData.Ctx,
			ltngenginemodels.GetDataFilepath(itemInfoData.DBMetaInfo.IndexListInfo().Path, strItemKey),
			ltngenginemodels.RawPathWithSepForFile(itemInfoData.TmpDelPaths.indexListTmpDelPath, strItemKey),
		); err != nil {
			// TODO: log
			itemInfoData.RespSignal <- err
			close(itemInfoData.RespSignal)
			continue
		}

		itemInfoData.RespSignal <- nil
		close(itemInfoData.RespSignal)
	}
}

func (s *deleteCascadeSaga) deleteRelationalItemFromDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeChannel.ActionRelationalItemChannel {
		err := s.deleteSaga.opSaga.e.deleteRelationalData(
			itemInfoData.Ctx, itemInfoData.DBMetaInfo, itemInfoData.Item, itemInfoData.TmpDelPaths)
		if err != nil {
			log.Println("deleteRelationalItemFromDiskOnThread:", err)
			itemInfoData.RespSignal <- err
			close(itemInfoData.RespSignal)
			continue
		}

		itemInfoData.RespSignal <- nil
		close(itemInfoData.RespSignal)
	}
}

func (s *deleteCascadeSaga) deleteTemporaryRecords(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeChannel.ActionDelTmpFiles {
		_, err := execx.DelDirsWithoutSepBothOSExec(itemInfoData.Ctx, itemInfoData.TmpDelPaths.tmpDelPath)
		if err != nil {
			itemInfoData.RespSignal <- err
			continue
		}

		itemInfoData.RespSignal <- nil
	}
}

// #####################################################################################################################

type deleteCascadeByIdxSaga struct {
	deleteSaga *deleteSaga
}

func newDeleteCascadeByIdxSaga(ctx context.Context, deleteSaga *deleteSaga) *deleteCascadeByIdxSaga {
	dcs := &deleteCascadeByIdxSaga{
		deleteSaga: deleteSaga,
	}

	go dcs.deleteItemFromDiskOnThread(ctx)
	go dcs.deleteIndexItemFromDiskOnThread(ctx)
	go dcs.deleteIndexingListItemFromDiskOnThread(ctx)

	go dcs.deleteRelationalItemFromDiskOnThread(ctx)
	go dcs.deleteTemporaryRecords(ctx)

	go dcs.recreateItemOnDiskOnThread(ctx)
	go dcs.recreateIndexItemOnDiskOnThread(ctx)
	go dcs.recreateIndexListItemOnDiskOnThread(ctx)

	go dcs.ListenAndTrigger(ctx)

	return dcs
}

func (s *deleteCascadeByIdxSaga) ListenAndTrigger(ctx context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeByIndex.InfoChannel {
		temporaryDelPaths, err := s.deleteSaga.createTmpDeletionPaths(itemInfoData.Ctx, itemInfoData.DBMetaInfo)
		if err != nil {
			itemInfoData.RespSignal <- err
			close(itemInfoData.RespSignal)
			continue
		}
		itemInfoData.TmpDelPaths = temporaryDelPaths

		if !itemInfoData.Opts.HasIdx {
			s.noIndexTrigger(ctx, itemInfoData)
		} else {
			s.indexTrigger(ctx, itemInfoData)
		}
	}
}

func (s *deleteCascadeByIdxSaga) noIndexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	deleteItemFromDiskRespSignal := make(chan error, 1)
	itemInfoDataActionItemChannel := itemInfoData.
		withRespChan(deleteItemFromDiskRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeByIndex.
		ActionItemChannel <- itemInfoDataActionItemChannel
	err := <-deleteItemFromDiskRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		itemInfoData.RespSignal <- err
		return
	}

	deleteRelationalItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionRelationalItemChannel := itemInfoData.
		withRespChan(deleteRelationalItemFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeByIndex.
		ActionRelationalItemChannel <- itemInfoDataForActionRelationalItemChannel
	err = <-deleteRelationalItemFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
	}

	deleteTemporaryRecordsFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionDelTmpFiles := itemInfoData.
		withRespChan(deleteTemporaryRecordsFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeByIndex.
		ActionDelTmpFiles <- itemInfoDataForActionDelTmpFiles
	err = <-deleteTemporaryRecordsFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData delete temporary data: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
	}

	itemInfoData.RespSignal <- err
}

func (s *deleteCascadeByIdxSaga) indexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	deleteItemFromDiskRespSignal := make(chan error, 1)
	itemInfoDataActionItemChannel := itemInfoData.
		withRespChan(deleteItemFromDiskRespSignal)
	deleteIndexItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionIndexItemChannel := itemInfoData.
		withRespChan(deleteIndexItemFromDiskOnThreadRespSignal)
	deleteIndexingListItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionIndexListItemChannel := itemInfoData.
		withRespChan(deleteIndexingListItemFromDiskOnThreadRespSignal)

	s.deleteSaga.deleteChannels.deleteCascadeByIndex.
		ActionItemChannel <- itemInfoDataActionItemChannel
	s.deleteSaga.deleteChannels.deleteCascadeByIndex.
		ActionIndexItemChannel <- itemInfoDataForActionIndexItemChannel
	s.deleteSaga.deleteChannels.deleteCascadeByIndex.
		ActionIndexListItemChannel <- itemInfoDataForActionIndexListItemChannel

	if err := ResponseAccumulator(
		deleteItemFromDiskRespSignal,
		deleteIndexItemFromDiskOnThreadRespSignal,
		deleteIndexingListItemFromDiskOnThreadRespSignal,
	); err != nil {
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		return
	}

	deleteRelationalItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionRelationalItemChannel := itemInfoData.
		withRespChan(deleteRelationalItemFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeByIndex.
		ActionRelationalItemChannel <- itemInfoDataForActionRelationalItemChannel
	err := <-deleteRelationalItemFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
	}

	deleteTemporaryRecordsFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionDelTmpFiles := itemInfoData.
		withRespChan(deleteTemporaryRecordsFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeByIndex.
		ActionDelTmpFiles <- itemInfoDataForActionDelTmpFiles
	err = <-deleteTemporaryRecordsFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData delete temporary data: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
	}

	itemInfoData.RespSignal <- err
}

func (s *deleteCascadeByIdxSaga) RollbackTrigger(ctx context.Context, itemInfoData *deleteItemInfoData) {
	if !itemInfoData.Opts.HasIdx {
		s.noIndexRollback(ctx, itemInfoData)
		return
	}

	s.indexRollback(ctx, itemInfoData)
}

func (s *deleteCascadeByIdxSaga) noIndexRollback(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	recreateItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackItemChannel := itemInfoData.withRespChan(recreateItemOnDiskRespSignal)
	s.deleteSaga.deleteChannels.deleteCascadeByIndex.RollbackItemChannel <- itemInfoDataForRollbackItemChannel
	err := <-recreateItemOnDiskRespSignal
	if err != nil {
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}
	//itemInfoData.RespSignal <- err
}

func (s *deleteCascadeByIdxSaga) indexRollback(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	recreateItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackItemChannel := itemInfoData.
		withRespChan(recreateItemOnDiskRespSignal)
	recreateIndexItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackIndexItemChannel := itemInfoData.
		withRespChan(recreateIndexItemOnDiskRespSignal)
	recreateIndexListItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackIndexListItemChannel := itemInfoData.
		withRespChan(recreateIndexListItemOnDiskRespSignal)

	s.deleteSaga.deleteChannels.deleteCascadeByIndex.
		RollbackItemChannel <- itemInfoDataForRollbackItemChannel
	s.deleteSaga.deleteChannels.deleteCascadeByIndex.
		RollbackIndexItemChannel <- itemInfoDataForRollbackIndexItemChannel
	s.deleteSaga.deleteChannels.deleteCascadeByIndex.
		RollbackIndexListItemChannel <- itemInfoDataForRollbackIndexListItemChannel

	if err := ResponseAccumulator(
		recreateItemOnDiskRespSignal,
		recreateIndexItemOnDiskRespSignal,
		recreateIndexListItemOnDiskRespSignal,
	); err != nil {
		log.Printf("error rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
		//itemInfoData.RespSignal <- err
		return
	}

	//itemInfoData.RespSignal <- nil
}

func (s *deleteCascadeByIdxSaga) deleteItemFromDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeByIndex.ActionItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteCascadeByIdxSaga) recreateItemOnDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeByIndex.RollbackItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteCascadeByIdxSaga) deleteIndexItemFromDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeByIndex.ActionIndexItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteCascadeByIdxSaga) recreateIndexItemOnDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeByIndex.RollbackIndexItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteCascadeByIdxSaga) deleteIndexingListItemFromDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeByIndex.ActionIndexListItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteCascadeByIdxSaga) recreateIndexListItemOnDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeByIndex.RollbackIndexListItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteCascadeByIdxSaga) deleteRelationalItemFromDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeByIndex.ActionRelationalItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteCascadeByIdxSaga) deleteTemporaryRecords(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeByIndex.ActionDelTmpFiles {
		itemInfoData.RespSignal <- nil
	}
}

// #####################################################################################################################

type deleteIdxOnlySaga struct {
	deleteSaga *deleteSaga
}

func newDeleteIdxOnlySaga(ctx context.Context, deleteSaga *deleteSaga) *deleteIdxOnlySaga {
	dcs := &deleteIdxOnlySaga{
		deleteSaga: deleteSaga,
	}

	go dcs.deleteItemFromDiskOnThread(ctx)
	go dcs.deleteIndexItemFromDiskOnThread(ctx)
	go dcs.deleteIndexingListItemFromDiskOnThread(ctx)

	go dcs.deleteRelationalItemFromDiskOnThread(ctx)
	go dcs.deleteTemporaryRecords(ctx)

	go dcs.recreateItemOnDiskOnThread(ctx)
	go dcs.recreateIndexItemOnDiskOnThread(ctx)
	go dcs.recreateIndexListItemOnDiskOnThread(ctx)

	go dcs.ListenAndTrigger(ctx)

	return dcs
}

func (s *deleteIdxOnlySaga) ListenAndTrigger(ctx context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.InfoChannel {
		temporaryDelPaths, err := s.deleteSaga.createTmpDeletionPaths(itemInfoData.Ctx, itemInfoData.DBMetaInfo)
		if err != nil {
			itemInfoData.RespSignal <- err
			close(itemInfoData.RespSignal)
			continue
		}
		itemInfoData.TmpDelPaths = temporaryDelPaths

		if !itemInfoData.Opts.HasIdx {
			s.noIndexTrigger(ctx, itemInfoData)
		} else {
			s.indexTrigger(ctx, itemInfoData)
		}
	}
}

func (s *deleteIdxOnlySaga) noIndexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	deleteItemFromDiskRespSignal := make(chan error, 1)
	itemInfoDataActionItemChannel := itemInfoData.
		withRespChan(deleteItemFromDiskRespSignal)
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		ActionItemChannel <- itemInfoDataActionItemChannel
	err := <-deleteItemFromDiskRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		itemInfoData.RespSignal <- err
		return
	}

	deleteRelationalItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionRelationalItemChannel := itemInfoData.
		withRespChan(deleteRelationalItemFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		ActionRelationalItemChannel <- itemInfoDataForActionRelationalItemChannel
	err = <-deleteRelationalItemFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
	}

	deleteTemporaryRecordsFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionDelTmpFiles := itemInfoData.
		withRespChan(deleteTemporaryRecordsFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		ActionDelTmpFiles <- itemInfoDataForActionDelTmpFiles
	err = <-deleteTemporaryRecordsFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData delete temporary data: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
	}

	itemInfoData.RespSignal <- err
}

func (s *deleteIdxOnlySaga) indexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	deleteItemFromDiskRespSignal := make(chan error, 1)
	itemInfoDataActionItemChannel := itemInfoData.
		withRespChan(deleteItemFromDiskRespSignal)
	deleteIndexItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionIndexItemChannel := itemInfoData.
		withRespChan(deleteIndexItemFromDiskOnThreadRespSignal)
	deleteIndexingListItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionIndexListItemChannel := itemInfoData.
		withRespChan(deleteIndexingListItemFromDiskOnThreadRespSignal)

	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		ActionItemChannel <- itemInfoDataActionItemChannel
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		ActionIndexItemChannel <- itemInfoDataForActionIndexItemChannel
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		ActionIndexListItemChannel <- itemInfoDataForActionIndexListItemChannel

	if err := ResponseAccumulator(
		deleteItemFromDiskRespSignal,
		deleteIndexItemFromDiskOnThreadRespSignal,
		deleteIndexingListItemFromDiskOnThreadRespSignal,
	); err != nil {
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		return
	}

	deleteRelationalItemFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionRelationalItemChannel := itemInfoData.
		withRespChan(deleteRelationalItemFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		ActionRelationalItemChannel <- itemInfoDataForActionRelationalItemChannel
	err := <-deleteRelationalItemFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
	}

	deleteTemporaryRecordsFromDiskOnThreadRespSignal := make(chan error, 1)
	itemInfoDataForActionDelTmpFiles := itemInfoData.
		withRespChan(deleteTemporaryRecordsFromDiskOnThreadRespSignal)
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		ActionDelTmpFiles <- itemInfoDataForActionDelTmpFiles
	err = <-deleteTemporaryRecordsFromDiskOnThreadRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData delete temporary data: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
	}

	itemInfoData.RespSignal <- err
}

func (s *deleteIdxOnlySaga) RollbackTrigger(ctx context.Context, itemInfoData *deleteItemInfoData) {
	if !itemInfoData.Opts.HasIdx {
		s.noIndexRollback(ctx, itemInfoData)
		return
	}

	s.indexRollback(ctx, itemInfoData)
}

func (s *deleteIdxOnlySaga) noIndexRollback(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	recreateItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackItemChannel := itemInfoData.withRespChan(recreateItemOnDiskRespSignal)
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.RollbackItemChannel <- itemInfoDataForRollbackItemChannel
	err := <-recreateItemOnDiskRespSignal
	if err != nil {
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}
	itemInfoData.RespSignal <- err
}

func (s *deleteIdxOnlySaga) indexRollback(
	_ context.Context, itemInfoData *deleteItemInfoData,
) {
	recreateItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackItemChannel := itemInfoData.
		withRespChan(recreateItemOnDiskRespSignal)
	recreateIndexItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackIndexItemChannel := itemInfoData.
		withRespChan(recreateIndexItemOnDiskRespSignal)
	recreateIndexListItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForRollbackIndexListItemChannel := itemInfoData.
		withRespChan(recreateIndexListItemOnDiskRespSignal)

	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		RollbackItemChannel <- itemInfoDataForRollbackItemChannel
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		RollbackIndexItemChannel <- itemInfoDataForRollbackIndexItemChannel
	s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.
		RollbackIndexListItemChannel <- itemInfoDataForRollbackIndexListItemChannel

	if err := ResponseAccumulator(
		recreateItemOnDiskRespSignal,
		recreateIndexItemOnDiskRespSignal,
		recreateIndexListItemOnDiskRespSignal,
	); err != nil {
		log.Printf("error rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
		itemInfoData.RespSignal <- err
		return
	}

	itemInfoData.RespSignal <- nil
}

func (s *deleteIdxOnlySaga) deleteItemFromDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.ActionItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteIdxOnlySaga) recreateItemOnDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.RollbackItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteIdxOnlySaga) deleteIndexItemFromDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.ActionIndexItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteIdxOnlySaga) recreateIndexItemOnDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.RollbackIndexItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteIdxOnlySaga) deleteIndexingListItemFromDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.ActionIndexListItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteIdxOnlySaga) recreateIndexListItemOnDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.RollbackIndexListItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteIdxOnlySaga) deleteRelationalItemFromDiskOnThread(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.ActionRelationalItemChannel {
		itemInfoData.RespSignal <- nil
	}
}

func (s *deleteIdxOnlySaga) deleteTemporaryRecords(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteIndexOnlyChannel.ActionDelTmpFiles {
		itemInfoData.RespSignal <- nil
	}
}

// #####################################################################################################################
