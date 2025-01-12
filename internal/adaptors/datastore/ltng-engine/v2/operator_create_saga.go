package v2

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	off_thread "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/op/off-thread"
)

type createSaga struct {
	opSaga *opSaga
}

func newCreateSaga(ctx context.Context, opSaga *opSaga) *createSaga {
	cs := &createSaga{
		opSaga: opSaga,
	}

	go cs.createItemOnDiskOnThread(ctx)
	go cs.createIndexItemOnDiskOnThread(ctx)
	go cs.createIndexListItemOnDiskOnThread(ctx)

	go cs.createRelationalItemOnDiskOnThread(ctx)

	go cs.deleteItemOnDiskOnThread(ctx)
	go cs.deleteIndexItemFromDiskOnThread(ctx)
	go cs.deleteIndexListItemFromDiskOnThread(ctx)

	go cs.ListenAndTrigger(ctx)

	return cs
}

func (s *createSaga) ListenAndTrigger(ctx context.Context) {
	for itemInfoData := range s.opSaga.crudChannels.CreateChannels.InfoChannel {
		if !itemInfoData.Opts.HasIdx {
			s.noIndexTrigger(ctx, itemInfoData)
		} else {
			s.indexTrigger(ctx, itemInfoData)
		}
	}
}

func (s *createSaga) noIndexTrigger(
	_ context.Context, itemInfoData *ltngenginemodels.ItemInfoData,
) {
	createItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateItemOnDisk := itemInfoData.WithRespChan(createItemOnDiskRespSignal)
	s.opSaga.crudChannels.CreateChannels.ActionItemChannel <- itemInfoDataForCreateItemOnDisk
	err := <-createItemOnDiskRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		itemInfoData.RespSignal <- err
		return
	}

	createRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateRelationalItemOnDisk := itemInfoData.WithRespChan(createRelationalItemOnDiskRespSignal)
	s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk
	err = <-createRelationalItemOnDiskRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
	}
	itemInfoData.RespSignal <- err
}

func (s *createSaga) indexTrigger(
	_ context.Context, itemInfoData *ltngenginemodels.ItemInfoData,
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
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		return
	}

	createRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateRelationalItemOnDisk := itemInfoData.WithRespChan(createRelationalItemOnDiskRespSignal)
	s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk
	err := <-createRelationalItemOnDiskRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
	}
	itemInfoData.RespSignal <- err
}

func (s *createSaga) RollbackTrigger(ctx context.Context, itemInfoData *ltngenginemodels.ItemInfoData) {
	if !itemInfoData.Opts.HasIdx {
		s.noIndexRollback(ctx, itemInfoData)
		return
	}

	s.indexRollback(ctx, itemInfoData)
}

func (s *createSaga) noIndexRollback(
	_ context.Context, itemInfoData *ltngenginemodels.ItemInfoData,
) {
	deleteItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteItemOnDisk := itemInfoData.WithRespChan(deleteItemOnDiskRespSignal)
	s.opSaga.crudChannels.CreateChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	err := <-deleteItemOnDiskRespSignal
	if err != nil {
		log.Printf("error rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}
	//itemInfoData.RespSignal <- err
}

func (s *createSaga) indexRollback(
	_ context.Context, itemInfoData *ltngenginemodels.ItemInfoData,
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
		log.Printf("error rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
		//itemInfoData.RespSignal <- err
		return
	}

	//itemInfoData.RespSignal <- nil
}

// createItemOnDiskOnThread stands for createItemOnDisk on thread.
func (s *createSaga) createItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.ActionItemChannel {
		err := s.opSaga.e.createItemOnDisk(v.Ctx, v.DBMetaInfo, v.Item)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

func (s *createSaga) deleteItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.RollbackItemChannel {
		strItemKey := hex.EncodeToString(v.Item.Key)
		filePath := ltngenginemodels.GetDataFilepath(v.DBMetaInfo.Path, strItemKey)
		err := os.Remove(filePath)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

func (s *createSaga) createIndexItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.ActionIndexItemChannel {
		op := off_thread.New("createIndexItemOnDisk")
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
	}
}

func (s *createSaga) deleteIndexItemFromDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.RollbackIndexItemChannel {
		var errAcc error
		for _, indexKey := range v.Opts.IndexingKeys {
			strItemKey := hex.EncodeToString(indexKey)
			filePath := ltngenginemodels.GetDataFilepath(v.DBMetaInfo.IndexInfo().Path, strItemKey)
			if err := os.Remove(filePath); err != nil {
				if errAcc == nil {
					errAcc = err
				} else {
					err = fmt.Errorf("%s: %w", errAcc, err)
					errAcc = fmt.Errorf("error deleting item on database: %w", err)
				}
			}
		}
		v.RespSignal <- errAcc
		close(v.RespSignal)
	}
}

func (s *createSaga) createIndexListItemOnDiskOnThread(
	ctx context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.ActionIndexListItemChannel {
		err := s.opSaga.e.createItemOnDisk(ctx,
			v.DBMetaInfo.IndexListInfo(),
			&ltngenginemodels.Item{
				Key:   v.Opts.ParentKey,
				Value: bytes.Join(v.Opts.IndexingKeys, []byte(ltngenginemodels.BytesSep)),
			},
		)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

func (s *createSaga) deleteIndexListItemFromDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.RollbackIndexListItemChannel {
		strItemKey := hex.EncodeToString(v.Item.Key)
		filePath := ltngenginemodels.GetDataFilepath(v.DBMetaInfo.IndexListInfo().Path, strItemKey)
		err := os.Remove(filePath)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

func (s *createSaga) createRelationalItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel {
		err := s.opSaga.e.createRelationalItemOnDisk(v.Ctx, v.DBMetaInfo, v.Item)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}
