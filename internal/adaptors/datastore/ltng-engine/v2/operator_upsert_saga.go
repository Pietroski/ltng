package v2

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesop"
	off_thread "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/op/off-thread"
)

type upsertSaga struct {
	opSaga *opSaga
}

func newUpsertSaga(ctx context.Context, opSaga *opSaga) *upsertSaga {
	us := &upsertSaga{
		opSaga: opSaga,
	}

	go us.upsertItemOnDiskOnThread(ctx)
	go us.upsertIndexItemOnDiskOnThread(ctx)
	go us.upsertIndexListItemOnDiskOnThread(ctx)

	go us.upsertRelationalItemOnDiskOnThread(ctx)

	go us.deleteItemOnDiskOnThread(ctx)
	go us.deleteIndexItemFromDiskOnThread(ctx)
	go us.deleteIndexListItemFromDiskOnThread(ctx)

	go us.ListenAndTrigger(ctx)

	return us
}

func (s *upsertSaga) ListenAndTrigger(ctx context.Context) {
	for itemInfoData := range s.opSaga.crudChannels.UpsertChannels.InfoChannel {
		if !itemInfoData.Opts.HasIdx {
			s.noIndexTrigger(ctx, itemInfoData)
		} else {
			s.indexTrigger(ctx, itemInfoData)
		}
	}
}

func (s *upsertSaga) noIndexTrigger(
	_ context.Context, itemInfoData *ltngenginemodels.ItemInfoData,
) {
	createItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateItemOnDisk := itemInfoData.WithRespChan(createItemOnDiskRespSignal)
	s.opSaga.crudChannels.UpsertChannels.ActionItemChannel <- itemInfoDataForCreateItemOnDisk
	err := <-createItemOnDiskRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	createRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateRelationalItemOnDisk := itemInfoData.WithRespChan(createRelationalItemOnDiskRespSignal)
	s.opSaga.crudChannels.UpsertChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk
	err = <-createRelationalItemOnDiskRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	itemInfoData.RespSignal <- nil
	close(itemInfoData.RespSignal)
}

func (s *upsertSaga) indexTrigger(
	_ context.Context, itemInfoData *ltngenginemodels.ItemInfoData,
) {
	createItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateItemOnDisk := itemInfoData.WithRespChan(createItemOnDiskRespSignal)
	createIndexItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateIndexItemOnDisk := itemInfoData.WithRespChan(createIndexItemOnDiskRespSignal)
	createIndexItemListOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateIndexItemListOnDisk := itemInfoData.WithRespChan(createIndexItemListOnDiskRespSignal)

	s.opSaga.crudChannels.UpsertChannels.ActionItemChannel <- itemInfoDataForCreateItemOnDisk
	s.opSaga.crudChannels.UpsertChannels.ActionIndexItemChannel <- itemInfoDataForCreateIndexItemOnDisk
	s.opSaga.crudChannels.UpsertChannels.ActionIndexListItemChannel <- itemInfoDataForCreateIndexItemListOnDisk

	if err := ResponseAccumulator(
		createItemOnDiskRespSignal,
		createIndexItemOnDiskRespSignal,
		createIndexItemListOnDiskRespSignal,
	); err != nil {
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	createRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateRelationalItemOnDisk := itemInfoData.WithRespChan(createRelationalItemOnDiskRespSignal)
	s.opSaga.crudChannels.UpsertChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk
	err := <-createRelationalItemOnDiskRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)
		return
	}

	itemInfoData.RespSignal <- nil
	close(itemInfoData.RespSignal)
}

func (s *upsertSaga) RollbackTrigger(ctx context.Context, itemInfoData *ltngenginemodels.ItemInfoData) {
	if !itemInfoData.Opts.HasIdx {
		s.noIndexRollback(ctx, itemInfoData)
		return
	}

	s.indexRollback(ctx, itemInfoData)
}

func (s *upsertSaga) noIndexRollback(
	_ context.Context, itemInfoData *ltngenginemodels.ItemInfoData,
) {
	deleteItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteItemOnDisk := itemInfoData.WithRespChan(deleteItemOnDiskRespSignal)
	s.opSaga.crudChannels.UpsertChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	err := <-deleteItemOnDiskRespSignal
	if err != nil {
		log.Printf("error rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}
}

func (s *upsertSaga) indexRollback(
	_ context.Context, itemInfoData *ltngenginemodels.ItemInfoData,
) {
	deleteItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteItemOnDisk := itemInfoData.WithRespChan(deleteItemOnDiskRespSignal)
	deleteIndexItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteIndexItemOnDisk := itemInfoData.WithRespChan(deleteIndexItemOnDiskRespSignal)
	deleteIndexItemListOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteIndexItemListOnDisk := itemInfoData.WithRespChan(deleteIndexItemListOnDiskRespSignal)

	s.opSaga.crudChannels.UpsertChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	s.opSaga.crudChannels.UpsertChannels.RollbackIndexItemChannel <- itemInfoDataForDeleteIndexItemOnDisk
	s.opSaga.crudChannels.UpsertChannels.RollbackIndexListItemChannel <- itemInfoDataForDeleteIndexItemListOnDisk

	if err := ResponseAccumulator(
		deleteItemOnDiskRespSignal,
		deleteIndexItemOnDiskRespSignal,
		deleteIndexItemListOnDiskRespSignal,
	); err != nil {
		log.Printf("error rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}
}

// upsertItemOnDiskOnThread stands for upsertItemOnDisk on thread.
func (s *upsertSaga) upsertItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.UpsertChannels.ActionItemChannel {
		err := s.opSaga.e.upsertItemOnDisk(v.Ctx, v.DBMetaInfo, v.Item)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

func (s *upsertSaga) deleteItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.UpsertChannels.RollbackItemChannel {
		strItemKey := hex.EncodeToString(v.Item.Key)
		filePath := ltngenginemodels.GetDataFilepath(v.DBMetaInfo.Path, strItemKey)
		err := os.Remove(filePath)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

func (s *upsertSaga) upsertIndexItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.UpsertChannels.ActionIndexItemChannel {
		indexingList, err := s.opSaga.e.loadIndexingList(v.Ctx, v.DBMetaInfo, v.Opts)
		if err != nil {
			v.RespSignal <- err
			close(v.RespSignal)
			continue
		}

		op := off_thread.New("upsertIndexItemOnDisk")
		op.OpX(func() (any, error) {
			keysToSave := bytesop.CalRightDiff(
				ltngenginemodels.IndexListToBytesList(indexingList),
				v.Opts.IndexingKeys)

			for _, indexKey := range keysToSave {
				if err := s.opSaga.e.upsertItemOnDisk(v.Ctx,
					v.DBMetaInfo.IndexInfo(),
					&ltngenginemodels.Item{
						Key:   indexKey,
						Value: v.Opts.ParentKey,
					},
				); err != nil {
					return nil, err
				}
			}

			return nil, nil
		})
		op.OpX(func() (any, error) {
			keysToDelete := bytesop.CalRightDiff(
				v.Opts.IndexingKeys,
				ltngenginemodels.IndexListToBytesList(indexingList))

			var errAcc error
			for _, indexKey := range keysToDelete {
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

			return nil, nil
		})

		err = op.WaitAndWrapErr()
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

func (s *upsertSaga) deleteIndexItemFromDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.UpsertChannels.RollbackIndexItemChannel {
		indexingList, err := s.opSaga.e.loadIndexingList(v.Ctx, v.DBMetaInfo, v.Opts)
		if err != nil {
			v.RespSignal <- err
			close(v.RespSignal)
			continue
		}

		op := off_thread.New("upsertIndexItemOnDisk")
		op.OpX(func() (any, error) {
			keysToSave := bytesop.CalRightDiff(
				v.Opts.IndexingKeys,
				ltngenginemodels.IndexListToBytesList(indexingList))

			for _, indexKey := range keysToSave {
				if err := s.opSaga.e.upsertItemOnDisk(v.Ctx,
					v.DBMetaInfo.IndexInfo(),
					&ltngenginemodels.Item{
						Key:   indexKey,
						Value: v.Opts.ParentKey,
					},
				); err != nil {
					return nil, err
				}
			}

			return nil, nil
		})
		op.OpX(func() (any, error) {
			keysToDelete := bytesop.CalRightDiff(
				ltngenginemodels.IndexListToBytesList(indexingList),
				v.Opts.IndexingKeys)

			var errAcc error
			for _, indexKey := range keysToDelete {
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

			return nil, nil
		})

		err = op.WaitAndWrapErr()
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

func (s *upsertSaga) upsertIndexListItemOnDiskOnThread(
	ctx context.Context,
) {
	for v := range s.opSaga.crudChannels.UpsertChannels.ActionIndexListItemChannel {
		err := s.opSaga.e.upsertItemOnDisk(ctx,
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

func (s *upsertSaga) deleteIndexListItemFromDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.UpsertChannels.RollbackIndexListItemChannel {
		strItemKey := hex.EncodeToString(v.Item.Key)
		filePath := ltngenginemodels.GetDataFilepath(v.DBMetaInfo.IndexListInfo().Path, strItemKey)
		err := os.Remove(filePath)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

func (s *upsertSaga) upsertRelationalItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.UpsertChannels.ActionRelationalItemChannel {
		err := s.opSaga.e.upsertRelationalItemOnDisk(v.Ctx, v.DBMetaInfo, v.Item)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}
