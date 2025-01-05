package v2

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"

	filequeuev1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/file_queue/v1"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesop"
	off_thread "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/op/off-thread"
)

func ResponseAccumulator(respSigChan ...chan error) error {
	var err error
	for _, sigChan := range respSigChan {
		sigErr := <-sigChan
		if sigErr != nil {
			if err == nil {
				err = sigErr
			} else {
				err = fmt.Errorf("%s: %w", sigErr, err)
			}
		}
	}

	return err
}

// #####################################################################################################################

type opSaga struct {
	e            *LTNGEngine
	fq           *filequeuev1.FileQueue
	crudChannels *ltngenginemodels.CrudChannels
}

func newOpSaga(ctx context.Context, e *LTNGEngine) *opSaga {
	op := &opSaga{
		e:  e,
		fq: e.fq,
		crudChannels: &ltngenginemodels.CrudChannels{
			OpSagaChannel:  ltngenginemodels.MakeOpChannels(),
			CreateChannels: ltngenginemodels.MakeOpChannels(),
			UpsertChannels: ltngenginemodels.MakeOpChannels(),
			DeleteChannels: ltngenginemodels.MakeOpChannels(),
		},
	}

	go op.ListenAndTrigger(ctx)

	newCreateSaga(ctx, op)
	newUpsertSaga(ctx, op)

	return op
}

func (op *opSaga) ListenAndTrigger(ctx context.Context) {
	for _ = range op.crudChannels.OpSagaChannel.QueueChannel {
		bs, err := op.fq.ReadFromCursor(ctx)
		if err != nil {
			log.Printf("error reading item from file queue: %v\n", err)
		}

		var itemInfoData ltngenginemodels.ItemInfoData
		if err = op.e.serializer.Deserialize(bs, &itemInfoData); err != nil {
			log.Printf("error deserializing item info data from file queue: %v", err)
		}

		respSignalChan := make(chan error)
		itemInfoData.RespSignal = respSignalChan
		itemInfoData.Ctx = context.Background()

		switch itemInfoData.OpType {
		case ltngenginemodels.OpTypeCreate:
			op.crudChannels.CreateChannels.InfoChannel <- &itemInfoData
		case ltngenginemodels.OpTypeUpsert:
			op.crudChannels.UpsertChannels.InfoChannel <- &itemInfoData
		case ltngenginemodels.OpTypeDelete:
			op.crudChannels.DeleteChannels.InfoChannel <- &itemInfoData
		default:
			log.Printf("unknown op type: %v", itemInfoData.OpType)
			return
		}

		if err = ResponseAccumulator(respSignalChan); err != nil {
			log.Printf("error accumulating item info data from file queue: %v", err)
		}
	}
}

// #####################################################################################################################

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
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}
	itemInfoData.RespSignal <- err
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
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
		itemInfoData.RespSignal <- err
		return
	}

	itemInfoData.RespSignal <- nil
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

// #####################################################################################################################

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
		log.Printf("\nerror on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		itemInfoData.RespSignal <- err
		return
	}

	createRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateRelationalItemOnDisk := itemInfoData.WithRespChan(createRelationalItemOnDiskRespSignal)
	s.opSaga.crudChannels.UpsertChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk
	err = <-createRelationalItemOnDiskRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
	}
	itemInfoData.RespSignal <- err
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
		return
	}

	createRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateRelationalItemOnDisk := itemInfoData.WithRespChan(createRelationalItemOnDiskRespSignal)
	s.opSaga.crudChannels.UpsertChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk
	err := <-createRelationalItemOnDiskRespSignal
	if err != nil {
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)
	}
	itemInfoData.RespSignal <- err
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
	itemInfoData.RespSignal <- err
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
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
		itemInfoData.RespSignal <- err
		return
	}

	itemInfoData.RespSignal <- nil
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

// #####################################################################################################################

type (
	deleteChannels struct {
		deleteCascadeChannel   *ltngenginemodels.OpChannels
		deleteCascadeByIndex   *ltngenginemodels.OpChannels
		deleteIndexOnlyChannel *ltngenginemodels.OpChannels
	}

	temporaryDeletionPaths struct {
		tmpDelPath           string
		indexTmpDelPath      string
		indexListTmpDelPath  string
		relationalTmpDelPath string
	}

	deleteItemInfoData struct {
		*ltngenginemodels.ItemInfoData
		TmpDelPaths *temporaryDeletionPaths
	}
)

func makeDeleteChannels() *deleteChannels {
	return &deleteChannels{
		deleteCascadeByIndex:   ltngenginemodels.MakeOpChannels(),
		deleteIndexOnlyChannel: ltngenginemodels.MakeOpChannels(),
		deleteCascadeChannel:   ltngenginemodels.MakeOpChannels(),
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

	// TODO: init listeners(go functions)

	go ds.ListenAndTrigger(ctx)

	return ds
}

func (s *deleteSaga) ListenAndTrigger(_ context.Context) {
	for itemInfoData := range s.opSaga.crudChannels.DeleteChannels.InfoChannel {
		switch itemInfoData.Opts.IndexProperties.IndexDeletionBehaviour {
		case ltngenginemodels.IndexOnly:
			s.deleteChannels.deleteIndexOnlyChannel.InfoChannel <- itemInfoData
		case ltngenginemodels.CascadeByIdx:
			s.deleteChannels.deleteCascadeByIndex.InfoChannel <- itemInfoData
		case ltngenginemodels.Cascade:
			s.deleteChannels.deleteCascadeChannel.InfoChannel <- itemInfoData
		case ltngenginemodels.None:
			fallthrough
		default:
			itemInfoData.RespSignal <- fmt.Errorf("invalid index deletion behaviour")
			return
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

type deleteCascadeSaga struct {
	deleteSaga *deleteSaga
}

func newDeleteCascadeSaga(ctx context.Context, deleteSaga *deleteSaga) *deleteCascadeSaga {
	dcs := &deleteCascadeSaga{
		deleteSaga: deleteSaga,
	}

	go dcs.deleteMainKey(ctx)
	go dcs.deleteIndexes(ctx)
	go dcs.deleteIndexingList(ctx)
	go dcs.deleteRelationalItem(ctx)
	go dcs.deleteTemporaryRecords(ctx)

	// TODO: add rollbacks

	return dcs
}

func (s *deleteCascadeSaga) ListenAndTrigger(ctx context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeChannel.InfoChannel {
		temporaryDelPaths, err := s.deleteSaga.createTmpDeletionPaths(itemInfoData.Ctx, itemInfoData.DBMetaInfo)
		if err != nil {
			itemInfoData.RespSignal <- err
			close(itemInfoData.RespSignal)
			continue
		}

		if !itemInfoData.Opts.HasIdx {
			s.noIndexTrigger(ctx, &deleteItemInfoData{
				ItemInfoData: itemInfoData,
				TmpDelPaths:  temporaryDelPaths,
			})
		} else {
			s.indexTrigger(ctx, &deleteItemInfoData{
				ItemInfoData: itemInfoData,
				TmpDelPaths:  temporaryDelPaths,
			})
		}
	}
}

func (s *deleteCascadeSaga) noIndexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
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

func (s *deleteCascadeSaga) indexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
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
	deleteItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteItemOnDisk := itemInfoData.WithRespChan(deleteItemOnDiskRespSignal)
	s.opSaga.crudChannels.CreateChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	err := <-deleteItemOnDiskRespSignal
	if err != nil {
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}
	itemInfoData.RespSignal <- err
}

func (s *deleteCascadeSaga) indexRollback(
	_ context.Context, itemInfoData *deleteItemInfoData,
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
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
		itemInfoData.RespSignal <- err
		return
	}

	itemInfoData.RespSignal <- nil
}

func (s *deleteCascadeSaga) deleteMainKey(_ context.Context) {
	for itemInfoData := range s.deleteSaga.deleteChannels.deleteCascadeChannel.ActionItemChannel {
		_ = itemInfoData
	}
}

func (s *deleteCascadeSaga) deleteIndexes(_ context.Context) {
	//
}

func (s *deleteCascadeSaga) deleteIndexingList(_ context.Context) {
	//
}

func (s *deleteCascadeSaga) deleteRelationalItem(_ context.Context) {
	//
}

func (s *deleteCascadeSaga) deleteTemporaryRecords(_ context.Context) {
	//
}

type deleteCascadeByIdxSaga struct {
	deleteSaga *deleteSaga
}

func newDeleteCascadeByIdxSaga(deleteSaga *deleteSaga) *deleteCascadeByIdxSaga {
	dcs := &deleteCascadeByIdxSaga{
		deleteSaga: deleteSaga,
	}

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

		if !itemInfoData.Opts.HasIdx {
			s.noIndexTrigger(ctx, &deleteItemInfoData{
				ItemInfoData: itemInfoData,
				TmpDelPaths:  temporaryDelPaths,
			})
		} else {
			s.indexTrigger(ctx, &deleteItemInfoData{
				ItemInfoData: itemInfoData,
				TmpDelPaths:  temporaryDelPaths,
			})
		}
	}
}

func (s *deleteCascadeByIdxSaga) noIndexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
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

func (s *deleteCascadeByIdxSaga) indexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
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
	deleteItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteItemOnDisk := itemInfoData.WithRespChan(deleteItemOnDiskRespSignal)
	s.opSaga.crudChannels.CreateChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	err := <-deleteItemOnDiskRespSignal
	if err != nil {
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}
	itemInfoData.RespSignal <- err
}

func (s *deleteCascadeByIdxSaga) indexRollback(
	_ context.Context, itemInfoData *deleteItemInfoData,
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
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
		itemInfoData.RespSignal <- err
		return
	}

	itemInfoData.RespSignal <- nil
}

func (s *deleteCascadeByIdxSaga) deleteMainKey(_ context.Context) {
	//
}

func (s *deleteCascadeByIdxSaga) deleteIndexes(_ context.Context) {
	//
}

func (s *deleteCascadeByIdxSaga) deleteIndexingList(_ context.Context) {
	//
}

func (s *deleteCascadeByIdxSaga) deleteRelationalItem(_ context.Context) {
	//
}

func (s *deleteCascadeByIdxSaga) deleteTemporaryRecords(_ context.Context) {
	//
}

type deleteIdxOnlySaga struct {
	deleteSaga *deleteSaga
}

func newDeleteIdxOnlySaga(deleteSaga *deleteSaga) *deleteIdxOnlySaga {
	dcs := &deleteIdxOnlySaga{
		deleteSaga: deleteSaga,
	}

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

		if !itemInfoData.Opts.HasIdx {
			s.noIndexTrigger(ctx, &deleteItemInfoData{
				ItemInfoData: itemInfoData,
				TmpDelPaths:  temporaryDelPaths,
			})
		} else {
			s.indexTrigger(ctx, &deleteItemInfoData{
				ItemInfoData: itemInfoData,
				TmpDelPaths:  temporaryDelPaths,
			})
		}
	}
}

func (s *deleteIdxOnlySaga) noIndexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
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

func (s *deleteIdxOnlySaga) indexTrigger(
	_ context.Context, itemInfoData *deleteItemInfoData,
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
	deleteItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteItemOnDisk := itemInfoData.WithRespChan(deleteItemOnDiskRespSignal)
	s.opSaga.crudChannels.CreateChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	err := <-deleteItemOnDiskRespSignal
	if err != nil {
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}
	itemInfoData.RespSignal <- err
}

func (s *deleteIdxOnlySaga) indexRollback(
	_ context.Context, itemInfoData *deleteItemInfoData,
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
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
		itemInfoData.RespSignal <- err
		return
	}

	itemInfoData.RespSignal <- nil
}

func (s *deleteIdxOnlySaga) deleteMainKey(_ context.Context) {
	//
}

func (s *deleteIdxOnlySaga) deleteIndexes(_ context.Context) {
	//
}

func (s *deleteIdxOnlySaga) deleteIndexingList(_ context.Context) {
	//
}

func (s *deleteIdxOnlySaga) deleteRelationalItem(_ context.Context) {
	//
}

func (s *deleteIdxOnlySaga) deleteTemporaryRecords(_ context.Context) {
	//
}
