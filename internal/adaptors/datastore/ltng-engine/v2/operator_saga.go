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
	go cs.deleteRelationalItemFromDiskOnThread(ctx)

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
	createRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateRelationalItemOnDisk := itemInfoData.WithRespChan(createRelationalItemOnDiskRespSignal)

	s.opSaga.crudChannels.CreateChannels.ActionItemChannel <- itemInfoDataForCreateItemOnDisk
	s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk

	if err := ResponseAccumulator(
		createItemOnDiskRespSignal,
		createRelationalItemOnDiskRespSignal,
	); err != nil {
		log.Printf("\nerror on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		// TODO: notify and send it to DLQ
	}
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
	createRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateRelationalItemOnDisk := itemInfoData.WithRespChan(createRelationalItemOnDiskRespSignal)

	s.opSaga.crudChannels.CreateChannels.ActionItemChannel <- itemInfoDataForCreateItemOnDisk
	s.opSaga.crudChannels.CreateChannels.ActionIndexItemChannel <- itemInfoDataForCreateIndexItemOnDisk
	s.opSaga.crudChannels.CreateChannels.ActionIndexListItemChannel <- itemInfoDataForCreateIndexItemListOnDisk
	s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk

	if err := ResponseAccumulator(
		createItemOnDiskRespSignal,
		createIndexItemOnDiskRespSignal,
		createIndexItemListOnDiskRespSignal,
		createRelationalItemOnDiskRespSignal,
	); err != nil {
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		// TODO: notify and send it to DLQ
		itemInfoData.RespSignal <- err
		return
	}

	itemInfoData.RespSignal <- nil
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
	deleteRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteRelationalItemOnDisk := itemInfoData.WithRespChan(deleteRelationalItemOnDiskRespSignal)

	s.opSaga.crudChannels.CreateChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	s.opSaga.crudChannels.CreateChannels.RollbackRelationalItemChannel <- itemInfoDataForDeleteRelationalItemOnDisk

	if err := ResponseAccumulator(
		deleteItemOnDiskRespSignal,
		deleteRelationalItemOnDiskRespSignal,
	); err != nil {
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}
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
	deleteRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteRelationalItemOnDisk := itemInfoData.WithRespChan(deleteRelationalItemOnDiskRespSignal)

	s.opSaga.crudChannels.CreateChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	s.opSaga.crudChannels.CreateChannels.RollbackIndexItemChannel <- itemInfoDataForDeleteIndexItemOnDisk
	s.opSaga.crudChannels.CreateChannels.RollbackIndexListItemChannel <- itemInfoDataForDeleteIndexItemListOnDisk
	s.opSaga.crudChannels.CreateChannels.RollbackRelationalItemChannel <- itemInfoDataForDeleteRelationalItemOnDisk

	if err := ResponseAccumulator(
		deleteItemOnDiskRespSignal,
		deleteIndexItemOnDiskRespSignal,
		deleteIndexItemListOnDiskRespSignal,
		deleteRelationalItemOnDiskRespSignal,
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
				if err := s.opSaga.e.createIndexItemOnDisk(v.Ctx,
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
		err := s.opSaga.e.createIndexItemOnDisk(ctx,
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

func (s *createSaga) deleteRelationalItemFromDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.RollbackRelationalItemChannel {
		err := s.opSaga.e.deleteRelationalData(v.Ctx, v.DBMetaInfo, v.Item.Key)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

// #####################################################################################################################

type upsertSaga struct {
	opSaga *opSaga
}

func newUpdateSaga(ctx context.Context, opSaga *opSaga) *upsertSaga {
	cs := &upsertSaga{
		opSaga: opSaga,
	}

	go cs.upsertItemOnDiskOnThread(ctx)
	go cs.upsertIndexItemOnDiskOnThread(ctx)
	go cs.upsertIndexListItemOnDiskOnThread(ctx)
	go cs.upsertRelationalItemOnDiskOnThread(ctx)

	go cs.deleteItemOnDiskOnThread(ctx)
	go cs.deleteIndexItemFromDiskOnThread(ctx)
	go cs.deleteIndexListItemFromDiskOnThread(ctx)
	go cs.deleteRelationalItemFromDiskOnThread(ctx)

	go cs.ListenAndTrigger(ctx)

	return cs
}

func (s *upsertSaga) ListenAndTrigger(ctx context.Context) {
	for itemInfoData := range s.opSaga.crudChannels.CreateChannels.InfoChannel {
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
	createRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateRelationalItemOnDisk := itemInfoData.WithRespChan(createRelationalItemOnDiskRespSignal)

	s.opSaga.crudChannels.CreateChannels.ActionItemChannel <- itemInfoDataForCreateItemOnDisk
	s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk

	if err := ResponseAccumulator(
		createItemOnDiskRespSignal,
		createRelationalItemOnDiskRespSignal,
	); err != nil {
		log.Printf("\nerror on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		// TODO: notify and send it to DLQ
	}
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
	createRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForCreateRelationalItemOnDisk := itemInfoData.WithRespChan(createRelationalItemOnDiskRespSignal)

	s.opSaga.crudChannels.CreateChannels.ActionItemChannel <- itemInfoDataForCreateItemOnDisk
	s.opSaga.crudChannels.CreateChannels.ActionIndexItemChannel <- itemInfoDataForCreateIndexItemOnDisk
	s.opSaga.crudChannels.CreateChannels.ActionIndexListItemChannel <- itemInfoDataForCreateIndexItemListOnDisk
	s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk

	if err := ResponseAccumulator(
		createItemOnDiskRespSignal,
		createIndexItemOnDiskRespSignal,
		createIndexItemListOnDiskRespSignal,
		createRelationalItemOnDiskRespSignal,
	); err != nil {
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		// TODO: notify and send it to DLQ
		itemInfoData.RespSignal <- err
		return
	}

	itemInfoData.RespSignal <- nil
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
	deleteRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteRelationalItemOnDisk := itemInfoData.WithRespChan(deleteRelationalItemOnDiskRespSignal)

	s.opSaga.crudChannels.CreateChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	s.opSaga.crudChannels.CreateChannels.RollbackRelationalItemChannel <- itemInfoDataForDeleteRelationalItemOnDisk

	if err := ResponseAccumulator(
		deleteItemOnDiskRespSignal,
		deleteRelationalItemOnDiskRespSignal,
	); err != nil {
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
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
	deleteRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteRelationalItemOnDisk := itemInfoData.WithRespChan(deleteRelationalItemOnDiskRespSignal)

	s.opSaga.crudChannels.CreateChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	s.opSaga.crudChannels.CreateChannels.RollbackIndexItemChannel <- itemInfoDataForDeleteIndexItemOnDisk
	s.opSaga.crudChannels.CreateChannels.RollbackIndexListItemChannel <- itemInfoDataForDeleteIndexItemListOnDisk
	s.opSaga.crudChannels.CreateChannels.RollbackRelationalItemChannel <- itemInfoDataForDeleteRelationalItemOnDisk

	if err := ResponseAccumulator(
		deleteItemOnDiskRespSignal,
		deleteIndexItemOnDiskRespSignal,
		deleteIndexItemListOnDiskRespSignal,
		deleteRelationalItemOnDiskRespSignal,
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
		op := off_thread.New("upsertIndexItemOnDisk")
		for _, indexKey := range v.Opts.IndexingKeys {
			op.OpX(func() (any, error) {
				if err := s.opSaga.e.upsertIndexItemOnDisk(v.Ctx,
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

func (s *upsertSaga) deleteIndexItemFromDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.UpsertChannels.RollbackIndexItemChannel {
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

// TODO: fetch existing ones, compare and delete|add
func (s *upsertSaga) upsertIndexListItemOnDiskOnThread(
	ctx context.Context,
) {
	for v := range s.opSaga.crudChannels.UpsertChannels.ActionIndexListItemChannel {
		err := s.opSaga.e.upsertIndexItemOnDisk(ctx,
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

// TODO: implement me!
func (s *upsertSaga) upsertRelationalItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.UpsertChannels.ActionRelationalItemChannel {
		err := s.opSaga.e.createRelationalItemOnDisk(v.Ctx, v.DBMetaInfo, v.Item)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

func (s *upsertSaga) deleteRelationalItemFromDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.UpsertChannels.RollbackRelationalItemChannel {
		err := s.opSaga.e.deleteRelationalData(v.Ctx, v.DBMetaInfo, v.Item.Key)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

// #####################################################################################################################
