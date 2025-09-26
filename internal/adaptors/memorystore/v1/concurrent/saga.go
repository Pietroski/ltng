package concurrentmemorystorev1

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
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
	ce           *LTNGCacheEngine
	crudChannels *ltngenginemodels.CrudChannels
}

func newOpSaga(ctx context.Context, ce *LTNGCacheEngine) *opSaga {
	op := &opSaga{
		ce: ce,
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

func (op *opSaga) ListenAndTrigger(_ context.Context) {
	for itemInfoData := range op.crudChannels.OpSagaChannel.InfoChannel {

		//respSignalChan := make(chan error)
		//itemInfoData.RespSignal = respSignalChan

		switch itemInfoData.OpType {
		case ltngenginemodels.OpTypeCreate:
			op.crudChannels.CreateChannels.InfoChannel <- itemInfoData
		case ltngenginemodels.OpTypeUpsert:
			op.crudChannels.UpsertChannels.InfoChannel <- itemInfoData
		case ltngenginemodels.OpTypeDelete:
			op.crudChannels.DeleteChannels.InfoChannel <- itemInfoData
		default:
			log.Printf("unknown op type: %v", itemInfoData.OpType)
			return
		}

		if err := ResponseAccumulator(itemInfoData.RespSignal); err != nil {
			log.Printf("error accumulating item info data from file queue on op type %v: %v",
				itemInfoData.OpType, err)
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
		log.Printf("error on trigger action itemInfoData: %+v: %v\n", itemInfoData, err)
	}
	itemInfoData.RespSignal <- err
	close(itemInfoData.RespSignal)
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
		log.Printf("error on trigger action itemInfoData: %+v: %v\n", itemInfoData, err)
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
		log.Printf("error on trigger action itemInfoData relational: %v: %v\n", itemInfoData, err)
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
	close(itemInfoData.RespSignal)
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

	err := ResponseAccumulator(
		deleteItemOnDiskRespSignal,
		deleteIndexItemOnDiskRespSignal,
		deleteIndexItemListOnDiskRespSignal,
	)
	if err != nil {
		log.Printf("error rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}
	itemInfoData.RespSignal <- err
	close(itemInfoData.RespSignal)
}

// createItemOnDiskOnThread stands for createItemOnDisk on thread.
func (s *createSaga) createItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.ActionItemChannel {
		key := bytes.Join(
			[][]byte{[]byte(v.DBMetaInfo.Name), v.Item.Key},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strKey := hex.EncodeToString(key)
		err := s.opSaga.ce.cache.Set(v.Ctx, strKey, v.Item.Value)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

func (s *createSaga) deleteItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.RollbackItemChannel {
		v.RespSignal <- nil
		close(v.RespSignal)
	}
}

func (s *createSaga) createIndexItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.ActionIndexItemChannel {
		var errAcc error
		for _, itemKey := range v.Opts.IndexingKeys {
			indexKey := bytes.Join(
				[][]byte{[]byte(v.DBMetaInfo.IndexInfo().Name), itemKey},
				[]byte(ltngenginemodels.BytesSliceSep),
			)
			strIndexKey := hex.EncodeToString(indexKey)
			if err := s.opSaga.ce.cache.Set(v.Ctx, strIndexKey, v.Opts.ParentKey); err != nil {
				if errAcc == nil {
					errAcc = err
				} else {
					errAcc = fmt.Errorf("%v: %w", errAcc, err)
				}
			}
		}
		v.RespSignal <- errAcc
		close(v.RespSignal)
	}
}

func (s *createSaga) deleteIndexItemFromDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.RollbackIndexItemChannel {
		v.RespSignal <- nil
		close(v.RespSignal)
	}
}

func (s *createSaga) createIndexListItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.ActionIndexListItemChannel {
		indexListKey := bytes.Join(
			[][]byte{[]byte(v.DBMetaInfo.IndexListInfo().Name), v.Opts.ParentKey},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strIndexListKey := hex.EncodeToString(indexListKey)
		err := s.opSaga.ce.cache.Set(v.Ctx, strIndexListKey, v.Opts.IndexingKeys)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

func (s *createSaga) deleteIndexListItemFromDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.RollbackIndexListItemChannel {
		v.RespSignal <- nil
		close(v.RespSignal)
	}
}

func (s *createSaga) createRelationalItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.opSaga.crudChannels.CreateChannels.ActionRelationalItemChannel {
		relationalKey := bytes.Join(
			[][]byte{[]byte(v.DBMetaInfo.RelationalInfo().Name)},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strRelationalKey := hex.EncodeToString(relationalKey)
		var value []*ltngenginemodels.Item
		if err := s.opSaga.ce.cache.Get(v.Ctx, strRelationalKey, &value, func() (interface{}, error) {
			return []*ltngenginemodels.Item{}, nil
		}); err != nil {
			v.RespSignal <- err
			close(v.RespSignal)
			return
		}
		value = append(value, &ltngenginemodels.Item{
			Key:   v.Item.Key,
			Value: v.Item.Value,
		})
		err := s.opSaga.ce.cache.Set(v.Ctx, strRelationalKey, value)
		v.RespSignal <- err
		close(v.RespSignal)
	}
}

// #####################################################################################################################
