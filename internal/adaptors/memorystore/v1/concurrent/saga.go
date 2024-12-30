package concurrentv1

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	off_thread "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/op/off-thread"
	"log"
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

type createSaga struct {
	e *LTNGCacheEngine
}

func newCreateSaga(ctx context.Context, e *LTNGCacheEngine) *createSaga {
	cs := &createSaga{
		e: e,
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
	for itemInfoData := range s.e.createChannel {
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

	s.e.crudChannels.CreateChannels.ActionItemChannel <- itemInfoDataForCreateItemOnDisk
	s.e.crudChannels.CreateChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk

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

	s.e.crudChannels.CreateChannels.ActionItemChannel <- itemInfoDataForCreateItemOnDisk
	s.e.crudChannels.CreateChannels.ActionIndexItemChannel <- itemInfoDataForCreateIndexItemOnDisk
	s.e.crudChannels.CreateChannels.ActionIndexListItemChannel <- itemInfoDataForCreateIndexItemListOnDisk
	s.e.crudChannels.CreateChannels.ActionRelationalItemChannel <- itemInfoDataForCreateRelationalItemOnDisk

	if err := ResponseAccumulator(
		createItemOnDiskRespSignal,
		createIndexItemOnDiskRespSignal,
		createIndexItemListOnDiskRespSignal,
		createRelationalItemOnDiskRespSignal,
	); err != nil {
		log.Printf("error on trigger action itemInfoData: %v: %v\n", itemInfoData, err)
		s.RollbackTrigger(itemInfoData.Ctx, itemInfoData)

		itemInfoData.RespSignal <- err
		close(itemInfoData.RespSignal)

		return

		// TODO: notify and send it to DLQ
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
	deleteRelationalItemOnDiskRespSignal := make(chan error, 1)
	itemInfoDataForDeleteRelationalItemOnDisk := itemInfoData.WithRespChan(deleteRelationalItemOnDiskRespSignal)

	s.e.crudChannels.CreateChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	s.e.crudChannels.CreateChannels.RollbackRelationalItemChannel <- itemInfoDataForDeleteRelationalItemOnDisk

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

	s.e.crudChannels.CreateChannels.RollbackItemChannel <- itemInfoDataForDeleteItemOnDisk
	s.e.crudChannels.CreateChannels.RollbackIndexItemChannel <- itemInfoDataForDeleteIndexItemOnDisk
	s.e.crudChannels.CreateChannels.RollbackIndexListItemChannel <- itemInfoDataForDeleteIndexItemListOnDisk
	s.e.crudChannels.CreateChannels.RollbackRelationalItemChannel <- itemInfoDataForDeleteRelationalItemOnDisk

	if err := ResponseAccumulator(
		deleteItemOnDiskRespSignal,
		deleteIndexItemOnDiskRespSignal,
		deleteIndexItemListOnDiskRespSignal,
		deleteRelationalItemOnDiskRespSignal,
	); err != nil {
		log.Printf("\nerror rolling back trigger for itemInfoData: %v: %v\n", itemInfoData, err)
	}
}

// createItemOnDiskOnThread stands for createItemOnDisk on thread.
func (s *createSaga) createItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.e.crudChannels.CreateChannels.ActionItemChannel {
		key := bytes.Join(
			[][]byte{[]byte(v.DBMetaInfo.Name), v.Item.Key},
			[]byte(ltngenginemodels.BytesSep),
		)
		strKey := hex.EncodeToString(key)
		s.e.opMtx.Lock(strKey, struct{}{})
		s.e.itemFileMapping[strKey] = &ltngenginemodels.FileData{
			Key:  key,
			Data: v.Item.Value,
		}
		s.e.opMtx.Unlock(strKey)
		v.RespSignal <- nil
		close(v.RespSignal)
	}
}

func (s *createSaga) deleteItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.e.crudChannels.CreateChannels.RollbackItemChannel {
		v.RespSignal <- nil
		close(v.RespSignal)
	}
}

func (s *createSaga) createIndexItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.e.crudChannels.CreateChannels.ActionIndexItemChannel {
		op := off_thread.New("createIndexItemOnDisk")
		for _, indexKey := range v.Opts.IndexingKeys {
			op.OpX(func() (any, error) {
				key := bytes.Join(
					[][]byte{[]byte(v.DBMetaInfo.IndexInfo().Name), indexKey},
					[]byte(ltngenginemodels.BytesSep),
				)
				strKey := hex.EncodeToString(key)
				s.e.opMtx.Lock(strKey, struct{}{})
				s.e.itemFileMapping[strKey] = &ltngenginemodels.FileData{
					Key:  key,
					Data: v.Opts.ParentKey,
				}
				s.e.opMtx.Unlock(strKey)

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
	for v := range s.e.crudChannels.CreateChannels.RollbackIndexItemChannel {
		v.RespSignal <- nil
		close(v.RespSignal)
	}
}

func (s *createSaga) createIndexListItemOnDiskOnThread(
	ctx context.Context,
) {
	for v := range s.e.crudChannels.CreateChannels.ActionIndexListItemChannel {
		key := bytes.Join(
			[][]byte{[]byte(v.DBMetaInfo.IndexListInfo().Name), v.Opts.ParentKey},
			[]byte(ltngenginemodels.BytesSep),
		)
		strKey := hex.EncodeToString(key)
		s.e.opMtx.Lock(strKey, struct{}{})
		s.e.itemFileMapping[strKey] = &ltngenginemodels.FileData{
			Key:  key,
			Data: bytes.Join(v.Opts.IndexingKeys, []byte(ltngenginemodels.BytesSep)),
		}
		s.e.opMtx.Unlock(strKey)

		v.RespSignal <- nil
		close(v.RespSignal)
	}
}

func (s *createSaga) deleteIndexListItemFromDiskOnThread(
	_ context.Context,
) {
	for v := range s.e.crudChannels.CreateChannels.RollbackIndexListItemChannel {
		v.RespSignal <- nil
		close(v.RespSignal)
	}
}

func (s *createSaga) createRelationalItemOnDiskOnThread(
	_ context.Context,
) {
	for v := range s.e.crudChannels.CreateChannels.ActionRelationalItemChannel {
		v.RespSignal <- nil
		close(v.RespSignal)
	}
}

func (s *createSaga) deleteRelationalItemFromDiskOnThread(
	_ context.Context,
) {
	for v := range s.e.crudChannels.CreateChannels.RollbackRelationalItemChannel {
		v.RespSignal <- nil
		close(v.RespSignal)
	}
}

// #####################################################################################################################
