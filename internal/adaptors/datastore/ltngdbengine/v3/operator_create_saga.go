package ltngdbenginev3

import (
	"context"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/loop"
	"gitlab.com/pietroski-software-company/golang/devex/saga"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
)

type createSaga struct {
	opSaga *opSaga
	cancel context.CancelFunc
}

func newCreateSaga(ctx context.Context, opSaga *opSaga) *createSaga {
	ctx, cancel := context.WithCancel(ctx)
	cs := &createSaga{
		opSaga: opSaga,
		cancel: cancel,
	}

	cs.opSaga.offThread.Op(func() {
		cs.ListenAndTrigger(ctx)
	})

	return cs
}

func (s *createSaga) ListenAndTrigger(ctx context.Context) {
	ctx = context.WithValue(ctx, "thread", "operator_create_saga-ListenAndTrigger")
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.CreateChannels.InfoChannel.Ch,
		func(itemInfoData *ltngdbenginemodelsv3.ItemInfoData) {
			if _, err := s.createItemInfoData(itemInfoData.Ctx, itemInfoData); err != nil {
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

func (s *createSaga) createItemInfoData(
	ctx context.Context,
	itemInfoData *ltngdbenginemodelsv3.ItemInfoData,
) (*ltngdbenginemodelsv3.ItemInfoData, error) {
	return itemInfoData, saga.NewListOperator(s.buildCreateItemInfoData(ctx, itemInfoData)...).Operate()
}

func (s *createSaga) buildCreateItemInfoData(
	_ context.Context,
	itemInfoData *ltngdbenginemodelsv3.ItemInfoData,
) []*saga.Operation {
	fileData := ltngdbenginemodelsv3.NewFileData(
		itemInfoData.DBMetaInfo, itemInfoData.Item)

	createItemOnDisk := func() error {
		encodedKey := itemInfoData.EncodedKey()
		filePath := ltngdbenginemodelsv3.GetDataFilepath(
			itemInfoData.DBMetaInfo.Path, encodedKey)

		fi, err := s.opSaga.e.createItemOnDisk(itemInfoData.Ctx, filePath, fileData)
		if err != nil {
			return err
		}
		s.opSaga.e.itemFileMapping.Set(itemInfoData.DBMetaInfo.LockStr(encodedKey), fi)

		return nil
	}
	deleteItemOnDisk := func() error {
		encodedKey := itemInfoData.EncodedKey()
		filePath := ltngdbenginemodelsv3.GetDataFilepath(
			itemInfoData.DBMetaInfo.Path, encodedKey)

		if err := os.Remove(filePath); err != nil {
			return err
		}
		s.opSaga.e.itemFileMapping.Delete(itemInfoData.DBMetaInfo.LockStr(encodedKey))

		return nil
	}

	createItemOnRelationalFile := func() error {
		if err := s.opSaga.e.upsertItemOnRelationalFile(
			itemInfoData.Ctx, itemInfoData.DBMetaInfo, itemInfoData.Item); err != nil {
			return err
		}

		return nil
	}

	if !itemInfoData.Opts.HasIdx {
		return []*saga.Operation{
			{
				Action: &saga.Action{
					Name:        "createItemOnDisk",
					Do:          createItemOnDisk,
					RetrialOpts: saga.DefaultRetrialOps,
				},
				Rollback: &saga.Rollback{
					Name:        "deleteItemOnDisk",
					Do:          deleteItemOnDisk,
					RetrialOpts: saga.DefaultRetrialOps,
				},
			},
			{
				Action: &saga.Action{
					Name:        "createItemOnRelationalFile",
					Do:          createItemOnRelationalFile,
					RetrialOpts: saga.DefaultRetrialOps,
				},
			},
		}
	}

	createIndexItemOnDisk := func() error {
		for _, indexKey := range itemInfoData.Opts.IndexingKeys {
			encodedKey := itemInfoData.EncodedKey()
			filePath := ltngdbenginemodelsv3.GetDataFilepath(
				itemInfoData.DBMetaInfo.IndexInfo().Path, encodedKey)
			fileData := ltngdbenginemodelsv3.NewFileData(
				itemInfoData.DBMetaInfo,
				&ltngdbenginemodelsv3.Item{
					Key:   indexKey,
					Value: itemInfoData.Opts.ParentKey,
				})

			fi, err := s.opSaga.e.createItemOnDisk(itemInfoData.Ctx, filePath, fileData)
			if err != nil {
				return err
			}
			s.opSaga.e.itemFileMapping.Set(itemInfoData.DBMetaInfo.IndexInfo().LockStr(encodedKey), fi)
		}

		return nil
	}
	deleteIndexItemOnDisk := func() error {
		encodedKey := itemInfoData.EncodedKey()
		filePath := ltngdbenginemodelsv3.GetDataFilepath(
			itemInfoData.DBMetaInfo.IndexInfo().Path, encodedKey)

		if err := os.Remove(filePath); err != nil {
			return err
		}
		s.opSaga.e.itemFileMapping.Delete(itemInfoData.DBMetaInfo.IndexInfo().LockStr(encodedKey))

		return nil
	}

	createIndexListItemOnDisk := func() error {
		encodedKey := itemInfoData.EncodedKey()
		filePath := ltngdbenginemodelsv3.GetDataFilepath(
			itemInfoData.DBMetaInfo.IndexListInfo().Path, encodedKey)

		fi, err := s.opSaga.e.createItemOnDisk(itemInfoData.Ctx, filePath, fileData)
		if err != nil {
			return err
		}
		s.opSaga.e.itemFileMapping.Set(itemInfoData.DBMetaInfo.IndexListInfo().LockStr(encodedKey), fi)

		return nil
	}
	deleteIndexListItemOnDisk := func() error {
		encodedKey := itemInfoData.EncodedKey()
		filePath := ltngdbenginemodelsv3.GetDataFilepath(
			itemInfoData.DBMetaInfo.IndexListInfo().Path, encodedKey)

		if err := os.Remove(filePath); err != nil {
			return err
		}
		s.opSaga.e.itemFileMapping.Delete(itemInfoData.DBMetaInfo.IndexListInfo().LockStr(encodedKey))

		return nil
	}

	return []*saga.Operation{
		{
			Action: &saga.Action{
				Name:        "createItemOnDisk",
				Do:          createItemOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteItemOnDisk",
				Do:          deleteItemOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createIndexItemOnDisk",
				Do:          createIndexItemOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteIndexItemOnDisk",
				Do:          deleteIndexItemOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createIndexListItemOnDisk",
				Do:          createIndexListItemOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteIndexListItemOnDisk",
				Do:          deleteIndexListItemOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createItemOnRelationalFile",
				Do:          createItemOnRelationalFile,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
	}
}
