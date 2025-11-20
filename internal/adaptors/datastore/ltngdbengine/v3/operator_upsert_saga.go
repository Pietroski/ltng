package ltngdbenginev3

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/loop"
	"gitlab.com/pietroski-software-company/golang/devex/saga"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesop"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

type upsertSaga struct {
	opSaga *opSaga
	cancel context.CancelFunc
}

func newUpsertSaga(ctx context.Context, opSaga *opSaga) *upsertSaga {
	ctx, cancel := context.WithCancel(ctx)
	us := &upsertSaga{
		opSaga: opSaga,
		cancel: cancel,
	}

	us.opSaga.offThread.Op(func() {
		us.ListenAndTrigger(ctx)
	})

	return us
}

func (s *upsertSaga) ListenAndTrigger(ctx context.Context) {
	ctx = context.WithValue(ctx, "thread", "operator_create_saga-ListenAndTrigger")
	loop.RunFromChannel(ctx,
		s.opSaga.crudChannels.CreateChannels.InfoChannel.Ch,
		func(itemInfoData *ltngdbenginemodelsv3.ItemInfoData) {
			if _, err := s.upsertItemInfoData(itemInfoData.Ctx, itemInfoData); err != nil {
				itemInfoData.RespSignal <- errorsx.Wrap(err, "error upserting item info data on disk")
				close(itemInfoData.RespSignal)

				return
			}

			itemInfoData.RespSignal <- nil
			close(itemInfoData.RespSignal)
		},
	)
	s.cancel()
}

func (s *upsertSaga) upsertItemInfoData(
	ctx context.Context,
	itemInfoData *ltngdbenginemodelsv3.ItemInfoData,
) (*ltngdbenginemodelsv3.ItemInfoData, error) {
	if !itemInfoData.Opts.HasIdx {
		return itemInfoData, saga.NewListOperator(
			s.buildUpsertItemInfoDataWithoutIndex(ctx, itemInfoData)...,
		).Operate()
	}

	return itemInfoData, saga.NewListOperator(
		s.buildUpsertItemInfoData(ctx, itemInfoData)...,
	).Operate()
}

func (s *upsertSaga) buildUpsertItemInfoDataWithoutIndex(
	_ context.Context,
	itemInfoData *ltngdbenginemodelsv3.ItemInfoData,
) []*saga.Operation {
	encodedStr := itemInfoData.EncodedKey()
	itemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.Path, encodedStr)
	tmpItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.TemporaryInfo().Path, encodedStr)

	relationalItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.RelationalInfo().Path, encodedStr)

	copyItemsToTemporaryLocations := func() error {
		if err := osx.CpFile(itemInfoData.Ctx, itemDataFilePath, tmpItemDataFilePath); err != nil {
			return err
		}

		return nil
	}
	decopyItemsFromTemporaryLocations := func() error {
		if err := osx.MvFile(itemInfoData.Ctx, tmpItemDataFilePath, itemDataFilePath); err != nil {
			s.opSaga.e.logger.Error(itemInfoData.Ctx,
				"error de-copying item info data on disk",
				"from", tmpItemDataFilePath, "to", itemDataFilePath,
				"error", err)
		}

		{ // upsert from file into relational row
			fm, err := mmap.NewFileManager(itemDataFilePath)
			if err != nil {
				return err
			}

			bs, err := fm.Read()
			if err != nil {
				return err
			}

			rfm, err := mmap.NewRelationalFileManager(relationalItemDataFilePath)
			if err != nil {
				return err
			}

			if _, err = rfm.UpsertByKey(itemInfoData.Ctx, itemInfoData.Item.Key, bs); err != nil {
				return err
			}
		}

		return nil
	}

	upsertItemOnDisk := func() error {
		fileData := ltngdbenginemodelsv3.NewFileData(
			itemInfoData.DBMetaInfo, itemInfoData.Item)

		fi, err := s.opSaga.e.upsertItemOnDisk(itemInfoData.Ctx,
			itemInfoData.DBMetaInfo, fileData)
		if err != nil {
			return err
		}

		encodedKey := itemInfoData.EncodedKey()
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

	upsertOnRelationFile := func() error {
		if err := s.opSaga.e.upsertItemOnRelationalFile(itemInfoData.Ctx,
			itemInfoData.DBMetaInfo, itemInfoData.Item); err != nil {
			return err
		}

		return nil
	}

	return []*saga.Operation{
		{
			Action: &saga.Action{
				Name:        "copyItemsToTemporaryLocations",
				Do:          copyItemsToTemporaryLocations,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "decopyItemsFromTemporaryLocations",
				Do:          decopyItemsFromTemporaryLocations,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "upsertItemOnDisk",
				Do:          upsertItemOnDisk,
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
				Name:        "upsertOnRelationFile",
				Do:          upsertOnRelationFile,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
	}
}

func (s *upsertSaga) buildUpsertItemInfoData(
	_ context.Context,
	itemInfoData *ltngdbenginemodelsv3.ItemInfoData,
) []*saga.Operation {
	encodedStr := itemInfoData.EncodedKey()
	itemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.Path, encodedStr)
	tmpItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.TemporaryInfo().Path, encodedStr)

	indexedItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.IndexInfo().Path, encodedStr)
	tmpIndexedItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.TemporaryIndexInfo().Path, encodedStr)

	indexedListItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.IndexListInfo().Path, encodedStr)
	tmpIndexedListItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.TemporaryIndexListInfo().Path, encodedStr)

	relationalItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.RelationalInfo().Path, encodedStr)

	copyItemsToTemporaryLocations := func() error {
		if err := osx.CpFile(itemInfoData.Ctx, itemDataFilePath, tmpItemDataFilePath); err != nil {
			return err
		}

		if err := osx.CpFile(itemInfoData.Ctx, indexedItemDataFilePath, tmpIndexedItemDataFilePath); err != nil {
			return err
		}

		if err := osx.CpFile(itemInfoData.Ctx, indexedListItemDataFilePath, tmpIndexedListItemDataFilePath); err != nil {
			return err
		}

		//if err := osx.CpFile(itemInfoData.Ctx, relationalItemDataFilePath, tmpRelationalItemDataFilePath); err != nil {
		//	return err
		//}

		return nil
	}
	decopyItemsFromTemporaryLocations := func() error {
		if err := osx.MvFile(itemInfoData.Ctx, tmpItemDataFilePath, itemDataFilePath); err != nil {
			s.opSaga.e.logger.Error(itemInfoData.Ctx,
				"error de-copying item info data on disk",
				"from", tmpItemDataFilePath, "to", itemDataFilePath,
				"error", err)
		}

		if err := osx.MvFile(itemInfoData.Ctx, tmpIndexedItemDataFilePath, indexedItemDataFilePath); err != nil {
			s.opSaga.e.logger.Error(itemInfoData.Ctx,
				"error de-copying item info data on disk",
				"from", tmpIndexedItemDataFilePath, "to", indexedItemDataFilePath,
				"error", err)
		}

		if err := osx.MvFile(itemInfoData.Ctx, tmpIndexedListItemDataFilePath, indexedListItemDataFilePath); err != nil {
			s.opSaga.e.logger.Error(itemInfoData.Ctx,
				"error de-copying item info data on disk",
				"from", tmpIndexedListItemDataFilePath, "to", indexedListItemDataFilePath,
				"error", err)
		}

		//if err := osx.MvFile(itemInfoData.Ctx, tmpRelationalItemDataFilePath, relationalItemDataFilePath); err != nil {
		//	s.opSaga.e.logger.Error(itemInfoData.Ctx,
		//		"error de-copying item info data on disk",
		//		"from", tmpRelationalItemDataFilePath, "to", relationalItemDataFilePath,
		//		"error", err)
		//}

		{ // upsert from file into relational row
			fm, err := mmap.NewFileManager(itemDataFilePath)
			if err != nil {
				return err
			}

			bs, err := fm.Read()
			if err != nil {
				return err
			}

			rfm, err := mmap.NewRelationalFileManager(relationalItemDataFilePath)
			if err != nil {
				return err
			}

			if _, err = rfm.UpsertByKey(itemInfoData.Ctx, itemInfoData.Item.Key, bs); err != nil {
				return err
			}
		}

		return nil
	}

	upsertItemOnDisk := func() error {
		fileData := ltngdbenginemodelsv3.NewFileData(
			itemInfoData.DBMetaInfo, itemInfoData.Item)

		fi, err := s.opSaga.e.upsertItemOnDisk(itemInfoData.Ctx,
			itemInfoData.DBMetaInfo, fileData)
		if err != nil {
			return err
		}

		encodedKey := itemInfoData.EncodedKey()
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

	upsertIndexItemOnDisk := func() error {
		indexingList, err := s.opSaga.e.loadIndexingList(
			itemInfoData.Ctx, itemInfoData.DBMetaInfo, itemInfoData.Opts)
		if err != nil {
			return errorsx.Wrap(err, "error loading indexing list")
		}

		keysToSave := bytesop.CalRightDiff(
			ltngdbenginemodelsv3.IndexListToBytesList(indexingList),
			itemInfoData.Opts.IndexingKeys)

		for _, indexKey := range keysToSave {
			indexFileData := ltngdbenginemodelsv3.NewFileData(
				itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
					Key:   indexKey,
					Value: itemInfoData.Opts.ParentKey,
				})
			if _, err = s.opSaga.e.createItemOnDisk(
				itemInfoData.Ctx,
				itemInfoData.DBMetaInfo.IndexInfo().Path,
				indexFileData,
			); err != nil {
				return err
			}
		}

		keysToDelete := bytesop.CalRightDiff(
			itemInfoData.Opts.IndexingKeys,
			ltngdbenginemodelsv3.IndexListToBytesList(indexingList))
		itemInfoData.IndexKeysToDelete = keysToDelete

		for _, indexKey := range keysToDelete {
			encodedIndexedStr := hex.EncodeToString(indexKey)
			filePath := ltngdbenginemodelsv3.GetDataFilepath(
				itemInfoData.DBMetaInfo.IndexInfo().Path, encodedIndexedStr)
			tmpFilePath := ltngdbenginemodelsv3.GetDataFilepath(
				itemInfoData.DBMetaInfo.IndexInfo().Path, encodedIndexedStr)

			if err = osx.MvFile(itemInfoData.Ctx, filePath, tmpFilePath); err != nil {
				return err
			}
		}

		return nil
	}
	deleteIndexItemFromDisk := func() error {
		indexingList, err := s.opSaga.e.loadIndexingList(
			itemInfoData.Ctx, itemInfoData.DBMetaInfo, itemInfoData.Opts)
		if err != nil {
			return errorsx.Wrap(err, "error loading indexing list")
		}

		keysToDelete := bytesop.CalRightDiff(
			ltngdbenginemodelsv3.IndexListToBytesList(indexingList),
			itemInfoData.Opts.IndexingKeys)

		for _, indexKey := range keysToDelete {
			encodedIndexedStr := hex.EncodeToString(indexKey)
			filePath := ltngdbenginemodelsv3.GetDataFilepath(
				itemInfoData.DBMetaInfo.IndexInfo().Path, encodedIndexedStr)

			if err = os.Remove(filePath); err != nil {
				s.opSaga.e.logger.Error(itemInfoData.Ctx,
					"error deleting index item from disk",
					"filePath", filePath,
					"error", err)
			}
		}

		return nil
	}

	upsertIndexListItemOnDisk := func() error {
		indexListFileData := ltngdbenginemodelsv3.NewFileData(
			itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
				Key: itemInfoData.Opts.ParentKey,
				Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
					[]byte(ltngdbenginemodelsv3.BsSep)),
			})

		fi, err := s.opSaga.e.upsertItemOnDisk(itemInfoData.Ctx,
			itemInfoData.DBMetaInfo.IndexListInfo(), indexListFileData)
		if err != nil {
			return err
		}

		encodedKey := itemInfoData.EncodedKey()
		s.opSaga.e.itemFileMapping.Set(itemInfoData.DBMetaInfo.LockStr(encodedKey), fi)

		return nil
	}
	deleteIndexListItemOnDisk := func() error {
		filePath := ltngdbenginemodelsv3.GetDataFilepath(
			itemInfoData.DBMetaInfo.IndexListInfo().Path, encodedStr)

		if err := os.Remove(filePath); err != nil {
			return err
		}
		s.opSaga.e.itemFileMapping.Delete(itemInfoData.DBMetaInfo.IndexListInfo().LockStr(encodedStr))

		return nil
	}

	upsertOnRelationFile := func() error {
		if err := s.opSaga.e.upsertItemOnRelationalFile(itemInfoData.Ctx,
			itemInfoData.DBMetaInfo, itemInfoData.Item); err != nil {
			return err
		}

		return nil
	}

	return []*saga.Operation{
		{
			Action: &saga.Action{
				Name:        "copyItemsToTemporaryLocations",
				Do:          copyItemsToTemporaryLocations,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "decopyItemsFromTemporaryLocations",
				Do:          decopyItemsFromTemporaryLocations,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "upsertItemOnDisk",
				Do:          upsertItemOnDisk,
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
				Name:        "upsertIndexItemOnDisk",
				Do:          upsertIndexItemOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteIndexItemFromDisk",
				Do:          deleteIndexItemFromDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "upsertIndexListItemOnDisk",
				Do:          upsertIndexListItemOnDisk,
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
				Name:        "upsertOnRelationFile",
				Do:          upsertOnRelationFile,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
	}
}
