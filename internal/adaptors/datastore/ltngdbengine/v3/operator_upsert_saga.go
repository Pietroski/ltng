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
		s.opSaga.crudChannels.UpsertChannels.InfoChannel.Ch,
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
	encodedKey := itemInfoData.EncodedKey()
	itemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.Path, encodedKey)
	tmpItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.TemporaryInfo().Path, encodedKey)

	relationalItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.RelationalInfo().Path, encodedKey)

	copyItemsToTemporaryLocations := func() error {
		if err := osx.CpFile(itemInfoData.Ctx, itemDataFilePath, tmpItemDataFilePath); err != nil {
			s.opSaga.e.logger.Debug(itemInfoData.Ctx,
				"error copying indexless item info data to temporary location on disk",
				"from", itemDataFilePath, "to", tmpItemDataFilePath,
				"error", err)
		}

		return nil
	}
	decopyItemsFromTemporaryLocations := func() error {
		if err := osx.MvFile(itemInfoData.Ctx, tmpItemDataFilePath, itemDataFilePath); err != nil {
			s.opSaga.e.logger.Debug(itemInfoData.Ctx,
				"error de-copying indexless item info data from temporary location on disk",
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

		fi, err := s.opSaga.e.upsertItemOnDisk(itemInfoData.Ctx, itemDataFilePath,
			itemInfoData.DBMetaInfo, fileData)
		if err != nil {
			return errorsx.Wrapf(err, "error upserting item info data on disk, filePath: %s", itemDataFilePath)
		}

		s.opSaga.e.itemFileMapping.Set(itemInfoData.DBMetaInfo.LockStrWithKey(encodedKey), fi)

		return nil
	}
	deleteItemOnDisk := func() error {
		filePath := ltngdbenginemodelsv3.GetDataFilepath(
			itemInfoData.DBMetaInfo.Path, encodedKey)

		if err := os.Remove(filePath); err != nil {
			return err
		}
		s.opSaga.e.itemFileMapping.Delete(itemInfoData.DBMetaInfo.LockStrWithKey(encodedKey))

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
	encodedKey := itemInfoData.EncodedKey()
	itemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.Path, encodedKey)
	tmpItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.TemporaryInfo().Path, encodedKey)

	indexedListItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.IndexListInfo().Path, encodedKey)
	tmpIndexedListItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.TemporaryIndexListInfo().Path, encodedKey)

	relationalItemDataFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		itemInfoData.DBMetaInfo.RelationalInfo().Path, ltngdbenginemodelsv3.RelationalDataStoreKey)

	copyItemsToTemporaryLocations := func() error {
		if err := osx.CpFile(itemInfoData.Ctx, itemDataFilePath, tmpItemDataFilePath); err != nil {
			s.opSaga.e.logger.Debug(itemInfoData.Ctx,
				"error copying item info data to temporary location on disk",
				"from", itemDataFilePath, "to", tmpItemDataFilePath,
				"error", err)
		}

		if _, err := osx.CpOnlyFilesFromDir(itemInfoData.Ctx,
			itemInfoData.DBMetaInfo.IndexInfo().Path,
			itemInfoData.DBMetaInfo.TemporaryIndexInfo().Path,
		); err != nil {
			s.opSaga.e.logger.Debug(itemInfoData.Ctx,
				"error copying indexed item info data to temporary location on disk",
				"from", itemInfoData.DBMetaInfo.IndexInfo().Path,
				"to", itemInfoData.DBMetaInfo.TemporaryIndexInfo().Path,
				"error", err)
		}

		if err := osx.CpFile(itemInfoData.Ctx, indexedListItemDataFilePath, tmpIndexedListItemDataFilePath); err != nil {
			s.opSaga.e.logger.Debug(itemInfoData.Ctx,
				"error copying indexed list item info data to temporary location on disk",
				"from", indexedListItemDataFilePath, "to", tmpIndexedListItemDataFilePath,
				"error", err)
		}

		return nil
	}
	decopyItemsFromTemporaryLocations := func() error {
		if err := osx.MvFile(itemInfoData.Ctx, tmpItemDataFilePath, itemDataFilePath); err != nil {
			s.opSaga.e.logger.Debug(itemInfoData.Ctx,
				"error de-copying item info data from temporary location on disk",
				"from", tmpItemDataFilePath, "to", itemDataFilePath,
				"error", err)
		}

		if _, err := osx.MvOnlyFilesFromDir(itemInfoData.Ctx,
			itemInfoData.DBMetaInfo.TemporaryIndexInfo().Path,
			itemInfoData.DBMetaInfo.IndexInfo().Path,
		); err != nil {
			s.opSaga.e.logger.Debug(itemInfoData.Ctx,
				"error de-copying indexed items info data from temporary on disk",
				"from", itemInfoData.DBMetaInfo.TemporaryIndexInfo().Path,
				"to", itemInfoData.DBMetaInfo.IndexInfo().Path,
				"error", err)
		}

		if err := osx.MvFile(itemInfoData.Ctx, tmpIndexedListItemDataFilePath, indexedListItemDataFilePath); err != nil {
			s.opSaga.e.logger.Debug(itemInfoData.Ctx,
				"error de-copying indexed list item info data from temporary on disk",
				"from", tmpIndexedListItemDataFilePath, "to", indexedListItemDataFilePath,
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

		fi, err := s.opSaga.e.upsertItemOnDisk(itemInfoData.Ctx, itemDataFilePath,
			itemInfoData.DBMetaInfo, fileData)
		if err != nil {
			return errorsx.Wrapf(err, "failed upserting item on disk - filekey: %s", itemInfoData.Item.Key)
		}

		s.opSaga.e.itemFileMapping.Set(itemInfoData.DBMetaInfo.LockStrWithKey(encodedKey), fi)

		return nil
	}
	deleteItemOnDisk := func() error {
		filePath := ltngdbenginemodelsv3.GetDataFilepath(
			itemInfoData.DBMetaInfo.Path, encodedKey)

		if err := os.Remove(filePath); err != nil {
			return err
		}
		s.opSaga.e.itemFileMapping.Delete(itemInfoData.DBMetaInfo.LockStrWithKey(encodedKey))

		return nil
	}

	upsertIndexItemOnDisk := func() error {
		indexingList, err := s.opSaga.e.loadIndexingList(
			itemInfoData.Ctx, itemInfoData.DBMetaInfo, itemInfoData.Opts)
		if err != nil {
			s.opSaga.e.logger.Debug(itemInfoData.Ctx,
				"error loading indexing list",
				"error", err)
		}

		keysToSave := bytesop.CalRightDiff(
			ltngdbenginemodelsv3.IndexListToBytesList(indexingList),
			itemInfoData.Opts.IndexingKeys)

		//s.opSaga.e.logger.Info(itemInfoData.Ctx,
		//	"upserting index items on disk",
		//	"keysToSave", keysToSave)
		for _, indexKey := range keysToSave {
			encodedIndexedStr := hex.EncodeToString(indexKey)
			filePath := ltngdbenginemodelsv3.GetDataFilepath(
				itemInfoData.DBMetaInfo.IndexInfo().Path, encodedIndexedStr)
			indexFileData := ltngdbenginemodelsv3.NewFileData(
				itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
					Key:   indexKey,
					Value: itemInfoData.Opts.ParentKey,
				})

			//s.opSaga.e.logger.Info(itemInfoData.Ctx,
			//	"upserting index items on disk",
			//	"keysToSave", keysToSave,
			//	"filePath", filePath,
			//)

			if _, err = s.opSaga.e.createItemOnDisk(itemInfoData.Ctx, filePath, indexFileData); err != nil {
				if errorsx.Is(errorsx.From(err), osx.ErrFileExists) {
					continue
				}

				return errorsx.Wrapf(err,
					"failed upserting/creating index item on disk - filekey: %s", encodedIndexedStr)
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
				itemInfoData.DBMetaInfo.TemporaryIndexInfo().Path, encodedIndexedStr)

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
				s.opSaga.e.logger.Debug(itemInfoData.Ctx,
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

		fi, err := s.opSaga.e.upsertItemOnDisk(itemInfoData.Ctx, indexedListItemDataFilePath,
			itemInfoData.DBMetaInfo.IndexListInfo(), indexListFileData)
		if err != nil {
			return errorsx.Wrapf(err,
				"failed upserting index list item on disk - filekey: %s", itemInfoData.Item.Key)
		}

		encodedKey := itemInfoData.EncodedKey()
		s.opSaga.e.itemFileMapping.Set(itemInfoData.DBMetaInfo.LockStrWithKey(encodedKey), fi)

		return nil
	}
	deleteIndexListItemOnDisk := func() error {
		filePath := ltngdbenginemodelsv3.GetDataFilepath(
			itemInfoData.DBMetaInfo.IndexListInfo().Path, encodedKey)

		if err := os.Remove(filePath); err != nil {
			return err
		}
		s.opSaga.e.itemFileMapping.Delete(itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(encodedKey))

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
				Name: "upsertIndexItemOnDisk",
				Do:   upsertIndexItemOnDisk,
				RetrialOpts: &saga.RetrialOpts{
					RetrialOnErr: true,
					RetrialCount: 0,
				},
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
