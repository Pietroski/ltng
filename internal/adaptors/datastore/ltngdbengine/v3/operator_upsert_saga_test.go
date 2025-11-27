package ltngdbenginev3

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/saga"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
	fileiomodels "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/models"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

func initUpsertSagaTestSuite(t *testing.T) *testSuite {
	ts := initTestSuite(t)
	ts.us = newUpsertSaga(ts.ctx, ts.e.opSaga)

	return ts
}

func TestUpsertSaga_buildUpsertItemInfoData(t *testing.T) {
	t.Run("buildUpsertItemInfoData - single item", func(t *testing.T) {
		t.Run("should create item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
			err := saga.NewListOperator(ops...).Operate()
			assert.NoError(t, err)

			// check files

			{ // item file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.Path, itemStrKey)

				fm, err := mmap.NewFileManager(itemFilePath)
				assert.NoError(t, err)
				defer func() {
					err = fm.Close()
					assert.NoError(t, err)
				}()

				bs, err := fm.Read()
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(bs, &fileData)
				assert.NoError(t, err)
				assert.EqualValues(t, expectedFileData.Key, fileData.Key)
				assert.EqualValues(t, expectedFileData.Data, fileData.Data)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
			}

			{ // indexed items files
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

					fm, err := mmap.NewFileManager(itemFilePath)
					assert.NoError(t, err)
					defer func() {
						err = fm.Close()
						assert.NoError(t, err)
					}()

					bs, err := fm.Read()
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo,
						&ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey,
						})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assert.EqualValues(t, expectedFileData.Key, fileData.Key)
					assert.EqualValues(t, expectedFileData.Data, fileData.Data)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
				}
			}

			{ // index list file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

				fm, err := mmap.NewFileManager(itemFilePath)
				assert.NoError(t, err)
				defer func() {
					err = fm.Close()
					assert.NoError(t, err)
				}()

				bs, err := fm.Read()
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key:   itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys, []byte(ltngdbenginemodelsv3.BsSep)),
					})

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(bs, &fileData)
				assert.NoError(t, err)
				assert.EqualValues(t, expectedFileData.Key, fileData.Key)
				assert.EqualValues(t, expectedFileData.Data, fileData.Data)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
			}

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assert.EqualValues(t, expectedFileData.Key, fileData.Key)
				assert.EqualValues(t, expectedFileData.Data, fileData.Data)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assert.EqualValues(t, expectedFileData, fileInfo.FileData)
			}

			{ // indexed items memory
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey,
						})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assert.EqualValues(t, expectedFileData, fileInfo.FileData)
				}
			}

			{ // index list item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key:   itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys, []byte(ltngdbenginemodelsv3.BsSep)),
					})

				fileInfo, ok := ts.e.itemFileMapping.Get(
					itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assert.EqualValues(t, expectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				//assert.EqualValues(t, expectedFileData, fileData)
				_ = expectedFileData
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to create item with index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
			ops[0].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err := saga.NewListOperator(ops...).Operate()
			assert.Error(t, err)

			// check files

			{ // item file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // indexed item files
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}
			}

			{ // index list file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				_, err = rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.False(t, ok)
				assert.Nil(t, fileInfo)
			}

			{ // indexed items memory
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}
			}

			{ // index list item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				fileInfo, ok := ts.e.itemFileMapping.Get(
					itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.False(t, ok)
				assert.Nil(t, fileInfo)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.Empty(t, foundResult)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to create indexed items with index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
			ops[1].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err := saga.NewListOperator(ops...).Operate()
			assert.Error(t, err)

			// verify created things
			{ // item file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // indexed item files
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}
			}

			{ // index list file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				_, err = rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.False(t, ok)
				assert.Nil(t, fileInfo)
			}

			{ // indexed items memory
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}
			}

			{ // index list item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				fileInfo, ok := ts.e.itemFileMapping.Get(
					itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.False(t, ok)
				assert.Nil(t, fileInfo)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.Empty(t, foundResult)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to create index list item with index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
			ops[2].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err := saga.NewListOperator(ops...).Operate()
			assert.Error(t, err)

			// verify created things
			{ // item file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // indexed item files
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}
			}

			{ // index list file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				_, err = rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to create relational data item with index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
			ops[3].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err := saga.NewListOperator(ops...).Operate()
			assert.Error(t, err)

			// verify created things
			{ // item file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // indexed item files
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}
			}

			{ // index list file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				_, err = rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should create item without index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, false)
			ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
			err := saga.NewListOperator(ops...).Operate()
			assert.NoError(t, err)

			// check files

			{ // item file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.Path, itemStrKey)

				fm, err := mmap.NewFileManager(itemFilePath)
				assert.NoError(t, err)
				defer func() {
					err = fm.Close()
					assert.NoError(t, err)
				}()

				bs, err := fm.Read()
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(bs, &fileData)
				assert.NoError(t, err)
				assert.EqualValues(t, expectedFileData.Key, fileData.Key)
				assert.EqualValues(t, expectedFileData.Data, fileData.Data)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
			}

			{ // indexed item files
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}
			}

			{ // index list file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assert.EqualValues(t, expectedFileData.Key, fileData.Key)
				assert.EqualValues(t, expectedFileData.Data, fileData.Data)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assert.EqualValues(t, expectedFileData, fileInfo.FileData)
			}

			{ // indexed items memory
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}
			}

			{ // index list item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				fileInfo, ok := ts.e.itemFileMapping.Get(
					itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.False(t, ok)
				assert.Nil(t, fileInfo)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				//assert.EqualValues(t, expectedFileData, fileData)
				_ = expectedFileData
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to create item without index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, false)
			ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
			ops[0].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err := saga.NewListOperator(ops...).Operate()
			assert.Error(t, err)

			// verify created things
			{ // item file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // indexed item files
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}
			}

			{ // index list file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				_, err = rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to create relational data item without index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, false)
			ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
			ops[1].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err := saga.NewListOperator(ops...).Operate()
			assert.Error(t, err)

			// verify created things
			{ // item file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // indexed item files
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}
			}

			{ // index list file
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				_, err = rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
			}

			// close database
			ts.e.Close()
		})
	})

	t.Run("buildUpsertItemInfoData - multiple items", func(t *testing.T) {
		t.Run("should create item with index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
				err := saga.NewListOperator(ops...).Operate()
				assert.NoError(t, err)

				// check files

				{ // item file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.Path, itemStrKey)

					fm, err := mmap.NewFileManager(itemFilePath)
					assert.NoError(t, err)
					defer func() {
						err = fm.Close()
						assert.NoError(t, err)
					}()

					bs, err := fm.Read()
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assert.EqualValues(t, expectedFileData.Key, fileData.Key)
					assert.EqualValues(t, expectedFileData.Data, fileData.Data)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
				}

				{ // indexed item files
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)
						itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
							itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

						fm, err := mmap.NewFileManager(itemFilePath)
						assert.NoError(t, err)
						defer func() {
							err = fm.Close()
							assert.NoError(t, err)
						}()

						bs, err := fm.Read()
						assert.NoError(t, err)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							itemInfoData.DBMetaInfo,
							&ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey,
							})

						var fileData ltngdbenginemodelsv3.FileData
						err = ts.e.serializer.Deserialize(bs, &fileData)
						assert.NoError(t, err)
						assert.EqualValues(t, expectedFileData.Key, fileData.Key)
						assert.EqualValues(t, expectedFileData.Data, fileData.Data)
						assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
						assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
					}
				}

				{ // index list file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

					fm, err := mmap.NewFileManager(itemFilePath)
					assert.NoError(t, err)
					defer func() {
						err = fm.Close()
						assert.NoError(t, err)
					}()

					bs, err := fm.Read()
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys, []byte(ltngdbenginemodelsv3.BsSep)),
						})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assert.EqualValues(t, expectedFileData.Key, fileData.Key)
					assert.EqualValues(t, expectedFileData.Data, fileData.Data)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
				}

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assert.EqualValues(t, expectedFileData.Key, fileData.Key)
					assert.EqualValues(t, expectedFileData.Data, fileData.Data)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assert.EqualValues(t, expectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey,
							})

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.True(t, ok)
						assert.EqualValues(t, expectedFileData, fileInfo.FileData)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys, []byte(ltngdbenginemodelsv3.BsSep)),
						})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assert.EqualValues(t, expectedFileData, fileInfo.FileData)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					//assert.EqualValues(t, expectedFileData, fileData)
					_ = expectedFileData
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to create item with index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
				ops[0].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err := saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// check files

				{ // item file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // indexed item files
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)
						itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
							itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

						file, err := osx.OpenFile(itemFilePath)
						assert.Error(t, err)
						assert.Nil(t, file)
					}
				}

				{ // index list file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					_, err = rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.False(t, ok)
						assert.Nil(t, fileInfo)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.Empty(t, foundResult)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to create indexed items with index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
				ops[1].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err := saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// check files

				{ // item file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // indexed item files
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)
						itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
							itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

						file, err := osx.OpenFile(itemFilePath)
						assert.Error(t, err)
						assert.Nil(t, file)
					}
				}

				{ // index list file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					_, err = rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.False(t, ok)
						assert.Nil(t, fileInfo)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.Empty(t, foundResult)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to create index list item with index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
				ops[2].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err := saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// check files

				{ // item file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // indexed item files
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)
						itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
							itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

						file, err := osx.OpenFile(itemFilePath)
						assert.Error(t, err)
						assert.Nil(t, file)
					}
				}

				{ // index list file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					_, err = rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.False(t, ok)
						assert.Nil(t, fileInfo)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.Empty(t, foundResult)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to create relational data item with index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
				ops[3].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err := saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// check files

				{ // item file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // indexed item files
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)
						itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
							itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

						file, err := osx.OpenFile(itemFilePath)
						assert.Error(t, err)
						assert.Nil(t, file)
					}
				}

				{ // index list file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					_, err = rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.False(t, ok)
						assert.Nil(t, fileInfo)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.Empty(t, foundResult)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should create item without index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, false)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
				err := saga.NewListOperator(ops...).Operate()
				assert.NoError(t, err)

				// check files

				{ // item file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.Path, itemStrKey)

					fm, err := mmap.NewFileManager(itemFilePath)
					assert.NoError(t, err)
					defer func() {
						err = fm.Close()
						assert.NoError(t, err)
					}()

					bs, err := fm.Read()
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assert.EqualValues(t, expectedFileData.Key, fileData.Key)
					assert.EqualValues(t, expectedFileData.Data, fileData.Data)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
				}

				{ // indexed item files
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)
						itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
							itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

						file, err := osx.OpenFile(itemFilePath)
						assert.Error(t, err)
						assert.Nil(t, file)
					}
				}

				{ // index list file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assert.EqualValues(t, expectedFileData.Key, fileData.Key)
					assert.EqualValues(t, expectedFileData.Data, fileData.Data)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assert.EqualValues(t, expectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.False(t, ok)
						assert.Nil(t, fileInfo)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					//assert.EqualValues(t, expectedFileData, fileData)
					_ = expectedFileData
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to create item without index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, false)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
				ops[0].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err := saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// check files

				{ // item file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // indexed item files
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)
						itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
							itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

						file, err := osx.OpenFile(itemFilePath)
						assert.Error(t, err)
						assert.Nil(t, file)
					}
				}

				{ // index list file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					_, err = rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.False(t, ok)
						assert.Nil(t, fileInfo)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.Empty(t, foundResult)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to create relational data item without index", func(t *testing.T) {
			ts := initCreateSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, false)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.cs.buildCreateItemInfoData(ts.ctx, itemInfoData)
				ops[1].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err := saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// check files

				{ // item file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // indexed item files
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)
						itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
							itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

						file, err := osx.OpenFile(itemFilePath)
						assert.Error(t, err)
						assert.Nil(t, file)
					}
				}

				{ // index list file
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					_, err = rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.False(t, ok)
						assert.Nil(t, fileInfo)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.Empty(t, foundResult)
				}
			}

			// close database
			ts.e.Close()
		})
	})
}
