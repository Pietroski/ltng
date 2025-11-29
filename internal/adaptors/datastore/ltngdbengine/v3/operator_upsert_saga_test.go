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

func assertFileData(t *testing.T, expectedFileData, fileData *ltngdbenginemodelsv3.FileData) {
	assert.EqualValues(t, expectedFileData.Key, fileData.Key)
	assert.EqualValues(t, expectedFileData.Data, fileData.Data)
	assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
	assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
}

func assertNotEqualFileData(t *testing.T, expectedFileData, fileData *ltngdbenginemodelsv3.FileData) {
	assert.EqualValues(t, expectedFileData.Key, fileData.Key)
	assert.NotEqualValues(t, expectedFileData.Data, fileData.Data)
	assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
	assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
}

func initUpsertSagaTestSuite(t *testing.T) *testSuite {
	ts := initTestSuite(t)
	ts.us = newUpsertSaga(ts.ctx, ts.e.opSaga)

	return ts
}

func TestUpsertSaga_buildUpsertItemInfoData_buildUpsertItemInfoDataWithoutIndex_creating_items(t *testing.T) {
	t.Run("buildUpsertItemInfoData - single item", func(t *testing.T) {
		t.Run("should upsert item with index", func(t *testing.T) {
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
				assertFileData(t, expectedFileData, &fileData)
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
					assertFileData(t, expectedFileData, &fileData)
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
				assertFileData(t, expectedFileData, &fileData)
			}

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
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
					assertFileData(t, expectedFileData, fileInfo.FileData)
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
				assertFileData(t, expectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
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

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert indexed items with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
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

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert index list item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
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

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert relational data item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
			ops[4].Action.Do = func() error {
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

			// close database
			ts.e.Close()
		})

		t.Run("should upsert item without index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, false)
			ops := ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, itemInfoData)
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
				assertFileData(t, expectedFileData, &fileData)
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

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
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

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert item without index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, false)
			ops := ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, itemInfoData)
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

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert relational data item without index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, false)
			ops := ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, itemInfoData)
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

			// close database
			ts.e.Close()
		})
	})

	t.Run("buildUpsertItemInfoData - multiple items", func(t *testing.T) {
		t.Run("should upsert item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
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
					assertFileData(t, expectedFileData, &fileData)
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
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						var fileData ltngdbenginemodelsv3.FileData
						err = ts.e.serializer.Deserialize(bs, &fileData)
						assert.NoError(t, err)
						assertFileData(t, expectedFileData, &fileData)
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
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.True(t, ok)
						assertFileData(t, expectedFileData, fileInfo.FileData)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
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

		t.Run("should fail to upsert indexed items with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
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

		t.Run("should fail to upsert index list item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
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

		t.Run("should fail to upsert relational data item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
				ops[4].Action.Do = func() error {
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

		t.Run("should upsert item without index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, false)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, itemInfoData)
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
					assertFileData(t, expectedFileData, &fileData)
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

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
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

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert item without index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, false)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, itemInfoData)
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

		t.Run("should fail to upsert relational data item without index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, false)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, itemInfoData)
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
	})
}

func TestUpsertSaga_buildUpsertItemInfoData_buildUpsertItemInfoDataWithoutIndex_upserting_items(t *testing.T) {
	t.Run("buildUpsertItemInfoData - single item", func(t *testing.T) {
		t.Run("should upsert item with index", func(t *testing.T) {
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
				assertFileData(t, expectedFileData, &fileData)
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
					assertFileData(t, expectedFileData, &fileData)
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
				assertFileData(t, expectedFileData, &fileData)
			}

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
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
					assertFileData(t, expectedFileData, fileInfo.FileData)
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
				assertFileData(t, expectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			newItemInfoData := updateItemInfoData(t, ts, itemInfoData, true)
			ops = ts.us.buildUpsertItemInfoData(ts.ctx, newItemInfoData)
			err = saga.NewListOperator(ops...).Operate()
			assert.NoError(t, err)

			// check files

			{ // item file
				itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					newItemInfoData.DBMetaInfo.Path, itemStrKey)

				fm, err := mmap.NewFileManager(itemFilePath)
				assert.NoError(t, err)
				defer func() {
					err = fm.Close()
					assert.NoError(t, err)
				}()

				bs, err := fm.Read()
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(bs, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			{ // indexed items files
				for _, indexKey := range newItemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						newItemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

					fm, err := mmap.NewFileManager(itemFilePath)
					assert.NoError(t, err)
					defer func() {
						err = fm.Close()
						assert.NoError(t, err)
					}()

					bs, err := fm.Read()
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo,
						&ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: newItemInfoData.Opts.ParentKey,
						})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}
			}

			{ // index list file
				itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					newItemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

				fm, err := mmap.NewFileManager(itemFilePath)
				assert.NoError(t, err)
				defer func() {
					err = fm.Close()
					assert.NoError(t, err)
				}()

				bs, err := fm.Read()
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key:   newItemInfoData.Item.Key,
						Value: bytes.Join(newItemInfoData.Opts.IndexingKeys, []byte(ltngdbenginemodelsv3.BsSep)),
					})

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(bs, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, newItemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, newItemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(newItemInfoData.DBMetaInfo, newItemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(newItemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
			}

			{ // indexed items memory
				for _, indexKey := range newItemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: newItemInfoData.Opts.ParentKey,
						})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						newItemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}
			}

			{ // index list item memory
				itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key:   newItemInfoData.Item.Key,
						Value: bytes.Join(newItemInfoData.Opts.IndexingKeys, []byte(ltngdbenginemodelsv3.BsSep)),
					})

				fileInfo, ok := ts.e.itemFileMapping.Get(
					newItemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := newItemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, newItemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(newItemInfoData.DBMetaInfo, newItemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
			err := saga.NewListOperator(ops...).Operate()
			assert.NoError(t, err)

			// check files

			func() { // item file
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
				assertFileData(t, expectedFileData, &fileData)
			}()

			func() { // indexed items files
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
					assertFileData(t, expectedFileData, &fileData)
				}
			}()

			func() { // index list file
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
				assertFileData(t, expectedFileData, &fileData)
			}()

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
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
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}
			}

			{ // index list item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key: itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})

				fileInfo, ok := ts.e.itemFileMapping.Get(
					itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			newItemInfoData := updateItemInfoData(t, ts, itemInfoData, true)
			ops = ts.us.buildUpsertItemInfoData(ts.ctx, newItemInfoData)
			ops[1].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err = saga.NewListOperator(ops...).Operate()
			assert.Error(t, err)

			// check files

			func() { // item file
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
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}()

			func() { // indexed items files
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
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey,
						})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: newItemInfoData.Opts.ParentKey,
						})
					assertFileData(t, notExpectedFileData, &fileData)
				}
			}()

			func() { // index list file
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
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key:   newItemInfoData.Item.Key,
						Value: bytes.Join(newItemInfoData.Opts.IndexingKeys, []byte(ltngdbenginemodelsv3.BsSep)),
					})
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}()

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
			}

			{ // indexed items memory
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: newItemInfoData.Opts.ParentKey})
					assertFileData(t, notExpectedFileData, fileInfo.FileData)
				}
			}

			{ // index list item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key: itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})

				fileInfo, ok := ts.e.itemFileMapping.Get(
					itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key: newItemInfoData.Item.Key,
						Value: bytes.Join(newItemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})
				assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert indexed items with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
			err := saga.NewListOperator(ops...).Operate()
			assert.NoError(t, err)

			// check files

			func() { // item file
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
				assertFileData(t, expectedFileData, &fileData)
			}()

			func() { // indexed items files
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
					assertFileData(t, expectedFileData, &fileData)
				}
			}()

			func() { // index list file
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
				assertFileData(t, expectedFileData, &fileData)
			}()

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
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
					assertFileData(t, expectedFileData, fileInfo.FileData)
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
				assertFileData(t, expectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			newItemInfoData := updateItemInfoData(t, ts, itemInfoData, true)
			ops = ts.us.buildUpsertItemInfoData(ts.ctx, newItemInfoData)
			ops[2].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err = saga.NewListOperator(ops...).Operate()
			assert.Error(t, err)

			// check files

			func() { // item file
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
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}()

			func() { // indexed items files
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
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: newItemInfoData.Opts.ParentKey})
					assertFileData(t, notExpectedFileData, &fileData)
				}
			}()

			func() { // index list file
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
						Key: itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(bs, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key: newItemInfoData.Item.Key,
						Value: bytes.Join(newItemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}()

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
			}

			{ // indexed items memory
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: newItemInfoData.Opts.ParentKey})
					assertFileData(t, notExpectedFileData, fileInfo.FileData)
				}
			}

			{ // index list item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key: itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})

				fileInfo, ok := ts.e.itemFileMapping.Get(
					itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key: newItemInfoData.Item.Key,
						Value: bytes.Join(newItemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})
				assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert index list item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
			err := saga.NewListOperator(ops...).Operate()
			assert.NoError(t, err)

			// check files

			func() { // item file
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
				assertFileData(t, expectedFileData, &fileData)
			}()

			func() { // indexed items files
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
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}
			}()

			func() { // index list file
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
						Key: itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(bs, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}()

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
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
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}
			}

			{ // index list item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key: itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})

				fileInfo, ok := ts.e.itemFileMapping.Get(
					itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			newItemInfoData := updateItemInfoData(t, ts, itemInfoData, true)
			ops = ts.us.buildUpsertItemInfoData(ts.ctx, newItemInfoData)
			ops[3].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err = saga.NewListOperator(ops...).Operate()
			assert.Error(t, err)

			// check files

			func() { // item file
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
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}()

			func() { // indexed items files
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
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: newItemInfoData.Opts.ParentKey})
					assertFileData(t, notExpectedFileData, &fileData)
				}
			}()

			func() { // index list file
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
						Key: itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(bs, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key: newItemInfoData.Item.Key,
						Value: bytes.Join(newItemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}()

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
			}

			{ // indexed items memory
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: newItemInfoData.Opts.ParentKey})
					assertFileData(t, notExpectedFileData, fileInfo.FileData)
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
				assertFileData(t, expectedFileData, fileInfo.FileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert relational data item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, true)
			ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
			err := saga.NewListOperator(ops...).Operate()
			assert.NoError(t, err)

			// check files

			func() { // item file
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
				assertFileData(t, expectedFileData, &fileData)
			}()

			func() { // indexed items files
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
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}
			}()

			func() { // index list file
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
						Key: itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(bs, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}()

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
			}

			{ // indexed items memory
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}
			}

			{ // index list item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key: itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})

				fileInfo, ok := ts.e.itemFileMapping.Get(
					itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			newItemInfoData := updateItemInfoData(t, ts, itemInfoData, true)
			ops = ts.us.buildUpsertItemInfoData(ts.ctx, newItemInfoData)
			ops[4].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err = saga.NewListOperator(ops...).Operate()
			assert.Error(t, err)

			// check files

			func() { // item file
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
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}()

			func() { // indexed items files
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
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: newItemInfoData.Opts.ParentKey})
					assertFileData(t, notExpectedFileData, &fileData)
				}
			}()

			func() { // index list file
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
						Key: itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(bs, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key: newItemInfoData.Item.Key,
						Value: bytes.Join(newItemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}()

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
			}

			{ // indexed items memory
				for _, indexKey := range itemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: itemInfoData.Opts.ParentKey})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   indexKey,
							Value: newItemInfoData.Opts.ParentKey})
					assertFileData(t, notExpectedFileData, fileInfo.FileData)
				}
			}

			{ // index list item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key: itemInfoData.Item.Key,
						Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})

				fileInfo, ok := ts.e.itemFileMapping.Get(
					itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
						Key: newItemInfoData.Item.Key,
						Value: bytes.Join(newItemInfoData.Opts.IndexingKeys,
							[]byte(ltngdbenginemodelsv3.BsSep))})
				assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should upsert item without index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, false)
			ops := ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, itemInfoData)
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
				assertFileData(t, expectedFileData, &fileData)
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

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
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

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			newItemInfoData := updateItemInfoData(t, ts, itemInfoData, false)
			ops = ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, newItemInfoData)
			err = saga.NewListOperator(ops...).Operate()
			assert.NoError(t, err)

			// check files

			{ // item file
				itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					newItemInfoData.DBMetaInfo.Path, itemStrKey)

				fm, err := mmap.NewFileManager(itemFilePath)
				assert.NoError(t, err)
				defer func() {
					err = fm.Close()
					assert.NoError(t, err)
				}()

				bs, err := fm.Read()
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(bs, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			{ // indexed item files
				for _, indexKey := range newItemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						newItemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}
			}

			{ // index list file
				itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)
				itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
					newItemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

				file, err := osx.OpenFile(itemFilePath)
				assert.Error(t, err)
				assert.Nil(t, file)
			}

			{ // relational data file
				rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, newItemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, newItemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(newItemInfoData.DBMetaInfo, newItemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(newItemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
			}

			{ // indexed items memory
				for _, indexKey := range newItemInfoData.Opts.IndexingKeys {
					itemStrKey := hex.EncodeToString(indexKey)

					fileInfo, ok := ts.e.itemFileMapping.Get(
						newItemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}
			}

			{ // index list item memory
				itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)

				fileInfo, ok := ts.e.itemFileMapping.Get(
					newItemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.False(t, ok)
				assert.Nil(t, fileInfo)
			}

			{ // relational data item in memory
				lockStr := newItemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, newItemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(newItemInfoData.DBMetaInfo, newItemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert item without index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, false)
			ops := ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, itemInfoData)
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
				assertFileData(t, expectedFileData, &fileData)
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

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
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

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			newItemInfoData := updateItemInfoData(t, ts, itemInfoData, false)
			ops = ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, newItemInfoData)
			ops[1].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err = saga.NewListOperator(ops...).Operate()
			assert.Error(t, err)

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
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
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

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
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

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert relational data item without index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoData := generateItemInfoData(t, ts, false)
			ops := ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, itemInfoData)
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
				assertFileData(t, expectedFileData, &fileData)
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

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
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

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				//assert.EqualValues(t, expectedFileData, fileData)
				_ = expectedFileData
			}

			newItemInfoData := updateItemInfoData(t, ts, itemInfoData, false)
			ops = ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, newItemInfoData)
			ops[2].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err = saga.NewListOperator(ops...).Operate()
			assert.Error(t, err)

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
				assertFileData(t, expectedFileData, &fileData)

				notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
					newItemInfoData.DBMetaInfo, newItemInfoData.Item)
				assertNotEqualFileData(t, notExpectedFileData, &fileData)
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

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// check memory

			{ // item memory
				itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assertFileData(t, expectedFileData, fileInfo.FileData)
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

				expectedFileData := ltngdbenginemodelsv3.NewFileData(
					itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assertFileData(t, expectedFileData, &fileData)
			}

			// close database
			ts.e.Close()
		})
	})

	t.Run("buildUpsertItemInfoData - multiple items", func(t *testing.T) {
		t.Run("should upsert item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
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
					assertFileData(t, expectedFileData, &fileData)
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
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						var fileData ltngdbenginemodelsv3.FileData
						err = ts.e.serializer.Deserialize(bs, &fileData)
						assert.NoError(t, err)
						assertFileData(t, expectedFileData, &fileData)
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
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.True(t, ok)
						assertFileData(t, expectedFileData, fileInfo.FileData)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				newItemInfoData := updateItemInfoData(t, ts, itemInfoData, true)
				ops = ts.us.buildUpsertItemInfoData(ts.ctx, newItemInfoData)
				err = saga.NewListOperator(ops...).Operate()
				assert.NoError(t, err)

				// check files

				{ // item file
					itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						newItemInfoData.DBMetaInfo.Path, itemStrKey)

					fm, err := mmap.NewFileManager(itemFilePath)
					assert.NoError(t, err)
					defer func() {
						err = fm.Close()
						assert.NoError(t, err)
					}()

					bs, err := fm.Read()
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				{ // indexed items files
					for _, indexKey := range newItemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)
						itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
							newItemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

						fm, err := mmap.NewFileManager(itemFilePath)
						assert.NoError(t, err)
						defer func() {
							err = fm.Close()
							assert.NoError(t, err)
						}()

						bs, err := fm.Read()
						assert.NoError(t, err)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: newItemInfoData.Opts.ParentKey})

						var fileData ltngdbenginemodelsv3.FileData
						err = ts.e.serializer.Deserialize(bs, &fileData)
						assert.NoError(t, err)
						assertFileData(t, expectedFileData, &fileData)
					}
				}

				{ // index list file
					itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						newItemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

					fm, err := mmap.NewFileManager(itemFilePath)
					assert.NoError(t, err)
					defer func() {
						err = fm.Close()
						assert.NoError(t, err)
					}()

					bs, err := fm.Read()
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: newItemInfoData.Item.Key,
							Value: bytes.Join(newItemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, newItemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, newItemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(newItemInfoData.DBMetaInfo, newItemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(newItemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range newItemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: newItemInfoData.Opts.ParentKey,
							})

						fileInfo, ok := ts.e.itemFileMapping.Get(
							newItemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.True(t, ok)
						assertFileData(t, expectedFileData, fileInfo.FileData)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key:   newItemInfoData.Item.Key,
							Value: bytes.Join(newItemInfoData.Opts.IndexingKeys, []byte(ltngdbenginemodelsv3.BsSep)),
						})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						newItemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // relational data item in memory
					lockStr := newItemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, newItemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(newItemInfoData.DBMetaInfo, newItemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
				err := saga.NewListOperator(ops...).Operate()
				assert.NoError(t, err)

				// check files

				func() { // item file
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
					assertFileData(t, expectedFileData, &fileData)
				}()

				func() { // indexed items files
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
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						var fileData ltngdbenginemodelsv3.FileData
						err = ts.e.serializer.Deserialize(bs, &fileData)
						assert.NoError(t, err)
						assertFileData(t, expectedFileData, &fileData)
					}
				}()

				func() { // index list file
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
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}()

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.True(t, ok)
						assertFileData(t, expectedFileData, fileInfo.FileData)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				newItemInfoData := updateItemInfoData(t, ts, itemInfoData, true)
				ops = ts.us.buildUpsertItemInfoData(ts.ctx, newItemInfoData)
				ops[1].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err = saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// check files

				func() { // item file
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
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}()

				func() { // indexed items files
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
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						var fileData ltngdbenginemodelsv3.FileData
						err = ts.e.serializer.Deserialize(bs, &fileData)
						assert.NoError(t, err)
						assertFileData(t, expectedFileData, &fileData)

						notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
							newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: newItemInfoData.Opts.ParentKey})
						assertFileData(t, notExpectedFileData, &fileData)
					}
				}()

				func() { // index list file
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
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: newItemInfoData.Item.Key,
							Value: bytes.Join(newItemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}()

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.True(t, ok)
						assertFileData(t, expectedFileData, fileInfo.FileData)

						notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
							newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: newItemInfoData.Opts.ParentKey})
						assertFileData(t, notExpectedFileData, fileInfo.FileData)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: newItemInfoData.Item.Key,
							Value: bytes.Join(newItemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})
					assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert indexed items with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
				err := saga.NewListOperator(ops...).Operate()
				assert.NoError(t, err)

				// check files

				func() { // item file
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
					assertFileData(t, expectedFileData, &fileData)
				}()

				func() { // indexed items files
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
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						var fileData ltngdbenginemodelsv3.FileData
						err = ts.e.serializer.Deserialize(bs, &fileData)
						assert.NoError(t, err)
						assertFileData(t, expectedFileData, &fileData)
					}
				}()

				func() { // index list file
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
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}()

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.True(t, ok)
						assertFileData(t, expectedFileData, fileInfo.FileData)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				newItemInfoData := updateItemInfoData(t, ts, itemInfoData, true)
				ops = ts.us.buildUpsertItemInfoData(ts.ctx, newItemInfoData)
				ops[2].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err = saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// check files

				func() { // item file
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
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}()

				func() { // indexed items files
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
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						var fileData ltngdbenginemodelsv3.FileData
						err = ts.e.serializer.Deserialize(bs, &fileData)
						assert.NoError(t, err)
						assertFileData(t, expectedFileData, &fileData)

						notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
							newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: newItemInfoData.Opts.ParentKey})
						assertFileData(t, notExpectedFileData, &fileData)
					}
				}()

				func() { // index list file
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
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: newItemInfoData.Item.Key,
							Value: bytes.Join(newItemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}()

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.True(t, ok)
						assertFileData(t, expectedFileData, fileInfo.FileData)

						notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
							newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: newItemInfoData.Opts.ParentKey})
						assertFileData(t, notExpectedFileData, fileInfo.FileData)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert index list item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
				err := saga.NewListOperator(ops...).Operate()
				assert.NoError(t, err)

				// check files

				func() { // item file
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
					assertFileData(t, expectedFileData, &fileData)
				}()

				func() { // indexed items files
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
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						var fileData ltngdbenginemodelsv3.FileData
						err = ts.e.serializer.Deserialize(bs, &fileData)
						assert.NoError(t, err)
						assertFileData(t, expectedFileData, &fileData)
					}
				}()

				func() { // index list file
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
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}()

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.True(t, ok)
						assertFileData(t, expectedFileData, fileInfo.FileData)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				newItemInfoData := updateItemInfoData(t, ts, itemInfoData, true)
				ops = ts.us.buildUpsertItemInfoData(ts.ctx, newItemInfoData)
				ops[3].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err = saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// check files

				func() { // item file
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
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}()

				func() { // indexed items files
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
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						var fileData ltngdbenginemodelsv3.FileData
						err = ts.e.serializer.Deserialize(bs, &fileData)
						assert.NoError(t, err)
						assertFileData(t, expectedFileData, &fileData)

						notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
							newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: newItemInfoData.Opts.ParentKey})
						assertFileData(t, notExpectedFileData, &fileData)
					}
				}()

				func() { // index list file
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
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}()

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.True(t, ok)
						assertFileData(t, expectedFileData, fileInfo.FileData)

						notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
							newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: newItemInfoData.Opts.ParentKey})
						assertFileData(t, notExpectedFileData, fileInfo.FileData)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert relational data item with index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, true)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoData(ts.ctx, itemInfoData)
				err := saga.NewListOperator(ops...).Operate()
				assert.NoError(t, err)

				// check files

				func() { // item file
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
					assertFileData(t, expectedFileData, &fileData)
				}()

				func() { // indexed items files
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
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						var fileData ltngdbenginemodelsv3.FileData
						err = ts.e.serializer.Deserialize(bs, &fileData)
						assert.NoError(t, err)
						assertFileData(t, expectedFileData, &fileData)
					}
				}()

				func() { // index list file
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
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}()

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.True(t, ok)
						assertFileData(t, expectedFileData, fileInfo.FileData)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				newItemInfoData := updateItemInfoData(t, ts, itemInfoData, true)
				ops = ts.us.buildUpsertItemInfoData(ts.ctx, newItemInfoData)
				ops[4].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err = saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// check files

				func() { // item file
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
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}()

				func() { // indexed items files
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
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						var fileData ltngdbenginemodelsv3.FileData
						err = ts.e.serializer.Deserialize(bs, &fileData)
						assert.NoError(t, err)
						assertFileData(t, expectedFileData, &fileData)

						notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
							newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: newItemInfoData.Opts.ParentKey})
						assertFileData(t, notExpectedFileData, &fileData)
					}
				}()

				func() { // index list file
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
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}()

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range itemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						expectedFileData := ltngdbenginemodelsv3.NewFileData(
							itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: itemInfoData.Opts.ParentKey})

						fileInfo, ok := ts.e.itemFileMapping.Get(
							itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.True(t, ok)
						assertFileData(t, expectedFileData, fileInfo.FileData)

						notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
							newItemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
								Key:   indexKey,
								Value: newItemInfoData.Opts.ParentKey})
						assertFileData(t, notExpectedFileData, fileInfo.FileData)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
							Key: itemInfoData.Item.Key,
							Value: bytes.Join(itemInfoData.Opts.IndexingKeys,
								[]byte(ltngdbenginemodelsv3.BsSep))})

					fileInfo, ok := ts.e.itemFileMapping.Get(
						itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
				}

				{ // relational data item in memory
					lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should upsert item without index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, false)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, itemInfoData)
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
					assertFileData(t, expectedFileData, &fileData)
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

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
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

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				newItemInfoData := updateItemInfoData(t, ts, itemInfoData, false)
				ops = ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, newItemInfoData)
				err = saga.NewListOperator(ops...).Operate()
				assert.NoError(t, err)

				// check files

				{ // item file
					itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						newItemInfoData.DBMetaInfo.Path, itemStrKey)

					fm, err := mmap.NewFileManager(itemFilePath)
					assert.NoError(t, err)
					defer func() {
						err = fm.Close()
						assert.NoError(t, err)
					}()

					bs, err := fm.Read()
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				{ // indexed item files
					for _, indexKey := range newItemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)
						itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
							newItemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)

						file, err := osx.OpenFile(itemFilePath)
						assert.Error(t, err)
						assert.Nil(t, file)
					}
				}

				{ // index list file
					itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)
					itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
						newItemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)

					file, err := osx.OpenFile(itemFilePath)
					assert.Error(t, err)
					assert.Nil(t, file)
				}

				{ // relational data file
					rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, newItemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, newItemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(newItemInfoData.DBMetaInfo, newItemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(newItemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
				}

				{ // indexed items memory
					for _, indexKey := range newItemInfoData.Opts.IndexingKeys {
						itemStrKey := hex.EncodeToString(indexKey)

						fileInfo, ok := ts.e.itemFileMapping.Get(
							newItemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
						assert.False(t, ok)
						assert.Nil(t, fileInfo)
					}
				}

				{ // index list item memory
					itemStrKey := hex.EncodeToString(newItemInfoData.Item.Key)

					fileInfo, ok := ts.e.itemFileMapping.Get(
						newItemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
					assert.False(t, ok)
					assert.Nil(t, fileInfo)
				}

				{ // relational data item in memory
					lockStr := newItemInfoData.DBMetaInfo.RelationalLockStr()
					rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
					assert.True(t, ok)

					foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, newItemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(newItemInfoData.DBMetaInfo, newItemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert item without index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, false)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, itemInfoData)
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
					assertFileData(t, expectedFileData, &fileData)
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

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
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

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					//assert.EqualValues(t, expectedFileData, fileData)
					_ = expectedFileData
				}

				newItemInfoData := updateItemInfoData(t, ts, itemInfoData, false)
				ops = ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, newItemInfoData)
				ops[1].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err = saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

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
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
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

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
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

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}
			}

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert relational data item without index", func(t *testing.T) {
			ts := initUpsertSagaTestSuite(t)
			itemInfoDataList := generateItemInfoDataList(t, ts, 10, false)

			for _, itemInfoData := range itemInfoDataList {
				ops := ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, itemInfoData)
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
					assertFileData(t, expectedFileData, &fileData)
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

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)
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

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)
				}

				newItemInfoData := updateItemInfoData(t, ts, itemInfoData, false)
				ops = ts.us.buildUpsertItemInfoDataWithoutIndex(ts.ctx, newItemInfoData)
				ops[2].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err = saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

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
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
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

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}

				// check memory

				{ // item memory
					itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
					assert.True(t, ok)
					assertFileData(t, expectedFileData, fileInfo.FileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, fileInfo.FileData)
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

					expectedFileData := ltngdbenginemodelsv3.NewFileData(
						itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assertFileData(t, expectedFileData, &fileData)

					notExpectedFileData := ltngdbenginemodelsv3.NewFileData(
						newItemInfoData.DBMetaInfo, newItemInfoData.Item)
					assertNotEqualFileData(t, notExpectedFileData, &fileData)
				}
			}

			// close database
			ts.e.Close()
		})
	})
}
