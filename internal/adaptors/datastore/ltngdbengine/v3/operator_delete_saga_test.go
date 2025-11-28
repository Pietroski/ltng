package ltngdbenginev3

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/saga"
	fileiomodels "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/models"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
)

func initDeleteCascadeSagaTestSuite(t *testing.T) *testSuite {
	ts := initTestSuite(t)
	ts.us = newUpsertSaga(ts.ctx, ts.e.opSaga)
	ts.ds = newDeleteSaga(ts.ctx, ts.e.opSaga)
	ts.dcs = newDeleteCascadeSaga(ts.ctx, ts.ds)

	return ts
}

func TestDeleteSagaCascade_buildCreateItemInfoData(t *testing.T) {
	t.Run("buildDeleteItemInfoData - single item", func(t *testing.T) {
		t.Run("should delete cascade item with index", func(t *testing.T) {
			ts := initDeleteCascadeSagaTestSuite(t)
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

			deleteInfoData := generateDeleteItemInfoDataFromItemInfoData(t, ts, itemInfoData, true)
			ops = ts.dcs.buildDeleteItemInfoData(ts.ctx, deleteInfoData)
			err = saga.NewListOperator(ops...).Operate()
			assert.NoError(t, err)

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

		t.Run("should fail to delete item with index", func(t *testing.T) {
			ts := initDeleteCascadeSagaTestSuite(t)
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

			deleteInfoData := generateDeleteItemInfoDataFromItemInfoData(t, ts, itemInfoData, true)
			ops = ts.dcs.buildDeleteItemInfoData(ts.ctx, deleteInfoData)
			ops[0].Action.Do = func() error {
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

			// close database
			ts.e.Close()
		})

		t.Run("should fail to upsert indexed items with index", func(t *testing.T) {}
	})

	t.Run("buildDeleteItemInfoData - multiple items", func(t *testing.T) {
		t.Run("should delete cascade item with index", func(t *testing.T) {
			ts := initDeleteCascadeSagaTestSuite(t)
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

				deleteInfoData := generateDeleteItemInfoDataFromItemInfoData(t, ts, itemInfoData, true)
				ops = ts.dcs.buildDeleteItemInfoData(ts.ctx, deleteInfoData)
				err = saga.NewListOperator(ops...).Operate()
				assert.NoError(t, err)

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

func TestDeleteSagaCascadeIndex_buildCreateItemInfoData(t *testing.T) {
	t.Run("buildDeleteItemInfoData - single item", func(t *testing.T) {
		t.Run("should delete cascade item with index", func(t *testing.T) {})
	})

	t.Run("buildDeleteItemInfoData - multiple items", func(t *testing.T) {
		t.Run("should delete cascade item with index", func(t *testing.T) {})
	})
}

func TestDeleteSagaIndexOnly_buildCreateItemInfoData(t *testing.T) {
	t.Run("buildDeleteItemInfoData - single item", func(t *testing.T) {
		t.Run("should delete cascade item with index", func(t *testing.T) {})
	})

	t.Run("buildDeleteItemInfoData - multiple items", func(t *testing.T) {
		t.Run("should delete cascade item with index", func(t *testing.T) {})
	})
}
