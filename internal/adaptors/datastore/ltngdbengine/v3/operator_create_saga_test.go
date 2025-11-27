package ltngdbenginev3

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/saga"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
	fileiomodels "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/models"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

func TestCreateSaga_ListenAndTrigger(t *testing.T) {
	//
}

// TODO: add memory checks as well

func TestCreateSaga_buildCreateItemInfoData(t *testing.T) {
	t.Run("buildCreateItemInfoData - single item", func(t *testing.T) {
		t.Run("should create item with index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			testUser := generateTestUser(t)
			byteValues := getValues(t, ts, testUser)

			traceID, err := uuid.NewV7()
			require.NoError(t, err)
			respSignal := make(chan error)
			itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
				Ctx:          ctx,
				RespSignal:   respSignal,
				TraceID:      traceID.String(),
				OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
				OpType:       ltngdbenginemodelsv3.OpTypeCreate,
				DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
				Item: &ltngdbenginemodelsv3.Item{
					Key:   byteValues.bsKey,
					Value: byteValues.bsValue,
				},
				Opts: &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:    true,
					ParentKey: byteValues.bsKey,
					IndexingKeys: [][]byte{
						byteValues.bsKey,
						byteValues.secondaryIndexBs,
						byteValues.extraUpsertIndex,
					},
				},
			}
			ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
			err = saga.NewListOperator(ops...).Operate()
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
				err = ts.ltngEngine.serializer.Deserialize(bs, &fileData)
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
					err = ts.ltngEngine.serializer.Deserialize(bs, &fileData)
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
				err = ts.ltngEngine.serializer.Deserialize(bs, &fileData)
				assert.NoError(t, err)
				assert.EqualValues(t, expectedFileData.Key, fileData.Key)
				assert.EqualValues(t, expectedFileData.Data, fileData.Data)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
			}

			{ // relational data file
				rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.ltngEngine.serializer.Deserialize(foundResult.BS, &fileData)
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

				fileInfo, ok := ts.ltngEngine.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
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

					fileInfo, ok := ts.ltngEngine.itemFileMapping.Get(
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

				fileInfo, ok := ts.ltngEngine.itemFileMapping.Get(
					itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
				assert.True(t, ok)
				assert.EqualValues(t, expectedFileData, fileInfo.FileData)
			}

			{ // relational data item in memory
				lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
				rfi, ok := ts.ltngEngine.relationalItemFileMapping.Get(lockStr)
				assert.True(t, ok)

				foundResult, err := rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.ltngEngine.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				//assert.EqualValues(t, expectedFileData, fileData)
				_ = expectedFileData
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should fail to create item with index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			testUser := generateTestUser(t)
			byteValues := getValues(t, ts, testUser)

			traceID, err := uuid.NewV7()
			require.NoError(t, err)
			respSignal := make(chan error)
			itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
				Ctx:          ctx,
				RespSignal:   respSignal,
				TraceID:      traceID.String(),
				OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
				OpType:       ltngdbenginemodelsv3.OpTypeCreate,
				DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
				Item: &ltngdbenginemodelsv3.Item{
					Key:   byteValues.bsKey,
					Value: byteValues.bsValue,
				},
				Opts: &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:    true,
					ParentKey: byteValues.bsKey,
					IndexingKeys: [][]byte{
						byteValues.bsKey,
						byteValues.secondaryIndexBs,
						byteValues.extraUpsertIndex,
					},
				},
			}
			ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
			ops[0].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err = saga.NewListOperator(ops...).Operate()
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
				rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				_, err = rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should fail to create indexed items with index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			testUser := generateTestUser(t)
			byteValues := getValues(t, ts, testUser)

			traceID, err := uuid.NewV7()
			require.NoError(t, err)
			respSignal := make(chan error)
			itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
				Ctx:          ctx,
				RespSignal:   respSignal,
				TraceID:      traceID.String(),
				OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
				OpType:       ltngdbenginemodelsv3.OpTypeCreate,
				DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
				Item: &ltngdbenginemodelsv3.Item{
					Key:   byteValues.bsKey,
					Value: byteValues.bsValue,
				},
				Opts: &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:    true,
					ParentKey: byteValues.bsKey,
					IndexingKeys: [][]byte{
						byteValues.bsKey,
						byteValues.secondaryIndexBs,
						byteValues.extraUpsertIndex,
					},
				},
			}
			ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
			ops[1].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err = saga.NewListOperator(ops...).Operate()
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
				rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				_, err = rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should fail to create index list item with index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			testUser := generateTestUser(t)
			byteValues := getValues(t, ts, testUser)

			traceID, err := uuid.NewV7()
			require.NoError(t, err)
			respSignal := make(chan error)
			itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
				Ctx:          ctx,
				RespSignal:   respSignal,
				TraceID:      traceID.String(),
				OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
				OpType:       ltngdbenginemodelsv3.OpTypeCreate,
				DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
				Item: &ltngdbenginemodelsv3.Item{
					Key:   byteValues.bsKey,
					Value: byteValues.bsValue,
				},
				Opts: &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:    true,
					ParentKey: byteValues.bsKey,
					IndexingKeys: [][]byte{
						byteValues.bsKey,
						byteValues.secondaryIndexBs,
						byteValues.extraUpsertIndex,
					},
				},
			}
			ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
			ops[2].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err = saga.NewListOperator(ops...).Operate()
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
				rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				_, err = rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should fail to create relational data item with index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			testUser := generateTestUser(t)
			byteValues := getValues(t, ts, testUser)

			traceID, err := uuid.NewV7()
			require.NoError(t, err)
			respSignal := make(chan error)
			itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
				Ctx:          ctx,
				RespSignal:   respSignal,
				TraceID:      traceID.String(),
				OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
				OpType:       ltngdbenginemodelsv3.OpTypeCreate,
				DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
				Item: &ltngdbenginemodelsv3.Item{
					Key:   byteValues.bsKey,
					Value: byteValues.bsValue,
				},
				Opts: &ltngdbenginemodelsv3.IndexOpts{
					HasIdx:    true,
					ParentKey: byteValues.bsKey,
					IndexingKeys: [][]byte{
						byteValues.bsKey,
						byteValues.secondaryIndexBs,
						byteValues.extraUpsertIndex,
					},
				},
			}
			ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
			ops[3].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err = saga.NewListOperator(ops...).Operate()
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
				rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				_, err = rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should create item without index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			testUser := generateTestUser(t)
			byteValues := getValues(t, ts, testUser)

			traceID, err := uuid.NewV7()
			require.NoError(t, err)
			respSignal := make(chan error)
			itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
				Ctx:          ctx,
				RespSignal:   respSignal,
				TraceID:      traceID.String(),
				OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
				OpType:       ltngdbenginemodelsv3.OpTypeCreate,
				DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
				Item: &ltngdbenginemodelsv3.Item{
					Key:   byteValues.bsKey,
					Value: byteValues.bsValue,
				},
				Opts: &ltngdbenginemodelsv3.IndexOpts{
					HasIdx: false,
				},
			}
			ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
			err = saga.NewListOperator(ops...).Operate()
			assert.NoError(t, err)

			// verify created things
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
				err = ts.ltngEngine.serializer.Deserialize(bs, &fileData)
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
				rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				foundResult, err := rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
				assert.NoError(t, err)

				expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

				var fileData ltngdbenginemodelsv3.FileData
				err = ts.ltngEngine.serializer.Deserialize(foundResult.BS, &fileData)
				assert.NoError(t, err)
				assert.EqualValues(t, expectedFileData.Key, fileData.Key)
				assert.EqualValues(t, expectedFileData.Data, fileData.Data)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
				assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should fail to create item without index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			testUser := generateTestUser(t)
			byteValues := getValues(t, ts, testUser)

			traceID, err := uuid.NewV7()
			require.NoError(t, err)
			respSignal := make(chan error)
			itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
				Ctx:          ctx,
				RespSignal:   respSignal,
				TraceID:      traceID.String(),
				OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
				OpType:       ltngdbenginemodelsv3.OpTypeCreate,
				DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
				Item: &ltngdbenginemodelsv3.Item{
					Key:   byteValues.bsKey,
					Value: byteValues.bsValue,
				},
				Opts: &ltngdbenginemodelsv3.IndexOpts{
					HasIdx: false,
				},
			}
			ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
			ops[0].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err = saga.NewListOperator(ops...).Operate()
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
				rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				_, err = rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should fail to create relational data item without index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			testUser := generateTestUser(t)
			byteValues := getValues(t, ts, testUser)

			traceID, err := uuid.NewV7()
			require.NoError(t, err)
			respSignal := make(chan error)
			itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
				Ctx:          ctx,
				RespSignal:   respSignal,
				TraceID:      traceID.String(),
				OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
				OpType:       ltngdbenginemodelsv3.OpTypeCreate,
				DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
				Item: &ltngdbenginemodelsv3.Item{
					Key:   byteValues.bsKey,
					Value: byteValues.bsValue,
				},
				Opts: &ltngdbenginemodelsv3.IndexOpts{
					HasIdx: false,
				},
			}
			ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
			ops[1].Action.Do = func() error {
				return errorsx.New("test error")
			}
			err = saga.NewListOperator(ops...).Operate()
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
				rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
				assert.NoError(t, err)

				_, err = rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
				assert.Error(t, err)
				assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
			}

			// close database
			ts.ltngEngine.Close()
		})
	})

	t.Run("buildCreateItemInfoData - multiple items", func(t *testing.T) {
		t.Run("should create item with index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			userCount := 10
			testUsers := generateTestUsers(t, userCount)

			for _, testUser := range testUsers {
				byteValues := getValues(t, ts, testUser)

				traceID, err := uuid.NewV7()
				require.NoError(t, err)
				respSignal := make(chan error)
				itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
					Ctx:          ctx,
					RespSignal:   respSignal,
					TraceID:      traceID.String(),
					OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
					OpType:       ltngdbenginemodelsv3.OpTypeCreate,
					DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
					Item: &ltngdbenginemodelsv3.Item{
						Key:   byteValues.bsKey,
						Value: byteValues.bsValue,
					},
					Opts: &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: byteValues.bsKey,
						IndexingKeys: [][]byte{
							byteValues.bsKey,
							byteValues.secondaryIndexBs,
							byteValues.extraUpsertIndex,
						},
					},
				}
				ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
				err = saga.NewListOperator(ops...).Operate()
				assert.NoError(t, err)

				// verify created things
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
					err = ts.ltngEngine.serializer.Deserialize(bs, &fileData)
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
						err = ts.ltngEngine.serializer.Deserialize(bs, &fileData)
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
					err = ts.ltngEngine.serializer.Deserialize(bs, &fileData)
					assert.NoError(t, err)
					assert.EqualValues(t, expectedFileData.Key, fileData.Key)
					assert.EqualValues(t, expectedFileData.Data, fileData.Data)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
				}

				{ // relational data file
					rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.ltngEngine.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assert.EqualValues(t, expectedFileData.Key, fileData.Key)
					assert.EqualValues(t, expectedFileData.Data, fileData.Data)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
				}
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should fail to create item with index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			userCount := 10
			testUsers := generateTestUsers(t, userCount)

			for _, testUser := range testUsers {
				byteValues := getValues(t, ts, testUser)

				traceID, err := uuid.NewV7()
				require.NoError(t, err)
				respSignal := make(chan error)
				itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
					Ctx:          ctx,
					RespSignal:   respSignal,
					TraceID:      traceID.String(),
					OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
					OpType:       ltngdbenginemodelsv3.OpTypeCreate,
					DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
					Item: &ltngdbenginemodelsv3.Item{
						Key:   byteValues.bsKey,
						Value: byteValues.bsValue,
					},
					Opts: &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: byteValues.bsKey,
						IndexingKeys: [][]byte{
							byteValues.bsKey,
							byteValues.secondaryIndexBs,
							byteValues.extraUpsertIndex,
						},
					},
				}
				ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
				ops[0].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err = saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// verify created things
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
					rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					_, err = rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
				}
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should fail to create indexed items with index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			userCount := 10
			testUsers := generateTestUsers(t, userCount)

			for _, testUser := range testUsers {
				byteValues := getValues(t, ts, testUser)

				traceID, err := uuid.NewV7()
				require.NoError(t, err)
				respSignal := make(chan error)
				itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
					Ctx:          ctx,
					RespSignal:   respSignal,
					TraceID:      traceID.String(),
					OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
					OpType:       ltngdbenginemodelsv3.OpTypeCreate,
					DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
					Item: &ltngdbenginemodelsv3.Item{
						Key:   byteValues.bsKey,
						Value: byteValues.bsValue,
					},
					Opts: &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: byteValues.bsKey,
						IndexingKeys: [][]byte{
							byteValues.bsKey,
							byteValues.secondaryIndexBs,
							byteValues.extraUpsertIndex,
						},
					},
				}
				ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
				ops[1].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err = saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// verify created things
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
					rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					_, err = rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
				}
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should fail to create index list item with index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			userCount := 10
			testUsers := generateTestUsers(t, userCount)

			for _, testUser := range testUsers {
				byteValues := getValues(t, ts, testUser)

				traceID, err := uuid.NewV7()
				require.NoError(t, err)
				respSignal := make(chan error)
				itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
					Ctx:          ctx,
					RespSignal:   respSignal,
					TraceID:      traceID.String(),
					OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
					OpType:       ltngdbenginemodelsv3.OpTypeCreate,
					DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
					Item: &ltngdbenginemodelsv3.Item{
						Key:   byteValues.bsKey,
						Value: byteValues.bsValue,
					},
					Opts: &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: byteValues.bsKey,
						IndexingKeys: [][]byte{
							byteValues.bsKey,
							byteValues.secondaryIndexBs,
							byteValues.extraUpsertIndex,
						},
					},
				}
				ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
				ops[2].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err = saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// verify created things
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
					rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					_, err = rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
				}
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should fail to create relational data item with index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			userCount := 10
			testUsers := generateTestUsers(t, userCount)

			for _, testUser := range testUsers {
				byteValues := getValues(t, ts, testUser)

				traceID, err := uuid.NewV7()
				require.NoError(t, err)
				respSignal := make(chan error)
				itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
					Ctx:          ctx,
					RespSignal:   respSignal,
					TraceID:      traceID.String(),
					OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
					OpType:       ltngdbenginemodelsv3.OpTypeCreate,
					DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
					Item: &ltngdbenginemodelsv3.Item{
						Key:   byteValues.bsKey,
						Value: byteValues.bsValue,
					},
					Opts: &ltngdbenginemodelsv3.IndexOpts{
						HasIdx:    true,
						ParentKey: byteValues.bsKey,
						IndexingKeys: [][]byte{
							byteValues.bsKey,
							byteValues.secondaryIndexBs,
							byteValues.extraUpsertIndex,
						},
					},
				}
				ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
				ops[3].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err = saga.NewListOperator(ops...).Operate()
				assert.Error(t, err)

				// verify created things
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
					rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					_, err = rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
				}
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should create item without index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			userCount := 10
			testUsers := generateTestUsers(t, userCount)

			for _, testUser := range testUsers {
				byteValues := getValues(t, ts, testUser)

				traceID, err := uuid.NewV7()
				require.NoError(t, err)
				respSignal := make(chan error)
				itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
					Ctx:          ctx,
					RespSignal:   respSignal,
					TraceID:      traceID.String(),
					OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
					OpType:       ltngdbenginemodelsv3.OpTypeCreate,
					DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
					Item: &ltngdbenginemodelsv3.Item{
						Key:   byteValues.bsKey,
						Value: byteValues.bsValue,
					},
					Opts: &ltngdbenginemodelsv3.IndexOpts{
						HasIdx: false,
					},
				}
				ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
				err = saga.NewListOperator(ops...).Operate()
				assert.NoError(t, err)

				// verify created things
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
					err = ts.ltngEngine.serializer.Deserialize(bs, &fileData)
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
					rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					foundResult, err := rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
					assert.NoError(t, err)

					expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)

					var fileData ltngdbenginemodelsv3.FileData
					err = ts.ltngEngine.serializer.Deserialize(foundResult.BS, &fileData)
					assert.NoError(t, err)
					assert.EqualValues(t, expectedFileData.Key, fileData.Key)
					assert.EqualValues(t, expectedFileData.Data, fileData.Data)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
					assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
				}
			}

			// close database
			ts.ltngEngine.Close()
		})

		t.Run("should fail to create item without index", func(t *testing.T) {
			ts := initTestSuite(t)
			ctx, cancel := context.WithCancel(ts.ctx)
			defer cancel()

			cs := newCreateSaga(ctx, ts.ltngEngine.opSaga)

			storeInfo := createTestStore(t, ctx, ts)

			userCount := 10
			testUsers := generateTestUsers(t, userCount)

			for _, testUser := range testUsers {
				byteValues := getValues(t, ts, testUser)

				traceID, err := uuid.NewV7()
				require.NoError(t, err)
				respSignal := make(chan error)
				itemInfoData := &ltngdbenginemodelsv3.ItemInfoData{
					Ctx:          ctx,
					RespSignal:   respSignal,
					TraceID:      traceID.String(),
					OpNatureType: ltngdbenginemodelsv3.OpNatureTypeItem,
					OpType:       ltngdbenginemodelsv3.OpTypeCreate,
					DBMetaInfo:   storeInfo.ManagerStoreMetaInfo(),
					Item: &ltngdbenginemodelsv3.Item{
						Key:   byteValues.bsKey,
						Value: byteValues.bsValue,
					},
					Opts: &ltngdbenginemodelsv3.IndexOpts{
						HasIdx: false,
					},
				}
				ops := cs.buildCreateItemInfoData(ctx, itemInfoData)
				ops[1].Action.Do = func() error {
					return errorsx.New("test error")
				}
				err = saga.NewListOperator(ops...).Operate()
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
					rfi, err := ts.ltngEngine.loadRelationalItemStoreFromMemoryOrDisk(ctx, itemInfoData.DBMetaInfo)
					assert.NoError(t, err)

					_, err = rfi.RelationalFileManager.Find(ctx, itemInfoData.Item.Key)
					assert.Error(t, err)
					assert.ErrorIs(t, err, fileiomodels.KeyNotFoundError)
				}
			}

			// close database
			ts.ltngEngine.Close()
		})
	})
}
