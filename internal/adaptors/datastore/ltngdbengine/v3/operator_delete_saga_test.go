package ltngdbenginev3

import (
	"testing"
)

func initDeleteCascadeSagaTestSuite(t *testing.T) *testSuite {
	ts := initTestSuite(t)
	ts.cs = newCreateSaga(ts.ctx, ts.e.opSaga)

	return ts
}

func TestDeleteSagaCascade_buildCreateItemInfoData(t *testing.T) {
	t.Run("buildDeleteItemInfoData - single item", func(t *testing.T) {
		//t.Run("should delete cascade item with index", func(t *testing.T) {
		//	ts := initDeleteCascadeSagaTestSuite(t)
		//	itemInfoData := generateDeleteItemInfoData(t, ts, true)
		//	ops := ts.ds.buildDeleteItemInfoData(ts.ctx, itemInfoData.ItemInfoData)
		//	err := saga.NewListOperator(ops...).Operate()
		//	assert.NoError(t, err)
		//
		//	// check files
		//
		//	{ // item file
		//		itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
		//		itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		//			itemInfoData.DBMetaInfo.Path, itemStrKey)
		//
		//		fm, err := mmap.NewFileManager(itemFilePath)
		//		assert.NoError(t, err)
		//		defer func() {
		//			err = fm.Close()
		//			assert.NoError(t, err)
		//		}()
		//
		//		bs, err := fm.Read()
		//		assert.NoError(t, err)
		//
		//		expectedFileData := ltngdbenginemodelsv3.NewFileData(
		//			itemInfoData.DBMetaInfo, itemInfoData.Item)
		//
		//		var fileData ltngdbenginemodelsv3.FileData
		//		err = ts.e.serializer.Deserialize(bs, &fileData)
		//		assert.NoError(t, err)
		//		assert.EqualValues(t, expectedFileData.Key, fileData.Key)
		//		assert.EqualValues(t, expectedFileData.Data, fileData.Data)
		//		assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
		//		assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
		//	}
		//
		//	{ // indexed items files
		//		for _, indexKey := range itemInfoData.Opts.IndexingKeys {
		//			itemStrKey := hex.EncodeToString(indexKey)
		//			itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		//				itemInfoData.DBMetaInfo.IndexInfo().Path, itemStrKey)
		//
		//			fm, err := mmap.NewFileManager(itemFilePath)
		//			assert.NoError(t, err)
		//			defer func() {
		//				err = fm.Close()
		//				assert.NoError(t, err)
		//			}()
		//
		//			bs, err := fm.Read()
		//			assert.NoError(t, err)
		//
		//			expectedFileData := ltngdbenginemodelsv3.NewFileData(
		//				itemInfoData.DBMetaInfo,
		//				&ltngdbenginemodelsv3.Item{
		//					Key:   indexKey,
		//					Value: itemInfoData.Opts.ParentKey,
		//				})
		//
		//			var fileData ltngdbenginemodelsv3.FileData
		//			err = ts.e.serializer.Deserialize(bs, &fileData)
		//			assert.NoError(t, err)
		//			assert.EqualValues(t, expectedFileData.Key, fileData.Key)
		//			assert.EqualValues(t, expectedFileData.Data, fileData.Data)
		//			assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
		//			assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
		//		}
		//	}
		//
		//	{ // index list file
		//		itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
		//		itemFilePath := ltngdbenginemodelsv3.GetDataFilepath(
		//			itemInfoData.DBMetaInfo.IndexListInfo().Path, itemStrKey)
		//
		//		fm, err := mmap.NewFileManager(itemFilePath)
		//		assert.NoError(t, err)
		//		defer func() {
		//			err = fm.Close()
		//			assert.NoError(t, err)
		//		}()
		//
		//		bs, err := fm.Read()
		//		assert.NoError(t, err)
		//
		//		expectedFileData := ltngdbenginemodelsv3.NewFileData(
		//			itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
		//				Key:   itemInfoData.Item.Key,
		//				Value: bytes.Join(itemInfoData.Opts.IndexingKeys, []byte(ltngdbenginemodelsv3.BsSep)),
		//			})
		//
		//		var fileData ltngdbenginemodelsv3.FileData
		//		err = ts.e.serializer.Deserialize(bs, &fileData)
		//		assert.NoError(t, err)
		//		assert.EqualValues(t, expectedFileData.Key, fileData.Key)
		//		assert.EqualValues(t, expectedFileData.Data, fileData.Data)
		//		assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
		//		assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
		//	}
		//
		//	{ // relational data file
		//		rfi, err := ts.e.loadRelationalItemStoreFromMemoryOrDisk(ts.ctx, itemInfoData.DBMetaInfo)
		//		assert.NoError(t, err)
		//
		//		foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
		//		assert.NoError(t, err)
		//
		//		expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)
		//
		//		var fileData ltngdbenginemodelsv3.FileData
		//		err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
		//		assert.NoError(t, err)
		//		assert.EqualValues(t, expectedFileData.Key, fileData.Key)
		//		assert.EqualValues(t, expectedFileData.Data, fileData.Data)
		//		assert.EqualValues(t, expectedFileData.Header.StoreInfo.Path, fileData.Header.StoreInfo.Path)
		//		assert.EqualValues(t, expectedFileData.Header.StoreInfo.Name, fileData.Header.StoreInfo.Name)
		//	}
		//
		//	// check memory
		//
		//	{ // item memory
		//		itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
		//
		//		expectedFileData := ltngdbenginemodelsv3.NewFileData(
		//			itemInfoData.DBMetaInfo, itemInfoData.Item)
		//
		//		fileInfo, ok := ts.e.itemFileMapping.Get(itemInfoData.DBMetaInfo.LockStrWithKey(itemStrKey))
		//		assert.True(t, ok)
		//		assert.EqualValues(t, expectedFileData, fileInfo.FileData)
		//	}
		//
		//	{ // indexed items memory
		//		for _, indexKey := range itemInfoData.Opts.IndexingKeys {
		//			itemStrKey := hex.EncodeToString(indexKey)
		//
		//			expectedFileData := ltngdbenginemodelsv3.NewFileData(
		//				itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
		//					Key:   indexKey,
		//					Value: itemInfoData.Opts.ParentKey,
		//				})
		//
		//			fileInfo, ok := ts.e.itemFileMapping.Get(
		//				itemInfoData.DBMetaInfo.IndexInfo().LockStrWithKey(itemStrKey))
		//			assert.True(t, ok)
		//			assert.EqualValues(t, expectedFileData, fileInfo.FileData)
		//		}
		//	}
		//
		//	{ // index list item memory
		//		itemStrKey := hex.EncodeToString(itemInfoData.Item.Key)
		//
		//		expectedFileData := ltngdbenginemodelsv3.NewFileData(
		//			itemInfoData.DBMetaInfo, &ltngdbenginemodelsv3.Item{
		//				Key:   itemInfoData.Item.Key,
		//				Value: bytes.Join(itemInfoData.Opts.IndexingKeys, []byte(ltngdbenginemodelsv3.BsSep)),
		//			})
		//
		//		fileInfo, ok := ts.e.itemFileMapping.Get(
		//			itemInfoData.DBMetaInfo.IndexListInfo().LockStrWithKey(itemStrKey))
		//		assert.True(t, ok)
		//		assert.EqualValues(t, expectedFileData, fileInfo.FileData)
		//	}
		//
		//	{ // relational data item in memory
		//		lockStr := itemInfoData.DBMetaInfo.RelationalLockStr()
		//		rfi, ok := ts.e.relationalItemFileMapping.Get(lockStr)
		//		assert.True(t, ok)
		//
		//		foundResult, err := rfi.RelationalFileManager.Find(ts.ctx, itemInfoData.Item.Key)
		//		assert.NoError(t, err)
		//
		//		expectedFileData := ltngdbenginemodelsv3.NewFileData(itemInfoData.DBMetaInfo, itemInfoData.Item)
		//
		//		var fileData ltngdbenginemodelsv3.FileData
		//		err = ts.e.serializer.Deserialize(foundResult.BS, &fileData)
		//		assert.NoError(t, err)
		//		//assert.EqualValues(t, expectedFileData, fileData)
		//		_ = expectedFileData
		//	}
		//
		//	// close database
		//	ts.e.Close()
		//})
	})

	t.Run("buildDeleteItemInfoData - multiple items", func(t *testing.T) {
		t.Run("should delete cascade item with index", func(t *testing.T) {})
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
