package ltngdbenginev3

import (
	"context"
	"os"
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

func (e *LTNGEngine) loadRelationalStatsStoreFromMemoryOrDisk(ctx context.Context) (
	rfi *ltngdbenginemodelsv3.RelationalFileInfo, err error,
) {
	var ok bool
	info := e.mngrStoreInfo.RelationalInfo()

	rfi, ok = e.relationalStoreFileMapping.Get(info.Name)
	if !ok {
		rfi, err = e.loadRelationalStatsStoreFromDisk(ctx)
		if err != nil {
			return nil, errorsx.Wrapf(err, "failed to load %s store info from disk",
				info.Name)
		}
		e.relationalStoreFileMapping.Set(info.Name, rfi)

		return rfi, nil
	}

	return rfi, nil
}

func (e *LTNGEngine) loadRelationalStatsStoreFromDisk(_ context.Context) (
	*ltngdbenginemodelsv3.RelationalFileInfo, error,
) {
	info := e.mngrStoreInfo.RelationalInfo()
	file, err := osx.OpenFile(ltngdbenginemodelsv3.GetStatsFilepath(
		info.Path, ltngdbenginemodelsv3.RelationalDataStoreKey))
	if err != nil {
		return nil, err
	}

	rfm, err := mmap.NewRelationalFileManagerFromFile(file)
	if err != nil {
		return nil, err
	}

	bs, err := rfm.Read()
	if err != nil {
		return nil, err
	}

	var fileData ltngdbenginemodelsv3.FileData
	if err = e.serializer.Deserialize(bs, &fileData); err != nil {
		return nil, errorsx.Wrapf(err, "failed to deserialize store stats header from relational file")
	}
	fileData.Header.StoreInfo.LastOpenedAt = time.Now().UTC().Unix()

	rfi := &ltngdbenginemodelsv3.RelationalFileInfo{
		File:                  file,
		FileData:              &fileData,
		RelationalFileManager: rfm,
	}

	return rfi, nil
}

func (e *LTNGEngine) createOpenRelationalStatsStoreOnDisk(
	ctx context.Context,
) (*ltngdbenginemodelsv3.RelationalFileInfo, error) {
	if rfi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx); err == nil {
		return rfi, nil
	}

	info := e.mngrStoreInfo.RelationalInfo()
	file, err := osx.OpenCreateFile(ltngdbenginemodelsv3.GetStatsFilepath(
		info.Path, ltngdbenginemodelsv3.RelationalDataStoreKey))
	if err != nil {
		return nil, err
	}

	info.CreatedAt = time.Now().UTC().Unix()
	info.LastOpenedAt = info.CreatedAt
	fileData := &ltngdbenginemodelsv3.FileData{
		Header: &ltngdbenginemodelsv3.Header{
			StoreInfo: info,
		},
	}

	return e.writeRelationalFileDataToFile(ctx, file, fileData)
}

func (e *LTNGEngine) writeRelationalFileDataToFile(
	_ context.Context,
	file *os.File,
	fileData *ltngdbenginemodelsv3.FileData,
) (*ltngdbenginemodelsv3.RelationalFileInfo, error) {
	rfm, err := mmap.NewRelationalFileManagerFromFile(file)
	if err != nil {
		return nil, errorsx.Wrap(err, "error mapping relational file store")
	}

	if _, err = rfm.Write(fileData); err != nil {
		return nil, errorsx.Wrap(err, "error writing to relational file store")
	}

	rfi := &ltngdbenginemodelsv3.RelationalFileInfo{
		File:                  file,
		FileData:              fileData,
		RelationalFileManager: rfm,
	}
	e.relationalStoreFileMapping.Set(rfi.FileData.Header.StoreInfo.Name, rfi)

	return rfi, nil
}

func (e *LTNGEngine) insertRelationalStats(
	ctx context.Context,
	storeInfo *ltngdbenginemodelsv3.StoreInfo,
) (err error) {
	rfi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx)
	if err != nil {
		return errorsx.Wrapf(err, "error getting or opening %s relational store",
			e.mngrStoreInfo.Name)
	}

	fileData := &ltngdbenginemodelsv3.FileData{
		Header: &ltngdbenginemodelsv3.Header{
			StoreInfo: storeInfo,
		},
	}

	if _, err = rfi.RelationalFileManager.Find(ctx, []byte(storeInfo.Name)); err == nil {
		return ltngdbenginemodelsv3.ErrStoreAlreadyExists.Wrapf(err, "store already exists: %s", storeInfo.Name)
	}

	if _, err = rfi.RelationalFileManager.Write(fileData); err != nil {
		return errorsx.Wrapf(err, "error upserting %s store on relational store", storeInfo.Name)
	}

	return nil
}

func (e *LTNGEngine) upsertRelationalStats(
	ctx context.Context,
	storeInfo *ltngdbenginemodelsv3.StoreInfo,
) (err error) {
	rfi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx)
	if err != nil {
		return errorsx.Wrapf(err, "error getting or opening %s relational store",
			e.mngrStoreInfo.RelationalInfo().Name)
	}

	fileData := &ltngdbenginemodelsv3.FileData{
		Header: &ltngdbenginemodelsv3.Header{
			StoreInfo: storeInfo,
		},
	}

	if _, err = rfi.RelationalFileManager.UpsertByKey(ctx, []byte(storeInfo.Name), fileData); err != nil {
		return errorsx.Wrapf(err, "error upserting %s store on relational store", storeInfo.Name)
	}

	return nil
}

// #####################################################################################################################
