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

func (e *LTNGEngine) createStoreOnDisk(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) (fi *ltngdbenginemodelsv3.FileInfo, err error) {
	file, err := osx.OpenCreateFile(ltngdbenginemodelsv3.GetStatsFilepath(info.Path, info.Name))
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

	return e.writeFileDataToFile(ctx, file, fileData)
}

func (e *LTNGEngine) writeFileDataToFile(
	_ context.Context,
	file *os.File,
	fileData *ltngdbenginemodelsv3.FileData,
) (*ltngdbenginemodelsv3.FileInfo, error) {
	fm, err := mmap.NewFileManagerFromFile(file)
	if err != nil {
		return nil, errorsx.Wrap(err, "error mapping file store")
	}

	if _, err = fm.Write(fileData); err != nil {
		return nil, errorsx.Wrap(err, "error writing file store")
	}

	fi := &ltngdbenginemodelsv3.FileInfo{
		FileManager: fm,
		File:        file,
		FileData:    fileData,
	}

	e.storeFileMapping.Set(fi.FileData.Header.StoreInfo.Name, fi)

	return fi, nil
}

func (e *LTNGEngine) loadStoreFromMemoryOrDisk(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) (fi *ltngdbenginemodelsv3.FileInfo, err error) {
	var ok bool
	fi, ok = e.storeFileMapping.Get(info.Name)
	if !ok {
		fi, err = e.loadStoreFromDisk(ctx, info)
		if err != nil {
			return nil, errorsx.Wrapf(err, "failed to load '%s' store info from disk", info.Name)
		}

		e.storeFileMapping.Set(info.Name, fi)

		return fi, nil
	}

	return fi, nil
}

func (e *LTNGEngine) loadStoreFromDisk(
	_ context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) (*ltngdbenginemodelsv3.FileInfo, error) {
	file, err := osx.OpenFile(ltngdbenginemodelsv3.GetStatsFilepath(info.Path, info.Name))
	if err != nil {
		return nil, err
	}

	fm, err := mmap.NewFileManagerFromFile(file)
	if err != nil {
		return nil, errorsx.Wrap(err, "error mapping file data")
	}

	bs, err := fm.Read()
	if err != nil {
		return nil, errorsx.Wrap(err, "error reading mapped file data")
	}

	var fileData ltngdbenginemodelsv3.FileData
	if err = e.serializer.Deserialize(bs, &fileData); err != nil {
		return nil, errorsx.Wrapf(err, "failed to deserialize '%s' store stats", info.Name)
	}
	fileData.Header.StoreInfo.LastOpenedAt = time.Now().UTC().Unix()

	return &ltngdbenginemodelsv3.FileInfo{
		File:        file,
		FileData:    &fileData,
		FileManager: fm,
	}, err
}

// #####################################################################################################################

func (e *LTNGEngine) createOpenRelationalStatsStoreOnDisk(
	ctx context.Context,
) (*ltngdbenginemodelsv3.RelationalFileInfo, error) {
	if rfi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx); err == nil {
		return rfi, nil
	}

	info := e.mngrStoreInfo.RelationalInfo()
	file, err := osx.OpenCreateFile(ltngdbenginemodelsv3.GetRelationalStatsFilepath(info.Path, ltngdbenginemodelsv3.RelationalDataStoreKey))
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
	e.relationalStoreFileMapping.Set(rfi.FileData.Header.StoreInfo.RelationalInfo().Name, rfi)

	return rfi, nil
}

func (e *LTNGEngine) loadRelationalStatsStoreFromMemoryOrDisk(
	ctx context.Context,
) (rfi *ltngdbenginemodelsv3.RelationalFileInfo, err error) {
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

func (e *LTNGEngine) loadRelationalStatsStoreFromDisk(
	ctx context.Context,
) (rfi *ltngdbenginemodelsv3.RelationalFileInfo, err error) {
	info := e.mngrStoreInfo
	file, err := osx.OpenFile(ltngdbenginemodelsv3.GetRelationalStatsFilepath(info.Path, ltngdbenginemodelsv3.RelationalDataStoreKey))
	if err != nil {
		return nil, err
	}

	rfi, err = e.getUpdatedRelationalFileInfo(ctx, file)
	if err != nil {
		return nil, err
	}

	return rfi, nil
}

func (e *LTNGEngine) getUpdatedRelationalFileInfo(
	_ context.Context, file *os.File,
) (*ltngdbenginemodelsv3.RelationalFileInfo, error) {
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

	info := &ltngdbenginemodelsv3.RelationalFileInfo{
		File:                  file,
		FileData:              &fileData,
		RelationalFileManager: rfm,
	}

	return info, nil
}

// #####################################################################################################################

func (e *LTNGEngine) insertRelationalStats(
	ctx context.Context,
	storeInfo *ltngdbenginemodelsv3.StoreInfo,
) (err error) {
	rfi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx)
	if err != nil {
		return errorsx.Wrapf(err, "error creating %s relational store",
			e.mngrStoreInfo.RelationalInfo().Name)
	}

	fileData := &ltngdbenginemodelsv3.FileData{
		Header: &ltngdbenginemodelsv3.Header{
			StoreInfo: storeInfo,
		},
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
		return errorsx.Wrapf(err, "error creating %s relational store",
			ltngdbenginemodelsv3.DBManagerStoreInfo.RelationalInfo().Name)
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
