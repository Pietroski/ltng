package v3

import (
	"context"
	"os"
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"

	v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdb/v3"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio/mmap"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

func (e *LTNGEngine) createStoreOnDisk(
	ctx context.Context,
	info *v4.StoreInfo,
) (fi *v4.FileInfo, err error) {
	file, err := osx.OpenCreateFile(v4.GetStatsFilepath(info.Path, info.Name))
	if err != nil {
		return nil, err
	}

	info.CreatedAt = time.Now().UTC().Unix()
	info.LastOpenedAt = info.CreatedAt
	fileData := &v4.FileData{
		Header: &v4.Header{
			StoreInfo: info,
		},
	}

	return e.writeFileDataToFile(ctx, file, fileData)
}

func (e *LTNGEngine) writeFileDataToFile(
	_ context.Context,
	file *os.File,
	fileData *v4.FileData,
) (*v4.FileInfo, error) {
	fm, err := mmap.NewFileManagerFromFile(file)
	if err != nil {
		return nil, errorsx.Wrap(err, "error mapping file store")
	}

	if _, err = fm.Write(fileData); err != nil {
		return nil, errorsx.Wrap(err, "error writing file store")
	}

	fi := &v4.FileInfo{
		FileManager: fm,
		File:        file,
		FileData:    fileData,
	}

	e.storeFileMapping.Set(fi.FileData.Header.StoreInfo.Name, fi)

	return fi, nil
}

func (e *LTNGEngine) loadStoreFromMemoryOrDisk(
	ctx context.Context,
	info *v4.StoreInfo,
) (fi *v4.FileInfo, err error) {
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
	info *v4.StoreInfo,
) (*v4.FileInfo, error) {
	file, err := osx.OpenFile(v4.GetStatsFilepath(info.Path, info.Name))
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

	var fileData v4.FileData
	if err = e.serializer.Deserialize(bs[4:], &fileData); err != nil {
		return nil, errorsx.Wrapf(err, "failed to deserialize '%s' store stats", info.Name)
	}
	fileData.Header.StoreInfo.LastOpenedAt = time.Now().UTC().Unix()

	return &v4.FileInfo{
		File:        file,
		FileData:    &fileData,
		FileManager: fm,
	}, err
}

// #####################################################################################################################

func (e *LTNGEngine) createOpenRelationalStatsStoreOnDisk(
	ctx context.Context,
) (*v4.RelationalFileInfo, error) {
	if fi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx); err == nil {
		return fi, nil
	}

	info := e.mngrStoreInfo
	file, err := osx.OpenCreateFile(v4.GetRelationalStatsFilepath(info.Path, info.Name))
	if err != nil {
		return nil, err
	}

	info.CreatedAt = time.Now().UTC().Unix()
	info.LastOpenedAt = info.CreatedAt
	fileData := &v4.FileData{
		Header: &v4.Header{
			StoreInfo: info,
		},
	}

	return e.writeRelationalFileDataToFile(ctx, file, fileData)
}

func (e *LTNGEngine) writeRelationalFileDataToFile(
	_ context.Context,
	file *os.File,
	fileData *v4.FileData,
) (*v4.RelationalFileInfo, error) {
	fm, err := mmap.NewRelationalFileManagerFromFile(file)
	if err != nil {
		return nil, errorsx.Wrap(err, "error mapping relational file store")
	}

	if _, err = fm.Write(fileData); err != nil {
		return nil, errorsx.Wrap(err, "error writing to relational file store")
	}

	fi := &v4.RelationalFileInfo{
		File:                  file,
		FileData:              fileData,
		RelationalFileManager: fm,
	}

	return fi, nil
}

func (e *LTNGEngine) loadRelationalStatsStoreFromMemoryOrDisk(
	ctx context.Context,
) (fi *v4.RelationalFileInfo, err error) {
	var ok bool
	info := e.mngrStoreInfo.RelationalInfo()

	fi, ok = e.relationalStoreFileMapping.Get(info.Name)
	if !ok {
		fi, err = e.loadRelationalStatsStoreFromDisk(ctx)
		if err != nil {
			return nil, errorsx.Wrapf(err, "failed to load %s store info from disk",
				info.Name)
		}

		e.relationalStoreFileMapping.Set(info.Name, fi)

		return fi, nil
	}

	return fi, nil
}

func (e *LTNGEngine) loadRelationalStatsStoreFromDisk(
	ctx context.Context,
) (fi *v4.RelationalFileInfo, err error) {
	info := e.mngrStoreInfo
	file, err := osx.OpenFile(v4.GetRelationalStatsFilepath(info.Path, info.Name))
	if err != nil {
		return nil, err
	}

	fi, err = e.getUpdatedRelationalFileInfo(ctx, file)
	if err != nil {
		return nil, err
	}

	return fi, nil
}

func (e *LTNGEngine) getUpdatedRelationalFileInfo(
	_ context.Context, file *os.File,
) (*v4.RelationalFileInfo, error) {
	rfm, err := mmap.NewRelationalFileManagerFromFile(file)
	if err != nil {
		return nil, err
	}

	bs, err := rfm.Read()
	if err != nil {
		return nil, err
	}

	var fileData v4.FileData
	if err = e.serializer.Deserialize(bs, &fileData); err != nil {
		return nil, errorsx.Wrapf(err, "failed to deserialize store stats header from relational file")
	}
	fileData.Header.StoreInfo.LastOpenedAt = time.Now().UTC().Unix()

	info := &v4.RelationalFileInfo{
		File:                  file,
		FileData:              &fileData,
		RelationalFileManager: rfm,
	}

	return info, nil
}

// #####################################################################################################################

func (e *LTNGEngine) insertRelationalStats(
	ctx context.Context,
	storeInfo *v4.StoreInfo,
) (err error) {
	fi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx)
	if err != nil {
		return errorsx.Wrapf(err, "error creating %s relational store",
			v4.DBManagerStoreInfo.RelationalInfo().Name)
	}

	if _, err = fi.File.Seek(0, 2); err != nil {
		return errorsx.Wrapf(err,
			"error seeking to the end of %s manager relational store",
			v4.DBManagerStoreInfo.RelationalInfo().Name)
	}

	fileData := &v4.FileData{
		Header: &v4.Header{
			StoreInfo: storeInfo,
		},
	}
	if _, err = e.writeRelationalFileDataToFile(ctx, fi.File, fileData); err != nil {
		return errorsx.Wrapf(err, "error creating %s store on relational store", storeInfo.Name)
	}

	return nil
}

func (e *LTNGEngine) upsertRelationalStats(
	ctx context.Context,
	storeInfo *v4.StoreInfo,
) (err error) {
	rfi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx)
	if err != nil {
		return errorsx.Wrapf(err, "error creating %s relational store",
			v4.DBManagerStoreInfo.RelationalInfo().Name)
	}

	fileData := &v4.FileData{
		Header: &v4.Header{
			StoreInfo: storeInfo,
		},
	}

	if _, err := rfi.RelationalFileManager.UpsertByKey(ctx, []byte(storeInfo.Name), fileData); err != nil {
		return errorsx.Wrapf(err, "error upserting %s store on relational store", storeInfo.Name)
	}

	return nil
}

// #####################################################################################################################
