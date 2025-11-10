package v2

import (
	"context"
	"io"
	"os"
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
)

func (e *LTNGEngine) createStoreOnDisk(
	ctx context.Context,
	info *ltngdata.StoreInfo,
) (fi *ltngdata.FileInfo, err error) {
	file, err := e.fileManager.OpenCreateFile(ctx,
		ltngdata.GetStatsFilepath(info.Path, info.Name))
	if err != nil {
		return nil, err
	}

	info.CreatedAt = time.Now().UTC().Unix()
	info.LastOpenedAt = info.CreatedAt
	fileData := &ltngdata.FileData{
		Header: &ltngdata.Header{
			StoreInfo: info,
		},
	}

	return e.writeFileDataToFile(ctx, file, fileData)
}

func (e *LTNGEngine) writeFileDataToFile(
	ctx context.Context,
	file *os.File,
	fileData *ltngdata.FileData,
) (*ltngdata.FileInfo, error) {
	bs, err := e.fileManager.WriteToFile(ctx, file, fileData)
	if err != nil {
		return nil, errorsx.Wrap(err, "error writing file data")
	}

	fi := &ltngdata.FileInfo{
		File:       file,
		FileData:   fileData,
		HeaderSize: uint32(len(bs)),
		DataSize:   uint32(len(fileData.Data)),
	}

	e.storeFileMapping.Set(fi.FileData.Header.StoreInfo.Name, fi)

	return fi, nil
}

func (e *LTNGEngine) loadStoreFromMemoryOrDisk(
	ctx context.Context,
	info *ltngdata.StoreInfo,
) (fi *ltngdata.FileInfo, err error) {
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
	ctx context.Context,
	info *ltngdata.StoreInfo,
) (*ltngdata.FileInfo, error) {
	bs, file, err := e.fileManager.OpenReadWholeFile(ctx,
		ltngdata.GetStatsFilepath(info.Path, info.Name))
	if err != nil {
		return nil, err
	}

	var fileData ltngdata.FileData
	if err = e.serializer.Deserialize(bs, &fileData); err != nil {
		return nil, errorsx.Wrapf(err, "failed to deserialize '%s' store stats", info.Name)
	}
	fileData.Header.StoreInfo.LastOpenedAt = time.Now().UTC().Unix()

	return &ltngdata.FileInfo{
		File:       file,
		FileData:   &fileData,
		HeaderSize: uint32(len(bs)),
		DataSize:   uint32(len(fileData.Data)),
	}, err
}

// #####################################################################################################################

func (e *LTNGEngine) createOpenRelationalStatsStoreOnDisk(
	ctx context.Context,
) (*ltngdata.FileInfo, error) {
	if fi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx); err == nil {
		return fi, nil
	}

	info := e.mngrStoreInfo
	file, err := e.fileManager.OpenCreateFile(ctx,
		ltngdata.GetRelationalStatsFilepath(info.Path, info.Name))
	if err != nil {
		return nil, err
	}

	info.CreatedAt = time.Now().UTC().Unix()
	info.LastOpenedAt = info.CreatedAt
	fileData := &ltngdata.FileData{
		Header: &ltngdata.Header{
			StoreInfo: info,
		},
	}

	return e.writeRelationalFileDataToFile(ctx, file, fileData)
}

func (e *LTNGEngine) writeRelationalFileDataToFile(
	ctx context.Context,
	file *os.File,
	fileData *ltngdata.FileData,
) (*ltngdata.FileInfo, error) {
	bs, err := e.fileManager.WriteToRelationalFile(ctx, file, fileData)
	if err != nil {
		return nil, errorsx.Wrap(err, "error writing file data")
	}

	fi := &ltngdata.FileInfo{
		File:       file,
		FileData:   fileData,
		HeaderSize: uint32(len(bs)),
		DataSize:   uint32(len(fileData.Data)),
	}

	return fi, nil
}

func (e *LTNGEngine) loadRelationalStatsStoreFromMemoryOrDisk(
	ctx context.Context,
) (fi *ltngdata.FileInfo, err error) {
	var ok bool
	info := e.mngrStoreInfo.RelationalInfo()

	fi, ok = e.storeFileMapping.Get(info.Name)
	if !ok {
		fi, err = e.loadRelationalStatsStoreFromDisk(ctx)
		if err != nil {
			return nil, errorsx.Wrapf(err, "failed to load %s store info from disk",
				info.Name)
		}

		e.storeFileMapping.Set(info.Name, fi)

		return fi, nil
	}

	return fi, nil
}

func (e *LTNGEngine) loadRelationalStatsStoreFromDisk(
	ctx context.Context,
) (fi *ltngdata.FileInfo, err error) {
	info := e.mngrStoreInfo

	file, err := e.fileManager.OpenFile(ctx,
		ltngdata.GetRelationalStatsFilepath(info.Path, info.Name))
	if err != nil {
		return nil, err
	}

	fi, err = e.GetUpdatedRelationalFileInfo(ctx, file)
	if err != nil {
		return nil, err
	}

	return fi, nil
}

func (e *LTNGEngine) GetUpdatedRelationalFileInfo(
	ctx context.Context, file *os.File,
) (*ltngdata.FileInfo, error) {
	bs, err := e.fileManager.ReadRelationalRow(ctx, file)
	if err != nil {
		return nil, err
	}

	var fileData ltngdata.FileData
	if err = e.serializer.Deserialize(bs, &fileData); err != nil {
		return nil, errorsx.Wrapf(err, "failed to deserialize store stats header from relational file")
	}
	fileData.Header.StoreInfo.LastOpenedAt = time.Now().UTC().Unix()

	info := &ltngdata.FileInfo{
		File:       file,
		FileData:   &fileData,
		HeaderSize: uint32(len(bs)),
		DataSize:   uint32(len(fileData.Data)),
	}

	return info, nil
}

// #####################################################################################################################

func (e *LTNGEngine) updateRelationalStatsStoreFile(
	ctx context.Context, fileData *ltngdata.FileData,
) (*ltngdata.FileInfo, error) {
	fi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx)
	if err != nil {
		return nil, err
	}

	if err = e.updateRelationalStats(ctx, fi, fileData, []byte(fileData.Header.StoreInfo.Name)); err != nil {
		return nil, errorsx.Wrap(err, "failed to update store stats manager file")
	}

	return fi, nil
}

// #####################################################################################################################

func (e *LTNGEngine) deleteFromRelationalStats(
	ctx context.Context,
	fi *ltngdata.FileInfo,
	info *ltngdata.FileData,
	key []byte,
) (err error) {
	// TODO: consider putting file readers and writers and managers to a sync.pool
	reader, err := rw.NewFileReader(ctx, fi, false)
	if err != nil {
		return errorsx.Wrapf(err, "error creating %s file reader", fi.File.Name())
	}

	upTo, from, err := reader.FindInFile(ctx, key)
	if err != nil {
		return errorsx.Wrapf(err, "error finding key: '%s' in file %s", key, fi.File.Name())
	}

	if err = reader.SetCursor(ctx, 0); err != nil {
		return errorsx.Wrap(err, "error setting file cursor")
	}

	{
		tmpFile, err := e.fileManager.OpenCreateFile(ctx, ltngdata.GetTemporaryRelationalStatsFilepath(
			info.Header.StoreInfo.Path, info.Header.StoreInfo.Name))
		if err != nil {
			return err
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
			return errorsx.Wrap(err, "error copying first part of the file to tmp file")
		}

		//if _, err = e.fileManager.WriteToRelationalFile(ctx, tmpFile, info); err != nil {
		//	return errorsx.Wrapf(err, "error updating info into tmp file %s", tmpFile.Name())
		//}

		if _, err = fi.File.Seek(int64(from), 0); err != nil {
			return errorsx.Wrap(err, "error seeking to file")
		}

		if _, err = io.Copy(tmpFile, fi.File); err != nil {
			return errorsx.Wrap(err, "error copying second part of the file to tmp file")
		}

		if err = tmpFile.Close(); err != nil {
			return errorsx.Wrapf(err, "failed to close tmp file %s", tmpFile.Name())
		}

		if err = fi.File.Close(); err != nil {
			return errorsx.Wrapf(err, "failed to close file %s", fi.File.Name())
		}

		newFilePath := ltngdata.GetRelationalStatsFilepath(
			info.Header.StoreInfo.Path, info.Header.StoreInfo.Name)
		if err = osx.MvFile(ctx, tmpFile.Name(), newFilePath); err != nil {
			return errorsx.Wrap(err, "error moving tmp file")
		}

		file, err := e.fileManager.OpenCreateFile(ctx, newFilePath)
		if err != nil {
			return errorsx.Wrapf(err, "error opening %s relational file", fi.File.Name())
		}

		fi.File = file
		e.storeFileMapping.Set(fi.FileData.Header.StoreInfo.Name, fi)
	}

	return
}

func (e *LTNGEngine) updateRelationalStats(
	ctx context.Context,
	fi *ltngdata.FileInfo,
	info *ltngdata.FileData,
	key []byte,
) (err error) {
	// TODO: consider putting file readers and writers and managers to a sync.pool
	reader, err := rw.NewFileReader(ctx, fi, false)
	if err != nil {
		return errorsx.Wrapf(err, "error creating %s file reader", fi.File.Name())
	}

	upTo, from, err := reader.FindInFile(ctx, key)
	if err != nil {
		return errorsx.Wrapf(err, "error finding key: '%s' in file %s", key, fi.File.Name())
	}

	if err = reader.SetCursor(ctx, 0); err != nil {
		return errorsx.Wrap(err, "error setting file cursor")
	}

	{
		tmpFile, err := e.fileManager.OpenCreateFile(ctx, ltngdata.GetTemporaryRelationalStatsFilepath(
			info.Header.StoreInfo.Path, info.Header.StoreInfo.Name))
		if err != nil {
			return err
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
			return errorsx.Wrap(err, "error copying first part of the file to tmp file")
		}

		if _, err = e.fileManager.WriteToRelationalFile(ctx, tmpFile, info); err != nil {
			return errorsx.Wrapf(err, "error updating info into tmp file %s", tmpFile.Name())
		}

		if _, err = fi.File.Seek(int64(from), 0); err != nil {
			return errorsx.Wrap(err, "error seeking to file")
		}

		if _, err = io.Copy(tmpFile, fi.File); err != nil {
			return errorsx.Wrap(err, "error copying second part of the file to tmp file")
		}

		if err = tmpFile.Sync(); err != nil {
			return errorsx.Wrapf(err, "failed to sync file - %s", tmpFile.Name())
		}

		if err = tmpFile.Close(); err != nil {
			return errorsx.Wrapf(err, "failed to close tmp file %s", tmpFile.Name())
		}

		if err = fi.File.Close(); err != nil {
			return errorsx.Wrapf(err, "failed to close file %s", fi.File.Name())
		}

		newFilePath := ltngdata.GetRelationalStatsFilepath(
			info.Header.StoreInfo.Path, info.Header.StoreInfo.Name)
		if err = osx.MvFile(ctx, tmpFile.Name(), newFilePath); err != nil {
			return errorsx.Wrap(err, "error moving tmp file")
		}

		file, err := e.fileManager.OpenCreateFile(ctx, newFilePath)
		if err != nil {
			return errorsx.Wrapf(err, "error opening %s relational file", fi.File.Name())
		}

		fi.File = file
		e.storeFileMapping.Set(fi.FileData.Header.StoreInfo.Name, fi)
	}

	return nil
}

func (e *LTNGEngine) upsertRelationalStats(
	ctx context.Context,
	fi *ltngdata.FileInfo,
	info *ltngdata.FileData,
	key []byte,
) error {
	if err := e.updateRelationalStats(ctx, fi, info, key); err != nil {
		if errorsx.Is(err, rw.KeyNotFoundError) {
			if _, err = fi.File.Seek(0, 2); err != nil {
				return errorsx.Wrap(err, "error seeking to file")
			}

			if _, err = e.writeRelationalFileDataToFile(ctx, fi.File, info); err != nil {
				return errorsx.Wrapf(err, "error upserting info into tmp file %s", fi.File.Name())
			}
		}

		return err
	}

	return nil
}

func (e *LTNGEngine) insertRelationalStats(
	ctx context.Context,
	storeInfo *ltngdata.StoreInfo,
) (err error) {
	fi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx)
	if err != nil {
		return errorsx.Wrapf(err, "error creating %s relational store",
			ltngdata.DBManagerStoreInfo.RelationalInfo().Name)
	}

	if _, err = fi.File.Seek(0, 2); err != nil {
		return errorsx.Wrapf(err,
			"error seeking to the end of %s manager relational store",
			ltngdata.DBManagerStoreInfo.RelationalInfo().Name)
	}

	fileData := &ltngdata.FileData{
		Header: &ltngdata.Header{
			StoreInfo: storeInfo,
		},
	}
	if _, err = e.writeRelationalFileDataToFile(ctx, fi.File, fileData); err != nil {
		return errorsx.Wrapf(err, "error creating %s store on relational store", storeInfo.Name)
	}

	return nil
}

// #####################################################################################################################
