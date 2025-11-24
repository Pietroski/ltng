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

func (e *LTNGEngine) loadStatsStoreFromMemoryOrDisk(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) (fi *ltngdbenginemodelsv3.FileInfo, err error) {
	var ok bool
	fi, ok = e.storeFileMapping.Get(info.Name)
	if !ok {
		fi, err = e.loadStatsStoreFromDisk(ctx, info)
		if err != nil {
			return nil, errorsx.Wrapf(err, "failed to load '%s' store info from disk", info.Name)
		}

		e.storeFileMapping.Set(info.Name, fi)

		return fi, nil
	}

	return fi, nil
}

func (e *LTNGEngine) loadStatsStoreFromDisk(
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

func (e *LTNGEngine) createStatsStoreOnDisk(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) (fi *ltngdbenginemodelsv3.FileInfo, err error) {
	file, err := osx.OpenCreateFile(ltngdbenginemodelsv3.GetStatsFilepath(info.Path, info.Name))
	if err != nil {
		return nil, errorsx.Wrap(err, "failed to open/create file for stats store")
	}

	info.CreatedAt = time.Now().UTC().Unix()
	info.LastOpenedAt = info.CreatedAt
	fileData := &ltngdbenginemodelsv3.FileData{
		Header: &ltngdbenginemodelsv3.Header{
			StoreInfo: info,
		},
	}

	fi, err = e.writeFileDataToFile(ctx, file, fileData)
	if err != nil {
		return nil, errorsx.Wrap(err, "failed to write file data to file")
	}

	return fi, nil
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

func (e *LTNGEngine) deleteStatsStoreFromDisk(
	_ context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) error {
	if err := os.Remove(ltngdbenginemodelsv3.GetStatsFilepath(info.Path, info.Name)); err != nil {
		return errorsx.Wrapf(err, "failed to remove '%s' store stats file", info.Name)
	}

	e.storeFileMapping.Delete(info.Name)

	return nil
}

// #####################################################################################################################
