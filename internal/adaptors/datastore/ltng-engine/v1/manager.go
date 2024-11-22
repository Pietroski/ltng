package v1

import (
	"context"
	"fmt"
	"os"
	"time"
)

func (e *LTNGEngine) createDataPathOnDisk(
	_ context.Context,
	info *DBInfo,
) error {
	path := getDataPath(info.Path)
	if err := os.MkdirAll(path, dbFilePerm); err != nil {
		return fmt.Errorf("error creating data directory %s: %v", path, err)
	}

	return nil
}

func (e *LTNGEngine) createStatsPathOnDisk(
	_ context.Context,
) error {
	path := getStatsPathWithSep()
	if err := os.MkdirAll(path, dbFilePerm); err != nil {
		return fmt.Errorf("error creating stats directory %s: %v", path, err)
	}

	return nil
}

func (e *LTNGEngine) createStoreStatsOnDisk(
	ctx context.Context,
	info *DBInfo,
) (*DBInfo, *os.File, error) {
	if dbInfo, file, err := e.loadStoreFromMemoryOrDisk(ctx, info); err == nil {
		return dbInfo, file, nil
	}

	file, err := os.OpenFile(
		getStatsFilepath(info.Name),
		os.O_CREATE|os.O_RDWR, dbFilePerm,
	)
	if err != nil {
		return nil, nil, err
	}

	info.CreatedAt = time.Now().UTC().Unix()
	info.LastOpenedAt = info.CreatedAt
	return e.writeStoreStatsToFile(ctx, file, info)
}

func (e *LTNGEngine) writeStoreStatsToFile(
	ctx context.Context,
	file *os.File,
	info *DBInfo,
) (*DBInfo, *os.File, error) {
	fileData := &FileData{
		FileStats: info,
		Data:      nil,
	}

	if _, err := e.writeToFile(ctx, file, fileData); err != nil {
		return nil, nil, fmt.Errorf("error writing file data: %v", err)
	}

	e.fileStoreMapping[info.Name] = &fileInfo{
		DBInfo: info,
		File:   file,
	}

	return info, file, nil
}

func (e *LTNGEngine) loadStoreFromMemoryOrDisk(
	ctx context.Context,
	info *DBInfo,
) (*DBInfo, *os.File, error) {
	e.opMtx.Lock(info.Name, struct{}{})
	defer e.opMtx.Unlock(info.Name)

	value, ok := e.fileStoreMapping[info.Name]
	if !ok {
		storeInfo, file, err := e.loadStoreStatsFromDisk(ctx, info)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load %s store info from disk: %v", info.Name, err)
		}

		e.fileStoreMapping[info.Name] = &fileInfo{
			DBInfo: storeInfo,
			File:   file,
		}

		return storeInfo, file, nil
	}

	return value.DBInfo, value.File, nil
}

func (e *LTNGEngine) loadStoreStatsFromDisk(
	ctx context.Context,
	info *DBInfo,
) (*DBInfo, *os.File, error) {
	bs, file, err := e.openReadFile(ctx, getStatsFilepath(info.Name))
	if err != nil {
		return nil, nil, err
	}

	var fileData FileData
	if err = e.serializer.Deserialize(bs, &fileData); err != nil {
		return nil, nil, fmt.Errorf("failed to deserialize store stats: %v", err)
	}

	fileData.FileStats.LastOpenedAt = time.Now().UTC().Unix()
	return e.writeStoreStatsToFile(ctx, file, fileData.FileStats)
}
