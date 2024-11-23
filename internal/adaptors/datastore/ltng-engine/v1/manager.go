package v1

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	lo "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/list-operator"
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
) (*fileInfo, error) {
	if fi, err := e.loadStoreFromMemoryOrDisk(ctx, info); err == nil {
		return fi, nil
	}

	file, err := os.OpenFile(
		getStatsFilepath(info.Name),
		os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, dbFilePerm,
	)
	if err != nil {
		return nil, err
	}

	info.CreatedAt = time.Now().UTC().Unix()
	info.LastOpenedAt = info.CreatedAt
	return e.writeStoreStatsToFile(ctx, file, info)
}

func (e *LTNGEngine) writeStoreStatsToFile(
	ctx context.Context,
	file *os.File,
	info *DBInfo,
) (*fileInfo, error) {
	fileData := &FileData{
		FileStats: info,
		Data:      nil,
	}

	bs, err := e.writeToFile(ctx, file, fileData)
	if err != nil {
		return nil, fmt.Errorf("error writing file data: %v", err)
	}

	fi := &fileInfo{
		DBInfo:     info,
		File:       file,
		HeaderSize: uint32(len(bs)),
	}

	e.fileStoreMapping[info.Name] = fi

	return fi, nil
}

func (e *LTNGEngine) loadStoreFromMemoryOrDisk(
	ctx context.Context,
	info *DBInfo,
) (*fileInfo, error) {
	e.opMtx.Lock(info.Name, struct{}{})
	defer e.opMtx.Unlock(info.Name)

	value, ok := e.fileStoreMapping[info.Name]
	if !ok {
		fi, err := e.loadStoreStatsFromDisk(ctx, info)
		if err != nil {
			return nil, fmt.Errorf("failed to load %s store info from disk: %v", info.Name, err)
		}

		return fi, nil
	}

	return value, nil
}

func (e *LTNGEngine) loadStoreStatsFromDisk(
	ctx context.Context,
	info *DBInfo,
) (*fileInfo, error) {
	bs, file, err := e.openReadFile(ctx, getStatsFilepath(info.Name))
	if err != nil {
		return nil, err
	}

	var fileData FileData
	if err = e.serializer.Deserialize(bs, &fileData); err != nil {
		return nil, fmt.Errorf("failed to deserialize store stats: %v", err)
	}

	fileData.FileStats.LastOpenedAt = time.Now().UTC().Unix()
	return e.writeStoreStatsToFile(ctx, file, fileData.FileStats)
}

func (e *LTNGEngine) deleteFromRelationalStats(
	ctx context.Context,
	fi *fileInfo,
	key []byte,
) (err error) {
	reader, err := newFileReader(ctx, fi)
	if err != nil {
		return fmt.Errorf("error creating %s file reader: %w",
			fi.File.Name(), err)
	}

	var deleted bool
	var upTo, from uint32
	for {
		bs, err := reader.read(ctx)
		if err != nil {
			if err == io.EOF {
				return nil
			}

			return fmt.Errorf("error reading %s file: %w", fi.File.Name(), err)
		}

		if bytes.Contains(bs, key) {
			deleted = true
			from = reader.yield()
			upTo = from - uint32(len(bs)+4)
			break
		}
	}

	if !deleted {
		return fmt.Errorf("key %v not deleted: key not found", key)
	}

	if _, err = fi.File.Seek(0, 0); err != nil {
		if err = os.Remove(getStatsFilepath(fi.DBInfo.TmpRelationalInfo().Name)); err != nil {
			return fmt.Errorf("error removing unecessary tmp %s file that failed to seek: %w", fi.DBInfo.Name, err)
		}

		return fmt.Errorf("error seeking to file: %w", err)
	}

	var tmpFile *os.File
	copyToTmpFile := func() error {
		tmpFile, err = os.OpenFile(
			getStatsFilepath(fi.DBInfo.TmpRelationalInfo().Name),
			os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, dbFilePerm,
		)
		if err != nil {
			return err
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
			return fmt.Errorf("error copying first part of the file to tmp file: %w", err)
		}

		if _, err = fi.File.Seek(int64(from), 0); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}

		if _, err = io.Copy(tmpFile, fi.File); err != nil {
			return fmt.Errorf("error copying second part of the file to tmp file: %w", err)
		}

		return nil
	}
	discardTmpFile := func() error {
		if _, err = fi.File.Seek(0, 0); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}

		if err = os.Remove(getStatsFilepath(fi.DBInfo.TmpRelationalInfo().Name)); err != nil {
			return fmt.Errorf("error removing unecessary tmp %s file: %w", fi.DBInfo.Name, err)
		}

		return nil
	}

	removeMainFile := func() error {
		if err = fi.File.Close(); err != nil {
			return fmt.Errorf("error closing original file: %w", err)
		}

		if err = os.Remove(getStatsFilepath(fi.DBInfo.Name)); err != nil {
			return fmt.Errorf("error removing %s file: %w", fi.DBInfo.Name, err)
		}

		return nil
	}
	reopenMainFile := func() error {
		fi, err = e.loadStoreFromMemoryOrDisk(ctx, dbManagerInfo.RelationalInfo())
		if err != nil {
			return fmt.Errorf("error re-opening %s manager relational store: %w", fi.DBInfo.Name, err)
		}

		e.fileStoreMapping[fi.DBInfo.Name] = fi

		return nil
	}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         copyToTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: discardTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         removeMainFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: reopenMainFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}

	if err = lo.New(operations...).Operate(); err != nil {
		return err
	}

	{ // renaming tmp file
		if err = os.Rename(
			getStatsFilepath(fi.DBInfo.TmpRelationalInfo().Name),
			getStatsFilepath(fi.DBInfo.Name),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		if _, err = tmpFile.Seek(0, 0); err != nil {
			return fmt.Errorf("error seeking now main relation file: %w", err)
		}

		e.fileStoreMapping[fi.DBInfo.Name].File = tmpFile
	}

	return
}
