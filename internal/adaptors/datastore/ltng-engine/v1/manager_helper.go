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
	info *StoreInfo,
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

// #####################################################################################################################

func (e *LTNGEngine) createOrOpenRelationalStatsStoreOnDisk(
	ctx context.Context,
) (*fileInfo, error) {
	info := dbManagerStoreInfo.RelationalInfo()
	if fi, err := e.loadRelationalStoreFromMemoryOrDisk(ctx); err == nil {
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
	fileData := &FileData{
		Header: &Header{
			StoreInfo: info,
		},
	}
	return e.writeRelationalStatsStoreToFile(ctx, file, fileData)
}

func (e *LTNGEngine) writeRelationalStatsStoreToFile(
	ctx context.Context,
	file *os.File,
	fileData *FileData,
) (*fileInfo, error) {
	bs, err := e.writeToRelationalFile(ctx, file, fileData)
	if err != nil {
		return nil, fmt.Errorf("error writing file data: %v", err)
	}

	fi := &fileInfo{
		File:       file,
		FileData:   fileData,
		HeaderSize: uint32(len(bs)),
		DataSize:   uint32(len(fileData.Data)),
	}

	return fi, nil
}

// #####################################################################################################################

func (e *LTNGEngine) createStatsStoreOnDisk(
	ctx context.Context,
	info *StoreInfo,
) (*fileInfo, error) {
	file, err := os.OpenFile(
		getStatsFilepath(info.Name),
		os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, dbFilePerm,
	)
	if err != nil {
		return nil, err
	}

	info.CreatedAt = time.Now().UTC().Unix()
	info.LastOpenedAt = info.CreatedAt
	fileData := &FileData{
		Header: &Header{
			StoreInfo: info,
		},
	}
	return e.writeStatsStoreToFile(ctx, file, fileData)
}

func (e *LTNGEngine) writeStatsStoreToFile(
	ctx context.Context,
	file *os.File,
	fileData *FileData,
) (*fileInfo, error) {
	bs, err := e.writeToFile(ctx, file, fileData)
	if err != nil {
		return nil, fmt.Errorf("error writing file data: %v", err)
	}

	fi := &fileInfo{
		File:       file,
		FileData:   fileData,
		HeaderSize: uint32(len(bs)),
		DataSize:   uint32(len(fileData.Data)),
	}
	e.storeFileMapping[fi.FileData.Header.StoreInfo.Name] = fi

	return fi, nil
}

// #####################################################################################################################

func (e *LTNGEngine) loadStoreFromMemoryOrDisk(
	ctx context.Context,
	info *StoreInfo,
) (*fileInfo, error) {
	value, ok := e.storeFileMapping[info.Name]
	if !ok {
		fi, err := e.loadStoreStatsFromDisk(ctx, info)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to load %s store info from disk: %v",
				info.Name, err)
		}

		e.storeFileMapping[info.Name] = fi

		return fi, nil
	}

	return value, nil
}

func (e *LTNGEngine) loadStoreStatsFromDisk(
	ctx context.Context,
	info *StoreInfo,
) (*fileInfo, error) {
	bs, file, err := e.openReadWholeFile(ctx, getStatsFilepath(info.Name))
	if err != nil {
		return nil, err
	}

	var fileData FileData
	if err = e.serializer.Deserialize(bs, &fileData); err != nil {
		return nil, fmt.Errorf("failed to deserialize store stats: %v", err)
	}
	fileData.Header.StoreInfo.LastOpenedAt = time.Now().UTC().Unix()

	err = file.Truncate(0)
	if err != nil {
		return nil, fmt.Errorf("failed to truncate store stats file: %v", err)
	}

	fi, err := e.writeStatsStoreToFile(ctx, file, &fileData)
	if err != nil {
		return nil, fmt.Errorf("error writing store stats file: %v", err)
	}

	if _, err = e.updateRelationalStatsFile(ctx, fi.FileData); err != nil {
		return nil, fmt.Errorf("error updateRelationalStatsFile: %v", err)
	}

	return fi, err
}

// #####################################################################################################################

func (e *LTNGEngine) loadRelationalStoreFromMemoryOrDisk(
	ctx context.Context,
) (*fileInfo, error) {
	value, ok := e.storeFileMapping[dbManagerStoreInfo.RelationalInfo().Name]
	if !ok {
		fi, err := e.loadRelationalStoreStatsFromDisk(ctx)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to load %s store info from disk: %v",
				dbManagerStoreInfo.RelationalInfo().Name, err)
		}

		e.storeFileMapping[dbManagerStoreInfo.RelationalInfo().Name] = fi

		return fi, nil
	}

	return value, nil
}

func (e *LTNGEngine) loadRelationalStoreStatsFromDisk(
	ctx context.Context,
) (*fileInfo, error) {
	file, err := e.openFile(ctx, getStatsFilepath(dbManagerStoreInfo.RelationalInfo().Name))
	if err != nil {
		return nil, err
	}

	fi, err := e.getRelationalFileInfo(ctx, file)
	if err != nil {
		return nil, err
	}

	if err = e.updateRelationalStats(
		ctx, fi, []byte(fi.FileData.Header.StoreInfo.Name), fi.FileData,
	); err != nil {
		return nil, fmt.Errorf("failed to update store stats manager file: %v", err)
	}

	return fi, nil
}

func (e *LTNGEngine) updateRelationalStatsFile(
	ctx context.Context, fileData *FileData,
) (*fileInfo, error) {
	fi, err := e.loadRelationalStoreFromMemoryOrDisk(ctx)
	if err != nil {
		return nil, err
	}

	if err = e.updateRelationalStats(
		ctx, fi, []byte(fileData.Header.StoreInfo.Name), fileData,
	); err != nil {
		return nil, fmt.Errorf("failed to update store stats manager file: %v", err)
	}

	return fi, nil
}

// #####################################################################################################################

// TODO: change the tmp place for the relational stats file

func (e *LTNGEngine) deleteFromRelationalStats(
	ctx context.Context,
	fi *fileInfo,
	key []byte,
) (err error) {
	reader, err := newFileReader(ctx, fi, true)
	if err != nil {
		return fmt.Errorf("error creating %s file reader: %w",
			fi.File.Name(), err)
	}

	var deleted bool
	var upTo, from uint32
	for {
		var bs []byte
		bs, err = reader.read(ctx)
		if err != nil {
			if err == io.EOF {
				break
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
		if err = os.Remove(getStatsFilepath(fi.FileData.Header.StoreInfo.TmpRelationalInfo().Name)); err != nil {
			return fmt.Errorf(
				"error removing unecessary tmp %s file that failed to seek: %w",
				fi.FileData.Header.StoreInfo.Name, err)
		}

		return fmt.Errorf("error seeking to file: %w", err)
	}

	var tmpFile *os.File
	copyToTmpFile := func() error {
		tmpFile, err = os.OpenFile(
			getStatsFilepath(fi.FileData.Header.StoreInfo.TmpRelationalInfo().Name),
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

		if err = tmpFile.Close(); err != nil {
			return fmt.Errorf("failed to sync file - %s | err: %v", tmpFile.Name(), err)
		}

		return nil
	}
	discardTmpFile := func() error {
		if _, err = fi.File.Seek(0, 0); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}

		if err = os.Remove(getStatsFilepath(fi.FileData.Header.StoreInfo.TmpRelationalInfo().Name)); err != nil {
			return fmt.Errorf("error removing unecessary tmp %s file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		return nil
	}

	removeMainFile := func() error {
		if err = fi.File.Close(); err != nil {
			return fmt.Errorf("error closing original file: %w", err)
		}

		if err = os.Remove(getStatsFilepath(fi.FileData.Header.StoreInfo.Name)); err != nil {
			return fmt.Errorf("error removing %s file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		return nil
	}
	reopenMainFile := func() error {
		fi, err = e.loadStoreFromMemoryOrDisk(ctx, dbManagerStoreInfo.RelationalInfo())
		if err != nil {
			return fmt.Errorf(
				"error re-opening %s manager relational store: %w",
				fi.FileData.Header.StoreInfo.Name, err)
		}

		e.storeFileMapping[fi.FileData.Header.StoreInfo.Name] = fi

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
			getStatsFilepath(fi.FileData.Header.StoreInfo.TmpRelationalInfo().Name),
			getStatsFilepath(fi.FileData.Header.StoreInfo.Name),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		var file *os.File
		file, err = e.openFile(ctx, getStatsFilepath(fi.FileData.Header.StoreInfo.Name))
		if err != nil {
			return fmt.Errorf("error opening %s file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		if _, ok := e.storeFileMapping[fi.FileData.Header.StoreInfo.Name]; !ok {
			e.storeFileMapping[fi.FileData.Header.StoreInfo.Name] = fi
		}
		fi.File = file
		e.storeFileMapping[fi.FileData.Header.StoreInfo.Name].File = file
	}

	return
}

func (e *LTNGEngine) updateRelationalStats(
	ctx context.Context,
	fi *fileInfo,
	key []byte,
	info *FileData,
) (err error) {
	reader, err := newFileReader(ctx, fi, false)
	if err != nil {
		return fmt.Errorf("error creating %s file reader: %w",
			fi.File.Name(), err)
	}

	var found bool
	var upTo, from uint32
	for {
		var bs []byte
		bs, err = reader.read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("error reading %s file: %w", fi.File.Name(), err)
		}

		if bytes.Contains(bs, key) {
			found = true
			from = reader.yield()
			upTo = from - uint32(len(bs)+4)
			break
		}
	}

	if !found {
		return fmt.Errorf("key '%s' not found: key not found", key)
	}

	if _, err = fi.File.Seek(0, 0); err != nil {
		if err = os.Remove(getStatsFilepath(fi.FileData.Header.StoreInfo.TmpRelationalInfo().Name)); err != nil {
			return fmt.Errorf("error removing unecessary tmp %s file that failed to seek: %w",
				fi.FileData.Header.StoreInfo.Name, err)
		}

		return fmt.Errorf("error seeking to file: %w", err)
	}

	var tmpFile *os.File
	copyToTmpFile := func() error {
		tmpFile, err = os.OpenFile(
			getStatsFilepath(fi.FileData.Header.StoreInfo.TmpRelationalInfo().Name),
			os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL|os.O_TRUNC, dbFilePerm,
		)
		if err != nil {
			return err
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
			return fmt.Errorf("error copying first part of the file to tmp file: %w", err)
		}

		if _, err = e.writeToRelationalFileWithNoSeek(ctx, tmpFile, info); err != nil {
			return fmt.Errorf("error updating info into tmp file %s: %w", tmpFile.Name(), err)
		}

		if _, err = fi.File.Seek(int64(from), 0); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}

		if _, err = io.Copy(tmpFile, fi.File); err != nil {
			return fmt.Errorf("error copying second part of the file to tmp file: %w", err)
		}

		if err = tmpFile.Close(); err != nil {
			return fmt.Errorf("failed to sync file - %s | err: %v", tmpFile.Name(), err)
		}

		return nil
	}
	discardTmpFile := func() error {
		if _, err = fi.File.Seek(0, 0); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}

		if err = os.Remove(getStatsFilepath(fi.FileData.Header.StoreInfo.TmpRelationalInfo().Name)); err != nil {
			return fmt.Errorf("error removing unecessary tmp %s file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		return nil
	}

	removeMainFile := func() error {
		//// TODO: remove it if possible
		//if err = fi.File.Close(); err != nil {
		//	return fmt.Errorf("error closing original file: %w", err)
		//}

		if err = os.Remove(getStatsFilepath(fi.FileData.Header.StoreInfo.Name)); err != nil {
			return fmt.Errorf("error removing %s file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		return nil
	}
	reopenMainFile := func() error {
		fi, err = e.loadRelationalStoreFromMemoryOrDisk(ctx)
		if err != nil {
			return fmt.Errorf("error re-opening %s manager relational store: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		e.storeFileMapping[fi.FileData.Header.StoreInfo.Name] = fi

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
			getStatsFilepath(fi.FileData.Header.StoreInfo.TmpRelationalInfo().Name),
			getStatsFilepath(fi.FileData.Header.StoreInfo.Name),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		var file *os.File
		file, err = e.openFile(ctx, getStatsFilepath(fi.FileData.Header.StoreInfo.Name))
		if err != nil {
			return fmt.Errorf("error opening %s file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		fi.File = file
		if _, ok := e.storeFileMapping[fi.FileData.Header.StoreInfo.Name]; !ok {
			e.storeFileMapping[fi.FileData.Header.StoreInfo.Name] = fi
		}
		e.storeFileMapping[fi.FileData.Header.StoreInfo.Name].File = file
	}

	return
}

func (e *LTNGEngine) upsertRelationalStats(
	ctx context.Context,
	fi *fileInfo,
	key []byte,
	info *FileData,
) (err error) {
	reader, err := newFileReader(ctx, fi, true)
	if err != nil {
		return fmt.Errorf("error creating %s file reader: %w",
			fi.File.Name(), err)
	}

	var found bool
	var upTo, from uint32
	for {
		var bs []byte
		bs, err = reader.read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("error reading %s file: %w", fi.File.Name(), err)
		}

		if bytes.Contains(bs, key) {
			found = true
			from = reader.yield()
			upTo = from - uint32(len(bs)+4)
			break
		}
	}

	if !found {
		if _, err = e.writeRelationalStatsStoreToFile(ctx, fi.File, info); err != nil {
			return fmt.Errorf("error upserting info into tmp file %s: %w", fi.File.Name(), err)
		}

		return nil
	}

	if _, err = fi.File.Seek(0, 0); err != nil {
		if err = os.Remove(getStatsFilepath(fi.FileData.Header.StoreInfo.TmpRelationalInfo().Name)); err != nil {
			return fmt.Errorf("error removing unecessary tmp %s file that failed to seek: %w",
				fi.FileData.Header.StoreInfo.Name, err)
		}

		return fmt.Errorf("error seeking to file: %w", err)
	}

	var tmpFile *os.File
	copyToTmpFile := func() error {
		tmpFile, err = os.OpenFile(
			getStatsFilepath(fi.FileData.Header.StoreInfo.TmpRelationalInfo().Name),
			os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, dbFilePerm,
		)
		if err != nil {
			return err
		}

		// example pair (upTo - from) | 102 - 154
		if _, err = io.CopyN(tmpFile, fi.File, int64(upTo)); err != nil {
			return fmt.Errorf("error copying first part of the file to tmp file: %w", err)
		}

		if _, err = e.writeToRelationalFileWithNoSeek(ctx, tmpFile, info); err != nil {
			return fmt.Errorf("error updating info into tmp file %s: %w", tmpFile.Name(), err)
		}

		if _, err = fi.File.Seek(int64(from), 0); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}

		if _, err = io.Copy(tmpFile, fi.File); err != nil {
			return fmt.Errorf("error copying second part of the file to tmp file: %w", err)
		}

		if err = tmpFile.Close(); err != nil {
			return fmt.Errorf("failed to sync file - %s | err: %v", tmpFile.Name(), err)
		}

		return nil
	}
	discardTmpFile := func() error {
		if _, err = fi.File.Seek(0, 0); err != nil {
			return fmt.Errorf("error seeking to file: %w", err)
		}

		if err = os.Remove(getStatsFilepath(fi.FileData.Header.StoreInfo.TmpRelationalInfo().Name)); err != nil {
			return fmt.Errorf("error removing unecessary tmp %s file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		return nil
	}

	removeMainFile := func() error {
		if err = fi.File.Close(); err != nil {
			return fmt.Errorf("error closing original file: %w", err)
		}

		if err = os.Remove(getStatsFilepath(fi.FileData.Header.StoreInfo.Name)); err != nil {
			return fmt.Errorf("error removing %s file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		return nil
	}
	reopenMainFile := func() error {
		fi, err = e.loadStoreFromMemoryOrDisk(ctx, dbManagerStoreInfo.RelationalInfo())
		if err != nil {
			return fmt.Errorf("error re-opening %s manager relational store: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		e.storeFileMapping[fi.FileData.Header.StoreInfo.Name] = fi

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
			getStatsFilepath(fi.FileData.Header.StoreInfo.TmpRelationalInfo().Name),
			getStatsFilepath(fi.FileData.Header.StoreInfo.Name),
		); err != nil {
			return fmt.Errorf("error renaming tmp file: %w", err)
		}

		var file *os.File
		file, err = e.openFile(ctx, getStatsFilepath(fi.FileData.Header.StoreInfo.Name))
		if err != nil {
			return fmt.Errorf("error opening %s file: %w", fi.FileData.Header.StoreInfo.Name, err)
		}

		if _, ok := e.storeFileMapping[fi.FileData.Header.StoreInfo.Name]; !ok {
			e.storeFileMapping[fi.FileData.Header.StoreInfo.Name] = fi
		}
		fi.File = file
		e.storeFileMapping[fi.FileData.Header.StoreInfo.Name].File = file
	}

	return
}

func (e *LTNGEngine) insertRelationalStats(
	ctx context.Context,
	storeInfo *StoreInfo,
) (err error) {
	fi, err := e.loadRelationalStoreFromMemoryOrDisk(ctx)
	if err != nil {
		return fmt.Errorf("error creating %s relational store: %w",
			dbManagerStoreInfo.RelationalInfo().Name, err)
	}

	if _, err = fi.File.Seek(0, 2); err != nil {
		return fmt.Errorf("error seeking to the end of %s manager relational store: %w",
			dbManagerStoreInfo.RelationalInfo().Name, err)
	}

	fileData := &FileData{
		Header: &Header{
			StoreInfo: storeInfo,
		},
	}
	if _, err = e.writeRelationalStatsStoreToFile(ctx, fi.File, fileData); err != nil {
		return fmt.Errorf("error creating %s store on relational store: %w", storeInfo.Name, err)
	}

	return nil
}

// #####################################################################################################################
