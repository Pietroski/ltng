package v3

import (
	"context"
	"io"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"

	v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdb/v3"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/fileio"
)

func (e *LTNGEngine) createStore(
	ctx context.Context,
	info *v4.StoreInfo,
) (*v4.StoreInfo, error) {
	e.kvLock.Lock(info.Name, struct{}{})
	defer e.kvLock.Unlock(info.Name)

	if info.Path == "" {
		return nil, errorsx.New("missing path")
	}

	if fi, err := e.loadStoreFromMemoryOrDisk(ctx, info); err == nil {
		return fi.FileData.Header.StoreInfo, nil
	}

	if err := e.createFullStoreOnDisk(ctx, info); err != nil {
		return nil, err
	}

	// TODO: check whether we can see createdAt and LastOpenedAt; we should because it is a pointer!
	return info, nil
}

func (e *LTNGEngine) loadStore(
	ctx context.Context,
	info *v4.StoreInfo,
) (*v4.StoreInfo, error) {
	e.kvLock.Lock(info.Name, struct{}{})
	defer e.kvLock.Unlock(info.Name)

	fi, err := e.loadStoreFromMemoryOrDisk(ctx, info)
	if err != nil {
		return nil, err
	}

	return fi.FileData.Header.StoreInfo, err
}

func (e *LTNGEngine) deleteStore(
	ctx context.Context,
	info *v4.StoreInfo,
) error {
	e.kvLock.Lock(info.Name, struct{}{})
	defer e.kvLock.Unlock(info.Name)

	fileStats, ok := e.storeFileMapping.Get(info.Name)
	if ok {
		_ = fileStats.File.Close()
	}

	e.storeFileMapping.Delete(info.Name)
	e.storeFileMapping.Delete(info.RelationalInfo().Name)

	return e.deleteFullStoreFromDisk(ctx, info)
}

func (e *LTNGEngine) listStores(
	ctx context.Context,
	pagination *ltngdata.Pagination,
) ([]*v4.StoreInfo, error) {
	var matchBox []*v4.StoreInfo
	if pagination.IsValid() {
		pRef := (pagination.PageID - 1) * pagination.PageSize
		pLimit := pRef + pagination.PageSize
		//if pLimit > uint64(len(matches)) {
		//	pLimit = uint64(len(matches))
		//}

		matchBox = make([]*v4.StoreInfo, pLimit)
	} else {
		return nil, errorsx.New("invalid pagination")
	}

	relationalInfoManager := v4.DBManagerStoreInfo.RelationalInfo()

	e.kvLock.Lock(relationalInfoManager.Name, struct{}{})
	defer e.kvLock.Unlock(relationalInfoManager.Name)

	fi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx)
	if err != nil {
		return nil, errorsx.Wrapf(err, "error loading %s relational store",
			relationalInfoManager.Name)
	}

	reader, err := fileio.NewFileReader(ctx, fi.File, true)
	if err != nil {
		return nil, errorsx.Errorf("error creating %s file reader",
			relationalInfoManager.Name).Wrap(err, "error")
	}

	var count int
	for idx := range matchBox {
		var bs []byte
		bs, err = reader.Read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, errorsx.Errorf("error reading lines from %s file",
				relationalInfoManager.Name).Wrap(err, "error")
		}
		if bs == nil {
			continue
		}

		var match v4.FileData
		if err = e.serializer.Deserialize(bs, &match); err != nil {
			return nil, err
		}

		matchBox[idx] = match.Header.StoreInfo
		count++
	}

	return matchBox[:count], nil
}
