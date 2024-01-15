package badgerdb_manager_adaptor_v3

import badgerdb_management_models_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v3/management"

func IndexedDBInfoFromDBInfo(dbInfo *badgerdb_management_models_v3.DBInfo) *badgerdb_management_models_v3.DBInfo {
	indexedDBName := dbInfo.Name + IndexedSuffixName
	indexedDBPath := dbInfo.Path + IndexedSuffixPath

	indexedDBInfo := &badgerdb_management_models_v3.DBInfo{
		Path:         indexedDBPath,
		Name:         indexedDBName,
		CreatedAt:    dbInfo.CreatedAt,
		LastOpenedAt: dbInfo.LastOpenedAt,
	}

	return indexedDBInfo
}

func IndexedListDBInfoFromDBInfo(dbInfo *badgerdb_management_models_v3.DBInfo) *badgerdb_management_models_v3.DBInfo {
	indexedDBName := dbInfo.Name + IndexedListSuffixName
	indexedDBPath := dbInfo.Path + IndexedListSuffixPath

	indexedDBInfo := &badgerdb_management_models_v3.DBInfo{
		Path:         indexedDBPath,
		Name:         indexedDBName,
		CreatedAt:    dbInfo.CreatedAt,
		LastOpenedAt: dbInfo.LastOpenedAt,
	}

	return indexedDBInfo
}
