package manager

import management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"

func IndexedDBInfoFromDBInfo(dbInfo *management_models.DBInfo) *management_models.DBInfo {
	indexedDBName := dbInfo.Name + IndexedSuffixName
	indexedDBPath := dbInfo.Path + IndexedSuffixPath

	indexedDBInfo := &management_models.DBInfo{
		Path:         indexedDBPath,
		Name:         indexedDBName,
		CreatedAt:    dbInfo.CreatedAt,
		LastOpenedAt: dbInfo.LastOpenedAt,
	}

	return indexedDBInfo
}

func IndexedListDBInfoFromDBInfo(dbInfo *management_models.DBInfo) *management_models.DBInfo {
	indexedDBName := dbInfo.Name + IndexedListSuffixName
	indexedDBPath := dbInfo.Path + IndexedListSuffixPath

	indexedDBInfo := &management_models.DBInfo{
		Path:         indexedDBPath,
		Name:         indexedDBName,
		CreatedAt:    dbInfo.CreatedAt,
		LastOpenedAt: dbInfo.LastOpenedAt,
	}

	return indexedDBInfo
}
