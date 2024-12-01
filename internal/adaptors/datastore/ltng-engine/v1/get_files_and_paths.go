package v1

func getFileLockName(base, key string) string {
	return base + suffixSep + key
}

func getDataPath(path string) string {
	return baseDataPath + sep + path
}

func getDataPathWithSep(path string) string {
	return getDataPath(path) + sep
}

func getStatsPathWithSep() string {
	return baseStatsPath + sep
}

func getRelationalStatsPathWithSep() string {
	return baseStatsPath + dbRelationalPath + sep
}

func getDataFilepath(path, filename string) string {
	return getDataPathWithSep(path) + filename + ext
}

func getStatsFilepath(storeName string) string {
	return getStatsPathWithSep() + storeName + ext
}

func getRelationalStatsFilepath(storeName string) string {
	return getRelationalStatsPathWithSep() + storeName + ext
}

func getTmpDelDataPath(path string) string {
	return dbTmpDelDataPath + sep + path
}

func getTmpDelStatsPath(path string) string {
	return dbTmpDelStatsPath + sep + path
}

func getTmpDelDataPathWithSep(path string) string {
	return dbTmpDelDataPath + sep + path + sep
}

func getTmpDelStatsPathWithSep(path string) string {
	return dbTmpDelStatsPath + sep + path + sep
}
