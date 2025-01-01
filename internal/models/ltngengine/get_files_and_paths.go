package ltngenginemodels

func GetFileLockName(base, key string) string {
	return base + SuffixSep + key
}

func GetDataPath(path string) string {
	return BaseDataPath + Sep + path
}

func GetDataPathWithSep(path string) string {
	return GetDataPath(path) + Sep
}

func GetStatsPathWithSep() string {
	return BaseStatsPath + Sep
}

func GetRelationalStatsPathWithSep() string {
	return BaseStatsPath + DBRelationalPath + Sep
}

func GetDataFilepath(path, filename string) string {
	return GetDataPathWithSep(path) + filename + Ext
}

func GetTmpDataFilepath(path, filename string) string {
	return GetDataPathWithSep(path) + TmpPrefix + filename + Ext
}

func GetStatsFilepath(storeName string) string {
	return GetStatsPathWithSep() + storeName + Ext
}

func GetRelationalStatsFilepath(storeName string) string {
	return GetRelationalStatsPathWithSep() + storeName + Ext
}

func GetTmpDelDataPath(path string) string {
	return DBTmpDelDataPath + Sep + path
}

func GetTmpDelStatsPath(path string) string {
	return DBTmpDelStatsPath + Sep + path
}

func GetTmpDelDataPathWithSep(path string) string {
	return DBTmpDelDataPath + Sep + path + Sep
}

func GetTmpDelDataFilePath(path, filename string) string {
	return DBTmpDelDataPath + Sep + path + Sep + filename + Ext
}

func GetTmpDelStatsPathWithSep(path string) string {
	return DBTmpDelStatsPath + Sep + path + Sep
}

func RawPathWithSepForFile(rawPathWithSep, filename string) string {
	return rawPathWithSep + filename + Ext
}
