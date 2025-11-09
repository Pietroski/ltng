package ltngenginemodels

import "path/filepath"

func GetFileLockName(base, key string) string {
	return base + InnerSep + key
}

func GetStatsPath(path string) string {
	return filepath.Join(BaseStatsPath, path)
}

func GetStatsFilepath(path, storeName string) string {
	return filepath.Join(BaseStatsPath, path, storeName) + Ext
}

func GetRelationalStatsPath(path string) string {
	return filepath.Join(BaseStatsPath, DBRelationalPath, path)
}

func GetDataPath(path string) string {
	return filepath.Join(BaseDataPath, path)
}

func GetDataFilepath(path, filename string) string {
	return filepath.Join(BaseDataPath, path, filename) + Ext
}

// #############

func GetRelationalStatsPathWithSep() string {
	return BaseStatsPath + DBRelationalPath + Sep
}

func GetTmpDataFilepath(path, filename string) string {
	return GetDataPath(path) + TmpPrefix + filename + Ext
	//return GetDataPathWithSep(path) + Tmp + Sep + filename + Ext
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

func GetTmpDelStatsPathFile(filename string) string {
	return DBTmpDelStatsPath + Sep + filename + Sep + filename + Ext
}

func RawPathWithSepForFile(rawPathWithSep, filename string) string {
	return rawPathWithSep + filename + Ext
}
