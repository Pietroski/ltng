package ltngenginemodels

import "testing"

func TestGetPaths(t *testing.T) {
	t.Log(GetStatsPath("test/path/"))
	t.Log(GetStatsFilepath("test/path/", "test-store"))

	t.Log(GetDataPath("test/path/"))
	t.Log(GetDataFilepath("test/path/", "test-store"))
}
