package osx

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/pietroski-software-company/golang/devex/random"
)

const (
	baseDir = ".osx_test_files"
)

var (
	srcDir    = filepath.Join(baseDir, "src_dir")
	dstDir    = filepath.Join(baseDir, "dst_dir")
	subSrcDir = filepath.Join(baseDir, "src_dir", "sub_src_dir")
	subDstDir = filepath.Join(baseDir, "dst_dir", "sub_dst_dir")
)

func GenerateFileContentPair(count int) map[string]string {
	fileContentMap := make(map[string]string, count)

	for i := 0; i < count; i++ {
		fileContentMap[random.String(10)] = random.String(20)
	}

	return fileContentMap
}

func CreateFiles(
	t testing.TB,
	fileLocation string,
	fileContentMap map[string]string,
) {
	for filename, content := range fileContentMap {
		err := os.WriteFile(fileLocation+"/"+filename, []byte(content), 0644)
		require.NoError(t, err)
	}
}

func PrepareTestFiles(
	t testing.TB,
	fileCount int,
) {
	PrepareTestDirs(t)

	fileContentMap := GenerateFileContentPair(fileCount)
	CreateFiles(t, srcDir, fileContentMap)
}

func PrepareTestDirs(
	t testing.TB,
) {
	var err error

	err = os.RemoveAll(baseDir)
	require.NoError(t, err)

	err = os.MkdirAll(srcDir, defaultFilePerm)
	require.NoError(t, err)

	err = os.MkdirAll(dstDir, defaultFilePerm)
	require.NoError(t, err)

	err = os.MkdirAll(subSrcDir, defaultFilePerm)
	require.NoError(t, err)

	err = os.MkdirAll(subDstDir, defaultFilePerm)
	require.NoError(t, err)
}
