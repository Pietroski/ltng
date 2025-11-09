package osx

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/pietroski-software-company/golang/devex/testingx"
)

func TestCpOnlyFilesFromDir(t *testing.T) {
	ctx := context.Background()

	fileCount := 20
	PrepareTestFiles(t, fileCount)

	actualFileCount, err := CpOnlyFilesFromDir(ctx, srcDir, dstDir)
	assert.NoError(t, err)
	assert.Equal(t, fileCount, actualFileCount)
}

func TestCpOnlyFilesFromDirAsync(t *testing.T) {
	ctx := context.Background()

	fileCount := 20
	PrepareTestFiles(t, fileCount)

	actualFileCount, err := CpOnlyFilesFromDirAsync(ctx, srcDir, dstDir)
	assert.NoError(t, err)
	assert.Equal(t, fileCount, actualFileCount)
}

func TestCpExec(t *testing.T) {
	ctx := context.Background()

	PrepareTestFiles(t, 20)

	err := CpExec(ctx, srcDir, dstDir)
	assert.NoError(t, err)
}

func TestBenchmarkOnlyFileCopies(t *testing.T) {
	ctx := context.Background()
	fileCount := 2000
	PrepareTestFiles(t, fileCount)

	var err error
	var actualFileCount int
	t.Log("std lib", testingx.NewBenchSync().CalcElapsed(func() {
		actualFileCount, err = CpOnlyFilesFromDir(ctx, srcDir, dstDir)
	}).String())
	assert.NoError(t, err)
	assert.Equal(t, fileCount, actualFileCount)

	PrepareTestFiles(t, fileCount)

	var asyncFileCount int
	t.Log("std lib with async", testingx.NewBenchSync().CalcElapsed(func() {
		asyncFileCount, err = CpOnlyFilesFromDirAsync(ctx, srcDir, dstDir)
	}).String())
	assert.NoError(t, err)
	assert.Equal(t, fileCount, asyncFileCount)

	PrepareTestFiles(t, fileCount)

	t.Log("bash wrapper", testingx.NewBenchSync().CalcElapsed(func() {
		err = CpExec(ctx, srcDir, dstDir)
	}).String())
	assert.NoError(t, err)
}

func TestMvOnlyFilesFromDir(t *testing.T) {
	ctx := context.Background()

	fileCount := 20
	PrepareTestFiles(t, fileCount)

	actualFileCount, err := MvOnlyFilesFromDir(ctx, srcDir, dstDir)
	assert.NoError(t, err)
	assert.Equal(t, fileCount, actualFileCount)
}

func TestMvOnlyFilesFromDirAsync(t *testing.T) {
	ctx := context.Background()

	fileCount := 20
	PrepareTestFiles(t, fileCount)

	actualFileCount, err := MvOnlyFilesFromDirAsync(ctx, srcDir, dstDir)
	assert.NoError(t, err)
	assert.Equal(t, fileCount, actualFileCount)
}

func TestMvExec(t *testing.T) {
	ctx := context.Background()

	PrepareTestFiles(t, 20)

	err := MvExec(ctx, srcDir, dstDir)
	assert.NoError(t, err)
}

func TestBenchmarkOnlyFileMoves(t *testing.T) {
	ctx := context.Background()
	fileCount := 2000
	PrepareTestFiles(t, fileCount)

	var err error
	var actualFileCount int
	t.Log("std lib", testingx.NewBenchSync().CalcElapsed(func() {
		actualFileCount, err = MvOnlyFilesFromDir(ctx, srcDir, dstDir)
	}).String())
	assert.NoError(t, err)
	assert.Equal(t, fileCount, actualFileCount)

	PrepareTestFiles(t, fileCount)

	var asyncFileCount int
	t.Log("std lib with async", testingx.NewBenchSync().CalcElapsed(func() {
		asyncFileCount, err = MvOnlyFilesFromDirAsync(ctx, srcDir, dstDir)
	}).String())
	assert.NoError(t, err)
	assert.Equal(t, fileCount, asyncFileCount)

	PrepareTestFiles(t, fileCount)

	t.Log("bash wrapper", testingx.NewBenchSync().CalcElapsed(func() {
		err = MvExec(ctx, srcDir, dstDir)
	}).String())
	assert.NoError(t, err)
}

func TestDelOnlyFilesFromDir(t *testing.T) {
	ctx := context.Background()

	fileCount := 20
	PrepareTestFiles(t, fileCount)

	actualFileCount, err := DelOnlyFilesFromDir(ctx, srcDir)
	assert.NoError(t, err)
	assert.Equal(t, fileCount, actualFileCount)
}

func TestDelOnlyFilesFromDirAsync(t *testing.T) {
	ctx := context.Background()

	fileCount := 20
	PrepareTestFiles(t, fileCount)

	actualFileCount, err := DelOnlyFilesFromDirAsync(ctx, srcDir)
	assert.NoError(t, err)
	assert.Equal(t, fileCount, actualFileCount)
}

func TestDelExec(t *testing.T) {
	ctx := context.Background()

	PrepareTestFiles(t, 20)

	err := DelExec(ctx, srcDir)
	assert.NoError(t, err)
}

func TestBenchmarkOnlyFileDeletions(t *testing.T) {
	ctx := context.Background()
	fileCount := 2000
	PrepareTestFiles(t, fileCount)

	var err error
	var actualFileCount int
	t.Log("std lib", testingx.NewBenchSync().CalcElapsed(func() {
		actualFileCount, err = DelOnlyFilesFromDir(ctx, srcDir)
	}).String())
	assert.NoError(t, err)
	assert.Equal(t, fileCount, actualFileCount)

	PrepareTestFiles(t, fileCount)

	var asyncFileCount int
	t.Log("std lib with async", testingx.NewBenchSync().CalcElapsed(func() {
		asyncFileCount, err = DelOnlyFilesFromDirAsync(ctx, srcDir)
	}).String())
	assert.NoError(t, err)
	assert.Equal(t, fileCount, asyncFileCount)

	PrepareTestFiles(t, fileCount)

	t.Log("bash wrapper", testingx.NewBenchSync().CalcElapsed(func() {
		err = DelExec(ctx, srcDir)
	}).String())
	assert.NoError(t, err)
}

func TestMvFile(t *testing.T) {
	t.Run("move non existent file", func(t *testing.T) {
		ctx := context.Background()
		err := MvFile(ctx, filepath.Join(srcDir, "file_01"), filepath.Join(dstDir, "file_01"))
		assert.Error(t, err)
	})

	t.Run("move existent file to an existent file", func(t *testing.T) {
		ctx := context.Background()
		PrepareTestDirs(t)

		file, err := os.OpenFile(filepath.Join(srcDir, "file_01"), os.O_RDONLY|os.O_CREATE, 0666)
		require.NoError(t, err)
		assert.NoError(t, file.Close())

		err = MvFile(ctx, filepath.Join(srcDir, "file_01"), filepath.Join(dstDir, "file_01"))
		t.Log(err)
		assert.NoError(t, err)
	})

	t.Run("move existent file to an existent file", func(t *testing.T) {
		ctx := context.Background()
		PrepareTestDirs(t)

		file, err := os.OpenFile(filepath.Join(srcDir, "file_01"), os.O_RDONLY|os.O_CREATE, 0666)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, file.Close())
		}()

		err = MvFile(ctx, filepath.Join(srcDir, "file_01"), filepath.Join(dstDir, "file_01"))
		t.Log(err)
		assert.NoError(t, err)
	})

	t.Run("move existent file to an existent file", func(t *testing.T) {
		ctx := context.Background()
		PrepareTestDirs(t)

		file, err := os.OpenFile(filepath.Join(dstDir, "file_01"), os.O_RDWR|os.O_CREATE, 0766)
		require.NoError(t, err)
		_, err = file.Write([]byte("hello-world"))
		require.NoError(t, err)
		assert.NoError(t, file.Close())

		file, err = os.OpenFile(filepath.Join(srcDir, "file_01"), os.O_RDWR|os.O_CREATE, 0766)
		require.NoError(t, err)
		_, err = file.Write([]byte("hello-world\n"))
		_, err = file.Write([]byte("hello-world again!!"))
		require.NoError(t, err)
		assert.NoError(t, file.Close())

		err = MvFile(ctx, filepath.Join(srcDir, "file_01"), filepath.Join(dstDir, "file_01"))
		t.Log(err)
		assert.NoError(t, err)
	})

	t.Run("move existent file to an existent file", func(t *testing.T) {
		PrepareTestDirs(t)

		file, err := os.OpenFile(filepath.Join(dstDir, "file_01"), os.O_RDWR|os.O_CREATE, 0766)
		require.NoError(t, err)
		_, err = file.Write([]byte("hello-world"))
		require.NoError(t, err)
		assert.NoError(t, file.Close())

		file, err = os.OpenFile(filepath.Join(srcDir, "file_01"), os.O_RDWR|os.O_CREATE, 0766)
		require.NoError(t, err)
		_, err = file.Write([]byte("hello-world\n"))
		_, err = file.Write([]byte("hello-world again!!"))
		require.NoError(t, err)
		assert.NoError(t, file.Close())

		err = os.Rename(filepath.Join(srcDir, "file_01"), filepath.Join(dstDir, "file_01"))
		t.Log(err)
		assert.NoError(t, err)
	})
}

func TestCleanupDir(t *testing.T) {
	PrepareTestDirs(t)

	ctx := context.Background()
	err := CleanupEmptyDirs(ctx, subDstDir)
	t.Log(err)
}
