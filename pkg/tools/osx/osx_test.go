package osx

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
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
