package v1

import (
	"context"
	"fmt"
	"os/exec"
)

func executor(cmd *exec.Cmd) ([]byte, error) {
	fmt.Println(cmd.String())
	return cmd.CombinedOutput()
}

func cpExec(_ context.Context, fromFilepath, toFilepath string) ([]byte, error) {
	//return executor(
	//	exec.Command("cp", "-R", fromFilepath, toFilepath))
	return executor(
		exec.Command("find", fromFilepath, "-maxdepth", "1", "-type", "f", "-exec", "cp", "{}", toFilepath, ";"))
}

func cpFileExec(_ context.Context, fromFilepath, toFilepath string) ([]byte, error) {
	return executor(
		exec.Command("find", fromFilepath, "-maxdepth", "1", "-type", "f", "-exec", "cp", "{}", toFilepath, ";"))
}

func delExec(_ context.Context, filepath string) ([]byte, error) {
	return executor(
		//exec.Command("find", filepath, "-maxdepth", "1", "-type", "d", "-name", "indexed", "-o", "-name", "indexed-list", "-o", "-name", "relational", "-exec", "rm", "{}", ";"))
		exec.Command("find", filepath, "-maxdepth", "1", "-type", "f", "-exec", "rm", "{}", ";"))
}

func cpStoreDirsExec(_ context.Context, fromFilepath, toFilepath string) ([]byte, error) {
	return executor(
		exec.Command("cp", "-R", fromFilepath+"/{indexed,indexed-list,relational}", toFilepath))
}

func delStoreDirsExec(_ context.Context, filepath string) ([]byte, error) {
	return executor(
		exec.Command("sh", "-c",
			fmt.Sprintf("rm -rf %s/{indexed,indexed-list,relational}", filepath)))

	//return nil, nil

	//return executor(
	//	exec.Command("find", filepath, "-maxdepth", "1", "-type", "d",
	//		"(", "-name", "indexed", "-o", "-name", "indexed-list", "-o", "-name", "relational", ")",
	//		"-exec", "rm", "-rf", "{}", ";"))
}

func delSoftDirExec(_ context.Context, filepath string) ([]byte, error) {
	return executor(
		exec.Command("rm", filepath))
}

func delHardExec(_ context.Context, filepath string) ([]byte, error) {
	return executor(
		exec.Command("rm", "-rf", filepath))
}

func delFileExec(_ context.Context, filepath string) ([]byte, error) {
	return executor(
		exec.Command("rm", filepath))
}

// -maxdepth 1
func countFilesExec(_ context.Context, filepath string) ([]byte, error) {
	return executor(
		exec.Command("find", filepath, "-maxdepth", "1", "-type", "f", "|", "wc", "-l"))
}

func countDirsExec(_ context.Context, filepath string) ([]byte, error) {
	return executor(
		exec.Command("find", filepath, "-maxdepth", "1", "-type", "d", "|", "wc", "-l"))
}

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

func getTmpDelDataPathWithSep(path string) string {
	return dbTmpDelDataPath + sep + path + sep
}

func getTmpDelStatsPathWithSep(path string) string {
	return dbTmpDelStatsPath + sep + path + sep
}
