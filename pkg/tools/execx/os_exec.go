package execx

import (
	"context"
	"fmt"
	"os/exec"
)

func Executor(cmd *exec.Cmd) ([]byte, error) {
	fmt.Println(cmd.String())
	return cmd.CombinedOutput()
}

func CpExec(_ context.Context, fromFilepath, toFilepath string) ([]byte, error) {
	//return executor(
	//	exec.Command("cp", "-R", fromFilepath, toFilepath))
	return Executor(
		exec.Command("find", fromFilepath, "-maxdepth", "1", "-type", "f", "-exec", "cp", "{}", toFilepath, ";"))
}

func CpFileExec(_ context.Context, fromFilepath, toFilepath string) ([]byte, error) {
	return Executor(
		exec.Command("sh", "-c", fmt.Sprintf("cp %v %v", fromFilepath, toFilepath)))
}

func MvFileExec(_ context.Context, fromFilepath, toFilepath string) ([]byte, error) {
	return Executor(
		exec.Command("sh", "-c", fmt.Sprintf("mv -f %s %s", fromFilepath, toFilepath)))
}

func DelExec(_ context.Context, filepath string) ([]byte, error) {
	return Executor(
		//exec.Command("find", filepath, "-maxdepth", "1", "-type", "d", "-name", "indexed", "-o", "-name", "indexed-list", "-o", "-name", "relational", "-exec", "rm", "{}", ";"))
		exec.Command("find", filepath, "-maxdepth", "1", "-type", "f", "-exec", "rm", "{}", ";"))
}

func CpStoreDirsExec(_ context.Context, fromFilepath, toFilepath string) ([]byte, error) {
	return Executor(
		exec.Command("cp", "-R", fromFilepath+"/{indexed,indexed-list,relational}", toFilepath))
}

func DelStoreDirsExec(_ context.Context, filepath string) ([]byte, error) {
	//return Executor(
	//	exec.Command("sh", "-c",
	//		fmt.Sprintf("rm -rf %s/{indexed,indexed-list,relational}", filepath)))

	return Executor(
		exec.Command("sh", "-c",
			fmt.Sprintf(`rm -rf %s/{indexed,indexed-list,relational} && 
					[ $(find %s -mindepth 1 -type d | wc -l) -eq 0 ] && rm -rf %s`,
				filepath, filepath, filepath)))
}

func DelDataStoreRawDirsExec(_ context.Context, filepath string) ([]byte, error) {
	return Executor(
		exec.Command("sh", "-c",
			fmt.Sprintf(`rm -rf %s{indexed,indexed-list,relational} && 
					[ $(find %s -mindepth 1 -type d | wc -l) -eq 0 ] && rm -rf %s`,
				filepath, filepath, filepath)))
}

func DelSoftDirExec(_ context.Context, filepath string) ([]byte, error) {
	return Executor(
		exec.Command("rm", filepath))
}

func DelHardExec(_ context.Context, filepath string) ([]byte, error) {
	return Executor(
		exec.Command("rm", "-rf", filepath))
}

func DelFileExec(_ context.Context, filepath string) ([]byte, error) {
	return Executor(
		exec.Command("rm", "-r", filepath))
}

// CountFilesExec -maxdepth 1
func CountFilesExec(_ context.Context, filepath string) ([]byte, error) {
	return Executor(
		exec.Command("find", filepath, "-maxdepth", "1", "-type", "f", "|", "wc", "-l"))
}

func CountDirsExec(_ context.Context, filepath string) ([]byte, error) {
	return Executor(
		exec.Command("find", filepath, "-maxdepth", "1", "-type", "d", "|", "wc", "-l"))
}
