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
		exec.Command("sh", "-c", fmt.Sprintf("cp %v %v", fromFilepath, toFilepath)))
}

func mvFileExec(_ context.Context, fromFilepath, toFilepath string) ([]byte, error) {
	return executor(
		exec.Command("sh", "-c", fmt.Sprintf("mv -f %v %v", fromFilepath, toFilepath)))
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
	//return executor(
	//	exec.Command("sh", "-c",
	//		fmt.Sprintf("rm -rf %s/{indexed,indexed-list,relational}", filepath)))

	return executor(
		exec.Command("sh", "-c",
			fmt.Sprintf(`rm -rf %s/{indexed,indexed-list,relational} && 
					[ $(find %s -mindepth 1 -type d | wc -l) -eq 0 ] && rm -rf %s`,
				filepath, filepath, filepath)))
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
		exec.Command("rm", "-r", filepath))
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
