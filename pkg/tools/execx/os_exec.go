package execx

import (
	"context"
	"fmt"

	"gitlab.com/pietroski-software-company/golang/devex/execx"
)

func CpExec(ctx context.Context, fromFilepath, toFilepath string) error {
	return execx.RunContext(ctx,
		"find", fromFilepath, "-maxdepth", "1", "-type", "f", "-exec", "cp", "{}", toFilepath, ";")
}

func CpFileExec(ctx context.Context, fromFilepath, toFilepath string) error {
	return execx.RunContext(ctx,
		"sh", "-c", fmt.Sprintf("cp %v %v", fromFilepath, toFilepath))
}

func MvFileExec(ctx context.Context, fromFilepath, toFilepath string) error {
	return execx.RunContext(ctx,
		"sh", "-c", fmt.Sprintf("mv -f %s %s", fromFilepath, toFilepath))
}

func DelExec(ctx context.Context, filepath string) error {
	//"find", filepath, "-maxdepth", "1", "-type", "d", "-name", "indexed", "-o", "-name", "indexed-list", "-o", "-name", "relational", "-exec", "rm", "{}", ";")
	return execx.RunContext(ctx,
		"find", filepath, "-maxdepth", "1", "-type", "f", "-exec", "rm", "{}", ";")
}

func CpStoreDirsExec(ctx context.Context, fromFilepath, toFilepath string) error {
	return execx.RunContext(ctx,
		"cp", "-R", fromFilepath+"/{indexed,indexed-list,relational}", toFilepath)
}

func DelStoreDirsExec(ctx context.Context, filepath string) error {
	//return Executor(
	//"sh", "-c",
	//		fmt.Sprintf("rm -rf %s/{indexed,indexed-list,relational}", filepath))

	return execx.RunContext(ctx,
		"sh", "-c",
		fmt.Sprintf(`rm -rf %s/{indexed,indexed-list,relational} && 
					[ $(find %s -mindepth 1 -type d | wc -l) -eq 0 ] && rm -rf %s`,
			filepath, filepath, filepath))
}
func DelDirsWithoutSepExec(ctx context.Context, filepath string) error {
	return execx.RunContext(ctx,
		"sh", "-c",
		fmt.Sprintf(`rm -rf %s{indexed,indexed-list,relational} && 
					[ $(find %s -mindepth 1 -type d | wc -l) -eq 0 ] && rm -rf %s`,
			filepath, filepath, filepath))
}

func DelDirsBothOSExec(ctx context.Context, filepath string) error {
	// Using single quotes to prevent shell interpretation of special characters
	cmd := fmt.Sprintf(`rm -rf '%[1]s/indexed' '%[1]s/indexed-list' '%[1]s/relational' && 
        dir_count=$(find '%[1]s' -mindepth 1 -type d 2>/dev/null | wc -l | tr -d " \t\n") && 
        [ "$dir_count" -eq 0 ] && 
        rm -rf '%[1]s'`, filepath)

	return execx.RunContext(ctx, "sh", "-c", cmd)
}

func DelDirsWithoutSepBothOSExec(ctx context.Context, filepath string) error {
	// Using single quotes to prevent shell interpretation of special characters
	cmd := fmt.Sprintf(`rm -rf '%[1]sindexed' '%[1]sindexed-list' '%[1]srelational' && 
        dir_count=$(find '%[1]s' -mindepth 1 -type d 2>/dev/null | wc -l | tr -d " \t\n") && 
        [ "$dir_count" -eq 0 ] && 
        rm -rf '%[1]s'`, filepath)

	return execx.RunContext(ctx, "sh", "-c", cmd)
}

func DelDataStoreRawDirsExec(ctx context.Context, filepath string) error {
	return execx.RunContext(ctx, "sh", "-c",
		fmt.Sprintf(`rm -rf %s{indexed,indexed-list,relational} && 
					[ $(find %s -mindepth 1 -type d | wc -l) -eq 0 ] && rm -rf %s`,
			filepath, filepath, filepath))
}

func DelSoftDirExec(ctx context.Context, filepath string) error {
	return execx.RunContext(ctx, "rm", filepath)
}

func DelHardExec(ctx context.Context, filepath string) error {
	return execx.RunContext(ctx, "rm", "-rf", filepath)
}

func DelFileExec(ctx context.Context, filepath string) error {
	return execx.RunContext(ctx, "rm", "-r", filepath)
}

// CountFilesExec -maxdepth 1
func CountFilesExec(ctx context.Context, filepath string) error {
	return execx.RunContext(ctx,
		"find", filepath, "-maxdepth", "1", "-type", "f", "|", "wc", "-l")
}

func CountDirsExec(ctx context.Context, filepath string) error {
	return execx.RunContext(ctx,
		"find", filepath, "-maxdepth", "1", "-type", "d", "|", "wc", "-l")
}
