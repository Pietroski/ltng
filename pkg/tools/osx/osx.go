package osx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/execx"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"
)

const (
	defaultFilePerm    = 0740 // 0755
	defaultWorkerCount = 128
)

// CpOnlyFilesFromDir copies only files from a directory to another directory.
// it returns how many files were copied and whether there were any errors or not.
// The function is a sync process, so it breaks and returns the first encountered error.
// it is equivalent to the following bash command:
// find $fromFilepath -maxdepth 1 -type f -exec cp {} $toFilepath ;
func CpOnlyFilesFromDir(ctx context.Context, fromFilepath, toFilepath string) (int, error) {
	// Read directory entries
	entries, err := os.ReadDir(fromFilepath)
	if err != nil {
		return 0, fmt.Errorf("failed to read directory: %w", err)
	}

	// Ensure destination directory exists
	if err = os.MkdirAll(toFilepath, defaultFilePerm); err != nil {
		return 0, fmt.Errorf("failed to create destination directory: %w", err)
	}

	var count int

	// Copy each regular file
	for _, entry := range entries {
		// Check context cancellation
		if ctx.Err() != nil {
			return count, ctx.Err()
		}

		// Skip non-regular files (directories, symlinks, etc.)
		if !entry.Type().IsRegular() {
			continue
		}

		srcPath := filepath.Join(fromFilepath, entry.Name())
		dstPath := filepath.Join(toFilepath, entry.Name())

		if err = copyFile(srcPath, dstPath); err != nil {
			return count, fmt.Errorf("failed to copy %s: %w", entry.Name(), err)
		}

		count++
	}

	return count, nil
}

// CpOnlyFilesFromDirAsync does the same as the CpOnlyFilesFromDir but in a concurrent way with multiple goroutines.
func CpOnlyFilesFromDirAsync(ctx context.Context, fromFilepath, toFilepath string) (int, error) {
	// Read directory entries
	entries, err := os.ReadDir(fromFilepath)
	if err != nil {
		return 0, fmt.Errorf("failed to read directory: %w", err)
	}

	// Ensure destination directory exists
	if err = os.MkdirAll(toFilepath, defaultFilePerm); err != nil {
		return 0, fmt.Errorf("failed to create destination directory: %w", err)
	}

	workers := min(len(entries), defaultWorkerCount)
	op := syncx.NewThreadOperator("concurrent_copier", syncx.WithThreadLimit(workers))
	count := atomic.Uint64{}

	// Copy each regular file
	for _, entry := range entries {
		// Check context cancellation
		if ctx.Err() != nil {
			err = op.WaitAndWrapErr()
			if err != nil {
				return int(count.Load()),
					errorsx.New("error waiting for concurrent copier").
						Wrap(err, "concurrent copier error").
						Wrap(ctx.Err(), "ctx error")
			}

			return int(count.Load()), ctx.Err()
		}

		op.OpX(func() (any, error) {
			// Skip non-regular files (directories, symlinks, etc.)
			if !entry.Type().IsRegular() {
				return nil, nil
			}

			srcPath := filepath.Join(fromFilepath, entry.Name())
			dstPath := filepath.Join(toFilepath, entry.Name())

			if err := copyFile(srcPath, dstPath); err != nil {
				return nil, fmt.Errorf("failed to copy %s: %w", entry.Name(), err)
			}

			count.Add(1)
			return nil, nil
		})
	}

	err = op.WaitAndWrapErr()
	return int(count.Load()), err
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		_ = sourceFile.Close()
	}()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		_ = destFile.Close()
	}()

	if _, err = io.Copy(destFile, sourceFile); err != nil {
		return err
	}

	// Preserve file permissions
	sourceInfo, err := os.Stat(src)
	if err != nil {
		return err
	}

	return os.Chmod(dst, sourceInfo.Mode())
}

func CpExec(ctx context.Context, fromFilepath, toFilepath string) error {
	return execx.RunContext(ctx,
		"find", fromFilepath, "-maxdepth", "1", "-type", "f", "-exec", "cp", "{}", toFilepath, ";")
}

// MvOnlyFilesFromDir moves only files from a directory to another directory.
// it returns how many files were moved and whether there were any errors or not.
// The function is a sync process, so it breaks and returns the first encountered error.
// it is equivalent to the following bash command:
// find $fromFilepath -maxdepth 1 -type f -exec mv {} $toFilepath ;
func MvOnlyFilesFromDir(ctx context.Context, fromFilepath, toFilepath string) (int, error) {
	// Read directory entries
	entries, err := os.ReadDir(fromFilepath)
	if err != nil {
		return 0, fmt.Errorf("failed to read directory: %w", err)
	}

	// Ensure destination directory exists
	if err = os.MkdirAll(toFilepath, defaultFilePerm); err != nil {
		return 0, fmt.Errorf("failed to create destination directory: %w", err)
	}

	var count int

	// Move each regular file
	for _, entry := range entries {
		// Check context cancellation
		if ctx.Err() != nil {
			return count, ctx.Err()
		}

		// Skip non-regular files (directories, symlinks, etc.)
		if !entry.Type().IsRegular() {
			continue
		}

		srcPath := filepath.Join(fromFilepath, entry.Name())
		dstPath := filepath.Join(toFilepath, entry.Name())

		if err = moveFile(srcPath, dstPath); err != nil {
			return count, fmt.Errorf("failed to move %s: %w", entry.Name(), err)
		}

		count++
	}

	return count, nil
}

// MvOnlyFilesFromDirAsync does the same as the MvOnlyFilesFromDir but in a concurrent way with multiple goroutines.
func MvOnlyFilesFromDirAsync(ctx context.Context, fromFilepath, toFilepath string) (int, error) {
	// Read directory entries
	entries, err := os.ReadDir(fromFilepath)
	if err != nil {
		return 0, fmt.Errorf("failed to read directory: %w", err)
	}

	// Ensure destination directory exists
	if err = os.MkdirAll(toFilepath, defaultFilePerm); err != nil {
		return 0, fmt.Errorf("failed to create destination directory: %w", err)
	}

	workers := min(len(entries), defaultWorkerCount)
	op := syncx.NewThreadOperator("concurrent_mover", syncx.WithThreadLimit(workers))
	count := atomic.Uint64{}

	// Move each regular file
	for _, entry := range entries {
		// Check context cancellation
		if ctx.Err() != nil {
			err = op.WaitAndWrapErr()
			if err != nil {
				return int(count.Load()),
					errorsx.New("error waiting for concurrent mover").
						Wrap(err, "concurrent mover error").
						Wrap(ctx.Err(), "ctx error")
			}

			return int(count.Load()), ctx.Err()
		}

		op.OpX(func() (any, error) {
			// Skip non-regular files (directories, symlinks, etc.)
			if !entry.Type().IsRegular() {
				return nil, nil
			}

			srcPath := filepath.Join(fromFilepath, entry.Name())
			dstPath := filepath.Join(toFilepath, entry.Name())

			if err := moveFile(srcPath, dstPath); err != nil {
				return nil, fmt.Errorf("failed to move %s: %w", entry.Name(), err)
			}

			count.Add(1)
			return nil, nil
		})
	}

	err = op.WaitAndWrapErr()
	return int(count.Load()), err
}

// moveFile moves a file from src to dst.
// It first attempts an efficient rename (works on same filesystem),
// and falls back to copy+delete for cross-filesystem moves.
func moveFile(src, dst string) error {
	// Remove destination if it exists (to match `mv -f` behavior)
	_ = os.Remove(dst)

	// Attempt to rename (fast path for same filesystem)
	err := os.Rename(src, dst)
	if err == nil {
		return nil
	}

	// Check if it's a cross-device error
	if linkErr, ok := err.(*os.LinkError); ok {
		// On Unix systems, cross-device moves fail with "invalid cross-device link"
		// On Windows, it might be a different error
		errStr := linkErr.Err.Error()
		if errStr == "invalid cross-device link" ||
			errStr == "cross-device link not permitted" ||
			strings.Contains(errStr, "cross-device") {
			// Fall back to copy + delete
			if err := copyFile(src, dst); err != nil {
				return fmt.Errorf("failed to copy file during move: %w", err)
			}

			if err := os.Remove(src); err != nil {
				return fmt.Errorf("failed to remove source file after copy: %w", err)
			}

			return nil
		}
	}

	return fmt.Errorf("failed to move file: %w", err)
}

// MvExec is the exec version for benchmarking comparison
func MvExec(ctx context.Context, fromFilepath, toFilepath string) error {
	return execx.RunContext(ctx,
		"find", fromFilepath, "-maxdepth", "1", "-type", "f", "-exec", "mv", "{}", toFilepath, ";")
}

// DelOnlyFilesFromDir deletes only files from a directory.
// it returns how many files were deleted and whether there were any errors or not.
// The function is a sync process, so it breaks and returns the first encountered error.
// it is equivalent to the following bash command:
// find $fromFilepath -maxdepth 1 -type f -exec rm {} $toFilepath ;
func DelOnlyFilesFromDir(ctx context.Context, filePath string) (int, error) {
	// Read directory entries
	entries, err := os.ReadDir(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read directory: %w", err)
	}

	var count int

	// Copy each regular file
	for _, entry := range entries {
		// Check context cancellation
		if ctx.Err() != nil {
			return count, ctx.Err()
		}

		// Skip non-regular files (directories, symlinks, etc.)
		if !entry.Type().IsRegular() {
			continue
		}

		delPath := filepath.Join(filePath, entry.Name())
		if err = os.Remove(delPath); err != nil {
			return count, fmt.Errorf("failed to remove %s: %w", entry.Name(), err)
		}

		count++
	}

	return count, nil
}

// DelOnlyFilesFromDirAsync does the same as the DelOnlyFilesFromDir but in a concurrent way with multiple goroutines.
func DelOnlyFilesFromDirAsync(ctx context.Context, filePath string) (int, error) {
	// Read directory entries
	entries, err := os.ReadDir(filePath)
	if err != nil {
		return 0, errorsx.New("failed to read directory").Wrap(err, "error")
	}

	workers := min(len(entries), 128)
	op := syncx.NewThreadOperator("concurrent_deleter", syncx.WithThreadLimit(workers))
	count := atomic.Uint64{}

	// Copy each regular file
	for _, entry := range entries {
		// Check context cancellation
		if ctx.Err() != nil {
			err = op.WaitAndWrapErr()
			if err != nil {
				return int(count.Load()),
					errorsx.New("error waiting for concurrent deleter").
						Wrap(err, "concurrent deleter error").
						Wrap(ctx.Err(), "ctx error")
			}

			return int(count.Load()), ctx.Err()
		}

		op.OpX(func() (any, error) {
			// Skip non-regular files (directories, symlinks, etc.)
			if !entry.Type().IsRegular() {
				return nil, nil
			}

			delPath := filepath.Join(filePath, entry.Name())
			if err := os.Remove(delPath); err != nil {
				return nil,
					errorsx.Errorf("failed to remove %s", entry.Name()).
						Wrap(err, "error")
			}

			count.Add(1)
			return nil, nil
		})
	}

	err = op.WaitAndWrapErr()
	return int(count.Load()), err
}

// DelExec deletes only files from a directory.
func DelExec(ctx context.Context, filepath string) error {
	//"find", filepath, "-maxdepth", "1", "-type", "d", "-name", "indexed", "-o", "-name", "indexed-list", "-o", "-name", "relational", "-exec", "rm", "{}", ";")
	return execx.RunContext(ctx,
		"find", filepath, "-maxdepth", "1", "-type", "f", "-exec", "rm", "{}", ";")
}

// ################################################################################################################## \\

// CpFile copies a single file from one path to another, preserving permissions.
func CpFile(ctx context.Context, fromFilepath, toFilepath string) error {
	// Check context cancellation
	if ctx.Err() != nil {
		return ctx.Err()
	}

	return copyFile(fromFilepath, toFilepath)
}

// MvFile moves a file from one path to another, overwriting the destination if it exists.
// This mimics the behavior of `mv -f`.
func MvFile(ctx context.Context, fromFilepath, toFilepath string) error {
	// Check context cancellation
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Remove destination if it exists (to match `mv -f` behavior)
	//_ = os.Remove(toFilepath)

	// Attempt to rename (works for same filesystem)
	err := os.Rename(fromFilepath, toFilepath)
	if err == nil {
		return nil
	}

	// If rename failed (likely cross-device), fall back to copy + delete
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) && linkErr.Err.Error() == "invalid cross-device link" {
		// Copy the file
		if err := copyFile(fromFilepath, toFilepath); err != nil {
			return errorsx.Wrap(err, "failed to copy file during move")
		}

		// Delete the source file
		if err := os.Remove(fromFilepath); err != nil {
			return errorsx.Wrap(err, "failed to remove source file after copy")
		}

		return nil
	}

	return errorsx.Wrap(err, "failed to move file")
}

var (
	ErrNotExist              = errorsx.New("file not exist")
	ErrNotFound              = errorsx.New("file not found")
	ErrNoSuchFileOrDirectory = errorsx.New("no such file or directory")
	ErrNotEmptyDir           = errorsx.New("directory not empty")
	ErrFileExists            = errorsx.New("file exists")
)

// CpFileExec copies a file from one path to another.
func CpFileExec(ctx context.Context, fromFilepath, toFilepath string) error {
	return execx.RunContext(ctx,
		"sh", "-c", fmt.Sprintf("cp %v %v", fromFilepath, toFilepath))
}

// MvFileExec moves a file from one path to another.
func MvFileExec(ctx context.Context, fromFilepath, toFilepath string) error {
	return execx.RunContext(ctx,
		"sh", "-c", fmt.Sprintf("mv -f %s %s", fromFilepath, toFilepath))
}

// ################################################################################################################## \\

// SoftDirCleanup cleans up the whole directory tree by:
// - removing all the existing files in the directory;
// - removing the directory itself if it does not contain any children.
func SoftDirCleanup(ctx context.Context, filePath string) error {
	if _, err := DelOnlyFilesFromDirAsync(ctx, filePath); err != nil {
		return errorsx.Wrapf(err, "error deleting stats path: %s", filePath)
	}

	// remove itself if there is no children anymore.
	if err := os.Remove(filePath); err != nil {
		if errorsx.Is(errorsx.From(err), ErrNotEmptyDir) ||
			errors.Is(errorsx.From(err), ErrNoSuchFileOrDirectory) {
			return nil
		}

		//if os.IsExist(err) || errors.Is(err, os.ErrNotExist) {
		//	return nil
		//}

		return errorsx.Wrapf(err, "error removing stats path: %s", filePath)
	}

	return nil
}

// CleanupDirs cleans up the whole directory tree by:
// - removing all the existing files in the directory;
// - removing the directory itself if it does not contain any children.
// - walk into parent directories and check whether they are excludables.
func CleanupDirs(ctx context.Context, filePath string) error {
	if _, err := DelOnlyFilesFromDirAsync(ctx, filePath); err != nil {
		return errorsx.Wrapf(err, "error deleting stats path: %s", filePath)
	}

	// remove itself if there is no children anymore.
	if err := os.Remove(filePath); err != nil {
		if errorsx.Is(errorsx.From(err), ErrNotEmptyDir) ||
			errors.Is(errorsx.From(err), ErrNoSuchFileOrDirectory) {
			return nil
		}

		//if os.IsExist(err) || errors.Is(err, os.ErrNotExist) {
		//	return nil
		//}

		return errorsx.Wrapf(err, "error removing stats path: %s", filePath)
	}

	// Walk up and remove empty parent directories
	parent := filepath.Dir(filePath)
	for parent != "." && parent != "/" {
		if err := os.Remove(parent); err != nil {
			// Stop if parent isn't empty or doesn't exist (both are OK)
			if errorsx.Is(errorsx.From(err), ErrNotEmptyDir) ||
				errors.Is(errorsx.From(err), ErrNoSuchFileOrDirectory) {
				break
			}
			return errorsx.Wrapf(err, "error removing parent dir: %s", parent)
		}
		parent = filepath.Dir(parent)
	}

	return nil
}

//// CleanupDirRecursively cleans up the whole directory by:
//// - doing everything CleanupDir does;
//// - walk into parent directories and check whether they are excludables.
//func CleanupDirRecursively(ctx context.Context, filePath string) error {
//	// Clean the target directory
//	if err := CleanupDir(ctx, filePath); err != nil {
//		return err
//	}
//
//	// Walk up and remove empty parent directories
//	parent := filepath.Dir(filePath)
//	for parent != "." && parent != "/" {
//		if err := os.Remove(parent); err != nil {
//			// Stop if parent isn't empty or doesn't exist (both are OK)
//			if errorsx.Is(errorsx.From(err), ErrNotEmptyDir) ||
//				errors.Is(errorsx.From(err), ErrNoSuchFileOrDirectory) {
//				break
//			}
//			return errorsx.Wrapf(err, "error removing parent dir: %s", parent)
//		}
//		parent = filepath.Dir(parent)
//	}
//
//	return nil
//}

// CleanupEmptyDirs cleans up the whole directory tree by:
// - removing the directory itself if it does not contain any children.
// - walk into parent directories and check whether they are excludables.
func CleanupEmptyDirs(ctx context.Context, filePath string) error {
	if err := os.Remove(filePath); err != nil {
		if errorsx.Is(errorsx.From(err), ErrNotEmptyDir) ||
			errors.Is(errorsx.From(err), ErrNoSuchFileOrDirectory) {
			return nil
		}

		return errorsx.Wrapf(err, "error removing stats path: %s", filePath)
	}

	// Walk up and remove empty parent directories
	parent := filepath.Dir(filePath)
	for parent != "." && parent != "/" {
		if err := os.Remove(parent); err != nil {
			// Stop if parent isn't empty or doesn't exist (both are OK)
			if errorsx.Is(errorsx.From(err), ErrNotEmptyDir) ||
				errors.Is(errorsx.From(err), ErrNoSuchFileOrDirectory) {
				break
			}
			return errorsx.Wrapf(err, "error removing parent dir: %s", parent)
		}
		parent = filepath.Dir(parent)
	}

	return nil
}

// ################################################################################################################## \\

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

// DelDirs removes specific subdirectories (indexed, indexed-list, relational)
// and if no subdirectories remain, removes the parent directory itself.
func DelDirs(ctx context.Context, dirPath string) error {
	// Check context cancellation
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Remove specific subdirectories
	dirsToRemove := []string{
		filepath.Join(dirPath, "indexed"),
		filepath.Join(dirPath, "indexed-list"),
		filepath.Join(dirPath, "relational"),
	}

	for _, dir := range dirsToRemove {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// RemoveAll doesn't error if path doesn't exist, but we check anyway for clarity
		if err := os.RemoveAll(dir); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove %s: %w", dir, err)
		}
	}

	// Count remaining subdirectories
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	dirCount := 0
	for _, entry := range entries {
		if entry.IsDir() {
			dirCount++
		}
	}

	// If no subdirectories remain, remove the parent directory
	if dirCount == 0 {
		if err := os.RemoveAll(dirPath); err != nil {
			return fmt.Errorf("failed to remove parent directory: %w", err)
		}
	}

	return nil
}

// DelDirsAsync does the same as DelDirs but with multiple goroutines concurrently.
func DelDirsAsync(ctx context.Context, dirPath string) error {
	// Check context cancellation
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Remove specific subdirectories concurrently
	dirsToRemove := []string{
		filepath.Join(dirPath, "indexed"),
		filepath.Join(dirPath, "indexed-list"),
		filepath.Join(dirPath, "relational"),
	}

	op := syncx.NewThreadOperator("concurrent_dir_deleter", syncx.WithThreadLimit(3))

	for _, dir := range dirsToRemove {
		dirToRemove := dir // capture for closure
		op.OpX(func() (any, error) {
			if err := os.RemoveAll(dirToRemove); err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("failed to remove %s: %w", dirToRemove, err)
			}
			return nil, nil
		})
	}

	if err := op.WaitAndWrapErr(); err != nil {
		return err
	}

	// Count remaining subdirectories
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	dirCount := 0
	for _, entry := range entries {
		if entry.IsDir() {
			dirCount++
		}
	}

	// If no subdirectories remain, remove the parent directory
	if dirCount == 0 {
		if err := os.RemoveAll(dirPath); err != nil {
			return fmt.Errorf("failed to remove parent directory: %w", err)
		}
	}

	return nil
}

// ################################################################################################################## \\

// DelSoftDir removes a file or empty directory only.
// Fails if the directory is non-empty (equivalent to `rm` without flags).
// equivalent to the following bash command:
// rm $filepath
func DelSoftDir(ctx context.Context, path string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to remove %s: %w", path, err)
	}

	return nil
}

// DelHard recursively removes a path and all its contents, ignoring errors
// if the path doesn't exist (equivalent to `rm -rf`).
// equivalent to the following bash command:
// rm -rf $filepath
func DelHard(ctx context.Context, path string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// RemoveAll doesn't return an error if path doesn't exist (like -f flag)
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("failed to remove %s: %w", path, err)
	}

	return nil
}

// DelRecursive recursively removes a path and all its contents.
// Unlike DelHard, this will return an error if the path doesn't exist.
// equivalent to the following bash command:
// rm -r $filepath
func DelRecursive(ctx context.Context, path string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Check if path exists first (to mimic `rm -r` without -f)
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("path does not exist: %w", err)
	}

	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("failed to remove %s: %w", path, err)
	}

	return nil
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

//// CountFilesExec -maxdepth 1
//func CountFilesExec(ctx context.Context, filepath string) error {
//	return execx.RunContext(ctx,
//		// "find", filepath, "-maxdepth", "1", "-type", "f", "|", "wc", "-l")
//		"sh", "-c", fmt.Sprintf("find %s -maxdepth 1 -type f | wc -l", filepath))
//}
//
//func CountDirsExec(ctx context.Context, filepath string) error {
//	return execx.RunContext(ctx,
//		// "find", filepath, "-maxdepth", "1", "-type", "d", "|", "wc", "-l")
//		"sh", "-c", fmt.Sprintf("find %s -maxdepth 1 -type d | wc -l", filepath))
//}
