package backup_restore

import (
	"context"
	"fmt"
	"os/exec"
)

const (
	rootDBPath      = ".db"
	defaultFilePath = "./db.bak"
)

func BackupRAW(_ context.Context, filePath string) error {
	err := exec.Command("tar", "-czf", filePath, rootDBPath).Run()
	if err != nil {
		return fmt.Errorf("failed to create backup file: %v", err)
	}

	return nil
}

func RestoreRAW(_ context.Context, filePath string) error {
	err := exec.Command("tar", "-xzf", filePath).Run()
	if err != nil {
		return fmt.Errorf("failed to restore backup file: %v", err)
	}

	return nil
}
