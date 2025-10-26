package execx

import (
	"context"
	"os/exec"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
)

func Run(cmdArgs ...string) error {
	if len(cmdArgs) < 1 {
		return errorsx.New("no command to execute")
	}

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)

	defaultLogger := slogx.DefaultLogger()
	defaultLogger.Debug(context.Background(), "executing command",
		"cmd", cmd.String())

	output, err := cmd.CombinedOutput()
	if err != nil {
		defaultLogger.Error(context.Background(), "error executing command",
			"cmd", cmd.String(), "output", string(output), "err", err)
		return err
	}

	defaultLogger.Debug(context.Background(), "command executed successfully",
		"cmd", cmd.String(), "output", string(output))
	return nil
}

func RunContext(ctx context.Context, cmdArgs ...string) error {
	if len(cmdArgs) < 1 {
		return errorsx.New("no command to execute")
	}

	cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)

	defaultLogger := slogx.DefaultLogger()
	defaultLogger.Debug(ctx, "executing context command",
		"cmd", cmd.String())

	output, err := cmd.CombinedOutput()
	if err != nil {
		defaultLogger.Error(ctx, "error executing context command",
			"cmd", cmd.String(), "output", string(output), "err", err)
		return err
	}

	defaultLogger.Debug(ctx, "context command executed successfully",
		"cmd", cmd.String(), "output", string(output))
	return nil
}
