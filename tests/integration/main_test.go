package integration_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	go_binder "gitlab.com/pietroski-software-company/tools/binder/go-binder/pkg/tools/binder"
	go_env_extractor "gitlab.com/pietroski-software-company/tools/env-extractor/go-env-extractor/pkg/tools/env-extractor"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	badgerdb_engine_v4 "gitlab.com/pietroski-software-company/lightning-db/cmd/ltngdb/badgerdb/v4"
	ltngdb_engine_v1 "gitlab.com/pietroski-software-company/lightning-db/cmd/ltngdb/ltngdb/v1"
	ltngdb_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/cmd/ltngdb/ltngdb/v2"
	ltng_node_config "gitlab.com/pietroski-software-company/lightning-db/internal/config/ltngdb"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
)

func ReadEnvFile(filename string) (map[string]string, error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, fmt.Errorf("file does not exist: %s: %v", filename, err)
	}

	envFile, err := os.OpenFile(filename, os.O_RDONLY, 0744)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = envFile.Close()
	}()

	envMapping := make(map[string]string)
	envReader := bufio.NewReader(envFile)
	for { // envReader.Buffered() > 0
		line, err := envReader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		line = strings.TrimSpace(line)
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			if len(parts) != 1 {
				err = os.Setenv(strings.TrimSpace(parts[0]), "")
				if err != nil {
					return nil, err
				}
			}

			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		envMapping[key] = value
		err = os.Setenv(key, value)
		if err != nil {
			return nil, err
		}
	}

	return envMapping, nil
}

const (
	refToEnvFilename = "../../"
	envFilename      = ".local.env"
)

//func TestMain(m *testing.M) {
//	_, err := ReadEnvFile(refToEnvFilename + envFilename)
//	if err != nil {
//		log.Fatalf("Error reading environment variables: %v", err)
//	}
//
//	offThread := concurrent.New("TestMain")
//	offThread.Op(func() {
//		main()
//	})
//	offThread.Wait()
//
//	os.Exit(m.Run())
//}

func main(ctx context.Context, cancel context.CancelFunc) {
	//ctx, cancelFn := context.WithCancel(context.Background())
	//defer cancelFn()

	var err error
	loggerPublishers := &go_logger.Publishers{}
	loggerOpts := &go_logger.Opts{
		Debug:   true,
		Publish: false,
	}
	logger := go_logger.NewGoLogger(ctx, loggerPublishers, loggerOpts).FromCtx(ctx)

	s := serializer.NewJsonSerializer()
	validator := go_validator.NewStructValidator()
	binder := go_binder.NewStructBinder(s, validator)

	cfg := &ltng_node_config.Config{}
	err = go_env_extractor.LoadEnvs(cfg)
	if err != nil {
		logger.Errorf(
			"failed to load ltng's node configs",
			go_logger.Mapper("err", err.Error()),
		)

		return
	}

	switch common_model.ToEngineVersionType(cfg.Node.Engine.Engine) {
	case common_model.BadgerDBV4EngineVersionType:
		badgerdb_engine_v4.StartV4(ctx, cancel, cfg, logger, s, binder, func(code int) {
			logger.Debugf("os.Exit", go_logger.Mapper("code", code))
		})
	case common_model.LightningEngineV1EngineVersionType:
		ltngdb_engine_v1.StartV1(ctx, cancel, cfg, logger, s, binder, func(code int) {
			logger.Debugf("os.Exit", go_logger.Mapper("code", code))
		})
	case common_model.LightningEngineV2EngineVersionType:
		fallthrough
	case common_model.DefaultEngineVersionType:
		fallthrough
	default:
		ltngdb_engine_v2.StartV2(ctx, cancel, cfg, logger, s, binder, func(code int) {
			logger.Debugf("os.Exit", go_logger.Mapper("code", code))
		})
	}
}
