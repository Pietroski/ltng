package data

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/badgerdb/v4"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	serializermodels "gitlab.com/pietroski-software-company/devex/golang/serializer/models"

	ltng_client "gitlab.com/pietroski-software-company/lightning-db/client"
	ltng_engine_v1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v1"
	ltng_engine_concurrent_v1 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v1/concurrent"
	ltng_engine_v2 "gitlab.com/pietroski-software-company/lightning-db/internal/adaptors/datastore/ltng-engine/v2"
	common_model "gitlab.com/pietroski-software-company/lightning-db/internal/models/common"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/execx"
)

const (
	relativePath      = "../../"
	dockerComposePath = "build/orchestrator/docker-compose-test.yml"
)

var (
	LTNGDBEngineType   = fmt.Sprintf("LTNG_ENGINE=%s", common_model.LightningEngineV2EngineVersionType)
	BadgerDBEngineType = fmt.Sprintf("LTNG_ENGINE=%s", common_model.BadgerDBV4EngineVersionType)
)

type TestBench interface {
	*testing.T | *testing.B | *require.TestingT | *assert.TestingT

	Cleanup(f func())
	Errorf(format string, args ...interface{})
	FailNow()
	Fatalf(format string, args ...any)
	Fatal(args ...any)
	Logf(format string, args ...interface{})
	Log(args ...interface{})
}

func DockerComposeUp[T TestBench](tb T) {
	_, err := execx.Executor(exec.Command("sh", "-c",
		fmt.Sprintf("docker compose -f %s up -d --build --remove-orphans",
			relativePath+dockerComposePath),
	))
	require.NoError(tb, err)

	time.Sleep(2 * time.Second)

	// Add a wait for container health check
	err = waitForContainer(tb, "test-integration-ltngdb-engine", 30*time.Second)
	require.NoError(tb, err)

	err = waitForContainer(tb, "test-integration-badgerdb-engine", 30*time.Second)
	require.NoError(tb, err)
}

func DockerComposeDown[T TestBench](tb T) {
	tb.Cleanup(func() {
		_, err := execx.Executor(exec.Command("sh", "-c",
			fmt.Sprintf("docker compose -f %s down",
				relativePath+dockerComposePath),
		))
		require.NoError(tb, err)
	})
}

// Add this helper function to wait for container readiness
func waitForContainer[T TestBench](tb T, containerName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		bs, err := execx.Executor(exec.Command("sh", "-c",
			fmt.Sprintf("docker inspect --format='{{.State.Running}}' %s", containerName)))
		tb.Logf("output: %s", bs)
		if err == nil && strings.TrimSpace(string(bs)) == "true" {
			// Optional: Add additional check for port readiness
			if err = checkPortReady("localhost", "50050", 5*time.Second); err == nil {
				return nil
			}
		}

		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("container %s did not become ready within %v", containerName, timeout)
}

// Optional: Add port checking
func checkPortReady(host, port string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), time.Second)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("port %s:%s not ready within timeout", host, port)
}

type (
	ClientTestSuite struct {
		Ctx            context.Context
		Serializer     serializermodels.Serializer
		LTNGDBClient   ltng_client.Client
		BadgerDBClient ltng_client.Client
	}

	EngineTestSuite struct {
		Ctx                    context.Context
		Serializer             serializermodels.Serializer
		LTNGDBEngine           *ltng_engine_v1.LTNGEngine
		LTNGDBConcurrentEngine *ltng_engine_concurrent_v1.LTNGEngine
		LTNGDBEngineV2         *ltng_engine_v2.LTNGEngine
		BadgerDBEngine         *BadgerDBEngine
	}

	BadgerDBEngine struct {
		DB       *badger.DB
		Manager  v4.Manager
		Operator v4.Operator
	}
)

func (cts *ClientTestSuite) TS() *TestSuite {
	return &TestSuite{
		Ctx:        cts.Ctx,
		Serializer: cts.Serializer,
	}
}

func (ets *EngineTestSuite) TS() *TestSuite {
	return &TestSuite{
		Ctx:        ets.Ctx,
		Serializer: ets.Serializer,
	}
}

type TestSuite struct {
	Ctx        context.Context
	Serializer serializermodels.Serializer
}

const (
	ltngFileQueueBasePath = ".ltngfq"
	ltngdbBasePath        = ".ltngdb"
	dbBasePath            = ".db"
)

func InitClientTestSuite[T TestBench](tb T) *ClientTestSuite {
	DockerComposeUp(tb)
	DockerComposeDown(tb)

	ctx := context.Background()
	clientLTNG, err := ltng_client.New(ctx, &ltng_client.Params{
		Address: "127.0.0.1:50050",
		Engine:  common_model.LightningEngineV2EngineVersionType.String(),
	})
	require.NoError(tb, err)

	clientBadger, err := ltng_client.New(ctx, &ltng_client.Params{
		Address: "127.0.0.1:50051",
		Engine:  common_model.BadgerDBV4EngineVersionType.String(),
	})
	require.NoError(tb, err)

	return &ClientTestSuite{
		Ctx:            ctx,
		Serializer:     serializer.NewRawBinarySerializer(),
		LTNGDBClient:   clientLTNG,
		BadgerDBClient: clientBadger,
	}
}

func InitLocalClientTestSuite[T TestBench](tb T, engineType common_model.EngineVersionType) *ClientTestSuite {
	ctx := context.Background()
	clientTestSuite := &ClientTestSuite{
		Ctx:        ctx,
		Serializer: serializer.NewRawBinarySerializer(),
	}

	switch engineType {
	case common_model.LightningEngineV2EngineVersionType:
		clientLTNG, err := ltng_client.New(ctx, &ltng_client.Params{
			Address: "127.0.0.1:50050",
			Engine:  common_model.LightningEngineV2EngineVersionType.String(),
		})
		require.NoError(tb, err)
		clientTestSuite.LTNGDBClient = clientLTNG
	case common_model.BadgerDBV4EngineVersionType:
		clientBadger, err := ltng_client.New(ctx, &ltng_client.Params{
			Address: "127.0.0.1:50051",
			Engine:  common_model.BadgerDBV4EngineVersionType.String(),
		})
		require.NoError(tb, err)
		clientTestSuite.BadgerDBClient = clientBadger
	}

	return clientTestSuite
}

func InitEngineTestSuite[T TestBench](tb T) *EngineTestSuite {
	ctx := context.Background()
	CleanupDirectories(tb)

	ltngDBEngine, err := ltng_engine_v1.New(ctx)
	require.NoError(tb, err)

	ltngDBEngineV2, err := ltng_engine_v2.New(ctx)
	require.NoError(tb, err)

	ltngDBConcurrentEngine, err := ltng_engine_concurrent_v1.New(ctx)
	require.NoError(tb, err)

	db, err := badger.Open(badger.DefaultOptions(v4.InternalLocalManagement))
	require.NoError(tb, err)

	badgerDBEngineManager, err := v4.NewBadgerLocalManagerV4(ctx,
		v4.WithDB(db),
	)
	require.NoError(tb, err)

	err = badgerDBEngineManager.Start()
	require.NoError(tb, err)

	badgerDBEngineOperator, err := v4.NewBadgerOperatorV4(ctx,
		v4.WithManager(badgerDBEngineManager),
	)
	require.NoError(tb, err)

	// time.Sleep(1 * time.Second)

	return &EngineTestSuite{
		Ctx:                    ctx,
		Serializer:             serializer.NewRawBinarySerializer(),
		LTNGDBEngine:           ltngDBEngine,
		LTNGDBConcurrentEngine: ltngDBConcurrentEngine,
		LTNGDBEngineV2:         ltngDBEngineV2,
		BadgerDBEngine: &BadgerDBEngine{
			DB:       db,
			Manager:  badgerDBEngineManager,
			Operator: badgerDBEngineOperator,
		},
	}
}

func CleanupDirectories[T TestBench](tb T) {
	ctx := context.Background()
	_, err := execx.DelHardExec(ctx, ltngFileQueueBasePath)
	require.NoError(tb, err)
	_, err = execx.DelHardExec(ctx, ltngdbBasePath)
	require.NoError(tb, err)
	_, err = execx.DelHardExec(ctx, dbBasePath)
	require.NoError(tb, err)
}

func CleanupProcesses[T TestBench](tb T) {
	bs, err := execx.Executor(exec.Command("sh", "-c", "lsof -ti:50000-51000 | xargs kill -9"))
	require.NoError(tb, err)
	tb.Logf("Cleaning up processes: %s", bs)
}

func SetTestDeadline[T TestBench](tb T) {
	timeout := time.Second * 5
	deadline := time.Now().Add(timeout)
	tb.Cleanup(func() {
		if time.Now().After(deadline) {
			tb.Fatal("test timed out")
		}
	})
}
