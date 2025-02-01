package benchmark

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	grpc_ltngdb "gitlab.com/pietroski-software-company/lightning-db/schemas/generated/go/ltngdb"
	"gitlab.com/pietroski-software-company/lightning-db/tests/data"
)

func TestNetworkLatency(t *testing.T) {
	t.Skip("allow it to run only locally")
	data.DockerComposeUp(t)
	defer data.DockerComposeDown(t)

	// Wait for containers to be ready with increased timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	waitForContainers(t, ctx)

	// Test latency to Docker container (LTNG)
	dockerLatencyLTNG, err := measureContainerLatency(t, "test-integration-ltngdb-engine", 50050)
	require.NoError(t, err)
	t.Logf("LTNG Docker container latency: %v", dockerLatencyLTNG)

	// Test latency to local LTNG
	localLatencyLTNG, err := measureLocalLatency(t, 50050)
	require.NoError(t, err)
	t.Logf("LTNG local latency: %v", localLatencyLTNG)

	// Test latency to Docker container (BadgerDB)
	dockerLatencyBadger, err := measureContainerLatency(t, "test-integration-badgerdb-engine", 50051)
	require.NoError(t, err)
	t.Logf("BadgerDB Docker container latency: %v", dockerLatencyBadger)

	// Test latency to local BadgerDB
	localLatencyBadger, err := measureLocalLatency(t, 50051)
	require.NoError(t, err)
	t.Logf("BadgerDB local latency: %v", localLatencyBadger)
}

func waitForContainers(t testing.TB, ctx context.Context) {
	t.Log("Waiting for containers to be ready...")
	maxRetries := 10           // Increased retries
	backoff := 3 * time.Second // Increased backoff

	for i := 0; i < maxRetries; i++ {
		// Check both containers
		cmd1 := exec.Command("docker", "inspect", "--format", "{{.State.Running}}", "test-integration-ltngdb-engine")
		cmd2 := exec.Command("docker", "inspect", "--format", "{{.State.Running}}", "test-integration-badgerdb-engine")

		out1, err1 := cmd1.Output()
		out2, err2 := cmd2.Output()

		if err1 != nil || err2 != nil ||
			strings.TrimSpace(string(out1)) != "true" ||
			strings.TrimSpace(string(out2)) != "true" {
			t.Logf("Attempt %d: Containers not running yet, waiting %v...", i+1, backoff)
			time.Sleep(backoff)
			continue
		}

		// Try connecting to both services with health check
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		}

		// Check LTNG service
		conn1, err1 := grpc.NewClient("localhost:50050", opts...)
		if err1 == nil {
			// Try a simple gRPC call to verify service is responding
			client := grpc_ltngdb.NewLightningDBClient(conn1)
			_, err := client.CreateStore(ctx, &grpc_ltngdb.CreateStoreRequest{
				Name: "health-check-store",
				Path: "health-check-store",
			})
			_ = conn1.Close()

			if err == nil || strings.Contains(err.Error(), "already exists") {
				t.Log("LTNG service is ready and responding")
				return
			}
			t.Logf("LTNG service not responding: %v", err)
		}

		// Check BadgerDB service
		conn2, err2 := grpc.NewClient("localhost:50051", opts...)
		if err2 == nil {
			// Try a simple gRPC call to verify service is responding
			client := grpc_ltngdb.NewLightningDBClient(conn2)
			_, err := client.CreateStore(ctx, &grpc_ltngdb.CreateStoreRequest{
				Name: "health-check-store",
				Path: "health-check-store",
			})
			conn2.Close()

			if err == nil || strings.Contains(err.Error(), "already exists") {
				t.Log("BadgerDB service is ready and responding")
				return
			}
			t.Logf("BadgerDB service not responding: %v", err)
		}

		t.Logf("Attempt %d: Services not ready, waiting %v... (LTNG err: %v, BadgerDB err: %v)",
			i+1, backoff, err1, err2)
		time.Sleep(backoff)
	}
	t.Fatal("Containers failed to become ready")
}

func measureNetworkLatency(t testing.TB, operation func() error) (time.Duration, error) {
	start := time.Now()
	err := operation()
	latency := time.Since(start)
	return latency, err
}

func measureContainerLatency(tb testing.TB, containerName string, port int) (time.Duration, error) {
	// Use localhost since we're exposing the port
	addr := net.JoinHostPort("localhost", fmt.Sprintf("%d", port))
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	}

	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to container: %v", err)
	}
	defer conn.Close()

	return time.Since(start), nil
}

func measureLocalLatency(tb testing.TB, port int) (time.Duration, error) {
	addr := net.JoinHostPort("localhost", fmt.Sprintf("%d", port))
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	}

	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return 0, fmt.Errorf("failed to connect locally: %v", err)
	}
	defer conn.Close()

	return time.Since(start), nil
}

func BenchmarkNetworkLatency(b *testing.B) {
	b.Run("DockerLatency", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			latency, err := measureContainerLatency(b, "test-integration-ltngdb-engine", 50050)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(1)
			b.ReportMetric(float64(latency.Nanoseconds())/1e6, "ms/op")
		}
	})

	b.Run("LocalLatency", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			latency, err := measureLocalLatency(b, 50050)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(1)
			b.ReportMetric(float64(latency.Nanoseconds())/1e6, "ms/op")
		}
	})
}
