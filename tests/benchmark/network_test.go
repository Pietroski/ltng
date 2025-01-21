package benchmark

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestNetworkLatency(t *testing.T) {
	// Test latency to Docker container
	dockerLatency := measureContainerLatency(t, "test-integration-ltngdb-engine", 50051)
	t.Logf("Docker container latency: %v", dockerLatency)

	// Test latency to local server
	localLatency := measureLocalLatency(t, 50051)
	t.Logf("Local server latency: %v", localLatency)
}

func measureContainerLatency(t *testing.T, containerName string, port int) time.Duration {
	addr := net.JoinHostPort(containerName, "50051")
	start := time.Now()
	
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
	require.NoError(t, err)
	defer conn.Close()
	
	return time.Since(start)
}

func measureLocalLatency(t *testing.T, port int) time.Duration {
	addr := net.JoinHostPort("localhost", "50051")
	start := time.Now()
	
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
	require.NoError(t, err)
	defer conn.Close()
	
	return time.Since(start)
}

func BenchmarkNetworkLatency(b *testing.B) {
	ctx := context.Background()
	b.Run("DockerLatency", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			measureContainerLatency(b, "test-integration-ltngdb-engine", 50051)
		}
	})
	
	b.Run("LocalLatency", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			measureLocalLatency(b, 50051)
		}
	})
}
