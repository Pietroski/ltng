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
	dockerLatency, err := measureContainerLatency(t, "test-integration-ltngdb-engine", 50051)
	require.NoError(t, err)
	t.Logf("Docker container latency: %v", dockerLatency)

	// Test latency to local server
	localLatency, err := measureLocalLatency(t, 50051)
	require.NoError(t, err)
	t.Logf("Local server latency: %v", localLatency)
}

func measureContainerLatency(tb testing.TB, containerName string, port int) (time.Duration, error) {
	addr := net.JoinHostPort(containerName, "50051")
	start := time.Now()
	
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	
	return time.Since(start), nil
}

func measureLocalLatency(tb testing.TB, port int) (time.Duration, error) {
	addr := net.JoinHostPort("localhost", "50051")
	start := time.Now()
	
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	
	return time.Since(start), nil
}

func BenchmarkNetworkLatency(b *testing.B) {
	b.Run("DockerLatency", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			latency, err := measureContainerLatency(b, "test-integration-ltngdb-engine", 50051)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(1)
			b.ReportMetric(float64(latency.Nanoseconds())/1e6, "ms/op")
		}
	})
	
	b.Run("LocalLatency", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			latency, err := measureLocalLatency(b, 50051)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(1)
			b.ReportMetric(float64(latency.Nanoseconds())/1e6, "ms/op")
		}
	})
}
