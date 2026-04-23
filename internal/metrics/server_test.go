package metrics_test

import (
	"context"
	"math"
	"net"
	"net/http/httptest"
	"testing"

	pb "github.com/jpricardo/henchman/gen/proto/henchman/v1"
	"github.com/jpricardo/henchman/internal/budget"
	"github.com/jpricardo/henchman/internal/metrics"
	"github.com/jpricardo/henchman/internal/registry"
	"github.com/jpricardo/henchman/internal/server"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestMetricsSmoke(t *testing.T) {
	defer goleak.VerifyNone(t)

	gb := budget.NewGlobalBudget(64 * 1024 * 1024)
	r := registry.NewRegistry(gb)
	defer r.Shutdown()

	lis := bufconn.Listen(1024 * 1024)
	grpcSrv := server.NewGRPCServer(r)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewCacheClient(conn)
	ctx := context.Background()

	resp, err := client.Register(ctx, &pb.RegisterRequest{
		InstanceId: "smoke-test",
		Config: &pb.InstanceConfig{
			MaxBytes:        64 * 1024 * 1024,
			MaxKeys:         math.MaxInt64,
			EvictionPolicy:  "lru",
			SweepIntervalMs: 500,
		},
	})
	require.NoError(t, err)

	authCtx := metadata.AppendToOutgoingContext(ctx, "x-hench-token", resp.Token)

	_, err = client.Set(authCtx, &pb.SetRequest{Key: "k1", Value: []byte("hello")})
	require.NoError(t, err)

	hitResp, err := client.Get(authCtx, &pb.GetRequest{Key: "k1"})
	require.NoError(t, err)
	require.True(t, hitResp.Found)

	missResp, err := client.Get(authCtx, &pb.GetRequest{Key: "nonexistent"})
	require.NoError(t, err)
	require.False(t, missResp.Found)

	metricsSrv := metrics.NewServer(9090, r)
	req := httptest.NewRequest("GET", "/metrics", nil)
	rr := httptest.NewRecorder()
	metricsSrv.Handler().ServeHTTP(rr, req)

	body := rr.Body.String()
	require.Contains(t, body, `henchman_hits_total{instance_id="smoke-test"} 1`)
	require.Contains(t, body, `henchman_misses_total{instance_id="smoke-test"} 1`)
	require.Contains(t, body, `henchman_bytes_cap{instance_id="smoke-test"}`)
	require.Contains(t, body, `henchman_keys_current{instance_id="smoke-test"}`)
	require.Contains(t, body, `henchman_evictions_total{instance_id="smoke-test"}`)
	require.Contains(t, body, `henchman_expired_swept_total{instance_id="smoke-test"}`)
}
