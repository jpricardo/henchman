package server_test

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/jpricardo/henchman/gen/proto/henchman/v1"
	"github.com/jpricardo/henchman/internal/budget"
	"github.com/jpricardo/henchman/internal/registry"
	"github.com/jpricardo/henchman/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024
const maxMemory = 64 * 1024 * 1024

func startServer(t *testing.T) (pb.CacheClient, func()) {
	lis := bufconn.Listen(bufSize)

	gb := budget.NewGlobalBudget(maxMemory)
	r := registry.NewRegistry(gb)
	grpcSrv := server.NewGRPCServer(r)

	go grpcSrv.Serve(lis)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	cleanup := func() {
		conn.Close()
		grpcSrv.Stop()
		r.Shutdown()
	}

	return pb.NewCacheClient(conn), cleanup
}

func ctxWithToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-hench-token", token)
}

func registerTestInstance(t *testing.T) (context.Context, pb.CacheClient, func()) {
	client, cleanup := startServer(t)

	ctx := context.Background()
	config := pb.InstanceConfig{
		MaxBytes:        maxMemory,
		MaxKeys:         math.MaxInt64,
		DefaultTtlMs:    0,
		EvictionPolicy:  "lru",
		SweepIntervalMs: 500,
	}
	request := pb.RegisterRequest{InstanceId: "test-instance", Config: &config}

	response, err := client.Register(ctx, &request)
	require.NoError(t, err)

	c := ctxWithToken(ctx, response.Token)
	return c, client, cleanup
}

func TestRegister(t *testing.T) {
	client, cleanup := startServer(t)
	defer cleanup()

	ctx := context.Background()
	config := pb.InstanceConfig{
		MaxBytes:        maxMemory,
		MaxKeys:         math.MaxInt64,
		DefaultTtlMs:    0,
		EvictionPolicy:  "lru",
		SweepIntervalMs: 500,
	}
	request := pb.RegisterRequest{InstanceId: "test-instance", Config: &config}

	_, err := client.Register(ctx, &request)
	require.NoError(t, err)

	_, err = client.Register(ctx, &request)
	require.NoError(t, err)
}

func TestGet_Hit(t *testing.T) {
	ctx, client, cleanup := registerTestInstance(t)
	defer cleanup()

	_, err := client.Set(ctx, &pb.SetRequest{Key: "key-1"})
	require.NoError(t, err)

	r, err := client.Get(ctx, &pb.GetRequest{Key: "key-1"})
	require.NoError(t, err)
	require.True(t, r.Found)

}

func TestGet_Miss(t *testing.T) {
	ctx, client, cleanup := registerTestInstance(t)
	defer cleanup()

	r, err := client.Get(ctx, &pb.GetRequest{Key: "key-1"})
	require.NoError(t, err)
	require.False(t, r.Found)
}

func TestGet_TTLExpires(t *testing.T) {
	ctx, client, cleanup := registerTestInstance(t)
	defer cleanup()

	ttlMs := int64(50)

	_, err := client.Set(ctx, &pb.SetRequest{Key: "key-1", TtlMs: ttlMs})
	require.NoError(t, err)

	time.Sleep(time.Duration(ttlMs) * time.Millisecond * 2)

	r, err := client.Get(ctx, &pb.GetRequest{Key: "key-1"})
	require.NoError(t, err)
	require.False(t, r.Found)
}

func TestGet_InvalidateAfterRead(t *testing.T) {
	ctx, client, cleanup := registerTestInstance(t)
	defer cleanup()

	_, err := client.Set(ctx, &pb.SetRequest{Key: "key-1", InvalidateAfterRead: true})
	require.NoError(t, err)

	r, err := client.Get(ctx, &pb.GetRequest{Key: "key-1"})
	require.NoError(t, err)
	require.True(t, r.Found)

	r, err = client.Get(ctx, &pb.GetRequest{Key: "key-1"})
	require.NoError(t, err)
	require.False(t, r.Found)
}

func TestDelete(t *testing.T) {
	ctx, client, cleanup := registerTestInstance(t)
	defer cleanup()

	_, err := client.Set(ctx, &pb.SetRequest{Key: "key-1"})
	require.NoError(t, err)

	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: "key-1"})
	require.NoError(t, err)

	r, err := client.Get(ctx, &pb.GetRequest{Key: "key-1"})
	require.NoError(t, err)
	require.False(t, r.Found)
}

func TestDelete_NotFound(t *testing.T) {
	ctx, client, cleanup := registerTestInstance(t)
	defer cleanup()

	_, err := client.Delete(ctx, &pb.DeleteRequest{Key: "nonexistent"})
	require.NoError(t, err)
}

func TestQuery_ExactKey(t *testing.T) {
	ctx, client, cleanup := registerTestInstance(t)
	defer cleanup()

	_, err := client.Set(ctx, &pb.SetRequest{Key: "key-1", Value: []byte("val-1")})
	require.NoError(t, err)

	r, err := client.Query(ctx, &pb.QueryRequest{Matcher: &pb.QueryRequest_ExactKey{ExactKey: "key-1"}})
	require.NoError(t, err)
	require.Len(t, r.Entries, 1)
	require.Equal(t, "key-1", r.Entries[0].Key)
	require.Equal(t, []byte("val-1"), r.Entries[0].Value)
}

func TestQuery_ExactKey_Miss(t *testing.T) {
	ctx, client, cleanup := registerTestInstance(t)
	defer cleanup()

	r, err := client.Query(ctx, &pb.QueryRequest{Matcher: &pb.QueryRequest_ExactKey{ExactKey: "nonexistent"}})
	require.NoError(t, err)
	require.Empty(t, r.Entries)
}

func TestQuery_KeyPrefix(t *testing.T) {
	ctx, client, cleanup := registerTestInstance(t)
	defer cleanup()

	_, err := client.Set(ctx, &pb.SetRequest{Key: "user:1", Value: []byte("a")})
	require.NoError(t, err)
	_, err = client.Set(ctx, &pb.SetRequest{Key: "user:2", Value: []byte("b")})
	require.NoError(t, err)
	_, err = client.Set(ctx, &pb.SetRequest{Key: "order:1", Value: []byte("c")})
	require.NoError(t, err)

	r, err := client.Query(ctx, &pb.QueryRequest{Matcher: &pb.QueryRequest_KeyPrefix{KeyPrefix: "user:"}})
	require.NoError(t, err)
	require.Len(t, r.Entries, 2)
}

func TestQuery_InvalidateMatched(t *testing.T) {
	ctx, client, cleanup := registerTestInstance(t)
	defer cleanup()

	_, err := client.Set(ctx, &pb.SetRequest{Key: "user:1"})
	require.NoError(t, err)
	_, err = client.Set(ctx, &pb.SetRequest{Key: "user:2"})
	require.NoError(t, err)

	_, err = client.Query(ctx, &pb.QueryRequest{
		Matcher:           &pb.QueryRequest_KeyPrefix{KeyPrefix: "user:"},
		InvalidateMatched: true,
	})
	require.NoError(t, err)

	r1, err := client.Get(ctx, &pb.GetRequest{Key: "user:1"})
	require.NoError(t, err)
	require.False(t, r1.Found)

	r2, err := client.Get(ctx, &pb.GetRequest{Key: "user:2"})
	require.NoError(t, err)
	require.False(t, r2.Found)
}

func TestFlush(t *testing.T) {
	ctx, client, cleanup := registerTestInstance(t)
	defer cleanup()

	_, err := client.Flush(ctx, &pb.FlushRequest{})
	require.NoError(t, err)

	_, err = client.Get(ctx, &pb.GetRequest{Key: "any"})
	st, _ := status.FromError(err)
	require.Equal(t, codes.Unauthenticated, st.Code())
}
