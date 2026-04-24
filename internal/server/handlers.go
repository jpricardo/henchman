package server

import (
	"context"
	"time"

	pb "github.com/jpricardo/henchman/gen/proto/henchman/v1"
	"github.com/jpricardo/henchman/internal/cache"
	"github.com/jpricardo/henchman/internal/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedCacheServer
	r *registry.Registry
}

func NewServer(r *registry.Registry) pb.CacheServer {
	return &server{r: r}
}

func NewGRPCServer(r *registry.Registry) *grpc.Server {
	s := grpc.NewServer(grpc.UnaryInterceptor(makeInterceptor(r)))
	pb.RegisterCacheServer(s, NewServer(r))
	return s
}

func (s *server) Register(ctx context.Context, r *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if r.Config.EvictionPolicy != "" && r.Config.EvictionPolicy != "lru" {
		return nil, status.Error(codes.InvalidArgument, "invalid eviction policy")
	}

	c := registry.RegisterConfig{
		PolicyFactory: func() cache.EvictionPolicy { return cache.NewLRUEvictionPolicy() },
		MaxBytes:      r.Config.MaxBytes,
		MaxKeys:       r.Config.MaxKeys,
		DefaultTTL:    time.Duration(r.Config.DefaultTtlMs) * time.Millisecond,
		SweepInterval: time.Duration(r.Config.SweepIntervalMs) * time.Millisecond,
		// TODO - Env, request params?
		ShardCount: 8,
	}

	token, err := s.r.Register(r.InstanceId, c)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.RegisterResponse{Token: token}, nil
}

func (s *server) Get(ctx context.Context, r *pb.GetRequest) (*pb.GetResponse, error) {
	instance := instanceFromContext(ctx)
	entry, found := instance.Get(r.Key, false)
	if !found || entry == nil {
		return &pb.GetResponse{Found: found}, nil
	}

	return &pb.GetResponse{Found: found, Value: entry.Value}, nil
}

func (s *server) Set(ctx context.Context, r *pb.SetRequest) (*pb.SetResponse, error) {
	instance := instanceFromContext(ctx)

	var expiresAt time.Time
	if r.TtlMs == 0 {
		d := instance.DefaultTTL()
		if d > 0 {
			expiresAt = time.Now().Add(d)
		}
		// zero time.Time = no expiry
	} else {
		expiresAt = time.Now().Add(time.Duration(r.TtlMs) * time.Millisecond)
	}

	e := cache.Entry{
		Key:                 r.Key,
		Value:               r.Value,
		ExpiresAt:           expiresAt,
		InvalidateAfterRead: r.InvalidateAfterRead,
	}

	err := instance.Set(&e)
	if err != nil {
		return nil, status.Error(codes.ResourceExhausted, err.Error())
	}

	return &pb.SetResponse{}, nil
}

func (s *server) Delete(ctx context.Context, r *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	instance := instanceFromContext(ctx)
	instance.Delete(r.Key)

	return &pb.DeleteResponse{}, nil
}

func (s *server) Query(ctx context.Context, r *pb.QueryRequest) (*pb.QueryResponse, error) {
	instance := instanceFromContext(ctx)
	entries := []*pb.KeyValue{}

	switch r.Matcher.(type) {
	case *pb.QueryRequest_ExactKey:
		entry, found := instance.Get(r.GetExactKey(), r.InvalidateMatched)
		if found && entry != nil {
			entries = append(entries, &pb.KeyValue{Key: entry.Key, Value: entry.Value})
		}

	case *pb.QueryRequest_KeyPrefix:
		values := instance.Query(r.GetKeyPrefix(), r.InvalidateMatched)
		for _, v := range values {
			entries = append(entries, &pb.KeyValue{Key: v.Key, Value: v.Value})
		}
	}

	return &pb.QueryResponse{Entries: entries}, nil
}

func (s *server) Flush(ctx context.Context, r *pb.FlushRequest) (*pb.FlushResponse, error) {
	instance := instanceFromContext(ctx)
	s.r.Remove(instance.Token())

	return &pb.FlushResponse{}, nil
}
