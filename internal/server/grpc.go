package server

import (
	"context"

	"github.com/jpricardo/henchman/internal/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type instanceKey struct{}

func makeInterceptor(r *registry.Registry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == "/henchman.v1.Cache/Register" {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		values := md.Get("x-hench-token")
		if !ok || len(values) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing or unknown token")
		}

		token := values[0]

		instance, ok := r.Resolve(token)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "unable to resolve")
		}

		ctx = context.WithValue(ctx, instanceKey{}, instance)
		return handler(ctx, req)
	}
}

func instanceFromContext(ctx context.Context) *registry.Instance {
	return ctx.Value(instanceKey{}).(*registry.Instance)
}
