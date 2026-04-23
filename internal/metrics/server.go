package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/jpricardo/henchman/internal/registry"
)

type Server struct {
	mux     *http.ServeMux
	httpSrv *http.Server
	reg     *registry.Registry
}

func NewServer(port int, reg *registry.Registry) *Server {
	s := &Server{reg: reg}
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/metrics", s.handleMetrics)
	s.httpSrv = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: s.mux,
	}
	return s
}

func (s *Server) Start() error {
	return s.httpSrv.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpSrv.Shutdown(ctx)
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snapshots := s.reg.Snapshots()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	var b strings.Builder

	writeFamily(&b, "henchman_hits_total", "counter", "Total number of cache hits.",
		snapshots, func(s registry.MetricsSnapshot) int64 { return s.Hits })

	writeFamily(&b, "henchman_misses_total", "counter", "Total number of cache misses.",
		snapshots, func(s registry.MetricsSnapshot) int64 { return s.Misses })

	writeFamily(&b, "henchman_evictions_total", "counter", "Total number of entries evicted to free space.",
		snapshots, func(s registry.MetricsSnapshot) int64 { return s.Evictions })

	writeFamily(&b, "henchman_expired_swept_total", "counter", "Total number of expired entries removed by background sweep.",
		snapshots, func(s registry.MetricsSnapshot) int64 { return s.ExpiredSwept })

	writeFamily(&b, "henchman_bytes_used", "gauge", "Current bytes used by cached entries.",
		snapshots, func(s registry.MetricsSnapshot) int64 { return s.BytesUsed })

	writeFamily(&b, "henchman_bytes_cap", "gauge", "Maximum bytes allocated to the instance.",
		snapshots, func(s registry.MetricsSnapshot) int64 { return s.BytesCap })

	writeFamily(&b, "henchman_keys_current", "gauge", "Current number of keys stored.",
		snapshots, func(s registry.MetricsSnapshot) int64 { return s.KeysCurrent })

	fmt.Fprint(w, b.String())
}

func writeFamily(b *strings.Builder, name, metricType, help string, snapshots []registry.MetricsSnapshot, getValue func(registry.MetricsSnapshot) int64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s %s\n", name, metricType)
	for _, s := range snapshots {
		fmt.Fprintf(b, "%s{instance_id=%q} %d\n", name, s.InstanceID, getValue(s))
	}
}
