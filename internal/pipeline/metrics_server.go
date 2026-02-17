package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ChaosHour/mysql2bq/internal/config"
	"github.com/ChaosHour/mysql2bq/internal/logging"
)

// MetricsServer provides HTTP endpoints for exposing CDC metrics
type MetricsServer struct {
	cfg     *config.Config
	logger  *logging.Logger
	metrics *CDCMetrics
	server  *http.Server
}

// NewMetricsServer creates a new metrics HTTP server
func NewMetricsServer(cfg *config.Config, logger *logging.Logger, metrics *CDCMetrics) *MetricsServer {
	return &MetricsServer{
		cfg:     cfg,
		logger:  logger,
		metrics: metrics,
	}
}

// Start starts the HTTP server in a goroutine
func (s *MetricsServer) Start(ctx context.Context) error {
	if !s.cfg.HTTP.Enabled {
		s.logger.Info("HTTP metrics server disabled")
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc(s.cfg.HTTP.Path, s.handleMetrics)
	mux.HandleFunc("/health", s.handleHealth)

	s.server = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", s.cfg.HTTP.Host, s.cfg.HTTP.Port),
		Handler: mux,
	}

	// Start server in goroutine
	go func() {
		s.logger.Info("Starting HTTP metrics server on %s:%d", s.cfg.HTTP.Host, s.cfg.HTTP.Port)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP metrics server failed: %v", err)
		}
	}()

	return nil
}

// Stop gracefully stops the HTTP server
func (s *MetricsServer) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}

	s.logger.Info("Stopping HTTP metrics server")
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return s.server.Shutdown(shutdownCtx)
}

// handleMetrics serves the metrics endpoint
func (s *MetricsServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snapshot := s.metrics.GetSnapshot()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(snapshot); err != nil {
		s.logger.Warn("Failed to encode metrics response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// handleHealth serves a simple health check endpoint
func (s *MetricsServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	health := map[string]interface{}{
		"status": "healthy",
		"time":   time.Now().UTC(),
		"uptime": s.metrics.GetSnapshot().Uptime.String(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(health); err != nil {
		s.logger.Warn("Failed to encode health response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}
