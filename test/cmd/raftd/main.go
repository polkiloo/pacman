package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
	raftdcs "github.com/polkiloo/pacman/internal/dcs/raft"
)

const (
	defaultHTTPAddress = ":8080"

	envHTTPAddress        = "PACMAN_DCS_RAFT_HTTP_ADDRESS"
	envClusterName        = "PACMAN_DCS_RAFT_CLUSTER_NAME"
	envTTL                = "PACMAN_DCS_RAFT_TTL"
	envRetryTimeout       = "PACMAN_DCS_RAFT_RETRY_TIMEOUT"
	envDataDir            = "PACMAN_DCS_RAFT_DATA_DIR"
	envBindAddress        = "PACMAN_DCS_RAFT_BIND_ADDRESS"
	envPeers              = "PACMAN_DCS_RAFT_PEERS"
	envBootstrap          = "PACMAN_DCS_RAFT_BOOTSTRAP"
	envApplyTimeout       = "PACMAN_DCS_RAFT_APPLY_TIMEOUT"
	envTransportTimeout   = "PACMAN_DCS_RAFT_TRANSPORT_TIMEOUT"
	envHeartbeatTimeout   = "PACMAN_DCS_RAFT_HEARTBEAT_TIMEOUT"
	envElectionTimeout    = "PACMAN_DCS_RAFT_ELECTION_TIMEOUT"
	envLeaderLeaseTimeout = "PACMAN_DCS_RAFT_LEADER_LEASE_TIMEOUT"
	envExpiryInterval     = "PACMAN_DCS_RAFT_EXPIRY_INTERVAL"
)

type server struct {
	backend dcs.DCS
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type campaignResponse struct {
	Lease dcs.LeaderLease `json:"lease"`
	Held  bool            `json:"held"`
}

type aliveResponse struct {
	Alive bool `json:"alive"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	config, httpAddress, err := loadConfig()
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	backend, err := raftdcs.New(config)
	if err != nil {
		logger.Error("create backend", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := backend.Close(); err != nil {
			logger.Warn("close backend", "error", err)
		}
	}()

	initializeCtx, initializeCancel := context.WithTimeout(context.Background(), config.RetryTimeout)
	defer initializeCancel()

	if err := backend.Initialize(initializeCtx); err != nil {
		logger.Error("initialize backend", "error", err)
		os.Exit(1)
	}

	handler := server{backend: backend}
	httpServer := &http.Server{
		Addr:    httpAddress,
		Handler: handler.routes(),
	}

	serveErrCh := make(chan error, 1)
	go func() {
		logger.Info("serving raft helper", "http", httpAddress, "raft", config.BindAddress)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErrCh <- err
		}
	}()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signalCh)

	select {
	case <-signalCh:
	case err := <-serveErrCh:
		logger.Error("serve http", "error", err)
		os.Exit(1)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("shutdown http", "error", err)
	}
}

func loadConfig() (raftdcs.Config, string, error) {
	clusterName := strings.TrimSpace(os.Getenv(envClusterName))
	if clusterName == "" {
		clusterName = "alpha"
	}

	ttl, err := durationFromEnv(envTTL, 750*time.Millisecond)
	if err != nil {
		return raftdcs.Config{}, "", err
	}

	retryTimeout, err := durationFromEnv(envRetryTimeout, 5*time.Second)
	if err != nil {
		return raftdcs.Config{}, "", err
	}

	applyTimeout, err := durationFromEnv(envApplyTimeout, retryTimeout)
	if err != nil {
		return raftdcs.Config{}, "", err
	}

	transportTimeout, err := durationFromEnv(envTransportTimeout, raftdcs.DefaultTransportTimeout)
	if err != nil {
		return raftdcs.Config{}, "", err
	}

	heartbeatTimeout, err := durationFromEnv(envHeartbeatTimeout, raftdcs.DefaultHeartbeatTimeout)
	if err != nil {
		return raftdcs.Config{}, "", err
	}

	electionTimeout, err := durationFromEnv(envElectionTimeout, raftdcs.DefaultElectionTimeout)
	if err != nil {
		return raftdcs.Config{}, "", err
	}

	leaderLeaseTimeout, err := durationFromEnv(envLeaderLeaseTimeout, raftdcs.DefaultLeaderLeaseTimeout)
	if err != nil {
		return raftdcs.Config{}, "", err
	}

	expiryInterval, err := durationFromEnv(envExpiryInterval, raftdcs.DefaultExpiryInterval)
	if err != nil {
		return raftdcs.Config{}, "", err
	}

	bootstrap, err := boolFromEnv(envBootstrap, false)
	if err != nil {
		return raftdcs.Config{}, "", err
	}

	httpAddress := strings.TrimSpace(os.Getenv(envHTTPAddress))
	if httpAddress == "" {
		httpAddress = defaultHTTPAddress
	}

	config := raftdcs.Config{
		ClusterName:        clusterName,
		TTL:                ttl,
		RetryTimeout:       retryTimeout,
		DataDir:            strings.TrimSpace(os.Getenv(envDataDir)),
		BindAddress:        strings.TrimSpace(os.Getenv(envBindAddress)),
		Peers:              splitCSV(os.Getenv(envPeers)),
		Bootstrap:          bootstrap,
		ApplyTimeout:       applyTimeout,
		TransportTimeout:   transportTimeout,
		HeartbeatTimeout:   heartbeatTimeout,
		ElectionTimeout:    electionTimeout,
		LeaderLeaseTimeout: leaderLeaseTimeout,
		ExpiryInterval:     expiryInterval,
	}

	return config.WithDefaults(), httpAddress, nil
}

func durationFromEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}

	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}

	return value, nil
}

func boolFromEnv(name string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}

	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", name, err)
	}

	return value, nil
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	peers := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			peers = append(peers, trimmed)
		}
	}

	return peers
}

func (srv server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", srv.handleHealth)
	mux.HandleFunc("/v1/key", srv.handleKey)
	mux.HandleFunc("/v1/list", srv.handleList)
	mux.HandleFunc("/v1/campaign", srv.handleCampaign)
	mux.HandleFunc("/v1/leader", srv.handleLeader)
	mux.HandleFunc("/v1/resign", srv.handleResign)
	mux.HandleFunc("/v1/touch", srv.handleTouch)
	mux.HandleFunc("/v1/alive", srv.handleAlive)
	mux.HandleFunc("/v1/watch", srv.handleWatch)
	return mux
}

func (srv server) handleHealth(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true})
}

func (srv server) handleKey(writer http.ResponseWriter, request *http.Request) {
	key := strings.TrimSpace(request.URL.Query().Get("key"))
	if key == "" {
		writeJSON(writer, http.StatusBadRequest, apiError{Code: "invalid_argument", Message: "missing key"})
		return
	}

	switch request.Method {
	case http.MethodGet:
		entry, err := srv.backend.Get(request.Context(), key)
		if err != nil {
			writeDCSError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, entry)
	case http.MethodPut:
		value, err := io.ReadAll(request.Body)
		if err != nil {
			writeJSON(writer, http.StatusBadRequest, apiError{Code: "invalid_argument", Message: "read body"})
			return
		}

		options := make([]dcs.SetOption, 0, 1)
		if rawTTL := strings.TrimSpace(request.URL.Query().Get("ttl_ms")); rawTTL != "" {
			ttlMillis, err := strconv.ParseInt(rawTTL, 10, 64)
			if err != nil {
				writeJSON(writer, http.StatusBadRequest, apiError{Code: "invalid_argument", Message: "invalid ttl_ms"})
				return
			}
			options = append(options, dcs.WithTTL(time.Duration(ttlMillis)*time.Millisecond))
		}

		if err := srv.backend.Set(request.Context(), key, value, options...); err != nil {
			writeDCSError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := srv.backend.Delete(request.Context(), key); err != nil {
			writeDCSError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (srv server) handleList(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	prefix := strings.TrimSpace(request.URL.Query().Get("prefix"))
	if prefix == "" {
		writeJSON(writer, http.StatusBadRequest, apiError{Code: "invalid_argument", Message: "missing prefix"})
		return
	}

	entries, err := srv.backend.List(request.Context(), prefix)
	if err != nil {
		writeDCSError(writer, err)
		return
	}

	writeJSON(writer, http.StatusOK, entries)
}

func (srv server) handleCampaign(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	candidate := strings.TrimSpace(request.URL.Query().Get("candidate"))
	if candidate == "" {
		writeJSON(writer, http.StatusBadRequest, apiError{Code: "invalid_argument", Message: "missing candidate"})
		return
	}

	lease, held, err := srv.backend.Campaign(request.Context(), candidate)
	if err != nil {
		writeDCSError(writer, err)
		return
	}

	writeJSON(writer, http.StatusOK, campaignResponse{Lease: lease, Held: held})
}

func (srv server) handleLeader(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	lease, ok, err := srv.backend.Leader(request.Context())
	if err != nil {
		writeDCSError(writer, err)
		return
	}

	writeJSON(writer, http.StatusOK, map[string]any{
		"ok":    ok,
		"lease": lease,
	})
}

func (srv server) handleResign(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if err := srv.backend.Resign(request.Context()); err != nil {
		writeDCSError(writer, err)
		return
	}

	writer.WriteHeader(http.StatusNoContent)
}

func (srv server) handleTouch(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	member := strings.TrimSpace(request.URL.Query().Get("member"))
	if member == "" {
		writeJSON(writer, http.StatusBadRequest, apiError{Code: "invalid_argument", Message: "missing member"})
		return
	}

	if err := srv.backend.Touch(request.Context(), member); err != nil {
		writeDCSError(writer, err)
		return
	}

	writer.WriteHeader(http.StatusNoContent)
}

func (srv server) handleAlive(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	member := strings.TrimSpace(request.URL.Query().Get("member"))
	if member == "" {
		writeJSON(writer, http.StatusBadRequest, apiError{Code: "invalid_argument", Message: "missing member"})
		return
	}

	alive, err := srv.backend.Alive(request.Context(), member)
	if err != nil {
		writeDCSError(writer, err)
		return
	}

	writeJSON(writer, http.StatusOK, aliveResponse{Alive: alive})
}

func (srv server) handleWatch(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	prefix := strings.TrimSpace(request.URL.Query().Get("prefix"))
	if prefix == "" {
		writeJSON(writer, http.StatusBadRequest, apiError{Code: "invalid_argument", Message: "missing prefix"})
		return
	}

	events, err := srv.backend.Watch(request.Context(), prefix)
	if err != nil {
		writeDCSError(writer, err)
		return
	}

	flusher, ok := writer.(http.Flusher)
	if !ok {
		writeJSON(writer, http.StatusInternalServerError, apiError{Code: "internal", Message: "streaming unsupported"})
		return
	}

	writer.Header().Set("Content-Type", "application/x-ndjson")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(http.StatusOK)
	flusher.Flush()

	encoder := json.NewEncoder(writer)
	for {
		select {
		case <-request.Context().Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}

			if err := encoder.Encode(event); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func writeJSON(writer http.ResponseWriter, status int, payload any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(payload); err != nil {
		slog.Default().Error("encode json response", "error", err)
	}
}

func writeDCSError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, dcs.ErrNotLeader):
		writeJSON(writer, http.StatusConflict, apiError{Code: "not_leader", Message: err.Error()})
	case errors.Is(err, dcs.ErrRevisionMismatch):
		writeJSON(writer, http.StatusConflict, apiError{Code: "revision_mismatch", Message: err.Error()})
	case errors.Is(err, dcs.ErrKeyNotFound):
		writeJSON(writer, http.StatusNotFound, apiError{Code: "key_not_found", Message: err.Error()})
	case errors.Is(err, dcs.ErrBackendUnavailable):
		writeJSON(writer, http.StatusServiceUnavailable, apiError{Code: "backend_unavailable", Message: err.Error()})
	default:
		writeJSON(writer, http.StatusInternalServerError, apiError{Code: "internal", Message: err.Error()})
	}
}
