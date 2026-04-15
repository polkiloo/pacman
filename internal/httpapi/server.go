package httpapi

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"

	apidoc "github.com/polkiloo/pacman/docs"
	agentmodel "github.com/polkiloo/pacman/internal/agent/model"
	"github.com/polkiloo/pacman/internal/cluster"
	"github.com/polkiloo/pacman/internal/controlplane"
)

// NodeStatusReader provides local node state from the control-plane store.
type NodeStatusReader interface {
	NodeStatus(nodeName string) (agentmodel.NodeStatus, bool)
	NodeStatuses() []agentmodel.NodeStatus
	ClusterSpec() (cluster.ClusterSpec, bool)
	ClusterStatus() (cluster.ClusterStatus, bool)
	MaintenanceStatus() cluster.MaintenanceModeStatus
	UpdateMaintenanceMode(context.Context, cluster.MaintenanceModeUpdateRequest) (cluster.MaintenanceModeStatus, error)
	History() []cluster.HistoryEntry
	CreateSwitchoverIntent(context.Context, controlplane.SwitchoverRequest) (controlplane.SwitchoverIntent, error)
	CancelSwitchover(context.Context) (cluster.Operation, error)
	CreateFailoverIntent(context.Context, controlplane.FailoverIntentRequest) (controlplane.FailoverIntent, error)
}

// OpenAPIDocumentProvider returns the published OpenAPI YAML served by pacmand.
type OpenAPIDocumentProvider func() ([]byte, error)

// Middleware decorates the Fiber app before PACMAN registers its built-in
// routes. Middleware may short-circuit requests or add supplemental endpoints.
type Middleware func(*fiber.Ctx) error

// MiddlewareFactory constructs an HTTP API middleware using the live state
// source bound to the current daemon.
type MiddlewareFactory func(NodeStatusReader) Middleware

// Config carries optional Server parameters.
type Config struct {
	// LivenessWindow is the maximum allowed age of the last heartbeat before
	// GET /liveness returns 503. Defaults to 30 seconds.
	LivenessWindow time.Duration
	// TLSConfig optionally enables TLS for the external PACMAN HTTP API.
	// When nil, the server listens in plaintext.
	TLSConfig *tls.Config
	// Authorizer optionally enforces authenticated access for administrative
	// control-plane endpoints. When nil, authentication is disabled.
	Authorizer Authorizer
	// OpenAPIDocument optionally overrides the published OpenAPI document
	// served by GET /openapi.yaml.
	OpenAPIDocument OpenAPIDocumentProvider
	// Middlewares are applied before built-in PACMAN routes are registered.
	Middlewares []Middleware
}

var errServerAlreadyStarted = errors.New("http api server is already started")

// Server is the PACMAN HTTP API server.
type Server struct {
	app            *fiber.App
	nodeName       string
	store          NodeStatusReader
	logger         *slog.Logger
	livenessWindow time.Duration
	tlsConfig      *tls.Config
	authorizer     Authorizer
	openAPIDoc     OpenAPIDocumentProvider
	requestSeq     atomic.Uint64
	openAPILoad    sync.Once
	openAPIBytes   []byte
	openAPIErr     error

	mu       sync.Mutex
	runDone  chan struct{}
	runErr   error
	listener net.Listener
	stopping bool
}

// New constructs and wires up the API server.
func New(nodeName string, store NodeStatusReader, logger *slog.Logger, cfg Config) *Server {
	lw := cfg.LivenessWindow
	if lw <= 0 {
		lw = 30 * time.Second
	}

	srv := &Server{
		nodeName:       nodeName,
		store:          store,
		logger:         logger,
		livenessWindow: lw,
		tlsConfig:      cfg.TLSConfig,
		authorizer:     cfg.Authorizer,
		openAPIDoc:     cfg.OpenAPIDocument,
	}

	if srv.openAPIDoc == nil {
		srv.openAPIDoc = apidoc.OpenAPIYAML
	}

	srv.app = fiber.New(fiber.Config{
		DisableStartupMessage: true,
		ReadTimeout:           10 * time.Second,
		WriteTimeout:          10 * time.Second,
	})

	srv.app.Use(srv.requestIDMiddleware())
	srv.app.Use(srv.accessLogMiddleware())
	srv.app.Use(recover.New())
	for _, middleware := range cfg.Middlewares {
		if middleware == nil {
			continue
		}

		srv.app.Use(fiber.Handler(middleware))
	}
	srv.registerRoutes()

	return srv
}

func (srv *Server) registerRoutes() {
	srv.addProbeRoute("/health", srv.handleHealth)
	srv.addProbeRoute("/liveness", srv.handleLiveness)
	srv.addProbeRoute("/readiness", srv.handleReadiness)
	srv.addProbeRoute("/primary", srv.handlePrimary)
	srv.addProbeRoute("/replica", srv.handleReplica)
	srv.app.Get("/openapi.yaml", srv.handleOpenAPIDocument)
	srv.app.Head("/openapi.yaml", srv.handleOpenAPIDocument)

	v1 := srv.app.Group("/api/v1", srv.apiCommonMiddleware())
	v1.Get("/cluster", srv.authMiddleware(AccessScopeClusterRead), srv.handleClusterStatus)
	v1.Get("/cluster/spec", srv.authMiddleware(AccessScopeClusterRead), srv.handleClusterSpec)
	v1.Get("/members", srv.authMiddleware(AccessScopeClusterRead), srv.handleMembers)
	v1.Get("/nodes/:nodeName", srv.authMiddleware(AccessScopeClusterRead), srv.handleNodeStatus)
	v1.Get("/history", srv.authMiddleware(AccessScopeClusterRead), srv.handleHistory)
	v1.Get("/maintenance", srv.authMiddleware(AccessScopeClusterRead), srv.handleMaintenanceStatus)
	v1.Put("/maintenance", srv.authMiddleware(AccessScopeClusterWrite), srv.requireJSONContentTypeMiddleware(), srv.handleMaintenanceUpdate)
	v1.Get("/diagnostics", srv.authMiddleware(AccessScopeClusterRead), srv.handleDiagnostics)
	v1.Post("/operations/switchover", srv.authMiddleware(AccessScopeClusterWrite), srv.requireJSONContentTypeMiddleware(), srv.handleSwitchoverCreate)
	v1.Delete("/operations/switchover", srv.authMiddleware(AccessScopeClusterWrite), srv.handleSwitchoverCancel)
	v1.Post("/operations/failover", srv.authMiddleware(AccessScopeClusterWrite), srv.requireJSONContentTypeMiddleware(), srv.handleFailoverCreate)

	srv.app.Use("/api/v1", srv.apiNotFoundMiddleware())
}

// addProbeRoute registers a probe handler for GET, HEAD, and OPTIONS on the
// given path. HEAD shares the GET handler; fasthttp suppresses the body
// automatically. OPTIONS returns an Allow header listing all supported methods.
func (srv *Server) addProbeRoute(path string, handler fiber.Handler) {
	srv.app.Get(path, handler)
	srv.app.Head(path, handler)
	srv.app.Options(path, handleProbeOptions)
}

func handleProbeOptions(c *fiber.Ctx) error {
	c.Set(fiber.HeaderAllow, "GET, HEAD, OPTIONS")
	return c.SendStatus(fiber.StatusOK)
}

// Start binds addr, serves requests in the background, and shuts down when ctx
// is cancelled.
func (srv *Server) Start(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	srv.mu.Lock()
	if srv.runDone != nil {
		srv.mu.Unlock()
		_ = listener.Close()
		return errServerAlreadyStarted
	}
	srv.runDone = make(chan struct{})
	srv.runErr = nil
	srv.listener = listener
	srv.stopping = false
	done := srv.runDone
	srv.mu.Unlock()

	go func() {
		<-ctx.Done()
		srv.mu.Lock()
		srv.stopping = true
		listener := srv.listener
		srv.mu.Unlock()

		if listener != nil {
			_ = listener.Close()
		}

		_ = srv.app.Shutdown()
	}()

	go func() {
		serveListener := listener
		if srv.tlsConfig != nil {
			serveListener = tls.NewListener(listener, srv.tlsConfig.Clone())
		}

		err := srv.app.Listener(serveListener)

		srv.mu.Lock()
		srv.listener = nil
		if srv.stopping && isClosedListenerError(err) {
			srv.runErr = nil
		} else {
			srv.runErr = err
		}
		srv.mu.Unlock()
		close(done)
	}()

	return nil
}

// Wait blocks until the server stops and returns the terminal serve error, if
// any.
func (srv *Server) Wait() error {
	srv.mu.Lock()
	done := srv.runDone
	srv.mu.Unlock()

	if done == nil {
		return nil
	}

	<-done

	srv.mu.Lock()
	defer srv.mu.Unlock()

	return srv.runErr
}

func isClosedListenerError(err error) bool {
	if err == nil || errors.Is(err, net.ErrClosed) {
		return true
	}

	return strings.Contains(err.Error(), "use of closed network connection")
}
