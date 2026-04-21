package agent

import (
	"testing"

	"github.com/polkiloo/pacman/internal/httpapi"
	"github.com/polkiloo/pacman/internal/postgres"
)

func TestWithHTTPAPIMiddlewareFactoryAppendsNonNilFactory(t *testing.T) {
	t.Parallel()

	daemon := &Daemon{}

	WithHTTPAPIMiddlewareFactory(nil)(daemon)
	if len(daemon.apiMiddlewares) != 0 {
		t.Fatalf("expected nil factory to be ignored, got %d middlewares", len(daemon.apiMiddlewares))
	}

	factory := func(httpapi.NodeStatusReader) httpapi.Middleware {
		return nil
	}

	WithHTTPAPIMiddlewareFactory(factory)(daemon)
	if len(daemon.apiMiddlewares) != 1 {
		t.Fatalf("expected one middleware factory, got %d", len(daemon.apiMiddlewares))
	}
}

func TestWithLocalPostgresCtlSetsPgCtlWhenPresent(t *testing.T) {
	t.Parallel()

	daemon := &Daemon{}

	WithLocalPostgresCtl(nil)(daemon)
	if daemon.pgCtl != nil {
		t.Fatalf("expected nil pg_ctl to be ignored, got %+v", daemon.pgCtl)
	}

	ctl := &postgres.PGCtl{BinDir: "/usr/pgsql-17/bin", DataDir: "/var/lib/pgsql/17/data"}
	WithLocalPostgresCtl(ctl)(daemon)

	if daemon.pgCtl != ctl {
		t.Fatalf("expected daemon pg_ctl to be set to %#v, got %#v", ctl, daemon.pgCtl)
	}
}

func TestWithAdminTokenSetsDaemonAdminToken(t *testing.T) {
	t.Parallel()

	daemon := &Daemon{}
	WithAdminToken("secret-token")(daemon)

	if daemon.adminToken != "secret-token" {
		t.Fatalf("expected daemon admin token to be set, got %q", daemon.adminToken)
	}
}
