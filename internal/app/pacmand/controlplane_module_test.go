package pacmand

import (
	"bytes"
	"context"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/agent"
	"go.uber.org/fx/fxtest"

	"github.com/polkiloo/pacman/internal/config"
	"github.com/polkiloo/pacman/internal/dcs"
	"github.com/polkiloo/pacman/internal/logging"
)

func TestNewControlPlaneAgentOptionReturnsNilWithoutDCSConfig(t *testing.T) {
	t.Parallel()

	option, err := newControlPlaneAgentOption(controlPlaneParams{
		Lifecycle: fxtest.NewLifecycle(t),
		Context:   context.Background(),
	})
	if err != nil {
		t.Fatalf("new control plane agent option: %v", err)
	}

	if option != nil {
		t.Fatal("expected nil agent option when dcs config is absent")
	}
}

func TestNewControlPlaneAgentOptionReturnsConfigError(t *testing.T) {
	t.Parallel()

	option, err := newControlPlaneAgentOption(controlPlaneParams{
		Lifecycle: fxtest.NewLifecycle(t),
		Context:   context.Background(),
		Config: &config.Config{
			DCS: &dcs.Config{
				Backend:      dcs.BackendEtcd,
				ClusterName:  "alpha",
				TTL:          time.Second,
				RetryTimeout: time.Second,
				Etcd: &dcs.EtcdConfig{
					Endpoints: []string{"127.0.0.1:2379"},
				},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "open configured dcs backend") {
		t.Fatalf("unexpected control plane agent option error: %v", err)
	}

	if option != nil {
		t.Fatal("expected nil option on invalid dcs config")
	}
}

func TestNewControlPlaneAgentOptionWrapsUnsupportedBackendError(t *testing.T) {
	t.Parallel()

	option, err := newControlPlaneAgentOption(controlPlaneParams{
		Lifecycle: fxtest.NewLifecycle(t),
		Context:   context.Background(),
		Config: &config.Config{
			DCS: &dcs.Config{
				Backend:      "bogus",
				ClusterName:  "alpha",
				TTL:          time.Second,
				RetryTimeout: time.Second,
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), `open configured dcs backend: unsupported dcs backend "bogus"`) {
		t.Fatalf("unexpected unsupported backend error: %v", err)
	}

	if option != nil {
		t.Fatal("expected nil option on unsupported backend")
	}
}

func TestOpenConfiguredDCSReturnsRaftConfigError(t *testing.T) {
	t.Parallel()

	backend, err := openConfiguredDCS(dcs.Config{
		Backend:      dcs.BackendRaft,
		ClusterName:  "alpha",
		TTL:          time.Second,
		RetryTimeout: time.Second,
	})
	if err == nil {
		t.Fatal("expected raft config error")
	}

	if !strings.Contains(err.Error(), dcs.ErrRaftConfigRequired.Error()) {
		t.Fatalf("unexpected raft config error: %v", err)
	}

	if backend != nil {
		t.Fatalf("expected nil backend on raft config error, got %#v", backend)
	}
}

func TestNewControlPlaneAgentOptionCreatesRaftBackedStore(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on loopback: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close loopback listener: %v", err)
	}

	lifecycle := fxtest.NewLifecycle(t)
	option, err := newControlPlaneAgentOption(controlPlaneParams{
		Lifecycle: lifecycle,
		Context:   context.Background(),
		Config: &config.Config{
			DCS: &dcs.Config{
				Backend:      dcs.BackendRaft,
				ClusterName:  "alpha",
				TTL:          time.Second,
				RetryTimeout: time.Second,
				Raft: &dcs.RaftConfig{
					DataDir:     t.TempDir(),
					BindAddress: address,
					Peers:       []string{address},
				},
			},
		},
		Logger: logging.New("pacmand", &bytes.Buffer{}),
	})
	if err != nil {
		t.Fatalf("new control plane agent option: %v", err)
	}

	if option == nil {
		t.Fatal("expected control-plane agent option for valid raft config")
	}

	daemon := &agent.Daemon{}
	option(daemon)

	fields := reflect.ValueOf(daemon).Elem()
	if fields.FieldByName("statePublisher").IsNil() {
		t.Fatal("expected control-plane store to set daemon statePublisher")
	}

	if fields.FieldByName("stateReader").IsNil() {
		t.Fatal("expected control-plane store to set daemon stateReader")
	}

	lifecycle.RequireStart()
	lifecycle.RequireStop()
}
