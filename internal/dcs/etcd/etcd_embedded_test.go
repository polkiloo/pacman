package etcd

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/polkiloo/pacman/internal/dcs"
	"github.com/polkiloo/pacman/internal/dcs/dcstest"
)

const embeddedTestConfigKey = "/pacman/alpha/config"

func TestBackendConformanceWithEmbeddedEtcd(t *testing.T) {
	endpoints := startEmbeddedEtcd(t)

	const ttl = time.Second

	dcstest.Run(t, dcstest.Config{
		TTL: ttl,
		New: func(t *testing.T) dcs.DCS {
			t.Helper()

			backend, err := New(dcs.Config{
				Backend:      dcs.BackendEtcd,
				ClusterName:  embeddedTestClusterName(t.Name()),
				TTL:          ttl,
				RetryTimeout: 2 * time.Second,
				Etcd: &dcs.EtcdConfig{
					Endpoints: endpoints,
				},
			})
			if err != nil {
				t.Fatalf("create etcd backend: %v", err)
			}

			return embeddedNamespacedBackend{
				DCS:       backend,
				namespace: "/_dcstest/" + embeddedTestClusterName(t.Name()),
			}
		},
	})
}

func TestEmbeddedEtcdBackendCloseMarksBackendUnavailable(t *testing.T) {
	endpoints := startEmbeddedEtcd(t)

	backend, err := New(dcs.Config{
		Backend:      dcs.BackendEtcd,
		ClusterName:  "alpha",
		TTL:          time.Second,
		RetryTimeout: time.Second,
		Etcd: &dcs.EtcdConfig{
			Endpoints: endpoints,
		},
	})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	if err := backend.Initialize(context.Background()); err != nil {
		t.Fatalf("initialize backend: %v", err)
	}

	if backend.internalPrefix != "/_pacman_internal/alpha" || backend.leaderKey == "" || backend.sessionsPrefix == "" {
		t.Fatalf("unexpected backend internals: %+v", backend)
	}

	if err := backend.Close(); err != nil {
		t.Fatalf("close backend: %v", err)
	}

	if err := backend.Close(); err != nil {
		t.Fatalf("second close should stay nil, got %v", err)
	}

	if _, err := backend.Get(context.Background(), embeddedTestConfigKey); !errors.Is(err, dcs.ErrBackendUnavailable) {
		t.Fatalf("expected closed backend to be unavailable, got %v", err)
	}
}

func TestEmbeddedEtcdBackendLoadersAndDeleteIfCurrent(t *testing.T) {
	endpoints := startEmbeddedEtcd(t)
	backend := newEmbeddedBackend(t, endpoints, "alpha-loaders", time.Second)

	ctx := context.Background()

	if current, ok, err := backend.loadEntry(ctx, embeddedTestConfigKey); err != nil || ok {
		t.Fatalf("expected missing entry, got current=%+v ok=%t err=%v", current, ok, err)
	}

	if current, ok, err := backend.loadLeader(ctx); err != nil || ok {
		t.Fatalf("expected missing leader, got current=%+v ok=%t err=%v", current, ok, err)
	}

	if term, modRevision, ok, err := backend.loadLeaderTerm(ctx); err != nil || ok || term != 0 || modRevision != 0 {
		t.Fatalf("expected missing leader term, got term=%d modRevision=%d ok=%t err=%v", term, modRevision, ok, err)
	}

	if err := backend.Set(ctx, " /pacman/alpha/config ", []byte("value"), dcs.WithTTL(time.Second)); err != nil {
		t.Fatalf("set entry: %v", err)
	}

	current, ok, err := backend.loadEntry(ctx, " /pacman/alpha/config ")
	if err != nil || !ok {
		t.Fatalf("expected stored entry, got current=%+v ok=%t err=%v", current, ok, err)
	}

	if current.value.Key != embeddedTestConfigKey || current.value.Revision != 1 || string(current.value.Value) != "value" {
		t.Fatalf("unexpected loaded entry: %+v", current)
	}

	if current.leaseID == clientv3.NoLease {
		t.Fatal("expected ttl-backed entry to carry a lease")
	}

	if _, held, err := backend.Campaign(ctx, "alpha-1"); err != nil || !held {
		t.Fatalf("campaign leader: held=%t err=%v", held, err)
	}

	leader, ok, err := backend.loadLeader(ctx)
	if err != nil || !ok {
		t.Fatalf("expected stored leader, got current=%+v ok=%t err=%v", leader, ok, err)
	}

	if leader.record.Leader != "alpha-1" || leader.record.Term != 1 {
		t.Fatalf("unexpected leader record: %+v", leader)
	}

	term, modRevision, ok, err := backend.loadLeaderTerm(ctx)
	if err != nil || !ok || term != 1 || modRevision == 0 {
		t.Fatalf("unexpected leader term state: term=%d modRevision=%d ok=%t err=%v", term, modRevision, ok, err)
	}

	if err := backend.deleteIfCurrent(ctx, backend.leaderKey, leader.modRevision-1); err != nil {
		t.Fatalf("delete stale leader revision: %v", err)
	}

	if _, ok, err := backend.loadLeader(ctx); err != nil || !ok {
		t.Fatalf("expected stale delete to preserve leader, ok=%t err=%v", ok, err)
	}

	if err := backend.deleteIfCurrent(ctx, backend.leaderKey, leader.modRevision); err != nil {
		t.Fatalf("delete current leader revision: %v", err)
	}

	if _, ok, err := backend.loadLeader(ctx); err != nil || ok {
		t.Fatalf("expected leader delete to remove leader, ok=%t err=%v", ok, err)
	}

	if _, err := backend.client.Put(ctx, backend.leaderTermKey, "not-a-number"); err != nil {
		t.Fatalf("seed invalid leader term: %v", err)
	}

	if _, _, _, err := backend.loadLeaderTerm(ctx); err == nil || !strings.Contains(err.Error(), "decode leader term") {
		t.Fatalf("expected invalid leader term decode error, got %v", err)
	}
}

func TestEmbeddedEtcdBackendLeaseHelpers(t *testing.T) {
	endpoints := startEmbeddedEtcd(t)
	backend := newEmbeddedBackend(t, endpoints, "alpha-leases", time.Second)

	ctx := context.Background()

	leaseID, err := backend.grantLease(ctx, time.Second)
	if err != nil {
		t.Fatalf("grant lease: %v", err)
	}

	if leaseID == clientv3.NoLease {
		t.Fatal("expected granted lease id")
	}

	if err := backend.refreshLease(ctx, leaseID); err != nil {
		t.Fatalf("refresh lease: %v", err)
	}

	alive, err := backend.leaseAlive(ctx, leaseID)
	if err != nil {
		t.Fatalf("lease alive: %v", err)
	}

	if !alive {
		t.Fatal("expected granted lease to be alive")
	}

	if _, err := backend.client.Revoke(ctx, leaseID); err != nil {
		t.Fatalf("revoke lease: %v", err)
	}

	if err := backend.refreshLease(ctx, leaseID); !errors.Is(err, dcs.ErrSessionExpired) {
		t.Fatalf("expected revoked lease refresh to expire session, got %v", err)
	}

	alive, err = backend.leaseAlive(ctx, leaseID)
	if err != nil {
		t.Fatalf("lease alive after revoke: %v", err)
	}

	if alive {
		t.Fatal("expected revoked lease to be dead")
	}
}

func newEmbeddedBackend(t *testing.T, endpoints []string, clusterName string, ttl time.Duration) *Backend {
	t.Helper()

	backend, err := New(dcs.Config{
		Backend:      dcs.BackendEtcd,
		ClusterName:  clusterName,
		TTL:          ttl,
		RetryTimeout: time.Second,
		Etcd: &dcs.EtcdConfig{
			Endpoints: endpoints,
		},
	})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	if err := backend.Initialize(context.Background()); err != nil {
		t.Fatalf("initialize backend: %v", err)
	}

	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	})

	return backend
}

func startEmbeddedEtcd(t *testing.T) []string {
	t.Helper()

	config := embed.NewConfig()
	config.Dir = t.TempDir()
	config.Logger = "zap"
	config.LogLevel = "error"

	clientURL := mustParseURL(t, "http://127.0.0.1:"+reservePort(t))
	peerURL := mustParseURL(t, "http://127.0.0.1:"+reservePort(t))
	config.ListenClientUrls = []url.URL{clientURL}
	config.AdvertiseClientUrls = []url.URL{clientURL}
	config.ListenPeerUrls = []url.URL{peerURL}
	config.AdvertisePeerUrls = []url.URL{peerURL}
	config.InitialCluster = config.InitialClusterFromName(config.Name)

	server, err := embed.StartEtcd(config)
	if err != nil {
		t.Fatalf("start embedded etcd: %v", err)
	}

	t.Cleanup(server.Close)

	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for embedded etcd readiness")
	}

	return []string{clientURL.String()}
}

func reservePort(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve tcp port: %v", err)
	}
	defer listener.Close()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split reserved address: %v", err)
	}

	return port
}

func mustParseURL(t *testing.T, raw string) url.URL {
	t.Helper()

	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url %q: %v", raw, err)
	}

	return *parsed
}

func embeddedTestClusterName(name string) string {
	replacer := strings.NewReplacer("/", "-", " ", "-", "_", "-", "(", "-", ")", "-", ":", "-")
	return strings.Trim(replacer.Replace(strings.ToLower(name)), "-")
}

type embeddedNamespacedBackend struct {
	dcs.DCS
	namespace string
}

func (backend embeddedNamespacedBackend) Get(ctx context.Context, key string) (dcs.KeyValue, error) {
	current, err := backend.DCS.Get(ctx, backend.rewriteKey(key))
	if err != nil {
		return dcs.KeyValue{}, err
	}

	return backend.stripEntry(current), nil
}

func (backend embeddedNamespacedBackend) Set(ctx context.Context, key string, value []byte, options ...dcs.SetOption) error {
	return backend.DCS.Set(ctx, backend.rewriteKey(key), value, options...)
}

func (backend embeddedNamespacedBackend) CompareAndSet(ctx context.Context, key string, value []byte, expectedRevision int64) error {
	return backend.DCS.CompareAndSet(ctx, backend.rewriteKey(key), value, expectedRevision)
}

func (backend embeddedNamespacedBackend) Delete(ctx context.Context, key string) error {
	return backend.DCS.Delete(ctx, backend.rewriteKey(key))
}

func (backend embeddedNamespacedBackend) List(ctx context.Context, prefix string) ([]dcs.KeyValue, error) {
	listed, err := backend.DCS.List(ctx, backend.rewriteKey(prefix))
	if err != nil {
		return nil, err
	}

	for index := range listed {
		listed[index] = backend.stripEntry(listed[index])
	}

	return listed, nil
}

func (backend embeddedNamespacedBackend) Watch(ctx context.Context, prefix string) (<-chan dcs.WatchEvent, error) {
	events, err := backend.DCS.Watch(ctx, backend.rewriteKey(prefix))
	if err != nil {
		return nil, err
	}

	out := make(chan dcs.WatchEvent, 16)
	go func() {
		defer close(out)

		for {
			select {
			case event, ok := <-events:
				if !ok {
					return
				}
				event.Key = backend.stripKey(event.Key)
				select {
				case out <- event:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

func (backend embeddedNamespacedBackend) rewriteKey(key string) string {
	return backend.namespace + strings.TrimSpace(key)
}

func (backend embeddedNamespacedBackend) stripEntry(entry dcs.KeyValue) dcs.KeyValue {
	entry.Key = backend.stripKey(entry.Key)
	return entry
}

func (backend embeddedNamespacedBackend) stripKey(key string) string {
	return strings.TrimPrefix(key, backend.namespace)
}
