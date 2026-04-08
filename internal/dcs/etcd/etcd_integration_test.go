//go:build integration

package etcd

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/polkiloo/pacman/internal/dcs"
	"github.com/polkiloo/pacman/internal/dcs/dcstest"
)

const endpointsEnvVar = "PACMAN_DCS_ETCD_ENDPOINTS"

func TestBackendConformance(t *testing.T) {
	const ttl = 1 * time.Second
	endpoints := parseEndpoints(os.Getenv(endpointsEnvVar))
	if len(endpoints) == 0 {
		t.Skipf("skipping etcd conformance without %s", endpointsEnvVar)
	}

	dcstest.Run(t, dcstest.Config{
		TTL: ttl,
		New: func(t *testing.T) dcs.DCS {
			t.Helper()

			backend, err := New(dcs.Config{
				Backend:      dcs.BackendEtcd,
				ClusterName:  testClusterName(t.Name()),
				TTL:          ttl,
				RetryTimeout: 5 * time.Second,
				Etcd: &dcs.EtcdConfig{
					Endpoints: endpoints,
				},
			})
			if err != nil {
				t.Fatalf("create etcd backend: %v", err)
			}

			return namespacedBackend{
				DCS:       backend,
				namespace: "/_dcstest/" + testClusterName(t.Name()),
			}
		},
	})
}

func testClusterName(name string) string {
	replacer := strings.NewReplacer("/", "-", " ", "-", "_", "-", "(", "-", ")", "-", ":", "-")
	return strings.Trim(replacer.Replace(strings.ToLower(name)), "-")
}

func parseEndpoints(raw string) []string {
	parts := strings.Split(raw, ",")
	endpoints := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			endpoints = append(endpoints, trimmed)
		}
	}

	return endpoints
}

type namespacedBackend struct {
	dcs.DCS
	namespace string
}

func (backend namespacedBackend) Get(ctx context.Context, key string) (dcs.KeyValue, error) {
	current, err := backend.DCS.Get(ctx, backend.rewriteKey(key))
	if err != nil {
		return dcs.KeyValue{}, err
	}

	return backend.stripEntry(current), nil
}

func (backend namespacedBackend) Set(ctx context.Context, key string, value []byte, options ...dcs.SetOption) error {
	return backend.DCS.Set(ctx, backend.rewriteKey(key), value, options...)
}

func (backend namespacedBackend) CompareAndSet(ctx context.Context, key string, value []byte, expectedRevision int64) error {
	return backend.DCS.CompareAndSet(ctx, backend.rewriteKey(key), value, expectedRevision)
}

func (backend namespacedBackend) Delete(ctx context.Context, key string) error {
	return backend.DCS.Delete(ctx, backend.rewriteKey(key))
}

func (backend namespacedBackend) List(ctx context.Context, prefix string) ([]dcs.KeyValue, error) {
	listed, err := backend.DCS.List(ctx, backend.rewriteKey(prefix))
	if err != nil {
		return nil, err
	}

	for index := range listed {
		listed[index] = backend.stripEntry(listed[index])
	}

	return listed, nil
}

func (backend namespacedBackend) Watch(ctx context.Context, prefix string) (<-chan dcs.WatchEvent, error) {
	events, err := backend.DCS.Watch(ctx, backend.rewriteKey(prefix))
	if err != nil {
		return nil, err
	}

	out := make(chan dcs.WatchEvent, 16)
	go func() {
		defer close(out)

		for event := range events {
			event.Key = backend.stripKey(event.Key)
			select {
			case out <- event:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

func (backend namespacedBackend) rewriteKey(key string) string {
	return backend.namespace + strings.TrimSpace(key)
}

func (backend namespacedBackend) stripEntry(entry dcs.KeyValue) dcs.KeyValue {
	entry.Key = backend.stripKey(entry.Key)
	return entry
}

func (backend namespacedBackend) stripKey(key string) string {
	return strings.TrimPrefix(key, backend.namespace)
}
