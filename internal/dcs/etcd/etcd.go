package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	rpctypes "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/polkiloo/pacman/internal/dcs"
)

const (
	watchBufferSize    = 16
	internalRootPrefix = "/_pacman_internal"
)

// Backend implements the DCS interface on top of etcd v3.
type Backend struct {
	config dcs.Config
	client *clientv3.Client

	internalPrefix string
	leaderKey      string
	leaderTermKey  string
	sessionsPrefix string

	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeCtx  context.Context
	closeFn   context.CancelFunc
}

type entryEnvelope struct {
	Value    []byte `json:"value"`
	Revision int64  `json:"revision"`
	TTLNanos int64  `json:"ttlNanos,omitempty"`
}

type entryState struct {
	value       dcs.KeyValue
	modRevision int64
	leaseID     clientv3.LeaseID
}

type leaderRecord struct {
	Leader            string `json:"leader"`
	Term              uint64 `json:"term"`
	AcquiredUnixNano  int64  `json:"acquiredUnixNano"`
	RenewedUnixNano   int64  `json:"renewedUnixNano"`
	ExpiresAtUnixNano int64  `json:"expiresAtUnixNano"`
}

type leaderState struct {
	record      leaderRecord
	modRevision int64
	leaseID     clientv3.LeaseID
}

var _ dcs.DCS = (*Backend)(nil)

// New constructs an etcd-backed DCS backend.
func New(config dcs.Config) (*Backend, error) {
	defaulted := config.WithDefaults()
	if err := defaulted.Validate(); err != nil {
		return nil, err
	}

	if defaulted.Backend != dcs.BackendEtcd {
		return nil, fmt.Errorf("dcs/etcd: unsupported backend %q", defaulted.Backend)
	}

	closeCtx, closeFn := context.WithCancel(context.Background())
	client, err := clientv3.New(clientv3.Config{
		Context:     closeCtx,
		Endpoints:   append([]string(nil), defaulted.Etcd.Endpoints...),
		Username:    defaulted.Etcd.Username,
		Password:    defaulted.Etcd.Password,
		DialTimeout: defaulted.RetryTimeout,
	})
	if err != nil {
		closeFn()
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	internalPrefix := internalRootPrefix + "/" + strings.TrimSpace(defaulted.ClusterName)

	return &Backend{
		config:         defaulted,
		client:         client,
		internalPrefix: internalPrefix,
		leaderKey:      internalPrefix + "/leader",
		leaderTermKey:  internalPrefix + "/leader-term",
		sessionsPrefix: internalPrefix + "/sessions/",
		closeCtx:       closeCtx,
		closeFn:        closeFn,
	}, nil
}

// Initialize verifies that the configured etcd cluster is reachable.
func (backend *Backend) Initialize(ctx context.Context) error {
	if err := backend.checkAvailable(ctx); err != nil {
		return err
	}

	requestCtx, cancel := backend.requestContext(ctx)
	defer cancel()

	_, err := backend.client.Get(requestCtx, backend.internalPrefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
	return backend.mapError(err)
}

// Close releases backend resources and terminates active watches.
func (backend *Backend) Close() error {
	var closeErr error

	backend.closeOnce.Do(func() {
		backend.mu.Lock()
		backend.closed = true
		backend.mu.Unlock()

		backend.closeFn()
		if backend.client != nil {
			closeErr = backend.client.Close()
		}
	})

	return closeErr
}

// Get reads the current value for a key.
func (backend *Backend) Get(ctx context.Context, key string) (dcs.KeyValue, error) {
	if err := backend.checkAvailable(ctx); err != nil {
		return dcs.KeyValue{}, err
	}

	requestCtx, cancel := backend.requestContext(ctx)
	defer cancel()

	current, ok, err := backend.loadEntry(requestCtx, key)
	if err != nil {
		return dcs.KeyValue{}, err
	}

	if !ok {
		return dcs.KeyValue{}, dcs.ErrKeyNotFound
	}

	return current.value.Clone(), nil
}

// Set writes a key unconditionally.
func (backend *Backend) Set(ctx context.Context, key string, value []byte, options ...dcs.SetOption) error {
	if err := backend.checkAvailable(ctx); err != nil {
		return err
	}

	requestCtx, cancel := backend.requestContext(ctx)
	defer cancel()

	applied := dcs.ApplySetOptions(options...)
	trimmedKey := strings.TrimSpace(key)

	for {
		current, ok, err := backend.loadEntry(requestCtx, trimmedKey)
		if err != nil {
			return err
		}

		nextRevision := int64(1)
		if ok {
			nextRevision = current.value.Revision + 1
		}

		leaseID := clientv3.NoLease
		if applied.TTL > 0 {
			leaseID, err = backend.grantLease(requestCtx, applied.TTL)
			if err != nil {
				return err
			}
		}

		encoded, err := marshalEntry(trimmedKey, value, nextRevision, applied.TTL)
		if err != nil {
			return err
		}

		response, err := backend.client.
			Txn(requestCtx).
			If(compareCurrent(trimmedKey, ok, current.modRevision)).
			Then(putOperation(trimmedKey, encoded, leaseID)).
			Commit()
		if err != nil {
			return backend.mapError(err)
		}

		if response.Succeeded {
			return nil
		}
	}
}

// CompareAndSet writes a key only if the expected revision still matches.
func (backend *Backend) CompareAndSet(ctx context.Context, key string, value []byte, expectedRevision int64) error {
	if err := backend.checkAvailable(ctx); err != nil {
		return err
	}

	requestCtx, cancel := backend.requestContext(ctx)
	defer cancel()

	trimmedKey := strings.TrimSpace(key)

	for {
		current, ok, err := backend.loadEntry(requestCtx, trimmedKey)
		if err != nil {
			return err
		}

		if !ok || current.value.Revision != expectedRevision {
			return dcs.ErrRevisionMismatch
		}

		encoded, err := marshalEntry(trimmedKey, value, current.value.Revision+1, current.value.TTL)
		if err != nil {
			return err
		}

		response, err := backend.client.
			Txn(requestCtx).
			If(compareCurrent(trimmedKey, true, current.modRevision)).
			Then(putOperation(trimmedKey, encoded, current.leaseID)).
			Commit()
		if err != nil {
			return backend.mapError(err)
		}

		if response.Succeeded {
			return nil
		}
	}
}

// Delete removes a key.
func (backend *Backend) Delete(ctx context.Context, key string) error {
	if err := backend.checkAvailable(ctx); err != nil {
		return err
	}

	requestCtx, cancel := backend.requestContext(ctx)
	defer cancel()

	trimmedKey := strings.TrimSpace(key)

	for {
		current, ok, err := backend.loadEntry(requestCtx, trimmedKey)
		if err != nil {
			return err
		}

		if !ok {
			return dcs.ErrKeyNotFound
		}

		response, err := backend.client.
			Txn(requestCtx).
			If(compareCurrent(trimmedKey, true, current.modRevision)).
			Then(clientv3.OpDelete(trimmedKey)).
			Commit()
		if err != nil {
			return backend.mapError(err)
		}

		if response.Succeeded {
			return nil
		}
	}
}

// List returns all visible keys under a prefix.
func (backend *Backend) List(ctx context.Context, prefix string) ([]dcs.KeyValue, error) {
	if err := backend.checkAvailable(ctx); err != nil {
		return nil, err
	}

	requestCtx, cancel := backend.requestContext(ctx)
	defer cancel()

	response, err := backend.client.Get(
		requestCtx,
		strings.TrimSpace(prefix),
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	if err != nil {
		return nil, backend.mapError(err)
	}

	listed := make([]dcs.KeyValue, 0, len(response.Kvs))
	for _, kv := range response.Kvs {
		current, err := decodeEntry(kv)
		if err != nil {
			return nil, err
		}

		listed = append(listed, current.value.Clone())
	}

	return listed, nil
}

// Campaign attempts to acquire or renew the logical PACMAN leader lease.
func (backend *Backend) Campaign(ctx context.Context, candidate string) (dcs.LeaderLease, bool, error) {
	if err := backend.checkAvailable(ctx); err != nil {
		return dcs.LeaderLease{}, false, err
	}

	trimmedCandidate := strings.TrimSpace(candidate)

	for {
		requestCtx, cancel := backend.requestContext(ctx)

		current, ok, err := backend.loadLeader(requestCtx)
		if err != nil {
			cancel()
			return dcs.LeaderLease{}, false, err
		}

		if ok {
			alive, err := backend.leaseAlive(requestCtx, current.leaseID)
			if err != nil {
				cancel()
				return dcs.LeaderLease{}, false, err
			}

			if alive && current.record.Leader != trimmedCandidate {
				cancel()
				return current.record.toLease(), false, nil
			}

			if !alive || current.record.toLease().ExpiresAt.Before(time.Now().UTC()) {
				err := backend.deleteIfCurrent(requestCtx, backend.leaderKey, current.modRevision)
				cancel()
				if err != nil {
					return dcs.LeaderLease{}, false, err
				}

				continue
			}

			if err := backend.refreshLease(requestCtx, current.leaseID); err != nil {
				cancel()
				if errors.Is(err, dcs.ErrSessionExpired) {
					continue
				}

				return dcs.LeaderLease{}, false, err
			}

			renewed := current.record.renew(trimmedCandidate, backend.config.TTL, time.Now().UTC())
			encoded, err := marshalLeader(renewed)
			if err != nil {
				cancel()
				return dcs.LeaderLease{}, false, err
			}

			response, err := backend.client.
				Txn(requestCtx).
				If(compareCurrent(backend.leaderKey, true, current.modRevision)).
				Then(putOperation(backend.leaderKey, encoded, current.leaseID)).
				Commit()
			cancel()
			if err != nil {
				return dcs.LeaderLease{}, false, backend.mapError(err)
			}

			if response.Succeeded {
				return renewed.toLease(), true, nil
			}

			continue
		}

		lastTerm, termModRevision, termExists, err := backend.loadLeaderTerm(requestCtx)
		if err != nil {
			cancel()
			return dcs.LeaderLease{}, false, err
		}

		nextTerm := lastTerm + 1
		if nextTerm == 0 {
			nextTerm = 1
		}

		leaseID, err := backend.grantLease(requestCtx, backend.config.TTL)
		if err != nil {
			cancel()
			return dcs.LeaderLease{}, false, err
		}

		now := time.Now().UTC()
		nextLeader := leaderRecord{
			Leader:            trimmedCandidate,
			Term:              nextTerm,
			AcquiredUnixNano:  now.UnixNano(),
			RenewedUnixNano:   now.UnixNano(),
			ExpiresAtUnixNano: now.Add(backend.config.TTL).UnixNano(),
		}

		encodedLeader, err := marshalLeader(nextLeader)
		if err != nil {
			cancel()
			return dcs.LeaderLease{}, false, err
		}

		response, err := backend.client.
			Txn(requestCtx).
			If(
				clientv3.Compare(clientv3.Version(backend.leaderKey), "=", 0),
				compareCurrent(backend.leaderTermKey, termExists, termModRevision),
			).
			Then(
				putOperation(backend.leaderKey, encodedLeader, leaseID),
				clientv3.OpPut(backend.leaderTermKey, strconv.FormatUint(nextTerm, 10)),
			).
			Commit()
		cancel()
		if err != nil {
			return dcs.LeaderLease{}, false, backend.mapError(err)
		}

		if response.Succeeded {
			return nextLeader.toLease(), true, nil
		}
	}
}

// Leader returns the currently active leader lease.
func (backend *Backend) Leader(ctx context.Context) (dcs.LeaderLease, bool, error) {
	if err := backend.checkAvailable(ctx); err != nil {
		return dcs.LeaderLease{}, false, err
	}

	requestCtx, cancel := backend.requestContext(ctx)
	defer cancel()

	current, ok, err := backend.loadLeader(requestCtx)
	if err != nil {
		return dcs.LeaderLease{}, false, err
	}

	if !ok {
		return dcs.LeaderLease{}, false, nil
	}

	alive, err := backend.leaseAlive(requestCtx, current.leaseID)
	if err != nil {
		return dcs.LeaderLease{}, false, err
	}

	if !alive {
		return dcs.LeaderLease{}, false, nil
	}

	lease := current.record.toLease()
	if lease.ExpiresAt.Before(time.Now().UTC()) {
		return dcs.LeaderLease{}, false, nil
	}

	return lease, true, nil
}

// Resign releases the active leader lease.
func (backend *Backend) Resign(ctx context.Context) error {
	if err := backend.checkAvailable(ctx); err != nil {
		return err
	}

	requestCtx, cancel := backend.requestContext(ctx)
	defer cancel()

	for {
		current, ok, err := backend.loadLeader(requestCtx)
		if err != nil {
			return err
		}

		if !ok {
			return dcs.ErrNoLeader
		}

		response, err := backend.client.
			Txn(requestCtx).
			If(compareCurrent(backend.leaderKey, true, current.modRevision)).
			Then(clientv3.OpDelete(backend.leaderKey)).
			Commit()
		if err != nil {
			return backend.mapError(err)
		}

		if response.Succeeded {
			return nil
		}
	}
}

// Touch refreshes a member session TTL.
func (backend *Backend) Touch(ctx context.Context, member string) error {
	if err := backend.checkAvailable(ctx); err != nil {
		return err
	}

	requestCtx, cancel := backend.requestContext(ctx)
	defer cancel()

	sessionKey := backend.sessionKey(member)

	for {
		response, err := backend.client.Get(requestCtx, sessionKey)
		if err != nil {
			return backend.mapError(err)
		}

		if len(response.Kvs) > 0 {
			existingLease := clientv3.LeaseID(response.Kvs[0].Lease)
			if existingLease != clientv3.NoLease {
				if err := backend.refreshLease(requestCtx, existingLease); err == nil {
					return nil
				} else if !errors.Is(err, dcs.ErrSessionExpired) {
					return err
				}
			}
		}

		leaseID, err := backend.grantLease(requestCtx, backend.config.TTL)
		if err != nil {
			return err
		}

		compares := []clientv3.Cmp{clientv3.Compare(clientv3.Version(sessionKey), "=", 0)}
		if len(response.Kvs) > 0 {
			compares = []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(sessionKey), "=", response.Kvs[0].ModRevision)}
		}

		txnResponse, err := backend.client.
			Txn(requestCtx).
			If(compares...).
			Then(clientv3.OpPut(sessionKey, strings.TrimSpace(member), clientv3.WithLease(leaseID))).
			Commit()
		if err != nil {
			return backend.mapError(err)
		}

		if txnResponse.Succeeded {
			return nil
		}
	}
}

// Alive reports whether a member session is currently live.
func (backend *Backend) Alive(ctx context.Context, member string) (bool, error) {
	if err := backend.checkAvailable(ctx); err != nil {
		return false, err
	}

	requestCtx, cancel := backend.requestContext(ctx)
	defer cancel()

	response, err := backend.client.Get(requestCtx, backend.sessionKey(member))
	if err != nil {
		return false, backend.mapError(err)
	}

	if len(response.Kvs) == 0 {
		return false, nil
	}

	leaseID := clientv3.LeaseID(response.Kvs[0].Lease)
	if leaseID == clientv3.NoLease {
		return true, nil
	}

	return backend.leaseAlive(requestCtx, leaseID)
}

// Watch subscribes to changes under a key prefix.
func (backend *Backend) Watch(ctx context.Context, prefix string) (<-chan dcs.WatchEvent, error) {
	if err := backend.checkAvailable(ctx); err != nil {
		return nil, err
	}

	events := make(chan dcs.WatchEvent, watchBufferSize)
	watchCtx, cancel := context.WithCancel(backend.closeCtx)
	watchCh := backend.client.Watch(watchCtx, strings.TrimSpace(prefix), clientv3.WithPrefix(), clientv3.WithPrevKV())

	go func() {
		defer cancel()
		defer close(events)

		for {
			select {
			case <-ctx.Done():
				return
			case <-backend.closeCtx.Done():
				return
			case response, ok := <-watchCh:
				if !ok {
					return
				}

				if response.Err() != nil {
					return
				}

				for _, currentEvent := range response.Events {
					mapped, ok, err := backend.mapWatchEvent(currentEvent)
					if err != nil {
						return
					}

					if !ok {
						continue
					}

					select {
					case events <- mapped:
					case <-ctx.Done():
						return
					case <-backend.closeCtx.Done():
						return
					}
				}
			}
		}
	}()

	return events, nil
}

func (backend *Backend) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, backend.config.RetryTimeout)
}

func (backend *Backend) checkAvailable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	backend.mu.RLock()
	defer backend.mu.RUnlock()

	if backend.closed {
		return dcs.ErrBackendUnavailable
	}

	return nil
}

func (backend *Backend) mapError(err error) error {
	if err == nil {
		return nil
	}

	if backend.isClosed() {
		return dcs.ErrBackendUnavailable
	}

	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, rpctypes.ErrGRPCNoLeader),
		errors.Is(err, rpctypes.ErrNoLeader),
		errors.Is(err, rpctypes.ErrTimeout),
		errors.Is(err, rpctypes.ErrTimeoutDueToConnectionLost),
		errors.Is(err, rpctypes.ErrTimeoutDueToLeaderFail):
		return dcs.ErrBackendUnavailable
	}

	grpcStatus, ok := status.FromError(err)
	if ok {
		switch grpcStatus.Code() {
		case codes.Unavailable, codes.Aborted:
			return dcs.ErrBackendUnavailable
		}
	}

	return err
}

func (backend *Backend) isClosed() bool {
	backend.mu.RLock()
	defer backend.mu.RUnlock()

	return backend.closed
}

func (backend *Backend) grantLease(ctx context.Context, ttl time.Duration) (clientv3.LeaseID, error) {
	response, err := backend.client.Grant(ctx, ttlToLeaseSeconds(ttl))
	if err != nil {
		return clientv3.NoLease, backend.mapError(err)
	}

	return response.ID, nil
}

func (backend *Backend) refreshLease(ctx context.Context, leaseID clientv3.LeaseID) error {
	if leaseID == clientv3.NoLease {
		return nil
	}

	response, err := backend.client.KeepAliveOnce(ctx, leaseID)
	if err != nil {
		if isLeaseNotFound(err) {
			return dcs.ErrSessionExpired
		}

		return backend.mapError(err)
	}

	if response == nil || response.TTL <= 0 {
		return dcs.ErrSessionExpired
	}

	return nil
}

func (backend *Backend) leaseAlive(ctx context.Context, leaseID clientv3.LeaseID) (bool, error) {
	if leaseID == clientv3.NoLease {
		return true, nil
	}

	response, err := backend.client.TimeToLive(ctx, leaseID)
	if err != nil {
		if isLeaseNotFound(err) {
			return false, nil
		}

		return false, backend.mapError(err)
	}

	return response != nil && response.TTL > 0, nil
}

func (backend *Backend) loadEntry(ctx context.Context, key string) (entryState, bool, error) {
	response, err := backend.client.Get(ctx, strings.TrimSpace(key))
	if err != nil {
		return entryState{}, false, backend.mapError(err)
	}

	if len(response.Kvs) == 0 {
		return entryState{}, false, nil
	}

	current, err := decodeEntry(response.Kvs[0])
	if err != nil {
		return entryState{}, false, err
	}

	return current, true, nil
}

func (backend *Backend) loadLeader(ctx context.Context) (leaderState, bool, error) {
	response, err := backend.client.Get(ctx, backend.leaderKey)
	if err != nil {
		return leaderState{}, false, backend.mapError(err)
	}

	if len(response.Kvs) == 0 {
		return leaderState{}, false, nil
	}

	current, err := decodeLeader(response.Kvs[0])
	if err != nil {
		return leaderState{}, false, err
	}

	return current, true, nil
}

func (backend *Backend) loadLeaderTerm(ctx context.Context) (uint64, int64, bool, error) {
	response, err := backend.client.Get(ctx, backend.leaderTermKey)
	if err != nil {
		return 0, 0, false, backend.mapError(err)
	}

	if len(response.Kvs) == 0 {
		return 0, 0, false, nil
	}

	currentTerm, err := strconv.ParseUint(string(response.Kvs[0].Value), 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("decode leader term: %w", err)
	}

	return currentTerm, response.Kvs[0].ModRevision, true, nil
}

func (backend *Backend) deleteIfCurrent(ctx context.Context, key string, modRevision int64) error {
	response, err := backend.client.
		Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", modRevision)).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		return backend.mapError(err)
	}

	if response.Succeeded {
		return nil
	}

	return nil
}

func (backend *Backend) sessionKey(member string) string {
	return backend.sessionsPrefix + strings.TrimSpace(member)
}

func (backend *Backend) mapWatchEvent(currentEvent *clientv3.Event) (dcs.WatchEvent, bool, error) {
	switch currentEvent.Type {
	case mvccpb.PUT:
		current, err := decodeEntry(currentEvent.Kv)
		if err != nil {
			return dcs.WatchEvent{}, false, err
		}

		return dcs.WatchEvent{
			Type:     dcs.EventPut,
			Key:      current.value.Key,
			Value:    append([]byte(nil), current.value.Value...),
			Revision: current.value.Revision,
		}, true, nil
	case mvccpb.DELETE:
		if currentEvent.PrevKv == nil {
			return dcs.WatchEvent{}, false, nil
		}

		previous, err := decodeEntry(currentEvent.PrevKv)
		if err != nil {
			return dcs.WatchEvent{}, false, err
		}

		eventType := dcs.EventDelete
		if currentEvent.PrevKv.Lease != 0 {
			requestCtx, cancel := backend.requestContext(backend.closeCtx)
			expired, err := backend.leaseAlive(requestCtx, clientv3.LeaseID(currentEvent.PrevKv.Lease))
			cancel()
			if err != nil {
				return dcs.WatchEvent{}, false, err
			}

			if !expired {
				eventType = dcs.EventExpired
			}
		}

		return dcs.WatchEvent{
			Type:     eventType,
			Key:      previous.value.Key,
			Revision: previous.value.Revision + 1,
		}, true, nil
	default:
		return dcs.WatchEvent{}, false, nil
	}
}

func compareCurrent(key string, exists bool, modRevision int64) clientv3.Cmp {
	trimmedKey := strings.TrimSpace(key)
	if !exists {
		return clientv3.Compare(clientv3.Version(trimmedKey), "=", 0)
	}

	return clientv3.Compare(clientv3.ModRevision(trimmedKey), "=", modRevision)
}

func putOperation(key string, value []byte, leaseID clientv3.LeaseID) clientv3.Op {
	trimmedKey := strings.TrimSpace(key)
	if leaseID == clientv3.NoLease {
		return clientv3.OpPut(trimmedKey, string(value))
	}

	return clientv3.OpPut(trimmedKey, string(value), clientv3.WithLease(leaseID))
}

func marshalEntry(key string, value []byte, revision int64, ttl time.Duration) ([]byte, error) {
	encoded, err := json.Marshal(entryEnvelope{
		Value:    append([]byte(nil), value...),
		Revision: revision,
		TTLNanos: ttl.Nanoseconds(),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal entry %q: %w", strings.TrimSpace(key), err)
	}

	return encoded, nil
}

func decodeEntry(kv *mvccpb.KeyValue) (entryState, error) {
	var encoded entryEnvelope
	if err := json.Unmarshal(kv.Value, &encoded); err != nil {
		return entryState{}, fmt.Errorf("decode entry %q: %w", string(kv.Key), err)
	}

	return entryState{
		value: dcs.KeyValue{
			Key:      string(kv.Key),
			Value:    append([]byte(nil), encoded.Value...),
			Revision: encoded.Revision,
			TTL:      time.Duration(encoded.TTLNanos),
		},
		modRevision: kv.ModRevision,
		leaseID:     clientv3.LeaseID(kv.Lease),
	}, nil
}

func marshalLeader(record leaderRecord) ([]byte, error) {
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshal leader lease: %w", err)
	}

	return encoded, nil
}

func decodeLeader(kv *mvccpb.KeyValue) (leaderState, error) {
	var encoded leaderRecord
	if err := json.Unmarshal(kv.Value, &encoded); err != nil {
		return leaderState{}, fmt.Errorf("decode leader lease: %w", err)
	}

	return leaderState{
		record:      encoded,
		modRevision: kv.ModRevision,
		leaseID:     clientv3.LeaseID(kv.Lease),
	}, nil
}

func (record leaderRecord) renew(candidate string, ttl time.Duration, now time.Time) leaderRecord {
	record.Leader = strings.TrimSpace(candidate)
	record.RenewedUnixNano = now.UnixNano()
	record.ExpiresAtUnixNano = now.Add(ttl).UnixNano()
	return record
}

func (record leaderRecord) toLease() dcs.LeaderLease {
	return dcs.LeaderLease{
		Leader:    record.Leader,
		Term:      record.Term,
		Acquired:  time.Unix(0, record.AcquiredUnixNano).UTC(),
		Renewed:   time.Unix(0, record.RenewedUnixNano).UTC(),
		ExpiresAt: time.Unix(0, record.ExpiresAtUnixNano).UTC(),
	}
}

func ttlToLeaseSeconds(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 1
	}

	seconds := ttl / time.Second
	if ttl%time.Second != 0 {
		seconds++
	}

	if seconds <= 0 {
		seconds = 1
	}

	return int64(seconds)
}

func isLeaseNotFound(err error) bool {
	return errors.Is(err, rpctypes.ErrLeaseNotFound) || errors.Is(err, rpctypes.ErrGRPCLeaseNotFound)
}
