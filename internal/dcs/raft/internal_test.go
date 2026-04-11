package raft

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"

	"github.com/polkiloo/pacman/internal/dcs"
)

func TestConfigFromDCS(t *testing.T) {
	t.Parallel()

	config, err := ConfigFromDCS(dcs.Config{
		Backend:      dcs.BackendRaft,
		ClusterName:  "alpha",
		TTL:          time.Second,
		RetryTimeout: 2 * time.Second,
		Raft: &dcs.RaftConfig{
			DataDir:           t.TempDir(),
			BindAddress:       "127.0.0.1:7100",
			Peers:             []string{"127.0.0.1:7100"},
			SnapshotInterval:  3 * time.Second,
			SnapshotThreshold: 99,
			TrailingLogs:      11,
		},
	})
	if err != nil {
		t.Fatalf("config from dcs: %v", err)
	}

	if config.ClusterName != "alpha" {
		t.Fatalf("unexpected cluster name: got %q", config.ClusterName)
	}

	if config.DataDir == "" || config.BindAddress != "127.0.0.1:7100" {
		t.Fatalf("unexpected resolved raft config: %+v", config)
	}

	if len(config.Peers) != 1 || config.Peers[0] != "127.0.0.1:7100" {
		t.Fatalf("unexpected peers: %+v", config.Peers)
	}

	if !config.Bootstrap {
		t.Fatal("expected single-node raft config to default bootstrap")
	}

	if config.SnapshotInterval != 3*time.Second || config.SnapshotThreshold != 99 || config.TrailingLogs != 11 {
		t.Fatalf("unexpected snapshot defaults: %+v", config)
	}

	if config.ApplyTimeout != 2*time.Second {
		t.Fatalf("unexpected apply timeout: got %s, want %s", config.ApplyTimeout, 2*time.Second)
	}

	if config.ExpiryInterval != maxExpiryInterval {
		t.Fatalf("unexpected expiry interval clamp: got %s, want %s", config.ExpiryInterval, maxExpiryInterval)
	}

	if config.Now == nil {
		t.Fatal("expected Now function to be installed")
	}
}

func TestConfigFromDCSRequiresRaftBackendConfig(t *testing.T) {
	t.Parallel()

	testCases := []dcs.Config{
		{
			Backend:      dcs.BackendEtcd,
			ClusterName:  "alpha",
			TTL:          time.Second,
			RetryTimeout: time.Second,
			Etcd:         &dcs.EtcdConfig{Endpoints: []string{"http://127.0.0.1:2379"}},
		},
		{
			Backend:      dcs.BackendRaft,
			ClusterName:  "alpha",
			TTL:          time.Second,
			RetryTimeout: time.Second,
		},
	}

	for _, testCase := range testCases {
		_, err := ConfigFromDCS(testCase)
		if !errors.Is(err, dcs.ErrRaftConfigRequired) {
			t.Fatalf("unexpected error for %+v: got %v, want %v", testCase, err, dcs.ErrRaftConfigRequired)
		}
	}
}

func TestClampDuration(t *testing.T) {
	t.Parallel()

	if got := clampDuration(5*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond); got != 10*time.Millisecond {
		t.Fatalf("expected lower clamp, got %s", got)
	}

	if got := clampDuration(15*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond); got != 15*time.Millisecond {
		t.Fatalf("expected value to remain in range, got %s", got)
	}

	if got := clampDuration(25*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond); got != 20*time.Millisecond {
		t.Fatalf("expected upper clamp, got %s", got)
	}
}

func TestSnapshotStateNormalizeAndClone(t *testing.T) {
	t.Parallel()

	state := snapshotState{}
	state.normalize()

	if state.Entries == nil || state.Sessions == nil {
		t.Fatalf("expected normalize to initialize maps: %+v", state)
	}

	state.Entries["/pacman/alpha/config"] = keyState{
		Key:      "/pacman/alpha/config",
		Value:    []byte("value"),
		Revision: 3,
		TTL:      time.Second,
	}
	state.Sessions["alpha-1"] = sessionState{ExpiresAt: time.Unix(10, 0).UTC()}

	cloned := state.clone()
	clonedEntry := cloned.Entries["/pacman/alpha/config"]
	clonedEntry.Value[0] = 'V'
	cloned.Entries["/pacman/alpha/config"] = clonedEntry
	cloned.Sessions["alpha-2"] = sessionState{ExpiresAt: time.Unix(20, 0).UTC()}

	if string(state.Entries["/pacman/alpha/config"].Value) != "value" {
		t.Fatalf("expected cloned entry value to be detached, got %q", state.Entries["/pacman/alpha/config"].Value)
	}

	if _, ok := state.Sessions["alpha-2"]; ok {
		t.Fatal("expected cloned session map to be detached")
	}
}

func TestFSMSnapshotClonesState(t *testing.T) {
	t.Parallel()

	broker := newWatchBroker(context.Background().Done())
	fsm := newFSM(broker)
	fsm.state.Entries["/pacman/alpha/config"] = keyState{
		Key:      "/pacman/alpha/config",
		Value:    []byte("value"),
		Revision: 2,
	}
	fsm.state.Sessions["alpha-1"] = sessionState{ExpiresAt: time.Unix(20, 0).UTC()}
	fsm.state.Leader = leaderState{Leader: "alpha-1", Term: 3}

	currentSnapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	snap, ok := currentSnapshot.(*snapshot)
	if !ok {
		t.Fatalf("unexpected snapshot type: %T", currentSnapshot)
	}

	fsm.state.Entries["/pacman/alpha/config"] = keyState{
		Key:      "/pacman/alpha/config",
		Value:    []byte("changed"),
		Revision: 9,
	}

	if string(snap.state.Entries["/pacman/alpha/config"].Value) != "value" {
		t.Fatalf("expected snapshot state to be detached, got %q", snap.state.Entries["/pacman/alpha/config"].Value)
	}
}

func TestFSMRestoreNormalizesAndClosesReader(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(snapshotState{
		Entries: map[string]keyState{
			"/pacman/alpha/config": {
				Key:      "/pacman/alpha/config",
				Value:    []byte("value"),
				Revision: 4,
				TTL:      time.Second,
			},
		},
		Leader: leaderState{
			Leader:    "alpha-1",
			Term:      2,
			Acquired:  time.Unix(10, 0).UTC(),
			Renewed:   time.Unix(11, 0).UTC(),
			ExpiresAt: time.Unix(20, 0).UTC(),
		},
	})
	if err != nil {
		t.Fatalf("marshal restore payload: %v", err)
	}

	reader := &trackingReadCloser{Reader: bytes.NewReader(payload)}
	fsm := newFSM(newWatchBroker(context.Background().Done()))
	if err := fsm.Restore(reader); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if !reader.closed {
		t.Fatal("expected restore reader to be closed")
	}

	got, ok := fsm.Get("/pacman/alpha/config", time.Unix(15, 0).UTC())
	if !ok || got.Revision != 4 || string(got.Value) != "value" {
		t.Fatalf("unexpected restored value: ok=%t value=%+v", ok, got)
	}

	if alive := fsm.Alive("alpha-1", time.Unix(15, 0).UTC()); alive {
		t.Fatal("expected absent restored session to stay absent")
	}

	lease, ok := fsm.Leader(time.Unix(15, 0).UTC())
	if !ok || lease.Leader != "alpha-1" || lease.Term != 2 {
		t.Fatalf("unexpected restored leader: ok=%t lease=%+v", ok, lease)
	}

	if fsm.state.Sessions == nil {
		t.Fatal("expected restore normalize to initialize sessions map")
	}
}

func TestSnapshotPersist(t *testing.T) {
	t.Parallel()

	snap := &snapshot{
		state: snapshotState{
			Entries: map[string]keyState{
				"/pacman/alpha/config": {Key: "/pacman/alpha/config", Value: []byte("value"), Revision: 1},
			},
		},
	}

	sink := &snapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist snapshot: %v", err)
	}

	if !sink.closed || sink.canceled {
		t.Fatalf("unexpected sink lifecycle: closed=%t canceled=%t", sink.closed, sink.canceled)
	}

	var restored snapshotState
	if err := json.Unmarshal(sink.Bytes(), &restored); err != nil {
		t.Fatalf("decode persisted snapshot: %v", err)
	}

	if string(restored.Entries["/pacman/alpha/config"].Value) != "value" {
		t.Fatalf("unexpected persisted snapshot payload: %+v", restored)
	}
}

func TestSnapshotPersistCancelsOnWriteError(t *testing.T) {
	t.Parallel()

	snap := &snapshot{state: snapshotState{Entries: map[string]keyState{}}}
	sink := &snapshotSink{writeErr: errors.New("boom")}

	if err := snap.Persist(sink); err == nil {
		t.Fatal("expected persist to fail")
	}

	if !sink.canceled || sink.closed {
		t.Fatalf("unexpected sink lifecycle after failure: closed=%t canceled=%t", sink.closed, sink.canceled)
	}

	snap.Release()
}

func TestTCPStreamLayerDial(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	accepted := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			if tlsConn, ok := conn.(*tls.Conn); ok {
				_ = tlsConn.Handshake()
			}
			_ = conn.Close()
		}
		close(accepted)
	}()

	layer := &tcpStreamLayer{listener: listener}
	conn, err := layer.Dial(hraft.ServerAddress(listener.Addr().String()), time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.Close()

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for accepted connection")
	}
}

func TestTCPStreamLayerDialWithTLSBranch(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	accepted := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			_ = conn.Close()
		}
		close(accepted)
	}()

	layer := &tcpStreamLayer{
		listener: listener,
		clientTLS: &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		},
	}

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}

	_, err = layer.Dial(hraft.ServerAddress(net.JoinHostPort("localhost", port)), time.Second)
	if err == nil {
		t.Fatal("expected tls dial against plain listener to fail")
	}

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for accept")
	}
}

func TestMapRaftError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		in   error
		want error
	}{
		{name: "nil", in: nil},
		{name: "not leader", in: hraft.ErrNotLeader, want: dcs.ErrNotLeader},
		{name: "leadership lost", in: hraft.ErrLeadershipLost, want: dcs.ErrNotLeader},
		{name: "transfer in progress", in: hraft.ErrLeadershipTransferInProgress, want: dcs.ErrNotLeader},
		{name: "not voter", in: hraft.ErrNotVoter, want: dcs.ErrNotLeader},
		{name: "shutdown", in: hraft.ErrRaftShutdown, want: dcs.ErrBackendUnavailable},
	}

	backend := &Backend{}
	for _, testCase := range testCases {
		got := backend.mapRaftError(testCase.in)
		if !errors.Is(got, testCase.want) {
			t.Fatalf("%s: unexpected mapped error: got %v, want %v", testCase.name, got, testCase.want)
		}
	}

	sentinel := errors.New("sentinel")
	if got := backend.mapRaftError(sentinel); !errors.Is(got, sentinel) {
		t.Fatalf("expected unknown error passthrough, got %v", got)
	}
}

func TestLogWarn(t *testing.T) {
	t.Parallel()

	var buffer bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelWarn}))

	backend := &Backend{config: Config{Logger: logger}}
	backend.logWarn("warning", "key", "value")
	if !strings.Contains(buffer.String(), "warning") || !strings.Contains(buffer.String(), "key=value") {
		t.Fatalf("unexpected logged warning: %q", buffer.String())
	}

	backend = &Backend{}
	backend.logWarn("ignored")
}

type trackingReadCloser struct {
	io.Reader
	closed bool
}

func (reader *trackingReadCloser) Close() error {
	reader.closed = true
	return nil
}

type snapshotSink struct {
	bytes.Buffer
	closed   bool
	canceled bool
	writeErr error
}

func (sink *snapshotSink) ID() string { return "snapshot" }

func (sink *snapshotSink) Write(payload []byte) (int, error) {
	if sink.writeErr != nil {
		return 0, sink.writeErr
	}

	return sink.Buffer.Write(payload)
}

func (sink *snapshotSink) Close() error {
	sink.closed = true
	return nil
}

func (sink *snapshotSink) Cancel() error {
	sink.canceled = true
	return nil
}
