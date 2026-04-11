package etcd

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	rpctypes "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/polkiloo/pacman/internal/dcs"
)

func TestNewRejectsInvalidAndUnsupportedConfig(t *testing.T) {
	t.Parallel()

	if _, err := New(dcs.Config{}); !errors.Is(err, dcs.ErrBackendRequired) {
		t.Fatalf("unexpected invalid-config error: got %v, want %v", err, dcs.ErrBackendRequired)
	}

	_, err := New(dcs.Config{
		Backend:      dcs.BackendRaft,
		ClusterName:  "alpha",
		TTL:          time.Second,
		RetryTimeout: time.Second,
		Raft: &dcs.RaftConfig{
			DataDir:     t.TempDir(),
			BindAddress: "127.0.0.1:7100",
			Peers:       []string{"127.0.0.1:7100"},
		},
	})
	if err == nil || err.Error() != `dcs/etcd: unsupported backend "raft"` {
		t.Fatalf("unexpected unsupported-backend error: got %v", err)
	}
}

func TestRequestContextHonorsDeadlineAndTimeout(t *testing.T) {
	t.Parallel()

	backend := &Backend{config: dcs.Config{RetryTimeout: 50 * time.Millisecond}}

	base, baseCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer baseCancel()

	withDeadline, cancel := backend.requestContext(base)
	defer cancel()

	baseDeadline, _ := base.Deadline()
	requestDeadline, ok := withDeadline.Deadline()
	if !ok {
		t.Fatal("expected request context to retain existing deadline")
	}

	if !requestDeadline.Equal(baseDeadline) {
		t.Fatalf("unexpected preserved deadline: got %v, want %v", requestDeadline, baseDeadline)
	}

	derived, derivedCancel := backend.requestContext(context.Background())
	defer derivedCancel()

	derivedDeadline, ok := derived.Deadline()
	if !ok {
		t.Fatal("expected request context without deadline to install backend timeout")
	}

	remaining := time.Until(derivedDeadline)
	if remaining <= 0 || remaining > backend.config.RetryTimeout {
		t.Fatalf("unexpected derived timeout window: got %s, want (0,%s]", remaining, backend.config.RetryTimeout)
	}
}

func TestCheckAvailableAndMapError(t *testing.T) {
	t.Parallel()

	backend := &Backend{}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := backend.checkAvailable(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected canceled check error: got %v, want %v", err, context.Canceled)
	}

	if err := backend.checkAvailable(context.Background()); err != nil {
		t.Fatalf("unexpected available error: %v", err)
	}

	backend.closed = true

	if err := backend.checkAvailable(context.Background()); !errors.Is(err, dcs.ErrBackendUnavailable) {
		t.Fatalf("unexpected closed check error: got %v, want %v", err, dcs.ErrBackendUnavailable)
	}

	if err := backend.mapError(errors.New("boom")); !errors.Is(err, dcs.ErrBackendUnavailable) {
		t.Fatalf("unexpected closed map error: got %v, want %v", err, dcs.ErrBackendUnavailable)
	}

	backend.closed = false

	testCases := []struct {
		name string
		in   error
		want error
	}{
		{name: "nil", in: nil},
		{name: "context canceled", in: context.Canceled, want: context.Canceled},
		{name: "context deadline exceeded", in: context.DeadlineExceeded, want: context.DeadlineExceeded},
		{name: "grpc no leader", in: rpctypes.ErrGRPCNoLeader, want: dcs.ErrBackendUnavailable},
		{name: "rpc no leader", in: rpctypes.ErrNoLeader, want: dcs.ErrBackendUnavailable},
		{name: "rpc timeout", in: rpctypes.ErrTimeout, want: dcs.ErrBackendUnavailable},
		{name: "rpc connection timeout", in: rpctypes.ErrTimeoutDueToConnectionLost, want: dcs.ErrBackendUnavailable},
		{name: "rpc leader timeout", in: rpctypes.ErrTimeoutDueToLeaderFail, want: dcs.ErrBackendUnavailable},
		{name: "grpc unavailable", in: status.Error(codes.Unavailable, "down"), want: dcs.ErrBackendUnavailable},
		{name: "grpc aborted", in: status.Error(codes.Aborted, "retry"), want: dcs.ErrBackendUnavailable},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := backend.mapError(testCase.in)
			if !errors.Is(got, testCase.want) {
				t.Fatalf("unexpected mapped error: got %v, want %v", got, testCase.want)
			}
		})
	}

	sentinel := errors.New("sentinel")
	if got := backend.mapError(sentinel); !errors.Is(got, sentinel) {
		t.Fatalf("unexpected passthrough error: got %v, want %v", got, sentinel)
	}
}

func TestTTLToLeaseSeconds(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		ttl  time.Duration
		want int64
	}{
		{ttl: 0, want: 1},
		{ttl: -time.Second, want: 1},
		{ttl: time.Second, want: 1},
		{ttl: 1500 * time.Millisecond, want: 2},
		{ttl: 2500 * time.Millisecond, want: 3},
	}

	for _, testCase := range testCases {
		if got := ttlToLeaseSeconds(testCase.ttl); got != testCase.want {
			t.Fatalf("unexpected lease seconds for %s: got %d, want %d", testCase.ttl, got, testCase.want)
		}
	}
}

func TestEntryEncodingRoundTripAndDecodeErrors(t *testing.T) {
	t.Parallel()

	encoded, err := marshalEntry(" /pacman/alpha/config ", []byte("value"), 7, 3*time.Second)
	if err != nil {
		t.Fatalf("marshal entry: %v", err)
	}

	current, err := decodeEntry(&mvccpb.KeyValue{
		Key:         []byte("/pacman/alpha/config"),
		Value:       encoded,
		ModRevision: 11,
		Lease:       19,
	})
	if err != nil {
		t.Fatalf("decode entry: %v", err)
	}

	if current.value.Key != "/pacman/alpha/config" {
		t.Fatalf("unexpected decoded key: got %q", current.value.Key)
	}

	if string(current.value.Value) != "value" {
		t.Fatalf("unexpected decoded value: got %q", current.value.Value)
	}

	if current.value.Revision != 7 || current.value.TTL != 3*time.Second {
		t.Fatalf("unexpected decoded entry metadata: %+v", current.value)
	}

	if current.modRevision != 11 || current.leaseID != 19 {
		t.Fatalf("unexpected decoded storage metadata: %+v", current)
	}

	if _, err := decodeEntry(&mvccpb.KeyValue{Key: []byte("/broken"), Value: []byte("{")}); err == nil {
		t.Fatal("expected decodeEntry to reject invalid json")
	}
}

func TestLeaderEncodingRoundTripAndDecodeErrors(t *testing.T) {
	t.Parallel()

	record := leaderRecord{
		Leader:            "alpha-1",
		Term:              3,
		AcquiredUnixNano:  10,
		RenewedUnixNano:   20,
		ExpiresAtUnixNano: 30,
	}

	encoded, err := marshalLeader(record)
	if err != nil {
		t.Fatalf("marshal leader: %v", err)
	}

	current, err := decodeLeader(&mvccpb.KeyValue{
		Key:         []byte("/_pacman_internal/alpha/leader"),
		Value:       encoded,
		ModRevision: 5,
		Lease:       7,
	})
	if err != nil {
		t.Fatalf("decode leader: %v", err)
	}

	if current.record != record {
		t.Fatalf("unexpected decoded leader record: got %+v, want %+v", current.record, record)
	}

	if current.modRevision != 5 || current.leaseID != 7 {
		t.Fatalf("unexpected decoded leader metadata: %+v", current)
	}

	if _, err := decodeLeader(&mvccpb.KeyValue{Value: []byte("{")}); err == nil {
		t.Fatal("expected decodeLeader to reject invalid json")
	}
}

func TestLeaderRecordRenewAndToLease(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0).UTC()
	record := leaderRecord{
		Leader:            "alpha-1",
		Term:              4,
		AcquiredUnixNano:  now.Add(-time.Minute).UnixNano(),
		RenewedUnixNano:   now.Add(-time.Second).UnixNano(),
		ExpiresAtUnixNano: now.UnixNano(),
	}

	renewed := record.renew(" beta-1 ", 5*time.Second, now)
	if renewed.Leader != "beta-1" {
		t.Fatalf("unexpected renewed leader: got %q", renewed.Leader)
	}

	if renewed.AcquiredUnixNano != record.AcquiredUnixNano {
		t.Fatalf("expected renew to preserve acquired time: got %d, want %d", renewed.AcquiredUnixNano, record.AcquiredUnixNano)
	}

	lease := renewed.toLease()
	if lease.Leader != "beta-1" || lease.Term != record.Term {
		t.Fatalf("unexpected lease after renew: %+v", lease)
	}

	if !lease.Renewed.Equal(now) || !lease.ExpiresAt.Equal(now.Add(5*time.Second)) {
		t.Fatalf("unexpected lease timing: %+v", lease)
	}
}

func TestCompareCurrentAndPutOperation(t *testing.T) {
	t.Parallel()

	absent := compareCurrent(" /pacman/alpha/config ", false, 9)
	if string(absent.KeyBytes()) != "/pacman/alpha/config" {
		t.Fatalf("unexpected absent compare key: got %q", absent.KeyBytes())
	}

	absentPB := pb.Compare(absent)
	if absentPB.Target != pb.Compare_VERSION {
		t.Fatalf("unexpected absent compare target: got %v", absentPB.Target)
	}

	if version := absentPB.GetVersion(); version != 0 {
		t.Fatalf("unexpected absent compare version: got %d, want %d", version, 0)
	}

	existing := compareCurrent(" /pacman/alpha/config ", true, 9)
	existingPB := pb.Compare(existing)
	if existingPB.Target != pb.Compare_MOD {
		t.Fatalf("unexpected existing compare target: got %v", existingPB.Target)
	}

	if modRevision := existingPB.GetModRevision(); modRevision != 9 {
		t.Fatalf("unexpected existing compare revision: got %d, want %d", modRevision, 9)
	}

	putWithoutLease := putOperation(" /pacman/alpha/config ", []byte("value"), clientv3.NoLease)
	if !putWithoutLease.IsPut() {
		t.Fatal("expected put operation without lease to be a put")
	}

	if string(putWithoutLease.KeyBytes()) != "/pacman/alpha/config" || string(putWithoutLease.ValueBytes()) != "value" {
		t.Fatalf("unexpected put operation without lease: key=%q value=%q", putWithoutLease.KeyBytes(), putWithoutLease.ValueBytes())
	}

	putWithLease := putOperation(" /pacman/alpha/config ", []byte("value"), 17)
	leaseField := reflect.ValueOf(putWithLease).FieldByName("leaseID")
	if !leaseField.IsValid() || clientv3.LeaseID(leaseField.Int()) != 17 {
		t.Fatalf("unexpected lease on put operation: got %v", leaseField)
	}
}

func TestSessionKeyAndLeaseNotFound(t *testing.T) {
	t.Parallel()

	backend := &Backend{sessionsPrefix: "/_pacman_internal/alpha/sessions/"}
	if got := backend.sessionKey(" beta-1 "); got != "/_pacman_internal/alpha/sessions/beta-1" {
		t.Fatalf("unexpected session key: got %q", got)
	}

	if !isLeaseNotFound(rpctypes.ErrLeaseNotFound) {
		t.Fatal("expected ErrLeaseNotFound to be recognized")
	}

	if !isLeaseNotFound(rpctypes.ErrGRPCLeaseNotFound) {
		t.Fatal("expected ErrGRPCLeaseNotFound to be recognized")
	}

	if isLeaseNotFound(errors.New("other")) {
		t.Fatal("expected unrelated error to be rejected")
	}
}

func TestMapWatchEvent(t *testing.T) {
	t.Parallel()

	backend := &Backend{
		config:   dcs.Config{RetryTimeout: time.Second},
		closeCtx: context.Background(),
	}

	encodedPut, err := marshalEntry("/pacman/alpha/status/alpha-1", []byte("up"), 4, 0)
	if err != nil {
		t.Fatalf("marshal put entry: %v", err)
	}

	putEvent, ok, err := backend.mapWatchEvent(&clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Key:   []byte("/pacman/alpha/status/alpha-1"),
			Value: encodedPut,
		},
	})
	if err != nil || !ok {
		t.Fatalf("unexpected mapped put result: event=%+v ok=%t err=%v", putEvent, ok, err)
	}

	if putEvent.Type != dcs.EventPut || putEvent.Key != "/pacman/alpha/status/alpha-1" || string(putEvent.Value) != "up" || putEvent.Revision != 4 {
		t.Fatalf("unexpected mapped put event: %+v", putEvent)
	}

	encodedDelete, err := marshalEntry("/pacman/alpha/status/alpha-2", []byte("down"), 6, 0)
	if err != nil {
		t.Fatalf("marshal delete entry: %v", err)
	}

	deleteEvent, ok, err := backend.mapWatchEvent(&clientv3.Event{
		Type: mvccpb.DELETE,
		PrevKv: &mvccpb.KeyValue{
			Key:   []byte("/pacman/alpha/status/alpha-2"),
			Value: encodedDelete,
		},
	})
	if err != nil || !ok {
		t.Fatalf("unexpected mapped delete result: event=%+v ok=%t err=%v", deleteEvent, ok, err)
	}

	if deleteEvent.Type != dcs.EventDelete || deleteEvent.Key != "/pacman/alpha/status/alpha-2" || deleteEvent.Revision != 7 {
		t.Fatalf("unexpected mapped delete event: %+v", deleteEvent)
	}

	ignoredDelete, ok, err := backend.mapWatchEvent(&clientv3.Event{Type: mvccpb.DELETE})
	if err != nil || ok {
		t.Fatalf("unexpected delete without prevkv result: event=%+v ok=%t err=%v", ignoredDelete, ok, err)
	}

	if _, ok, err := backend.mapWatchEvent(&clientv3.Event{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Key: []byte("/broken"), Value: []byte("{")},
	}); err == nil || ok {
		t.Fatalf("expected invalid put watch payload to fail, got ok=%t err=%v", ok, err)
	}

	if _, ok, err := backend.mapWatchEvent(&clientv3.Event{
		Type:   mvccpb.DELETE,
		PrevKv: &mvccpb.KeyValue{Key: []byte("/broken"), Value: []byte("{")},
	}); err == nil || ok {
		t.Fatalf("expected invalid delete watch payload to fail, got ok=%t err=%v", ok, err)
	}

	ignoredUnknown, ok, err := backend.mapWatchEvent(&clientv3.Event{Type: mvccpb.Event_EventType(99)})
	if err != nil || ok {
		t.Fatalf("unexpected unknown watch result: event=%+v ok=%t err=%v", ignoredUnknown, ok, err)
	}
}
