package dcs

import (
	"errors"
	"testing"
	"time"
)

func TestConfigWithDefaults(t *testing.T) {
	t.Parallel()

	original := Config{
		Backend:     BackendRaft,
		ClusterName: "alpha",
		Raft: &RaftConfig{
			DataDir:     "/var/lib/pacman/raft",
			BindAddress: "127.0.0.1:7100",
			Peers:       []string{"127.0.0.1:7100"},
		},
	}

	got := original.WithDefaults()

	if got.TTL != DefaultTTL {
		t.Fatalf("unexpected ttl default: got %s, want %s", got.TTL, DefaultTTL)
	}

	if got.RetryTimeout != DefaultRetryTimeout {
		t.Fatalf("unexpected retry timeout default: got %s, want %s", got.RetryTimeout, DefaultRetryTimeout)
	}

	if got.Raft == nil {
		t.Fatal("expected raft defaults to be applied")
	}

	if got.Raft.SnapshotInterval != DefaultRaftSnapshotInterval {
		t.Fatalf("unexpected raft snapshot interval default: got %s, want %s", got.Raft.SnapshotInterval, DefaultRaftSnapshotInterval)
	}

	if got.Raft.SnapshotThreshold != DefaultRaftSnapshotThreshold {
		t.Fatalf("unexpected raft snapshot threshold default: got %d, want %d", got.Raft.SnapshotThreshold, DefaultRaftSnapshotThreshold)
	}

	if got.Raft.TrailingLogs != DefaultRaftTrailingLogs {
		t.Fatalf("unexpected raft trailing logs default: got %d, want %d", got.Raft.TrailingLogs, DefaultRaftTrailingLogs)
	}

	if original.TTL != 0 || original.RetryTimeout != 0 {
		t.Fatalf("expected defaults to avoid mutating original config, got %+v", original)
	}

	if original.Raft == nil {
		t.Fatal("expected original raft config to remain available")
	}

	if original.Raft.SnapshotInterval != 0 || original.Raft.SnapshotThreshold != 0 || original.Raft.TrailingLogs != 0 {
		t.Fatalf("expected defaults to avoid mutating original raft config, got %+v", original.Raft)
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		config  Config
		wantErr error
	}{
		{
			name: "valid raft config",
			config: Config{
				Backend:      BackendRaft,
				ClusterName:  "alpha",
				TTL:          30 * time.Second,
				RetryTimeout: 10 * time.Second,
				Raft: &RaftConfig{
					DataDir:     "/var/lib/pacman/raft",
					BindAddress: "127.0.0.1:7100",
					Peers:       []string{"127.0.0.1:7100"},
				},
			},
		},
		{
			name: "valid etcd config",
			config: Config{
				Backend:      BackendEtcd,
				ClusterName:  "alpha",
				TTL:          30 * time.Second,
				RetryTimeout: 10 * time.Second,
				Etcd: &EtcdConfig{
					Endpoints: []string{"https://127.0.0.1:2379"},
					Username:  "pacman",
					Password:  "secret",
				},
			},
		},
		{
			name: "backend required",
			config: Config{
				ClusterName:  "alpha",
				TTL:          30 * time.Second,
				RetryTimeout: 10 * time.Second,
			},
			wantErr: ErrBackendRequired,
		},
		{
			name: "backend must be valid",
			config: Config{
				Backend:      Backend("consul"),
				ClusterName:  "alpha",
				TTL:          30 * time.Second,
				RetryTimeout: 10 * time.Second,
			},
			wantErr: ErrBackendInvalid,
		},
		{
			name: "cluster name required",
			config: Config{
				Backend:      BackendRaft,
				TTL:          30 * time.Second,
				RetryTimeout: 10 * time.Second,
				Raft: &RaftConfig{
					DataDir:     "/var/lib/pacman/raft",
					BindAddress: "127.0.0.1:7100",
					Peers:       []string{"127.0.0.1:7100"},
				},
			},
			wantErr: ErrClusterNameRequired,
		},
		{
			name: "ttl must be positive",
			config: Config{
				Backend:      BackendRaft,
				ClusterName:  "alpha",
				RetryTimeout: 10 * time.Second,
				Raft: &RaftConfig{
					DataDir:     "/var/lib/pacman/raft",
					BindAddress: "127.0.0.1:7100",
					Peers:       []string{"127.0.0.1:7100"},
				},
			},
			wantErr: ErrTTLRequired,
		},
		{
			name: "retry timeout must be positive",
			config: Config{
				Backend:     BackendRaft,
				ClusterName: "alpha",
				TTL:         30 * time.Second,
				Raft: &RaftConfig{
					DataDir:     "/var/lib/pacman/raft",
					BindAddress: "127.0.0.1:7100",
					Peers:       []string{"127.0.0.1:7100"},
				},
			},
			wantErr: ErrRetryTimeoutRequired,
		},
		{
			name: "raft backend requires raft config",
			config: Config{
				Backend:      BackendRaft,
				ClusterName:  "alpha",
				TTL:          30 * time.Second,
				RetryTimeout: 10 * time.Second,
			},
			wantErr: ErrRaftConfigRequired,
		},
		{
			name: "raft backend rejects etcd block",
			config: Config{
				Backend:      BackendRaft,
				ClusterName:  "alpha",
				TTL:          30 * time.Second,
				RetryTimeout: 10 * time.Second,
				Raft: &RaftConfig{
					DataDir:     "/var/lib/pacman/raft",
					BindAddress: "127.0.0.1:7100",
					Peers:       []string{"127.0.0.1:7100"},
				},
				Etcd: &EtcdConfig{
					Endpoints: []string{"https://127.0.0.1:2379"},
				},
			},
			wantErr: ErrEtcdConfigUnexpected,
		},
		{
			name: "etcd backend requires etcd config",
			config: Config{
				Backend:      BackendEtcd,
				ClusterName:  "alpha",
				TTL:          30 * time.Second,
				RetryTimeout: 10 * time.Second,
			},
			wantErr: ErrEtcdConfigRequired,
		},
		{
			name: "etcd backend rejects raft block",
			config: Config{
				Backend:      BackendEtcd,
				ClusterName:  "alpha",
				TTL:          30 * time.Second,
				RetryTimeout: 10 * time.Second,
				Raft: &RaftConfig{
					DataDir:     "/var/lib/pacman/raft",
					BindAddress: "127.0.0.1:7100",
					Peers:       []string{"127.0.0.1:7100"},
				},
				Etcd: &EtcdConfig{
					Endpoints: []string{"https://127.0.0.1:2379"},
				},
			},
			wantErr: ErrRaftConfigUnexpected,
		},
		{
			name: "raft bind address must be valid",
			config: Config{
				Backend:      BackendRaft,
				ClusterName:  "alpha",
				TTL:          30 * time.Second,
				RetryTimeout: 10 * time.Second,
				Raft: &RaftConfig{
					DataDir:     "/var/lib/pacman/raft",
					BindAddress: "broken",
					Peers:       []string{"127.0.0.1:7100"},
				},
			},
			wantErr: ErrRaftBindAddressInvalid,
		},
		{
			name: "etcd endpoint must be valid",
			config: Config{
				Backend:      BackendEtcd,
				ClusterName:  "alpha",
				TTL:          30 * time.Second,
				RetryTimeout: 10 * time.Second,
				Etcd: &EtcdConfig{
					Endpoints: []string{"broken"},
				},
			},
			wantErr: ErrEtcdEndpointInvalid,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.config.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("unexpected validation error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestConfigRedactedAndHasInlineSecrets(t *testing.T) {
	t.Parallel()

	config := Config{
		Backend:      BackendEtcd,
		ClusterName:  "alpha",
		TTL:          30 * time.Second,
		RetryTimeout: 10 * time.Second,
		Etcd: &EtcdConfig{
			Endpoints: []string{"https://127.0.0.1:2379"},
			Username:  "pacman",
			Password:  "secret",
		},
	}

	if !config.HasInlineSecrets() {
		t.Fatal("expected etcd password to count as an inline secret")
	}

	redacted := config.Redacted()
	if redacted.Etcd == nil {
		t.Fatal("expected redacted etcd config")
	}

	if redacted.Etcd.Password != "<redacted>" {
		t.Fatalf("unexpected redacted password: got %q, want %q", redacted.Etcd.Password, "<redacted>")
	}

	if redacted.Etcd.Username != "pacman" {
		t.Fatalf("expected non-secret etcd fields to be preserved, got %q", redacted.Etcd.Username)
	}

	if config.Etcd == nil || config.Etcd.Password != "secret" {
		t.Fatalf("expected original etcd password to remain unchanged, got %+v", config.Etcd)
	}
}

func TestBackendIsValid(t *testing.T) {
	t.Parallel()

	if !BackendRaft.IsValid() {
		t.Fatal("expected raft backend to be valid")
	}

	if !BackendEtcd.IsValid() {
		t.Fatal("expected etcd backend to be valid")
	}

	if Backend("consul").IsValid() {
		t.Fatal("expected unknown backend to be invalid")
	}
}
