package config

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestReinitWALGDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Reinit: &ReinitConfig{
			WALG: &WALGConfig{
				Repository: WALGRepositoryConfig{
					Provider: WALGRepositoryProviderS3,
					Prefix:   "s3://pacman-backups/alpha",
				},
			},
		},
	}

	got := cfg.WithDefaults()
	if got.Reinit == nil || got.Reinit.WALG == nil {
		t.Fatalf("expected reinit wal-g defaults, got %+v", got.Reinit)
	}

	if got.Reinit.WALG.Binary != DefaultWALGBinary {
		t.Fatalf("unexpected wal-g binary default: got %q, want %q", got.Reinit.WALG.Binary, DefaultWALGBinary)
	}

	if got.Reinit.WALG.Restore.BackupName != DefaultWALGRestoreBackupName {
		t.Fatalf("unexpected wal-g restore backup default: got %q, want %q", got.Reinit.WALG.Restore.BackupName, DefaultWALGRestoreBackupName)
	}

	if cfg.Reinit == nil || cfg.Reinit.WALG == nil || cfg.Reinit.WALG.Binary != "" {
		t.Fatalf("expected defaults to avoid mutating original, got %+v", cfg.Reinit)
	}
}

func TestReinitConfigValidate(t *testing.T) {
	t.Parallel()

	validWALG := WALGConfig{
		Binary: DefaultWALGBinary,
		Repository: WALGRepositoryConfig{
			Provider: WALGRepositoryProviderS3,
			Prefix:   "s3://pacman-backups/alpha",
		},
		Credentials: WALGCredentialsConfig{
			InheritEnvironment: []string{"AWS_SESSION_TOKEN"},
			EnvironmentFiles: map[string]string{
				"AWS_ACCESS_KEY_ID":     "/run/secrets/aws-access-key-id",
				"AWS_SECRET_ACCESS_KEY": "/run/secrets/aws-secret-access-key",
			},
		},
	}

	testCases := []struct {
		name    string
		reinit  ReinitConfig
		wantErr error
	}{
		{
			name:   "valid wal-g reinit",
			reinit: ReinitConfig{WALG: &validWALG},
		},
		{
			name:    "missing wal-g section",
			reinit:  ReinitConfig{},
			wantErr: ErrReinitWALGConfigRequired,
		},
		{
			name: "missing binary",
			reinit: ReinitConfig{WALG: &WALGConfig{
				Repository: validWALG.Repository,
			}},
			wantErr: ErrReinitWALGBinaryRequired,
		},
		{
			name: "missing provider",
			reinit: ReinitConfig{WALG: &WALGConfig{
				Binary: DefaultWALGBinary,
				Repository: WALGRepositoryConfig{
					Prefix: "s3://pacman-backups/alpha",
				},
			}},
			wantErr: ErrReinitWALGRepositoryProviderRequired,
		},
		{
			name: "invalid provider",
			reinit: ReinitConfig{WALG: &WALGConfig{
				Binary: DefaultWALGBinary,
				Repository: WALGRepositoryConfig{
					Provider: WALGRepositoryProvider("tape"),
					Prefix:   "s3://pacman-backups/alpha",
				},
			}},
			wantErr: ErrReinitWALGRepositoryProviderInvalid,
		},
		{
			name: "missing prefix",
			reinit: ReinitConfig{WALG: &WALGConfig{
				Binary: DefaultWALGBinary,
				Repository: WALGRepositoryConfig{
					Provider: WALGRepositoryProviderS3,
				},
			}},
			wantErr: ErrReinitWALGRepositoryPrefixRequired,
		},
		{
			name: "invalid credential name",
			reinit: ReinitConfig{WALG: &WALGConfig{
				Binary:     DefaultWALGBinary,
				Repository: validWALG.Repository,
				Credentials: WALGCredentialsConfig{
					Environment: map[string]string{"aws-secret-access-key": "secret"},
				},
			}},
			wantErr: ErrReinitWALGCredentialNameInvalid,
		},
		{
			name: "duplicate credential source",
			reinit: ReinitConfig{WALG: &WALGConfig{
				Binary:     DefaultWALGBinary,
				Repository: validWALG.Repository,
				Credentials: WALGCredentialsConfig{
					InheritEnvironment: []string{"AWS_ACCESS_KEY_ID"},
					EnvironmentFiles: map[string]string{
						"AWS_ACCESS_KEY_ID": "/run/secrets/aws-access-key-id",
					},
				},
			}},
			wantErr: ErrReinitWALGCredentialSourceConflict,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.reinit.Validate()
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("validate reinit error: got %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestWALGBackupFetchCommandSelectsRestoreSource(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		walg       WALGConfig
		dataDir    string
		wantBinary string
		wantArgs   []string
		wantErr    error
	}{
		{
			name: "latest backup by default",
			walg: WALGConfig{
				Repository: WALGRepositoryConfig{
					Provider: WALGRepositoryProviderFilesystem,
					Prefix:   "/var/lib/pacman/walg",
				},
			},
			dataDir:    "/var/lib/postgresql/17/main",
			wantBinary: DefaultWALGBinary,
			wantArgs:   []string{"backup-fetch", "/var/lib/postgresql/17/main", DefaultWALGRestoreBackupName},
		},
		{
			name: "explicit backup name",
			walg: WALGConfig{
				Binary: "/usr/local/bin/wal-g",
				Repository: WALGRepositoryConfig{
					Provider: WALGRepositoryProviderFilesystem,
					Prefix:   "/var/lib/pacman/walg",
				},
				Restore: WALGRestoreConfig{BackupName: "base_000000010000000000000005"},
			},
			dataDir:    " /restore/pgdata ",
			wantBinary: "/usr/local/bin/wal-g",
			wantArgs:   []string{"backup-fetch", "/restore/pgdata", "base_000000010000000000000005"},
		},
		{
			name: "missing data dir",
			walg: WALGConfig{
				Repository: WALGRepositoryConfig{
					Provider: WALGRepositoryProviderFilesystem,
					Prefix:   "/var/lib/pacman/walg",
				},
			},
			wantErr: ErrReinitWALGRestoreDataDirRequired,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			gotBinary, gotArgs, err := testCase.walg.BackupFetchCommand(testCase.dataDir)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("backup-fetch command error: got %v, want %v", err, testCase.wantErr)
			}
			if testCase.wantErr != nil {
				return
			}
			if gotBinary != testCase.wantBinary {
				t.Fatalf("backup-fetch binary: got %q, want %q", gotBinary, testCase.wantBinary)
			}
			if !reflect.DeepEqual(gotArgs, testCase.wantArgs) {
				t.Fatalf("backup-fetch args: got %+v, want %+v", gotArgs, testCase.wantArgs)
			}
		})
	}
}

func TestWALGRestoreEnvironmentIncludesRepositoryAndCredentials(t *testing.T) {
	t.Parallel()

	walg := WALGConfig{
		Binary: DefaultWALGBinary,
		Repository: WALGRepositoryConfig{
			Provider: WALGRepositoryProviderS3,
			Prefix:   "s3://pacman-backups/alpha",
			Endpoint: "https://s3.example.test",
			Region:   "us-east-1",
		},
		Credentials: WALGCredentialsConfig{
			InheritEnvironment: []string{"AWS_SESSION_TOKEN"},
			EnvironmentFiles: map[string]string{
				"AWS_ACCESS_KEY_ID": "/run/secrets/aws-access-key-id",
			},
		},
	}

	got, err := walg.RestoreEnvironment(
		func(name string) (string, bool) {
			if name == "AWS_SESSION_TOKEN" {
				return "session-token", true
			}
			return "", false
		},
		func(path string) ([]byte, error) {
			if path != "/run/secrets/aws-access-key-id" {
				t.Fatalf("unexpected credential file path: %q", path)
			}
			return []byte("access-key\n"), nil
		},
	)
	if err != nil {
		t.Fatalf("restore environment: %v", err)
	}

	want := map[string]string{
		"WALG_S3_PREFIX":    "s3://pacman-backups/alpha",
		"AWS_ENDPOINT":      "https://s3.example.test",
		"AWS_REGION":        "us-east-1",
		"AWS_SESSION_TOKEN": "session-token",
		"AWS_ACCESS_KEY_ID": "access-key",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected restore environment: got %+v, want %+v", got, want)
	}
}

func TestWALFetchRestoreCommandEmbedsRepositoryAndCredentials(t *testing.T) {
	t.Parallel()

	walg := WALGConfig{
		Binary: "/usr/local/bin/wal-g",
		Repository: WALGRepositoryConfig{
			Provider: WALGRepositoryProviderS3,
			Prefix:   "s3://pacman-backups/alpha",
			Endpoint: "https://s3.example.test",
			Region:   "us-east-1",
		},
		Credentials: WALGCredentialsConfig{
			InheritEnvironment: []string{"AWS_SESSION_TOKEN"},
			EnvironmentFiles: map[string]string{
				"AWS_ACCESS_KEY_ID": "/run/secrets/aws-access-key-id",
			},
		},
	}

	got, err := walg.WALFetchRestoreCommand(
		func(name string) (string, bool) {
			if name == "AWS_SESSION_TOKEN" {
				return "session token", true
			}
			return "", false
		},
		func(path string) ([]byte, error) {
			if path != "/run/secrets/aws-access-key-id" {
				t.Fatalf("unexpected credential file path: %q", path)
			}
			return []byte("access'key\n"), nil
		},
	)
	if err != nil {
		t.Fatalf("build WAL-G wal-fetch restore command: %v", err)
	}

	want := "env " +
		"'AWS_ACCESS_KEY_ID=access'\"'\"'key' " +
		"'AWS_ENDPOINT=https://s3.example.test' " +
		"'AWS_REGION=us-east-1' " +
		"'AWS_SESSION_TOKEN=session token' " +
		"'WALG_S3_PREFIX=s3://pacman-backups/alpha' " +
		"'/usr/local/bin/wal-g' wal-fetch '%f' '%p'"
	if got != want {
		t.Fatalf("restore command: got %q, want %q", got, want)
	}
}

func TestWALGRepositoryEnvironmentProviders(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		repository WALGRepositoryConfig
		want       map[string]string
	}{
		{
			name: "gcs",
			repository: WALGRepositoryConfig{
				Provider: WALGRepositoryProviderGCS,
				Prefix:   "gs://pacman-backups/alpha",
			},
			want: map[string]string{"WALG_GS_PREFIX": "gs://pacman-backups/alpha"},
		},
		{
			name: "azure",
			repository: WALGRepositoryConfig{
				Provider: WALGRepositoryProviderAzure,
				Prefix:   "azure://pacman-backups/alpha",
			},
			want: map[string]string{"WALG_AZ_PREFIX": "azure://pacman-backups/alpha"},
		},
		{
			name: "filesystem",
			repository: WALGRepositoryConfig{
				Provider: WALGRepositoryProviderFilesystem,
				Prefix:   "/var/lib/pacman/walg",
			},
			want: map[string]string{"WALG_FILE_PREFIX": "/var/lib/pacman/walg"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := testCase.repository.Environment()
			if !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("repository environment: got %+v, want %+v", got, testCase.want)
			}
		})
	}
}

func TestWALGCredentialsResolveEnvironment(t *testing.T) {
	t.Parallel()

	credentials := WALGCredentialsConfig{
		InheritEnvironment: []string{"AWS_SESSION_TOKEN"},
		Environment: map[string]string{
			"AWS_REGION": " us-east-1 ",
		},
		EnvironmentFiles: map[string]string{
			"AWS_ACCESS_KEY_ID":     "/run/secrets/aws-access-key-id",
			"AWS_SECRET_ACCESS_KEY": "/run/secrets/aws-secret-access-key",
		},
	}

	got, err := credentials.ResolveEnvironment(
		func(name string) (string, bool) {
			if name == "AWS_SESSION_TOKEN" {
				return "session-token", true
			}
			return "", false
		},
		func(path string) ([]byte, error) {
			switch path {
			case "/run/secrets/aws-access-key-id":
				return []byte("access-key\n"), nil
			case "/run/secrets/aws-secret-access-key":
				return []byte("secret-key\n"), nil
			default:
				t.Fatalf("unexpected credential file path: %q", path)
				return nil, nil
			}
		},
	)
	if err != nil {
		t.Fatalf("resolve wal-g environment: %v", err)
	}

	want := map[string]string{
		"AWS_SESSION_TOKEN":     "session-token",
		"AWS_REGION":            "us-east-1",
		"AWS_ACCESS_KEY_ID":     "access-key",
		"AWS_SECRET_ACCESS_KEY": "secret-key",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected wal-g environment: got %+v, want %+v", got, want)
	}
}

func TestWALGCredentialsResolveEnvironmentPropagatesReadError(t *testing.T) {
	t.Parallel()

	readErr := errors.New("permission denied")
	_, err := (WALGCredentialsConfig{
		EnvironmentFiles: map[string]string{
			"AWS_SECRET_ACCESS_KEY": "/run/secrets/aws-secret-access-key",
		},
	}).ResolveEnvironment(nil, func(string) ([]byte, error) {
		return nil, readErr
	})
	if err == nil {
		t.Fatal("expected credential file read error")
	}
	if !errors.Is(err, readErr) {
		t.Fatalf("expected wrapped read error, got %v", err)
	}
}

func TestConfigRedactedMasksWALGCredentialsWithoutMutatingOriginal(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Reinit: &ReinitConfig{
			WALG: &WALGConfig{
				Repository: WALGRepositoryConfig{
					Provider: WALGRepositoryProviderS3,
					Prefix:   "s3://pacman-backups/alpha",
				},
				Credentials: WALGCredentialsConfig{
					Environment: map[string]string{
						"AWS_SECRET_ACCESS_KEY": "secret-key",
					},
					EnvironmentFiles: map[string]string{
						"AWS_ACCESS_KEY_ID": "/run/secrets/aws-access-key-id",
					},
				},
			},
		},
	}

	redacted := cfg.Redacted()
	if redacted.Reinit == nil || redacted.Reinit.WALG == nil {
		t.Fatalf("expected redacted reinit config, got %+v", redacted.Reinit)
	}

	credentials := redacted.Reinit.WALG.Credentials
	if credentials.Environment["AWS_SECRET_ACCESS_KEY"] != redactedSecretValue {
		t.Fatalf("unexpected inline credential redaction: %+v", credentials.Environment)
	}
	if credentials.EnvironmentFiles["AWS_ACCESS_KEY_ID"] != redactedSecretValue {
		t.Fatalf("unexpected credential file redaction: %+v", credentials.EnvironmentFiles)
	}

	if cfg.Reinit.WALG.Credentials.Environment["AWS_SECRET_ACCESS_KEY"] != "secret-key" {
		t.Fatalf("expected original inline credential to remain unchanged: %+v", cfg.Reinit.WALG.Credentials.Environment)
	}
	if cfg.Reinit.WALG.Credentials.EnvironmentFiles["AWS_ACCESS_KEY_ID"] != "/run/secrets/aws-access-key-id" {
		t.Fatalf("expected original credential file path to remain unchanged: %+v", cfg.Reinit.WALG.Credentials.EnvironmentFiles)
	}
}

func TestConfigHasInlineSecretsIncludesWALGCredentials(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Reinit: &ReinitConfig{
			WALG: &WALGConfig{
				Credentials: WALGCredentialsConfig{
					Environment: map[string]string{
						"AWS_SECRET_ACCESS_KEY": "secret-key",
					},
				},
			},
		},
	}

	if !cfg.HasInlineSecrets() {
		t.Fatal("expected WAL-G inline environment to count as a secret")
	}
}

func TestConfigStringRedactsWALGCredentials(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Reinit: &ReinitConfig{
			WALG: &WALGConfig{
				Credentials: WALGCredentialsConfig{
					Environment: map[string]string{
						"AWS_SECRET_ACCESS_KEY": "secret-key",
					},
					EnvironmentFiles: map[string]string{
						"AWS_ACCESS_KEY_ID": "/run/secrets/aws-access-key-id",
					},
				},
			},
		},
	}

	formatted := cfg.String()
	if strings.Contains(formatted, "secret-key") {
		t.Fatalf("expected String output to redact inline WAL-G credential, got %q", formatted)
	}
	if strings.Contains(formatted, "/run/secrets/aws-access-key-id") {
		t.Fatalf("expected String output to redact WAL-G credential file path, got %q", formatted)
	}
	if !strings.Contains(formatted, redactedSecretValue) {
		t.Fatalf("expected String output to contain redaction marker, got %q", formatted)
	}
}
