//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/polkiloo/pacman/test/testenv"
)

const postgresExtensionStartupTimeout = 90 * time.Second

type pgextNodeConfig struct {
	NodeName        string
	NodeRole        string
	APIAddress      string
	ControlAddress  string
	HelperPath      string
	PostgresDataDir string
	ClusterName     string
	InitialPrimary  string
	SeedAddresses   []string
	ExpectedMembers []string
}

type postgresExtensionNode struct {
	Base     string
	Service  *testenv.Service
	Client   *http.Client
	Database string
	Username string
	Password string
}

func startPostgresExtensionNode(t *testing.T, cfg pgextNodeConfig) postgresExtensionNode {
	t.Helper()

	cfg = cfg.withDefaults()

	env := testenv.New(t)
	name := sanitizeIntegrationName(t.Name())
	testenv.RequireLocalImage(t, pgextTestImage())

	service := env.StartService(t, testenv.ServiceConfig{
		Name:         name + "-pgext",
		Image:        pgextTestImage(),
		ExposedPorts: []string{"5432/tcp", "8080/tcp"},
		Env: map[string]string{
			"POSTGRES_DB":       "pacman",
			"POSTGRES_USER":     "pacman",
			"POSTGRES_PASSWORD": "pacman",
			"PGDATABASE":        "pacman",
			"PGUSER":            "pacman",
			"PGPASSWORD":        "pacman",
			"PGSSLMODE":         "disable",
		},
		Cmd:          pgextPostgresCommand(cfg),
		WaitStrategy: wait.ForListeningPort("5432/tcp").WithStartupTimeout(postgresExtensionStartupTimeout),
	})

	return postgresExtensionNode{
		Base:     "http://" + service.Address(t, "8080"),
		Service:  service,
		Client:   &http.Client{Timeout: 3 * time.Second},
		Database: "pacman",
		Username: "pacman",
		Password: "pacman",
	}
}

func (cfg pgextNodeConfig) withDefaults() pgextNodeConfig {
	defaulted := cfg

	if strings.TrimSpace(defaulted.NodeName) == "" {
		defaulted.NodeName = "alpha-pgext"
	}
	if strings.TrimSpace(defaulted.NodeRole) == "" {
		defaulted.NodeRole = "data"
	}
	if strings.TrimSpace(defaulted.APIAddress) == "" {
		defaulted.APIAddress = "0.0.0.0:8080"
	}
	if strings.TrimSpace(defaulted.ControlAddress) == "" {
		defaulted.ControlAddress = "0.0.0.0:9090"
	}
	if strings.TrimSpace(defaulted.HelperPath) == "" {
		defaulted.HelperPath = "pacmand"
	}
	if strings.TrimSpace(defaulted.PostgresDataDir) == "" {
		defaulted.PostgresDataDir = "/var/lib/postgresql/data"
	}
	if strings.TrimSpace(defaulted.ClusterName) == "" {
		defaulted.ClusterName = "alpha"
	}
	if strings.TrimSpace(defaulted.InitialPrimary) == "" {
		defaulted.InitialPrimary = defaulted.NodeName
	}
	if len(defaulted.SeedAddresses) == 0 {
		defaulted.SeedAddresses = []string{defaulted.ControlAddress}
	}
	if len(defaulted.ExpectedMembers) == 0 {
		defaulted.ExpectedMembers = []string{defaulted.NodeName}
	}

	return defaulted
}

func pgextPostgresCommand(cfg pgextNodeConfig) []string {
	return []string{
		"postgres",
		"-c", "shared_preload_libraries=pacman_agent",
		"-c", "listen_addresses=*",
		"-c", fmt.Sprintf("pacman.node_name=%s", cfg.NodeName),
		"-c", fmt.Sprintf("pacman.node_role=%s", cfg.NodeRole),
		"-c", fmt.Sprintf("pacman.api_address=%s", cfg.APIAddress),
		"-c", fmt.Sprintf("pacman.control_address=%s", cfg.ControlAddress),
		"-c", fmt.Sprintf("pacman.helper_path=%s", cfg.HelperPath),
		"-c", fmt.Sprintf("pacman.postgres_data_dir=%s", cfg.PostgresDataDir),
		"-c", "pacman.postgres_listen_address=127.0.0.1",
		"-c", "pacman.postgres_port=5432",
		"-c", fmt.Sprintf("pacman.cluster_name=%s", cfg.ClusterName),
		"-c", fmt.Sprintf("pacman.initial_primary=%s", cfg.InitialPrimary),
		"-c", fmt.Sprintf("pacman.seed_addresses=%s", strings.Join(cfg.SeedAddresses, ",")),
		"-c", fmt.Sprintf("pacman.expected_members=%s", strings.Join(cfg.ExpectedMembers, ",")),
	}
}

func pgextTestImage() string {
	if image := strings.TrimSpace(os.Getenv("PACMAN_TEST_PGEXT_IMAGE")); image != "" {
		return image
	}

	return "pacman-pgext-postgres:local"
}

func openPGExtDB(t *testing.T, node postgresExtensionNode) *sql.DB {
	t.Helper()

	db, err := openDB(
		node.Service.Host(t),
		node.Service.Port(t, "5432"),
		node.Database,
		node.Username,
		node.Password,
	)
	if err != nil {
		t.Fatalf("open postgres extension fixture: %v", err)
	}

	return db
}

func waitForHelperPID(t *testing.T, service *testenv.Service, timeout time.Duration) int {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		result := service.Exec(t, "sh", "-lc", helperPIDLookupScript())
		if result.ExitCode == 0 {
			value, err := strconv.Atoi(strings.TrimSpace(result.Output))
			if err == nil && value > 0 {
				return value
			}
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("PACMAN helper process did not appear within %s", timeout)
	return 0
}

func helperPIDLookupScript() string {
	return `
for entry in /proc/[0-9]*; do
	if [ ! -r "$entry/cmdline" ]; then
		continue
	fi

	cmd=$(tr '\000' ' ' <"$entry/cmdline")
	case "$cmd" in
		*"pacmand -pgext-env"*)
			echo "${entry##*/}"
			exit 0
			;;
	esac
done

exit 1
`
}

func waitForHelperPIDChange(t *testing.T, service *testenv.Service, oldPID int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		result := service.Exec(t, "sh", "-lc", helperPIDLookupScript())
		if result.ExitCode == 0 {
			value, err := strconv.Atoi(strings.TrimSpace(result.Output))
			if err == nil && value > 0 && value != oldPID {
				return
			}
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("PACMAN helper process was not restarted after terminating pid %d within %s", oldPID, timeout)
}

func waitForHTTPUnavailable(t *testing.T, client *http.Client, rawURL string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response, err := client.Get(rawURL)
		if err != nil {
			return
		}

		response.Body.Close()
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("endpoint %s remained reachable for %s", rawURL, timeout)
}

func waitForLogContains(t *testing.T, service *testenv.Service, want string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(service.Logs(t), want) {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("service logs did not contain %q within %s", want, timeout)
}
