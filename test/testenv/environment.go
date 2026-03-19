package testenv

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const defaultRunnerImage = "pacman-test:local"
const defaultPostgresImage = "postgres:17-alpine"

// RunnerConfig configures a long-lived PACMAN test container on a shared test network.
type RunnerConfig struct {
	Name       string
	Aliases    []string
	Env        map[string]string
	Files      []testcontainers.ContainerFile
	Entrypoint []string
	Cmd        []string
}

// Environment manages the shared Docker network and runner image for integration tests.
type Environment struct {
	ctx           context.Context
	image         string
	postgresImage string
	network       *testcontainers.DockerNetwork
	namePrefix    string
}

// Runner is a long-lived PACMAN test container used during integration tests.
type Runner struct {
	ctx       context.Context
	name      string
	container testcontainers.Container
}

// Node represents a PACMAN node topology with PostgreSQL and pacmand containers on the same test network.
type Node struct {
	Name     string
	Pacmand  *Runner
	Postgres *Postgres
}

// Postgres represents a PostgreSQL fixture container attached to the shared test network.
type Postgres struct {
	ctx       context.Context
	name      string
	alias     string
	database  string
	username  string
	password  string
	container *tcpostgres.PostgresContainer
}

// New creates a new integration test environment backed by Docker and testcontainers.
func New(t *testing.T) *Environment {
	t.Helper()

	ctx := context.Background()
	image := os.Getenv("PACMAN_TEST_IMAGE")
	if strings.TrimSpace(image) == "" {
		image = defaultRunnerImage
	}

	postgresImage := os.Getenv("PACMAN_TEST_POSTGRES_IMAGE")
	if strings.TrimSpace(postgresImage) == "" {
		postgresImage = defaultPostgresImage
	}

	requireLocalImage(ctx, t, image)

	nw, err := network.New(ctx, network.WithAttachable())
	if err != nil {
		t.Fatalf("create integration test network: %v", err)
	}

	t.Cleanup(func() {
		if err := nw.Remove(ctx); err != nil {
			t.Logf("remove integration test network: %v", err)
		}
	})

	return &Environment{
		ctx:           ctx,
		image:         image,
		postgresImage: postgresImage,
		network:       nw,
		namePrefix:    sanitizeName(t.Name()),
	}
}

// NetworkName returns the Docker network name used by the environment.
func (e *Environment) NetworkName() string {
	return e.network.Name
}

// StartPacmandNode starts a long-lived pacmand runner in the shared network.
func (e *Environment) StartPacmandNode(t *testing.T, name string, aliases ...string) *Runner {
	t.Helper()

	return e.StartRunner(t, RunnerConfig{
		Name:    name,
		Aliases: aliases,
		Env: map[string]string{
			"PACMAN_TEST_ROLE": "pacmand",
			"PACMAN_TEST_NODE": name,
		},
	})
}

// StartNode starts a PostgreSQL 17 container and a paired pacmand container on the shared network.
func (e *Environment) StartNode(t *testing.T, name string) *Node {
	t.Helper()

	postgresAlias := fmt.Sprintf("%s-postgres", name)
	postgresFixture := e.StartPostgres(t, name, postgresAlias)

	pacmand := e.StartRunner(t, RunnerConfig{
		Name:    name,
		Aliases: []string{name},
		Env: map[string]string{
			"PACMAN_TEST_ROLE":              "pacmand",
			"PACMAN_TEST_NODE":              name,
			"PACMAN_TEST_POSTGRES_HOST":     postgresFixture.Alias(),
			"PACMAN_TEST_POSTGRES_PORT":     "5432",
			"PACMAN_TEST_POSTGRES_DATABASE": postgresFixture.Database(),
			"PACMAN_TEST_POSTGRES_USERNAME": postgresFixture.Username(),
			"PACMAN_TEST_POSTGRES_PASSWORD": postgresFixture.Password(),
		},
	})

	return &Node{
		Name:     name,
		Pacmand:  pacmand,
		Postgres: postgresFixture,
	}
}

// StartPacmanctl starts a long-lived pacmanctl runner in the shared network.
func (e *Environment) StartPacmanctl(t *testing.T, name string, aliases ...string) *Runner {
	t.Helper()

	return e.StartRunner(t, RunnerConfig{
		Name:    name,
		Aliases: aliases,
		Env: map[string]string{
			"PACMAN_TEST_ROLE": "pacmanctl",
		},
	})
}

// StartPostgres starts a PostgreSQL fixture container on the shared network.
func (e *Environment) StartPostgres(t *testing.T, name, alias string) *Postgres {
	t.Helper()

	if strings.TrimSpace(name) == "" {
		t.Fatal("postgres fixture name must be provided")
	}

	if strings.TrimSpace(alias) == "" {
		t.Fatal("postgres fixture alias must be provided")
	}

	database := "pacman"
	username := "pacman"
	password := "pacman"

	container, err := tcpostgres.Run(
		e.ctx,
		e.postgresImage,
		tcpostgres.WithDatabase(database),
		tcpostgres.WithUsername(username),
		tcpostgres.WithPassword(password),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.WithName(fmt.Sprintf("%s-%s-%d", e.namePrefix, sanitizeName(alias), time.Now().UnixNano())),
		network.WithNetwork([]string{alias}, e.network),
	)
	if err != nil {
		t.Fatalf("start postgres fixture %q: %v", name, err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate postgres fixture %q: %v", name, err)
		}
	})

	return &Postgres{
		ctx:       e.ctx,
		name:      name,
		alias:     alias,
		database:  database,
		username:  username,
		password:  password,
		container: container,
	}
}

// StartRunner starts a generic PACMAN runner container on the shared network.
func (e *Environment) StartRunner(t *testing.T, cfg RunnerConfig) *Runner {
	t.Helper()

	if strings.TrimSpace(cfg.Name) == "" {
		t.Fatal("runner name must be provided")
	}

	aliases := cfg.Aliases
	if len(aliases) == 0 {
		aliases = []string{cfg.Name}
	}

	options := []testcontainers.ContainerCustomizer{
		testcontainers.WithName(fmt.Sprintf("%s-%s-%d", e.namePrefix, sanitizeName(cfg.Name), time.Now().UnixNano())),
		testcontainers.WithEnv(cfg.Env),
		testcontainers.WithFiles(cfg.Files...),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{
			"/bin/sh", "-lc", "test -x /usr/local/bin/pacmand && test -x /usr/local/bin/pacmanctl",
		}).WithStartupTimeout(30 * time.Second)),
		network.WithNetwork(aliases, e.network),
	}

	if len(cfg.Entrypoint) > 0 {
		options = append(options, testcontainers.WithEntrypoint(cfg.Entrypoint...))
	}

	if len(cfg.Cmd) > 0 {
		options = append(options, testcontainers.WithCmd(cfg.Cmd...))
	}

	container, err := testcontainers.Run(e.ctx, e.image, options...)
	if err != nil {
		t.Fatalf("start runner %q: %v", cfg.Name, err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(e.ctx); err != nil {
			t.Logf("terminate runner %q: %v", cfg.Name, err)
		}
	})

	return &Runner{
		ctx:       e.ctx,
		name:      cfg.Name,
		container: container,
	}
}

// Name returns the logical runner name used by the test scenario.
func (r *Runner) Name() string {
	return r.name
}

// RequireExec executes a command in the runner and fails the test if execution fails.
func (r *Runner) RequireExec(t *testing.T, cmd ...string) string {
	t.Helper()

	exitCode, reader, err := r.container.Exec(r.ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("exec %q in %q: %v", strings.Join(cmd, " "), r.name, err)
	}

	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read exec output for %q in %q: %v", strings.Join(cmd, " "), r.name, err)
	}

	if exitCode != 0 {
		t.Fatalf("exec %q in %q returned %d: %s", strings.Join(cmd, " "), r.name, exitCode, string(output))
	}

	return string(output)
}

// NetworkAliases returns the Docker network alias map for the runner.
func (r *Runner) NetworkAliases(t *testing.T) map[string][]string {
	t.Helper()

	aliases, err := r.container.NetworkAliases(r.ctx)
	if err != nil {
		t.Fatalf("load network aliases for %q: %v", r.name, err)
	}

	return aliases
}

// Networks returns the Docker networks attached to the runner.
func (r *Runner) Networks(t *testing.T) []string {
	t.Helper()

	networks, err := r.container.Networks(r.ctx)
	if err != nil {
		t.Fatalf("load networks for %q: %v", r.name, err)
	}

	return networks
}

// Name returns the logical PostgreSQL fixture name.
func (p *Postgres) Name() string {
	return p.name
}

// Alias returns the shared-network alias used for PostgreSQL access from pacmand.
func (p *Postgres) Alias() string {
	return p.alias
}

// Database returns the configured database name.
func (p *Postgres) Database() string {
	return p.database
}

// Username returns the configured database username.
func (p *Postgres) Username() string {
	return p.username
}

// Password returns the configured database password.
func (p *Postgres) Password() string {
	return p.password
}

// NetworkAliases returns the Docker network alias map for the PostgreSQL fixture.
func (p *Postgres) NetworkAliases(t *testing.T) map[string][]string {
	t.Helper()

	aliases, err := p.container.NetworkAliases(p.ctx)
	if err != nil {
		t.Fatalf("load network aliases for postgres fixture %q: %v", p.name, err)
	}

	return aliases
}

// Networks returns the Docker networks attached to the PostgreSQL fixture.
func (p *Postgres) Networks(t *testing.T) []string {
	t.Helper()

	networks, err := p.container.Networks(p.ctx)
	if err != nil {
		t.Fatalf("load networks for postgres fixture %q: %v", p.name, err)
	}

	return networks
}

func requireLocalImage(ctx context.Context, t *testing.T, image string) {
	t.Helper()

	cmd := exec.CommandContext(ctx, "docker", "image", "inspect", image)
	if err := cmd.Run(); err != nil {
		t.Fatalf("required local test image %q is missing; run `make docker-build-test-image` first: %v", image, err)
	}
}

func sanitizeName(value string) string {
	replacer := strings.NewReplacer("/", "-", "_", "-", " ", "-", ":", "-")
	return strings.ToLower(replacer.Replace(value))
}
