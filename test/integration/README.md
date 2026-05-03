# Integration Test Groups

The integration suite is split into focused groups so failures are easier to isolate and rerun.

Image build targets used by the integration groups:

- `make docker-build-test-image`
- `make docker-build-pgext-image`
- `make docker-build-ansible-install-image`

Each grouped target declares the image builds it needs, so running a group directly is enough locally and in CI.

Grouped targets:

- `make test-integration-control-plane`
- `make test-integration-patroni`
- `make test-integration-postgres`
- `make test-integration-ha`
- `make test-integration-install`
  `test-integration-install` compiles and runs the dedicated `./test/installintegration` package, so it does not affect `./test/integration` compile scope.

Run the full grouped suite:

```bash
make test-integration
```

Run one group with extra Go test flags:

```bash
GO_TEST_INTEGRATION_FLAGS='-count=1 -v' make test-integration-ha
```

Current grouping:

- `control-plane`: cluster smoke, daemon startup, topology validation, HTTP/native APIs, shared state projection, HTTPS, and peer mTLS coverage
- `patroni`: Patroni API compatibility, migration behavior, and backup/restore cloud rendering
- `postgres`: PostgreSQL replication fixture, subprocess/workflow behavior, promotion/standby rendering, maintenance mode, and PostgreSQL extension lifecycle
- `ha`: failover, switchover, quorum, rejoin, and end-to-end HA flows
- `install`: 3-node installation and bootstrap validation for the Ansible deployment path
