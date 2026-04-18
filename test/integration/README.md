# Integration Test Groups

The integration suite is split into focused groups so failures are easier to isolate and rerun.

Local prerequisites:

- `make docker-build-test-image`
- `make docker-build-pgext-image`
- `make docker-build-ansible-install-image`

Grouped targets:

- `make test-integration-smoke`
- `make test-integration-security`
- `make test-integration-patroni`
- `make test-integration-pgext`
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

- `smoke`: cluster smoke, daemon startup, HTTP API, and shared control-plane projection
- `security`: HTTPS and peer mTLS coverage
- `patroni`: Patroni compatibility and fixture-backed monitoring endpoints
- `pgext`: PostgreSQL extension lifecycle and SQL-backed observation
- `ha`: failover, switchover, quorum, and rejoin flows
- `install`: 3-node installation and bootstrap validation for the Ansible deployment path
