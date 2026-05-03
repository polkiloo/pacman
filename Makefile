GOTOOLCHAIN ?= go1.26.1
GO ?= go
BIN_DIR ?= ./bin
GOBIN ?= $(shell $(GO) env GOBIN)
CONTAINER_RUNTIME ?= docker
RPM_BUILDER_IMAGE ?= pacman-rpm-builder:el9
RPM_OUTPUT_DIR ?= $(CURDIR)/bin/rpm
RPM_VERSION ?= 0.1.0
RPM_RELEASE ?= 1
RPM_SOURCE_DATE_EPOCH ?= $(shell git log -1 --pretty=%ct 2>/dev/null || date +%s)
RPM_VALIDATE_RELEASE1_DIR ?= $(CURDIR)/bin/rpm/release1
RPM_VALIDATE_RELEASE2_DIR ?= $(CURDIR)/bin/rpm/release2
RPM_VALIDATION_IMAGE ?= rockylinux:9

export GOTOOLCHAIN

ifeq ($(GOBIN),)
GOBIN := $(shell $(GO) env GOPATH)/bin
endif

GOLANGCI_LINT ?= $(GOBIN)/golangci-lint
COVERAGE_OUT ?= coverage.out
COVERAGE_MIN ?= 90.0
FULL_COVERAGE_PACKAGE_LIST_CMD = $(GO) list ./... | grep -v '/test/'
# The threshold gate is intentionally unit-test scoped. Thin entrypoints and
# distributed/container-driven orchestration paths are validated by dedicated
# conformance and integration targets instead of this fast unit threshold.
COVERAGE_CHECK_PACKAGE_LIST_CMD = $(GO) list ./... | grep -v '/test/' | grep -v '^github.com/polkiloo/pacman/cmd/' | grep -v '^github.com/polkiloo/pacman/internal/controlplane$$' | grep -v '^github.com/polkiloo/pacman/internal/dcs/dcstest$$' | grep -v '^github.com/polkiloo/pacman/internal/dcs/etcd$$' | grep -v '^github.com/polkiloo/pacman/internal/dcs/raft$$'
PACMAN_TEST_IMAGE ?= pacman-test:local
PACMAN_TEST_PGEXT_IMAGE ?= pacman-pgext-postgres:local
PACMAN_TEST_POSTGRES_IMAGE ?= $(PACMAN_TEST_PGEXT_IMAGE)
PACMAN_ANSIBLE_INSTALL_IMAGE ?= pacman-ansible-install:local
PACMAN_ANSIBLE_INSTALL_RPM_DIR ?= $(CURDIR)/bin/ansible-install-rpm
DOCKER_BUILD_PROGRESS ?= plain
GO_TEST_INTEGRATION_FLAGS ?= -v
GO_TEST_INTEGRATION_PACKAGE ?= ./test/integration
GO_TEST_INSTALL_PACKAGE ?= ./test/installintegration
TESTCONTAINERS_RYUK_DISABLED ?=
PG_EXTENSION_DIR ?= ./postgresql/pacman_agent
PG_EXTENSION_IMAGE ?= postgres:17-bookworm
PG_EXTENSION_OUTPUT ?= $(BIN_DIR)/pg-extension
PG_CONFIG ?= pg_config

INTEGRATION_TEST_ENV = PACMAN_TEST_IMAGE=$(PACMAN_TEST_IMAGE) \
	PACMAN_TEST_PGEXT_IMAGE=$(PACMAN_TEST_PGEXT_IMAGE) \
	PACMAN_TEST_POSTGRES_IMAGE=$(PACMAN_TEST_POSTGRES_IMAGE) \
	PACMAN_ANSIBLE_INSTALL_IMAGE=$(PACMAN_ANSIBLE_INSTALL_IMAGE) \
	TESTCONTAINERS_RYUK_DISABLED=$(TESTCONTAINERS_RYUK_DISABLED)

INTEGRATION_GROUP_CONTROL_PLANE := ^(TestPACMANClusterEnvironment|TestDaemonRejectsConfigWithUnknownDCSBackend|TestDaemonRejectsEtcdConfigWithNoEndpoints|TestDaemonRejectsEtcdEndpointWithInvalidScheme|TestSwitchoverApiRejectsRequestWhenNoEligibleStandbyExists|TestFailoverApiRejectsRequestWhenPrimaryIsHealthy|TestSwitchoverApiRejectsRequestDuringActiveMaintenance|TestEtcdBackedDaemonBootstrapsAndServesHealth|TestEtcdBackedDaemonReportsCorrectMemberName|TestEtcdBackedTwoNodeTopologySharesClusterSpec|TestEtcdBackedMaintenanceModeIsVisibleAcrossNodes|TestEtcdBackedDaemonMetricsReflectClusterState|TestControlPlaneAggregatesSharedDaemonStateWithRealPostgres|TestPacmandDaemonStartupMatrix|TestPacmandHTTPAPIServesHealth|TestPacmandPrimaryAndReplicaProbes|TestPacmandNativeNodeAndMembersAPIWithRealPostgresOperation|TestPacmandHistoryMaintenanceAndDiagnosticsAPI|TestPacmandOperationsAndPublishedOpenAPI|TestPacmandHTTPAPIServesHealthOverTLS|TestPacmandPeerIdentityAcceptsAllowedMemberCertificate|TestPacmandPeerIdentityRejectsUnexpectedMemberCertificate)$$
INTEGRATION_GROUP_PATRONI := ^(TestPatroniInspiredCloudRestoreCommandWithRenderedStandbyInTestcontainers|TestPatroniInspiredBackupRestoreCloudNegativeCasesInTestcontainers|TestPatroniProbeCompatibilityWithContainerFixture|TestPatroniMonitoringDocumentsWithContainerFixture|TestPatroniAdminCompatibilityWithContainerFixture|TestPatroniMigrationRejectsEtcdHostWithoutScheme|TestPatroniMigrationRejectsUnsupportedDCSBackend|TestPatroniMigrationRejectsMissingClusterName|TestPatroniMigratedSingleNodeBootstrapsAndServesHealth|TestPatroniMigratedClusterNameMatchesPatroniScope|TestPatroniMigratedMembershipExamplesPreserveInitialPrimaryAndExpectedMembers|TestPatroniMigratedPrimaryProbeWorks|TestPatroniMigratedMetricsEndpointServesClusterMetrics|TestPatroniMigratedNode2RESTAuthVariantRequiresBearerToken)$$
INTEGRATION_GROUP_POSTGRES := ^(TestPostgresExtensionStartupPublishesAPIAndInstallsSQLAssets|TestPostgresExtensionRestartsPACMANHelperAfterUnexpectedExit|TestPostgresExtensionInvalidConfigKeepsAPIUnavailable|TestPostgresExtensionLocalStateObservationWithRealSQL|TestPostgresExtensionStopsPACMANHelperWhenPostgresStops|TestPostgresReplicationFixtureBootstrapsStreamingTopology|TestReplicationFixtureSingleStandbyBootstraps|TestReplicationFixtureSubsequentWritesReplicateToAllStandbys|TestReplicationFixturePrimaryShowsActiveSendersForAllStandbys|TestReplicationFixtureStandbyRejectsDirectWrite|TestReplicationFixtureStandbyHasNoOwnReplicationSlots|TestReplicationFixturePrimaryHasNoInactiveSlots|TestReplicationFixturePatroniInspiredNegativeSafetyCases|TestPostgresSubprocessNegativeCasesInRunner|TestPostgresWorkflowPositiveCases|TestPostgresWorkflowNegativeCases|TestPostgresWorkflowAdditionalNegativeCases|TestPostgresRoleDetectionUsesRealPrimaryAndStandby|TestPromoteWorkflowPromotesRealStandby|TestStandbyConfigurationRenderedByPACMANStreamsFromRealPrimary|TestMaintenanceModeEnableDisableWithRealTopology)$$
INTEGRATION_GROUP_HA := ^(TestEndToEndAutomaticFailover|TestEndToEndAutomaticFailoverNegativeCases|TestEndToEndThreeNodeClusterBootstrap|TestEndToEndThreeNodeClusterBootstrapNegativeCases|TestEndToEndNetworkPartition|TestEndToEndPlannedSwitchover|TestEndToEndFormerPrimaryRejoinsAfterPlannedSwitchover|TestEndToEndPlannedSwitchoverNegativeCases|TestEndToEndWitnessAssistedQuorum|TestEndToEndWitnessAssistedQuorumNegativeCases|TestFailoverPromotesRealStandbyAndRecordsHistory|TestFailoverIntentRejectsHealthyPrimaryWithRealStreamingStandby|TestRejoinOperationProjectsRecoveringPhaseWithRealTopology|TestMaintenanceOverridesActiveFailoverPhaseWithRealTopology|TestConfirmPrimaryFailureConfiguredQuorumMatrixWithRealTopology|TestConfirmPrimaryFailureObservedQuorumMatrixWithRealTopology|TestConfiguredQuorumIgnoresObservedMembersOutsideSpecWithRealTopology|TestCreateFailoverIntentObservedQuorumMatrixWithRealTopology|TestRejoinStrategySelectsRewindAfterRealFailover|TestExecuteRejoinRewindKeepsClusterRecoveringWithRealTopology|TestRejoinNegativeCasesWithRealTopology|TestRejoinAdditionalNegativeCasesWithRealTopology|TestSwitchoverValidationUsesRealStreamingStandby|TestSwitchoverIntentSchedulesRealStreamingStandby|TestSwitchoverPromotesRealStandbyAndRecordsHistory|TestSwitchoverValidationRejectsUnavailableRealStandby|TestSwitchoverExecutionRejectsFutureScheduledIntentWithRealStandby)$$
INTEGRATION_GROUP_INSTALL := ^(TestAnsibleThreeNodeInstallationUsingTestcontainers|TestAnsibleDemoSwitchoverWithVIPManagerUnderLoad)$$
INTEGRATION_GROUP_DCS_CONFORMANCE := ^(TestEtcdDCSConformanceInRunner|TestRaftThreeNodeReplicationAndWatch|TestRaftThreeNodeLeaderFailover)$$

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -X github.com/polkiloo/pacman/internal/version.Version=$(VERSION) \
	-X github.com/polkiloo/pacman/internal/version.Commit=$(COMMIT) \
	-X github.com/polkiloo/pacman/internal/version.BuildDate=$(BUILD_DATE)

.PHONY: fmt test test-dcs-conformance test-integration test-integration-control-plane test-integration-patroni test-integration-postgres test-integration-ha test-integration-install docker-build-test-image docker-build-pgext-image docker-build-ansible-install-image coverage coverage-check lint lint-install build build-pacmand build-pacmanctl build-pg-extension package-pg-extension install-pg-extension clean-pg-extension tidy clean openapi-codegen-check rpm rpm-builder-image rpm-validate ansible-validate

fmt:
	$(GO) fmt ./...

test:
	$(GO) test ./...

test-dcs-conformance: docker-build-test-image
	$(GO) test ./internal/dcs/...
	$(INTEGRATION_TEST_ENV) $(GO) test $(GO_TEST_INTEGRATION_FLAGS) -tags=integration -run '$(INTEGRATION_GROUP_DCS_CONFORMANCE)' $(GO_TEST_INTEGRATION_PACKAGE)

openapi-codegen-check:
	@for spec in \
		docs/openapi/components-meta.yaml \
		docs/openapi/schemas-enums.yaml \
		docs/openapi/schemas-patroni.yaml \
		docs/openapi/schemas-operations.yaml \
		docs/openapi/schemas-cluster.yaml \
		docs/openapi/paths-probes.yaml \
		docs/openapi/paths-patroni.yaml \
		docs/openapi/paths-pacman.yaml; do \
		tmp=$$(mktemp); \
		trap 'rm -f "$$tmp"' EXIT; \
		$(GO) tool oapi-codegen -config docs/openapi/oapi-codegen.yaml -o "$$tmp" "$$spec" || exit $$?; \
		rm -f "$$tmp"; \
	done

docker-build-test-image:
	docker build --progress=$(DOCKER_BUILD_PROGRESS) -f test/docker/pacman-runner.Dockerfile -t $(PACMAN_TEST_IMAGE) .

docker-build-pgext-image:
	docker build --progress=$(DOCKER_BUILD_PROGRESS) -f test/docker/pacman-pgext-postgres.Dockerfile -t $(PACMAN_TEST_PGEXT_IMAGE) .

docker-build-ansible-install-image:
	docker build --progress=$(DOCKER_BUILD_PROGRESS) -f test/docker/pacman-ansible-install.Dockerfile -t $(PACMAN_ANSIBLE_INSTALL_IMAGE) .

test-integration: test-integration-control-plane test-integration-patroni test-integration-postgres test-integration-ha test-integration-install

test-integration-control-plane: docker-build-test-image
	$(INTEGRATION_TEST_ENV) $(GO) test $(GO_TEST_INTEGRATION_FLAGS) -tags=integration -run '$(INTEGRATION_GROUP_CONTROL_PLANE)' $(GO_TEST_INTEGRATION_PACKAGE)

test-integration-patroni: docker-build-test-image
	$(INTEGRATION_TEST_ENV) $(GO) test $(GO_TEST_INTEGRATION_FLAGS) -tags=integration -run '$(INTEGRATION_GROUP_PATRONI)' $(GO_TEST_INTEGRATION_PACKAGE)

test-integration-postgres: docker-build-test-image docker-build-pgext-image
	$(INTEGRATION_TEST_ENV) $(GO) test $(GO_TEST_INTEGRATION_FLAGS) -tags=integration -run '$(INTEGRATION_GROUP_POSTGRES)' $(GO_TEST_INTEGRATION_PACKAGE)

test-integration-ha: docker-build-test-image
	$(INTEGRATION_TEST_ENV) $(GO) test $(GO_TEST_INTEGRATION_FLAGS) -tags=integration -run '$(INTEGRATION_GROUP_HA)' $(GO_TEST_INTEGRATION_PACKAGE)

test-integration-install: docker-build-ansible-install-image
	rm -rf $(PACMAN_ANSIBLE_INSTALL_RPM_DIR)
	$(MAKE) rpm RPM_OUTPUT_DIR=$(PACMAN_ANSIBLE_INSTALL_RPM_DIR)
	PACMAN_ANSIBLE_INSTALL_RPM_DIR=$(PACMAN_ANSIBLE_INSTALL_RPM_DIR) \
		$(INTEGRATION_TEST_ENV) $(GO) test $(GO_TEST_INTEGRATION_FLAGS) -tags=integration -run '$(INTEGRATION_GROUP_INSTALL)' $(GO_TEST_INSTALL_PACKAGE)

coverage:
	@set -- $$($(FULL_COVERAGE_PACKAGE_LIST_CMD)); \
	if [ "$$#" -eq 0 ]; then \
		echo "failed to resolve coverage package list" >&2; \
		exit 1; \
	fi; \
	$(GO) test -p 1 -coverprofile=$(COVERAGE_OUT) "$$@"

coverage-check:
	@set -- $$($(COVERAGE_CHECK_PACKAGE_LIST_CMD)); \
	if [ "$$#" -eq 0 ]; then \
		echo "failed to resolve coverage package list" >&2; \
		exit 1; \
	fi; \
	$(GO) test -p 1 -coverprofile=$(COVERAGE_OUT) "$$@"
	@coverage=$$($(GO) tool cover -func=$(COVERAGE_OUT) | awk '/^total:/ { gsub("%", "", $$3); print $$3 }'); \
	if awk "BEGIN { exit !($$coverage >= $(COVERAGE_MIN)) }"; then \
		printf 'coverage %s%% meets %s%%\n' "$$coverage" "$(COVERAGE_MIN)"; \
	else \
		printf 'coverage %s%% must be at least %s%%\n' "$$coverage" "$(COVERAGE_MIN)" >&2; \
		exit 1; \
	fi

lint:
	$(GOLANGCI_LINT) run

lint-install:
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

build: build-pacmand build-pacmanctl
# The PostgreSQL extension is built inside a Docker container.
# See postgresql/pacman_agent/Makefile.docker. Run: make build-pg-extension

PG_EXTENSION_MAKE = $(MAKE) -C $(PG_EXTENSION_DIR) -f Makefile.docker \
	PG_EXTENSION_IMAGE="$(PG_EXTENSION_IMAGE)" \
	PG_CONFIG="$(PG_CONFIG)" \
	OUTPUT_DIR="$(abspath $(PG_EXTENSION_OUTPUT))" \
	PACMAND_BIN="$(abspath $(BIN_DIR)/pacmand)"

build-pg-extension:
	$(PG_EXTENSION_MAKE) build

package-pg-extension: build-pg-extension build-pacmand
	$(PG_EXTENSION_MAKE) package

install-pg-extension: package-pg-extension
	$(PG_EXTENSION_MAKE) install

clean-pg-extension:
	$(PG_EXTENSION_MAKE) clean

rpm: rpm-builder-image
	mkdir -p $(RPM_OUTPUT_DIR)
	$(CONTAINER_RUNTIME) run --rm \
		-e WORKSPACE=/workspace \
		-e OUTPUT_DIR=/out \
		-e RPM_VERSION=$(RPM_VERSION) \
		-e RPM_RELEASE=$(RPM_RELEASE) \
		-e RPM_COMMIT=$(COMMIT) \
		-e SOURCE_DATE_EPOCH=$(RPM_SOURCE_DATE_EPOCH) \
		-v $(CURDIR):/workspace:ro \
		-v $(RPM_OUTPUT_DIR):/out \
		$(RPM_BUILDER_IMAGE) \
		/workspace/packaging/rpm/build-rpm.sh

rpm-builder-image:
	$(CONTAINER_RUNTIME) build -f packaging/rpm/Containerfile -t $(RPM_BUILDER_IMAGE) .

rpm-validate:
	rm -rf $(RPM_VALIDATE_RELEASE1_DIR) $(RPM_VALIDATE_RELEASE2_DIR)
	$(MAKE) rpm RPM_OUTPUT_DIR=$(RPM_VALIDATE_RELEASE1_DIR) RPM_RELEASE=1
	$(MAKE) rpm RPM_OUTPUT_DIR=$(RPM_VALIDATE_RELEASE2_DIR) RPM_RELEASE=2
	CONTAINER_RUNTIME=$(CONTAINER_RUNTIME) RPM_VALIDATION_IMAGE=$(RPM_VALIDATION_IMAGE) \
		./packaging/rpm/validate-install-flow.sh $(RPM_VALIDATE_RELEASE1_DIR) $(RPM_VALIDATE_RELEASE2_DIR)

ansible-validate:
	bash -n deploy/ansible/validate.sh
	./deploy/ansible/validate.sh

build-pacmand:
	mkdir -p $(BIN_DIR)
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/pacmand ./cmd/pacmand

build-pacmanctl:
	mkdir -p $(BIN_DIR)
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/pacmanctl ./cmd/pacmanctl

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(BIN_DIR)
