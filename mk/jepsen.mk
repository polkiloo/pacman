.PHONY: jepsen-ci-check jepsen-list-cases jepsen-check-case-targets jepsen-smoke jepsen-nightly jepsen-case jepsen-case-append-smoke-none jepsen-case-append-switchover-switchover jepsen-case-append-failover-kill jepsen-case-append-failover-packet jepsen-case-append-failover-packet-kill jepsen-case-append-failover-primary-dcs-partition jepsen-case-append-failover-primary-replication-partition jepsen-case-append-failover-failover-chain jepsen-case-append-reinit-reinit-replica jepsen-case-append-reinit-reinit-replica-kill-target jepsen-case-append-reinit-reinit-replica-kill-source jepsen-case-append-reinit-reinit-replica-dcs-partition-target jepsen-case-append-reinit-reinit-replica-dcs-partition-primary jepsen-case-append-reinit-reinit-replica-repeated jepsen-case-append-reinit-reinit-replica-with-lag jepsen-case-append-reinit-reinit-replica-walg-fetch-failure jepsen-case-append-reinit-reinit-replica-concurrent-request jepsen-case-append-reinit-reinit-replica-after-failover jepsen-case-open-transaction-failover-kill jepsen-case-vip-routing-switchover jepsen-case-append-dcs-quorum-dcs-kill-one jepsen-case-append-dcs-quorum-dcs-lose-majority jepsen-case-append-dcs-quorum-primary-dcs-majority-partition jepsen-case-append-dcs-quorum-dcs-full-restart jepsen-case-append-dcs-quorum-dcs-slow-network jepsen-case-single-key-register-packet jepsen-case-read-committed-txn-slow-network jepsen-case-serializable-txn-packet-kill jepsen-case-append-failover-repeated-failure jepsen-case-append-sync-kill jepsen-case-append-sync-sync-standby-kill jepsen-case-append-sync-two-none jepsen-case-append-strict-sync-no-standby jepsen-case-append-max-lag-lagging-replica-failover jepsen-case-append-check-timeline-stale-timeline-failover jepsen-docker-smoke jepsen-docker-nightly jepsen-docker-case jepsen-docker-stability-append-smoke-none jepsen-docker-case-append-smoke-none jepsen-docker-case-append-switchover-switchover jepsen-docker-case-append-failover-kill jepsen-docker-case-append-failover-packet jepsen-docker-case-append-failover-packet-kill jepsen-docker-case-append-failover-primary-dcs-partition jepsen-docker-case-append-failover-primary-replication-partition jepsen-docker-case-append-failover-failover-chain jepsen-docker-case-append-reinit-reinit-replica jepsen-docker-case-append-reinit-reinit-replica-kill-target jepsen-docker-case-append-reinit-reinit-replica-kill-source jepsen-docker-case-append-reinit-reinit-replica-dcs-partition-target jepsen-docker-case-append-reinit-reinit-replica-dcs-partition-primary jepsen-docker-case-append-reinit-reinit-replica-repeated jepsen-docker-case-append-reinit-reinit-replica-with-lag jepsen-docker-case-append-reinit-reinit-replica-walg-fetch-failure jepsen-docker-case-append-reinit-reinit-replica-concurrent-request jepsen-docker-case-append-reinit-reinit-replica-after-failover jepsen-docker-case-open-transaction-failover-kill jepsen-docker-case-vip-routing-switchover jepsen-docker-case-append-dcs-quorum-dcs-kill-one jepsen-docker-case-append-dcs-quorum-dcs-lose-majority jepsen-docker-case-append-dcs-quorum-primary-dcs-majority-partition jepsen-docker-case-append-dcs-quorum-dcs-full-restart jepsen-docker-case-append-dcs-quorum-dcs-slow-network jepsen-docker-case-single-key-register-packet jepsen-docker-case-read-committed-txn-slow-network jepsen-docker-case-serializable-txn-packet-kill jepsen-docker-case-append-failover-repeated-failure jepsen-docker-case-append-sync-kill jepsen-docker-case-append-sync-sync-standby-kill jepsen-docker-case-append-sync-two-none jepsen-docker-case-append-strict-sync-no-standby jepsen-docker-case-append-max-lag-lagging-replica-failover jepsen-docker-case-append-check-timeline-stale-timeline-failover

PACMAN_JEPSEN_STABILITY_RUNS ?= 3

jepsen-ci-check:
	go run ./tools/jepsenctl cases validate
	tmpdir=$$(mktemp -d); \
		PACMAN_JEPSEN_DIR="$$tmpdir/missing-jepsen" \
		PACMAN_JEPSEN_CI_ARTIFACT_DIR="$$tmpdir/artifacts" \
		$(JEPSEN_CI_SCRIPT) smoke

jepsen-list-cases:
	go run ./tools/jepsenctl cases list

jepsen-check-case-targets:
	go run ./tools/jepsenctl cases validate

jepsen-smoke:
	$(MAKE) rpm RPM_OUTPUT_DIR=$(PACMAN_ANSIBLE_INSTALL_RPM_DIR)
	$(JEPSEN_CI_SCRIPT) smoke

jepsen-nightly:
	$(MAKE) rpm RPM_OUTPUT_DIR=$(PACMAN_ANSIBLE_INSTALL_RPM_DIR)
	$(JEPSEN_CI_SCRIPT) nightly

jepsen-case:
	$(MAKE) rpm RPM_OUTPUT_DIR=$(PACMAN_ANSIBLE_INSTALL_RPM_DIR)
	$(JEPSEN_CI_SCRIPT) case $(PACMAN_JEPSEN_CASE)

jepsen-case-append-smoke-none:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-smoke-none

jepsen-case-append-switchover-switchover:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-switchover-switchover

jepsen-case-append-failover-kill:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-failover-kill

jepsen-case-append-failover-packet:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-failover-packet

jepsen-case-append-failover-packet-kill:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-failover-packet-kill

jepsen-case-append-failover-primary-dcs-partition:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-failover-primary-dcs-partition

jepsen-case-append-failover-primary-replication-partition:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-failover-primary-replication-partition

jepsen-case-append-failover-failover-chain:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-failover-failover-chain

jepsen-case-append-reinit-reinit-replica:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica

jepsen-case-append-reinit-reinit-replica-kill-target:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-kill-target

jepsen-case-append-reinit-reinit-replica-kill-source:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-kill-source

jepsen-case-append-reinit-reinit-replica-dcs-partition-target:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-dcs-partition-target

jepsen-case-append-reinit-reinit-replica-dcs-partition-primary:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-dcs-partition-primary

jepsen-case-append-reinit-reinit-replica-repeated:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-repeated

jepsen-case-append-reinit-reinit-replica-with-lag:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-with-lag

jepsen-case-append-reinit-reinit-replica-walg-fetch-failure:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-walg-fetch-failure

jepsen-case-append-reinit-reinit-replica-concurrent-request:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-concurrent-request

jepsen-case-append-reinit-reinit-replica-after-failover:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-after-failover

jepsen-case-open-transaction-failover-kill:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=open-transaction-failover-kill

jepsen-case-vip-routing-switchover:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=vip-routing-switchover

jepsen-case-append-dcs-quorum-dcs-kill-one:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-dcs-quorum-dcs-kill-one

jepsen-case-append-dcs-quorum-dcs-lose-majority:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-dcs-quorum-dcs-lose-majority

jepsen-case-append-dcs-quorum-primary-dcs-majority-partition:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-dcs-quorum-primary-dcs-majority-partition

jepsen-case-append-dcs-quorum-dcs-full-restart:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-dcs-quorum-dcs-full-restart

jepsen-case-append-dcs-quorum-dcs-slow-network:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-dcs-quorum-dcs-slow-network

jepsen-case-single-key-register-packet:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=single-key-register-packet

jepsen-case-read-committed-txn-slow-network:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=read-committed-txn-slow-network

jepsen-case-serializable-txn-packet-kill:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=serializable-txn-packet-kill

jepsen-case-append-failover-repeated-failure:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-failover-repeated-failure

jepsen-case-append-sync-kill:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-sync-kill

jepsen-case-append-sync-sync-standby-kill:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-sync-sync-standby-kill

jepsen-case-append-sync-two-none:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-sync-two-none

jepsen-case-append-strict-sync-no-standby:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-strict-sync-no-standby

jepsen-case-append-max-lag-lagging-replica-failover:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-max-lag-lagging-replica-failover

jepsen-case-append-check-timeline-stale-timeline-failover:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-check-timeline-stale-timeline-failover

jepsen-docker-smoke:
	$(MAKE) rpm RPM_OUTPUT_DIR=$(PACMAN_ANSIBLE_INSTALL_RPM_DIR)
	$(JEPSEN_DOCKER_SCRIPT) smoke

jepsen-docker-nightly:
	$(MAKE) rpm RPM_OUTPUT_DIR=$(PACMAN_ANSIBLE_INSTALL_RPM_DIR)
	$(JEPSEN_DOCKER_SCRIPT) nightly

jepsen-docker-case:
	$(MAKE) rpm RPM_OUTPUT_DIR=$(PACMAN_ANSIBLE_INSTALL_RPM_DIR)
	$(JEPSEN_DOCKER_SCRIPT) case $(PACMAN_JEPSEN_CASE)

jepsen-docker-stability-append-smoke-none:
	$(MAKE) rpm RPM_OUTPUT_DIR=$(PACMAN_ANSIBLE_INSTALL_RPM_DIR)
	@i=1; while [ $$i -le "$(PACMAN_JEPSEN_STABILITY_RUNS)" ]; do \
		echo "==> append-smoke:none stability run $$i/$(PACMAN_JEPSEN_STABILITY_RUNS)"; \
		$(JEPSEN_DOCKER_SCRIPT) case append-smoke-none || exit $$?; \
		i=$$((i + 1)); \
	done

jepsen-docker-case-append-smoke-none:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-smoke-none

jepsen-docker-case-append-switchover-switchover:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-switchover-switchover

jepsen-docker-case-append-failover-kill:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-failover-kill

jepsen-docker-case-append-failover-packet:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-failover-packet

jepsen-docker-case-append-failover-packet-kill:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-failover-packet-kill

jepsen-docker-case-append-failover-primary-dcs-partition:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-failover-primary-dcs-partition

jepsen-docker-case-append-failover-primary-replication-partition:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-failover-primary-replication-partition

jepsen-docker-case-append-failover-failover-chain:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-failover-failover-chain

jepsen-docker-case-append-reinit-reinit-replica:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica

jepsen-docker-case-append-reinit-reinit-replica-kill-target:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-kill-target

jepsen-docker-case-append-reinit-reinit-replica-kill-source:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-kill-source

jepsen-docker-case-append-reinit-reinit-replica-dcs-partition-target:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-dcs-partition-target

jepsen-docker-case-append-reinit-reinit-replica-dcs-partition-primary:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-dcs-partition-primary

jepsen-docker-case-append-reinit-reinit-replica-repeated:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-repeated

jepsen-docker-case-append-reinit-reinit-replica-with-lag:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-with-lag

jepsen-docker-case-append-reinit-reinit-replica-walg-fetch-failure:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-walg-fetch-failure

jepsen-docker-case-append-reinit-reinit-replica-concurrent-request:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-concurrent-request

jepsen-docker-case-append-reinit-reinit-replica-after-failover:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-reinit-reinit-replica-after-failover

jepsen-docker-case-open-transaction-failover-kill:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=open-transaction-failover-kill

jepsen-docker-case-vip-routing-switchover:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=vip-routing-switchover

jepsen-docker-case-append-dcs-quorum-dcs-kill-one:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-dcs-quorum-dcs-kill-one

jepsen-docker-case-append-dcs-quorum-dcs-lose-majority:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-dcs-quorum-dcs-lose-majority

jepsen-docker-case-append-dcs-quorum-primary-dcs-majority-partition:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-dcs-quorum-primary-dcs-majority-partition

jepsen-docker-case-append-dcs-quorum-dcs-full-restart:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-dcs-quorum-dcs-full-restart

jepsen-docker-case-append-dcs-quorum-dcs-slow-network:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-dcs-quorum-dcs-slow-network

jepsen-docker-case-single-key-register-packet:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=single-key-register-packet

jepsen-docker-case-read-committed-txn-slow-network:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=read-committed-txn-slow-network

jepsen-docker-case-serializable-txn-packet-kill:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=serializable-txn-packet-kill

jepsen-docker-case-append-failover-repeated-failure:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-failover-repeated-failure

jepsen-docker-case-append-sync-kill:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-sync-kill

jepsen-docker-case-append-sync-sync-standby-kill:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-sync-sync-standby-kill

jepsen-docker-case-append-sync-two-none:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-sync-two-none

jepsen-docker-case-append-strict-sync-no-standby:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-strict-sync-no-standby

jepsen-docker-case-append-max-lag-lagging-replica-failover:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-max-lag-lagging-replica-failover

jepsen-docker-case-append-check-timeline-stale-timeline-failover:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-check-timeline-stale-timeline-failover
