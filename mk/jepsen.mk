.PHONY: jepsen-ci-check jepsen-list-cases jepsen-smoke jepsen-nightly jepsen-case jepsen-case-append-smoke-none jepsen-case-append-switchover-switchover jepsen-case-append-failover-kill jepsen-case-append-failover-packet jepsen-case-append-failover-packet-kill jepsen-case-append-failover-primary-dcs-partition jepsen-case-append-failover-primary-replication-partition jepsen-case-append-failover-failover-chain jepsen-case-open-transaction-failover-kill jepsen-case-vip-routing-switchover jepsen-case-append-dcs-quorum-dcs-kill-one jepsen-case-append-dcs-quorum-dcs-lose-majority jepsen-case-append-dcs-quorum-primary-dcs-majority-partition jepsen-case-single-key-register-packet jepsen-case-read-committed-txn-slow-network jepsen-case-serializable-txn-packet-kill jepsen-case-append-failover-repeated-failure jepsen-docker-smoke jepsen-docker-nightly jepsen-docker-case jepsen-docker-case-append-smoke-none jepsen-docker-case-append-switchover-switchover jepsen-docker-case-append-failover-kill jepsen-docker-case-append-failover-packet jepsen-docker-case-append-failover-packet-kill jepsen-docker-case-append-failover-primary-dcs-partition jepsen-docker-case-append-failover-primary-replication-partition jepsen-docker-case-append-failover-failover-chain jepsen-docker-case-open-transaction-failover-kill jepsen-docker-case-vip-routing-switchover jepsen-docker-case-append-dcs-quorum-dcs-kill-one jepsen-docker-case-append-dcs-quorum-dcs-lose-majority jepsen-docker-case-append-dcs-quorum-primary-dcs-majority-partition jepsen-docker-case-single-key-register-packet jepsen-docker-case-read-committed-txn-slow-network jepsen-docker-case-serializable-txn-packet-kill jepsen-docker-case-append-failover-repeated-failure

jepsen-ci-check:
	bash -n $(JEPSEN_CI_SCRIPT)
	tmpdir=$$(mktemp -d); \
		PACMAN_JEPSEN_DIR="$$tmpdir/missing-jepsen" \
		PACMAN_JEPSEN_CI_ARTIFACT_DIR="$$tmpdir/artifacts" \
		$(JEPSEN_CI_SCRIPT) smoke

jepsen-list-cases:
	./jepsen/bin/list-cases

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

jepsen-case-single-key-register-packet:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=single-key-register-packet

jepsen-case-read-committed-txn-slow-network:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=read-committed-txn-slow-network

jepsen-case-serializable-txn-packet-kill:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=serializable-txn-packet-kill

jepsen-case-append-failover-repeated-failure:
	$(MAKE) jepsen-case PACMAN_JEPSEN_CASE=append-failover-repeated-failure

jepsen-docker-smoke:
	$(MAKE) rpm RPM_OUTPUT_DIR=$(PACMAN_ANSIBLE_INSTALL_RPM_DIR)
	$(JEPSEN_DOCKER_SCRIPT) smoke

jepsen-docker-nightly:
	$(MAKE) rpm RPM_OUTPUT_DIR=$(PACMAN_ANSIBLE_INSTALL_RPM_DIR)
	$(JEPSEN_DOCKER_SCRIPT) nightly

jepsen-docker-case:
	$(MAKE) rpm RPM_OUTPUT_DIR=$(PACMAN_ANSIBLE_INSTALL_RPM_DIR)
	$(JEPSEN_DOCKER_SCRIPT) case $(PACMAN_JEPSEN_CASE)

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

jepsen-docker-case-single-key-register-packet:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=single-key-register-packet

jepsen-docker-case-read-committed-txn-slow-network:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=read-committed-txn-slow-network

jepsen-docker-case-serializable-txn-packet-kill:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=serializable-txn-packet-kill

jepsen-docker-case-append-failover-repeated-failure:
	$(MAKE) jepsen-docker-case PACMAN_JEPSEN_CASE=append-failover-repeated-failure
