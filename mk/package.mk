.PHONY: rpm rpm-builder-image rpm-validate ansible-validate

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
	@if ! $(CONTAINER_RUNTIME) image inspect $(RPM_BUILDER_BASE_IMAGE) >/dev/null 2>&1; then \
		attempt=1; \
		while true; do \
			if $(CONTAINER_RUNTIME) pull $(RPM_BUILDER_BASE_IMAGE); then \
				break; \
			fi; \
			if [ "$$attempt" -ge "$(RPM_BUILDER_DOCKER_RETRY_ATTEMPTS)" ]; then \
				exit 1; \
			fi; \
			printf 'pull %s failed on attempt %s/%s\n' "$(RPM_BUILDER_BASE_IMAGE)" "$$attempt" "$(RPM_BUILDER_DOCKER_RETRY_ATTEMPTS)" >&2; \
			attempt=$$((attempt + 1)); \
			sleep "$(RPM_BUILDER_DOCKER_RETRY_DELAY_SECONDS)"; \
		done; \
	fi
	@attempt=1; \
	while true; do \
		if $(CONTAINER_RUNTIME) build \
			--build-arg RPM_BUILDER_BASE_IMAGE=$(RPM_BUILDER_BASE_IMAGE) \
			-f packaging/rpm/Containerfile \
			-t $(RPM_BUILDER_IMAGE) .; then \
			break; \
		fi; \
		if [ "$$attempt" -ge "$(RPM_BUILDER_DOCKER_RETRY_ATTEMPTS)" ]; then \
			exit 1; \
		fi; \
		printf 'build %s failed on attempt %s/%s\n' "$(RPM_BUILDER_IMAGE)" "$$attempt" "$(RPM_BUILDER_DOCKER_RETRY_ATTEMPTS)" >&2; \
		attempt=$$((attempt + 1)); \
		sleep "$(RPM_BUILDER_DOCKER_RETRY_DELAY_SECONDS)"; \
	done

rpm-validate:
	rm -rf $(RPM_VALIDATE_RELEASE1_DIR) $(RPM_VALIDATE_RELEASE2_DIR)
	$(MAKE) rpm RPM_OUTPUT_DIR=$(RPM_VALIDATE_RELEASE1_DIR) RPM_RELEASE=1
	$(MAKE) rpm RPM_OUTPUT_DIR=$(RPM_VALIDATE_RELEASE2_DIR) RPM_RELEASE=2
	CONTAINER_RUNTIME=$(CONTAINER_RUNTIME) RPM_VALIDATION_IMAGE=$(RPM_VALIDATION_IMAGE) \
		./packaging/rpm/validate-install-flow.sh $(RPM_VALIDATE_RELEASE1_DIR) $(RPM_VALIDATE_RELEASE2_DIR)

ansible-validate:
	bash -n deploy/ansible/validate.sh
	./deploy/ansible/validate.sh
