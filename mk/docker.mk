.PHONY: docker-build-test-image docker-build-pgext-image docker-build-ansible-install-image

docker-build-test-image:
	@attempt=1; \
	while true; do \
		if $(CONTAINER_RUNTIME) build --progress=$(DOCKER_BUILD_PROGRESS) -f test/docker/pacman-runner.Dockerfile -t $(PACMAN_TEST_IMAGE) .; then \
			break; \
		fi; \
		if [ "$$attempt" -ge "$(DOCKER_BUILD_RETRY_ATTEMPTS)" ]; then \
			exit 1; \
		fi; \
		printf 'build %s failed on attempt %s/%s\n' "$(PACMAN_TEST_IMAGE)" "$$attempt" "$(DOCKER_BUILD_RETRY_ATTEMPTS)" >&2; \
		attempt=$$((attempt + 1)); \
		sleep "$(DOCKER_BUILD_RETRY_DELAY_SECONDS)"; \
	done

docker-build-pgext-image:
	@attempt=1; \
	while true; do \
		if $(CONTAINER_RUNTIME) build --progress=$(DOCKER_BUILD_PROGRESS) -f test/docker/pacman-pgext-postgres.Dockerfile -t $(PACMAN_TEST_PGEXT_IMAGE) .; then \
			break; \
		fi; \
		if [ "$$attempt" -ge "$(DOCKER_BUILD_RETRY_ATTEMPTS)" ]; then \
			exit 1; \
		fi; \
		printf 'build %s failed on attempt %s/%s\n' "$(PACMAN_TEST_PGEXT_IMAGE)" "$$attempt" "$(DOCKER_BUILD_RETRY_ATTEMPTS)" >&2; \
		attempt=$$((attempt + 1)); \
		sleep "$(DOCKER_BUILD_RETRY_DELAY_SECONDS)"; \
	done

docker-build-ansible-install-image:
	@attempt=1; \
	while true; do \
		if $(CONTAINER_RUNTIME) build --progress=$(DOCKER_BUILD_PROGRESS) -f test/docker/pacman-ansible-install.Dockerfile -t $(PACMAN_ANSIBLE_INSTALL_IMAGE) .; then \
			break; \
		fi; \
		if [ "$$attempt" -ge "$(DOCKER_BUILD_RETRY_ATTEMPTS)" ]; then \
			exit 1; \
		fi; \
		printf 'build %s failed on attempt %s/%s\n' "$(PACMAN_ANSIBLE_INSTALL_IMAGE)" "$$attempt" "$(DOCKER_BUILD_RETRY_ATTEMPTS)" >&2; \
		attempt=$$((attempt + 1)); \
		sleep "$(DOCKER_BUILD_RETRY_DELAY_SECONDS)"; \
	done
