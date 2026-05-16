.PHONY: docker-build-test-image docker-build-pgext-image docker-build-ansible-install-image

docker-build-test-image:
	docker build --progress=$(DOCKER_BUILD_PROGRESS) -f test/docker/pacman-runner.Dockerfile -t $(PACMAN_TEST_IMAGE) .

docker-build-pgext-image:
	docker build --progress=$(DOCKER_BUILD_PROGRESS) -f test/docker/pacman-pgext-postgres.Dockerfile -t $(PACMAN_TEST_PGEXT_IMAGE) .

docker-build-ansible-install-image:
	docker build --progress=$(DOCKER_BUILD_PROGRESS) -f test/docker/pacman-ansible-install.Dockerfile -t $(PACMAN_ANSIBLE_INSTALL_IMAGE) .
