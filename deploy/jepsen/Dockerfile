ARG PACMAN_JEPSEN_GO_BASE_IMAGE=golang:1.26.1-alpine
ARG PACMAN_JEPSEN_DOCKER_BASE_IMAGE=docker:27-cli
FROM ${PACMAN_JEPSEN_GO_BASE_IMAGE} AS jepsenctl-builder

WORKDIR /src
COPY go.mod go.sum ./
COPY tools/jepsenctl ./tools/jepsenctl
RUN go build -trimpath -ldflags="-s -w" -o /out/jepsenctl ./tools/jepsenctl

FROM ${PACMAN_JEPSEN_DOCKER_BASE_IMAGE}

RUN apk add --no-cache \
	bash \
	ca-certificates \
	docker-cli-compose \
	git

COPY --from=jepsenctl-builder /out/jepsenctl /usr/local/bin/jepsenctl

WORKDIR /workspace
ENTRYPOINT []
