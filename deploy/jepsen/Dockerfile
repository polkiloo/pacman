FROM golang:1.26.1-alpine AS jepsenctl-builder

WORKDIR /src
COPY go.mod go.sum ./
COPY tools/jepsenctl ./tools/jepsenctl
RUN go build -trimpath -ldflags="-s -w" -o /out/jepsenctl ./tools/jepsenctl

FROM docker:27-cli

RUN apk add --no-cache \
	ca-certificates \
	docker-cli-compose \
	git

COPY --from=jepsenctl-builder /out/jepsenctl /usr/local/bin/jepsenctl

WORKDIR /workspace
ENTRYPOINT []
