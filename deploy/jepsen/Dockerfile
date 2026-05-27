FROM golang:1.26.1-alpine AS jepsenctl-builder

WORKDIR /src
COPY go.mod go.sum ./
COPY tools/jepsenctl ./tools/jepsenctl
RUN go build -trimpath -ldflags="-s -w" -o /out/jepsenctl ./tools/jepsenctl

FROM docker:27-cli

RUN apk add --no-cache \
	bash \
	ca-certificates \
	curl \
	docker-cli-compose \
	git \
	iproute2 \
	iptables \
	jq \
	netcat-openbsd \
	openjdk21-jdk \
	openssh-client \
	postgresql-client \
	procps

COPY --from=jepsenctl-builder /out/jepsenctl /usr/local/bin/jepsenctl

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk

RUN curl -fsSL https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein \
		-o /usr/local/bin/lein \
	&& chmod +x /usr/local/bin/lein \
	&& lein version

WORKDIR /workspace
ENTRYPOINT []
