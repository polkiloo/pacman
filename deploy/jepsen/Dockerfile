FROM docker:27-cli

RUN apk add --no-cache \
	bash \
	ca-certificates \
	curl \
	docker-cli-compose \
	git \
	go \
	iproute2 \
	iptables \
	jq \
	netcat-openbsd \
	openjdk21-jdk \
	openssh-client \
	postgresql-client \
	procps

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk

RUN curl -fsSL https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein \
		-o /usr/local/bin/lein \
	&& chmod +x /usr/local/bin/lein \
	&& lein version

WORKDIR /workspace
ENTRYPOINT []
