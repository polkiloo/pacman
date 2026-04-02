FROM golang:1.26.1-bookworm AS gobuild

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /out/pacmand ./cmd/pacmand

FROM postgres:17-bookworm AS pgext-build

RUN apt-get update && \
	apt-get install -y --no-install-recommends build-essential postgresql-server-dev-17 && \
	rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY postgresql/pacman_agent/ ./

RUN make && make install

FROM postgres:17-bookworm

COPY --from=pgext-build /usr/lib/postgresql/17/lib/pacman_agent.so /usr/lib/postgresql/17/lib/pacman_agent.so
COPY --from=pgext-build /usr/share/postgresql/17/extension/pacman_agent.control /usr/share/postgresql/17/extension/pacman_agent.control
COPY --from=pgext-build /usr/share/postgresql/17/extension/pacman_agent--0.1.0.sql /usr/share/postgresql/17/extension/pacman_agent--0.1.0.sql
COPY --from=gobuild /out/pacmand /usr/local/bin/pacmand
