FROM golang:1.26.1 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /out/pacmand ./cmd/pacmand && \
	CGO_ENABLED=0 go build -o /out/pacmanctl ./cmd/pacmanctl

FROM alpine:3.22

RUN apk add --no-cache ca-certificates postgresql17-client

COPY --from=build /out/pacmand /usr/local/bin/pacmand
COPY --from=build /out/pacmanctl /usr/local/bin/pacmanctl

CMD ["/bin/sh", "-lc", "sleep infinity"]
