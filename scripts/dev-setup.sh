#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

: "${GO:=go}"
: "${GOCACHE:=$repo_root/.gocache}"
: "${GOLANGCI_LINT_CACHE:=$repo_root/.cache/golangci-lint}"
: "${GOLANGCI_LINT:=$(GOBIN=$($GO env GOBIN); if [ -n "$GOBIN" ]; then printf '%s/golangci-lint' "$GOBIN"; else printf '%s/bin/golangci-lint' "$($GO env GOPATH)"; fi)}"

export GOCACHE GOLANGCI_LINT_CACHE GOLANGCI_LINT

mkdir -p "$GOCACHE" "$GOLANGCI_LINT_CACHE" "$repo_root/bin"

printf 'downloading Go modules\n'
"$GO" mod download

if [ ! -x "$GOLANGCI_LINT" ]; then
	printf 'installing golangci-lint into %s\n' "$(dirname -- "$GOLANGCI_LINT")"
	make lint-install
else
	printf 'golangci-lint already available at %s\n' "$GOLANGCI_LINT"
fi

printf 'local development environment is ready\n'
