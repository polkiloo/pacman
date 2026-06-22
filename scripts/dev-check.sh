#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

: "${GO:=go}"
: "${GOCACHE:=$repo_root/.gocache}"
: "${GOLANGCI_LINT_CACHE:=$repo_root/.cache/golangci-lint}"

export GOCACHE GOLANGCI_LINT_CACHE

usage() {
	cat <<'USAGE'
usage: scripts/dev-check.sh [quick|full|fmt|test|lint|build]

quick  run fmt, build, unit tests, and lint (default)
full   run quick plus coverage threshold and OpenAPI codegen checks
fmt    run go fmt and go mod tidy
test   run unit tests
lint   run golangci-lint
build  build pacmand and pacmanctl
USAGE
}

mode=${1:-quick}
case "$mode" in
quick)
	make fmt
	make tidy
	make build
	make test
	make lint
	;;
full)
	"$0" quick
	make coverage-check
	make openapi-codegen-check
	;;
fmt)
	make fmt
	make tidy
	;;
test)
	make test
	;;
lint)
	make lint
	;;
build)
	make build
	;;
-h|--help|help)
	usage
	;;
*)
	usage >&2
	exit 2
	;;
esac
