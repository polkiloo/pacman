#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

usage() {
	cat <<'USAGE'
usage: scripts/dev-clean.sh [--all]

Removes local build outputs and generated test artifacts.
Use --all to also remove repo-local Go and lint caches.
USAGE
}

remove_all=false
case "${1:-}" in
"")
	;;
--all)
	remove_all=true
	;;
-h|--help|help)
	usage
	exit 0
	;;
*)
	usage >&2
	exit 2
	;;
esac

rm -rf bin
rm -f coverage.out
rm -f ./*.coverprofile ./*.test profile.cov
rm -rf .cache/golangci-lint

if [ "$remove_all" = true ]; then
	rm -rf .gocache
fi

printf 'local development artifacts removed\n'
