#!/usr/bin/env sh
set -eu

if [ "${1:-0}" = "0" ] && command -v systemctl >/dev/null 2>&1; then
  systemctl --no-reload disable --now pacmand.service >/dev/null 2>&1 || :
fi
