#!/usr/bin/env sh
set -eu

getent group pacman >/dev/null 2>&1 || groupadd -r pacman
getent passwd pacman >/dev/null 2>&1 || \
  useradd -r -g pacman -d /var/lib/pacman -s /sbin/nologin \
    -c "PACMAN service account" pacman
