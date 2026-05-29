#!/usr/bin/env sh
set -eu

systemd-tmpfiles --create /usr/lib/tmpfiles.d/pacman.conf >/dev/null 2>&1 || :
if [ ! -s /etc/pacman/admin-token ]; then
  umask 0037
  head -c 32 /dev/urandom | base64 > /etc/pacman/admin-token
fi
chown root:pacman /etc/pacman/admin-token >/dev/null 2>&1 || :
chmod 0640 /etc/pacman/admin-token >/dev/null 2>&1 || :

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload >/dev/null 2>&1 || :
  systemctl preset pacmand.service >/dev/null 2>&1 || :
fi
