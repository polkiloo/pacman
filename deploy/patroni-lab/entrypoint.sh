#!/bin/sh
set -eu

chmod 0700 /var/lib/postgresql/data
exec patroni "$@"
