#!/usr/bin/env bash
set -euo pipefail

release1_dir="${1:?release1 rpm directory is required}"
release2_dir="${2:?release2 rpm directory is required}"
container_runtime="${CONTAINER_RUNTIME:-docker}"
validation_image="${RPM_VALIDATION_IMAGE:-rockylinux:9}"
container_name="pacman-rpm-validate-$$"

cleanup() {
  "${container_runtime}" rm -f "${container_name}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

main_pkg_path() {
  local dir="$1"
  find "${dir}" -maxdepth 1 -type f -name 'pacman-[0-9]*.rpm' \
    ! -name '*.src.rpm' \
    ! -name 'pacman-postgresql17-agent-*' | sort | head -n1
}

pgext_pkg_path() {
  local dir="$1"
  find "${dir}" -maxdepth 1 -type f -name 'pacman-postgresql17-agent-*.rpm' | sort | head -n1
}

release1_main="$(main_pkg_path "${release1_dir}")"
release1_pgext="$(pgext_pkg_path "${release1_dir}")"
release2_main="$(main_pkg_path "${release2_dir}")"
release2_pgext="$(pgext_pkg_path "${release2_dir}")"

if [[ -z "${release1_main}" || -z "${release1_pgext}" || -z "${release2_main}" || -z "${release2_pgext}" ]]; then
  echo "failed to discover expected RPM artifacts" >&2
  exit 1
fi

"${container_runtime}" run -d --name "${container_name}" \
  -v "${release1_dir}:/rpms/release1:ro" \
  -v "${release2_dir}:/rpms/release2:ro" \
  "${validation_image}" sleep infinity >/dev/null

"${container_runtime}" exec "${container_name}" bash -lc '
set -euo pipefail
arch="$(uname -m)"

dnf install -y dnf-plugins-core
dnf config-manager --set-enabled crb
dnf install -y "https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-${arch}/pgdg-redhat-repo-latest.noarch.rpm"
dnf -qy module disable postgresql

main_release1="$(find /rpms/release1 -maxdepth 1 -type f -name "pacman-[0-9]*.${arch}.rpm" ! -name "pacman-postgresql17-agent-*" | sort | head -n1)"
pgext_release1="$(find /rpms/release1 -maxdepth 1 -type f -name "pacman-postgresql17-agent-*.${arch}.rpm" | sort | head -n1)"
main_release2="$(find /rpms/release2 -maxdepth 1 -type f -name "pacman-[0-9]*.${arch}.rpm" ! -name "pacman-postgresql17-agent-*" | sort | head -n1)"
pgext_release2="$(find /rpms/release2 -maxdepth 1 -type f -name "pacman-postgresql17-agent-*.${arch}.rpm" | sort | head -n1)"
expected_main_release1="$(rpm -qp --qf "%{VERSION}-%{RELEASE}\n" "${main_release1}")"
expected_pgext_release1="$(rpm -qp --qf "%{VERSION}-%{RELEASE}\n" "${pgext_release1}")"
expected_main_release2="$(rpm -qp --qf "%{VERSION}-%{RELEASE}\n" "${main_release2}")"
expected_pgext_release2="$(rpm -qp --qf "%{VERSION}-%{RELEASE}\n" "${pgext_release2}")"

dnf install -y "${main_release1}" "${pgext_release1}"
rpm -q pacman pacman-postgresql17-agent
test "$(rpm -q --qf "%{VERSION}-%{RELEASE}\n" pacman)" = "${expected_main_release1}"
test "$(rpm -q --qf "%{VERSION}-%{RELEASE}\n" pacman-postgresql17-agent)" = "${expected_pgext_release1}"
getent passwd pacman >/dev/null
getent group pacman >/dev/null
test -f /usr/lib/systemd/system/pacmand.service
test -f /etc/pacman/pacmand.yaml
test -f /etc/pacman/admin-token
test -s /etc/pacman/admin-token
test "$(stat -c "%a" /etc/pacman/admin-token)" = "640"
test "$(stat -c "%G" /etc/pacman/admin-token)" = "pacman"
test -d /var/lib/pacman
test -d /var/lib/pacman/raft
test -d /var/log/pacman
test "$(stat -c "%U:%G" /var/lib/pacman)" = "pacman:pacman"
test "$(stat -c "%U:%G" /var/log/pacman)" = "pacman:pacman"
test -f /usr/pgsql-17/lib/pacman_agent.so
test -f /usr/pgsql-17/share/extension/pacman_agent.control
test -f /usr/pgsql-17/share/extension/pacman_agent--0.1.0.sql

dnf upgrade -y "${main_release2}" "${pgext_release2}"
test "$(rpm -q --qf "%{VERSION}-%{RELEASE}\n" pacman)" = "${expected_main_release2}"
test "$(rpm -q --qf "%{VERSION}-%{RELEASE}\n" pacman-postgresql17-agent)" = "${expected_pgext_release2}"
test -s /etc/pacman/admin-token

dnf downgrade -y "${main_release1}" "${pgext_release1}"
test "$(rpm -q --qf "%{VERSION}-%{RELEASE}\n" pacman)" = "${expected_main_release1}"
test "$(rpm -q --qf "%{VERSION}-%{RELEASE}\n" pacman-postgresql17-agent)" = "${expected_pgext_release1}"

dnf remove -y pacman-postgresql17-agent pacman
! rpm -q pacman
! rpm -q pacman-postgresql17-agent
'
