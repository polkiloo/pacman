#!/usr/bin/env bash
set -euo pipefail

workspace="${WORKSPACE:-/workspace}"
output_dir="${OUTPUT_DIR:-/out}"
rpm_version="${RPM_VERSION:-0.1.0}"
rpm_release="${RPM_RELEASE:-1}"
rpm_commit="${RPM_COMMIT:-none}"
source_date_epoch="${SOURCE_DATE_EPOCH:-$(date +%s)}"

topdir="$(mktemp -d /tmp/pacman-rpmbuild.XXXXXX)"
trap 'rm -rf "${topdir}"' EXIT

mkdir -p \
  "${topdir}/BUILD" \
  "${topdir}/BUILDROOT" \
  "${topdir}/RPMS" \
  "${topdir}/SOURCES" \
  "${topdir}/SPECS" \
  "${topdir}/SRPMS" \
  "${output_dir}"

source_tarball="${topdir}/SOURCES/pacman-${rpm_version}.tar.gz"

tar \
  --sort=name \
  --mtime="@${source_date_epoch}" \
  --owner=0 \
  --group=0 \
  --numeric-owner \
  --exclude=.git \
  --exclude=.github \
  --exclude=bin \
  --exclude=coverage.out \
  --transform="s,^\\.,pacman-${rpm_version}," \
  -czf "${source_tarball}" \
  -C "${workspace}" \
  .

cp "${workspace}/packaging/rpm/pacman.spec" "${topdir}/SPECS/pacman.spec"

export SOURCE_DATE_EPOCH="${source_date_epoch}"

rpmbuild \
  --define "_topdir ${topdir}" \
  --define "version ${rpm_version}" \
  --define "release ${rpm_release}" \
  --define "commit ${rpm_commit}" \
  -ba "${topdir}/SPECS/pacman.spec"

find "${topdir}/RPMS" "${topdir}/SRPMS" -type f -name '*.rpm' -exec cp {} "${output_dir}/" \;
