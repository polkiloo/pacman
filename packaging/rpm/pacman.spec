Name:           pacman
Version:        %{?version}%{!?version:0.1.0}
Release:        %{?release}%{!?release:1}%{?dist}
Summary:        Postgres Autonomous Cluster Manager
License:        Apache-2.0
URL:            https://github.com/polkiloo/pacman
Source0:        %{name}-%{version}.tar.gz

%global debug_package %{nil}

%global commit %{?commit}%{!?commit:none}
%global pg17_prefix /usr/pgsql-17

BuildRequires:  gcc
BuildRequires:  golang
BuildRequires:  make
BuildRequires:  postgresql17-devel
BuildRequires:  systemd-rpm-macros

Requires:       systemd
Requires(pre):  shadow-utils
Requires(post): coreutils
Requires(post): systemd
Requires(postun): systemd
Requires(preun): systemd
Suggests:       etcd
Suggests:       postgresql17-server

%description
PACMAN is a PostgreSQL HA control plane that provides process-mode node agents,
cluster coordination, failover, switchover, and explicit rejoin workflows.

%package -n pacman-postgresql17-agent
Summary:        PostgreSQL 17 PACMAN background-worker extension
Requires:       pacman = %{version}-%{release}
Requires:       postgresql17-server

%description -n pacman-postgresql17-agent
PostgreSQL 17 extension assets for PACMAN background-worker mode.

%pre
getent group pacman >/dev/null || groupadd -r pacman
getent passwd pacman >/dev/null || \
    useradd -r -g pacman -d %{_localstatedir}/lib/pacman -s /sbin/nologin \
        -c "PACMAN service account" pacman

%post
systemd-tmpfiles --create %{_tmpfilesdir}/pacman.conf >/dev/null 2>&1 || :
if [ ! -s %{_sysconfdir}/pacman/admin-token ]; then
    umask 0037
    head -c 32 /dev/urandom | base64 > %{_sysconfdir}/pacman/admin-token
fi
chown root:pacman %{_sysconfdir}/pacman/admin-token >/dev/null 2>&1 || :
chmod 0640 %{_sysconfdir}/pacman/admin-token >/dev/null 2>&1 || :
%systemd_post pacmand.service

%preun
%systemd_preun pacmand.service

%postun
%systemd_postun pacmand.service

%prep
%autosetup

%build
export GOFLAGS="-buildvcs=false -mod=readonly -trimpath"
export CGO_ENABLED=0
export VERSION="%{version}"
export COMMIT="%{commit}"
export BUILD_DATE="$(date -u -d "@${SOURCE_DATE_EPOCH}" +%Y-%m-%dT%H:%M:%SZ)"

mkdir -p build
go build \
    -ldflags "-X github.com/polkiloo/pacman/internal/version.Version=${VERSION} -X github.com/polkiloo/pacman/internal/version.Commit=${COMMIT} -X github.com/polkiloo/pacman/internal/version.BuildDate=${BUILD_DATE}" \
    -o build/pacmand \
    ./cmd/pacmand
go build \
    -ldflags "-X github.com/polkiloo/pacman/internal/version.Version=${VERSION} -X github.com/polkiloo/pacman/internal/version.Commit=${COMMIT} -X github.com/polkiloo/pacman/internal/version.BuildDate=${BUILD_DATE}" \
    -o build/pacmanctl \
    ./cmd/pacmanctl
make -C postgresql/pacman_agent PG_CONFIG=%{pg17_prefix}/bin/pg_config

%install
install -Dpm0755 build/pacmand %{buildroot}%{_bindir}/pacmand
install -Dpm0755 build/pacmanctl %{buildroot}%{_bindir}/pacmanctl
install -Dpm0644 packaging/rpm/systemd/pacmand.service %{buildroot}%{_unitdir}/pacmand.service
install -Dpm0644 packaging/rpm/sysconfig/pacmand %{buildroot}%{_sysconfdir}/sysconfig/pacmand
install -Dpm0644 packaging/rpm/config/pacmand.yaml %{buildroot}%{_sysconfdir}/pacman/pacmand.yaml
install -Dpm0644 packaging/rpm/tmpfiles/pacman.conf %{buildroot}%{_tmpfilesdir}/pacman.conf
install -d %{buildroot}%{_localstatedir}/lib/pacman
install -d %{buildroot}%{_localstatedir}/lib/pacman/raft
install -d %{buildroot}%{_localstatedir}/log/pacman
make -C postgresql/pacman_agent PG_CONFIG=%{pg17_prefix}/bin/pg_config DESTDIR=%{buildroot} install

%files
%license LICENSE
%doc packaging/rpm/README.md
%{_bindir}/pacmand
%{_bindir}/pacmanctl
%{_unitdir}/pacmand.service
%{_tmpfilesdir}/pacman.conf
%config(noreplace) %{_sysconfdir}/sysconfig/pacmand
%config(noreplace) %{_sysconfdir}/pacman/pacmand.yaml
%ghost %config(noreplace) %attr(0640,root,pacman) %{_sysconfdir}/pacman/admin-token
%dir %attr(0750,pacman,pacman) %{_localstatedir}/lib/pacman
%dir %attr(0750,pacman,pacman) %{_localstatedir}/lib/pacman/raft
%dir %attr(0750,pacman,pacman) %{_localstatedir}/log/pacman

%files -n pacman-postgresql17-agent
%license LICENSE
%doc packaging/rpm/README.md
%{pg17_prefix}/lib/pacman_agent.so
%{pg17_prefix}/lib/bitcode/pacman_agent.index.bc
%{pg17_prefix}/lib/bitcode/pacman_agent/pacman_agent.bc
%{pg17_prefix}/share/extension/pacman_agent.control
%{pg17_prefix}/share/extension/pacman_agent--0.1.0.sql

%changelog
* Thu Apr 16 2026 PACMAN Maintainers <maintainers@pacman.local> - %{version}-%{release}
- Initial RPM packaging scaffold
