FROM rockylinux:9

ARG TARGETARCH

RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
        amd64) RPM_ARCH=x86_64 ;; \
        arm64) RPM_ARCH=aarch64 ;; \
        *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    dnf install -y dnf-plugins-core; \
    dnf config-manager --set-enabled crb; \
    printf '%s\n' \
        '[pacman_test_etcd]' \
        'name=PACMAN test etcd repository' \
        "baseurl=https://yum.oracle.com/repo/OracleLinux/OL9/olcne19/${RPM_ARCH}/" \
        'enabled=1' \
        'gpgcheck=0' \
        > /etc/yum.repos.d/pacman-test-etcd.repo; \
    dnf install -y "https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-${RPM_ARCH}/pgdg-redhat-repo-latest.noarch.rpm"; \
    dnf -qy module disable postgresql; \
    dnf install -y \
        ansible-core \
        bash \
        ca-certificates \
        etcd \
        findutils \
        hostname \
        iproute \
        procps-ng \
        python3 \
        shadow-utils \
        sudo \
        which; \
    dnf clean all; \
    rm -rf /var/cache/dnf

WORKDIR /workspace
CMD ["/bin/sh", "-lc", "sleep infinity"]
