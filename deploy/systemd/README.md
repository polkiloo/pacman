# Systemd Deployment Assets

This directory contains canonical host-side `systemd` assets for PACMAN
process-mode deployments outside the RPM payload.

Use these files when you need to:

- review the packaged `pacmand` service without unpacking the RPM
- vendor the unit into another packaging flow
- bootstrap a lab or image build that installs binaries without the RPM

Files:

- `pacmand.service`
  canonical long-running PACMAN service unit
- `pacmand.sysconfig.example`
  example environment file consumed by the service unit

The service unit deliberately keeps PACMAN state under `/var/lib/pacman` and
expects the node config at `/etc/pacman/pacmand.yaml`.

Persistent paths to preserve across upgrades:

- `/etc/pacman/`
- `/var/lib/pacman/`
- `/var/lib/pacman/raft/` for embedded Raft deployments
- `/var/log/pacman/`
