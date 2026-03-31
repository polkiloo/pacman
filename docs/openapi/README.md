# OpenAPI Source

`docs/openapi.yaml` is the entrypoint OpenAPI document for docs and contract tests.

For maintainability, it delegates path items and schema groups to external YAML files in this directory using `$ref`. This layout follows `oapi-codegen`'s documented external-reference and import-mapping flow.

The main modules are:

- `paths-probes.yaml`: Patroni-compatible probe endpoints
- `paths-patroni.yaml`: Patroni-compatible monitoring and admin endpoints
- `paths-pacman.yaml`: PACMAN-native `/api/v1/*` endpoints
- `components-meta.yaml`: reusable parameters, responses, and security schemes
- `schemas-patroni.yaml`: Patroni and node-observation schemas
- `schemas-cluster.yaml`: PACMAN cluster, member, maintenance, and diagnostics schemas
- `schemas-operations.yaml`: request, operation, and error schemas
- `schemas-enums.yaml`: shared enum schemas
- `oapi-codegen.yaml`: same-package import mapping for generating from the split module specs

`oapi-codegen` should be pointed at the split module specs in this directory, not at the umbrella `docs/openapi.yaml`, because the umbrella file is only an index of external references.

Validate that the split modules are consumable by `oapi-codegen` with:

```bash
make openapi-codegen-check
```
