package apidoc

import (
	"embed"

	contractapi "github.com/polkiloo/pacman/internal/api"
)

// embeddedSpec holds the published PACMAN OpenAPI contract and its split
// fragments so pacmand can serve the resolved document from the binary.
//
//go:embed openapi.yaml openapi/*.yaml
var embeddedSpec embed.FS

// OpenAPIYAML returns the resolved repository OpenAPI document as YAML.
func OpenAPIYAML() ([]byte, error) {
	return contractapi.ResolveDocumentFS(embeddedSpec, "openapi.yaml")
}
