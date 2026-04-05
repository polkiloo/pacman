package apidoc

import (
	"bytes"
	"testing"
)

func TestOpenAPIYAMLReturnsResolvedDocument(t *testing.T) {
	t.Parallel()

	document, err := OpenAPIYAML()
	if err != nil {
		t.Fatalf("resolve openapi document: %v", err)
	}

	if len(document) == 0 {
		t.Fatal("expected embedded openapi document")
	}

	if !bytes.Contains(document, []byte("openapi:")) {
		t.Fatalf("expected openapi header in document, got %q", string(document))
	}

	if !bytes.Contains(document, []byte("/api/v1/cluster")) {
		t.Fatalf("expected pacman api path in document, got %q", string(document))
	}
}
