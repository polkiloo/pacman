//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	contractapi "github.com/polkiloo/pacman/internal/api"
	"github.com/polkiloo/pacman/test/testenv"
)

func startPatroniCompatibilityFixture(t *testing.T, env *testenv.Environment, name, profile string) *testenv.Service {
	t.Helper()

	return env.StartService(t, testenv.ServiceConfig{
		Name:         name,
		Image:        "python:3.12-alpine",
		Env:          map[string]string{"PATRONI_FIXTURE_PROFILE": profile},
		ExposedPorts: []string{"8008/tcp"},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            strings.NewReader(patroniCompatibilityFixtureServerSource),
				ContainerFilePath: "/fixture/server.py",
				FileMode:          0o755,
			},
		},
		Cmd: []string{"python", "-u", "/fixture/server.py"},
		WaitStrategy: wait.ForHTTP("/patroni").
			WithPort("8008/tcp").
			WithStartupTimeout(60 * time.Second),
	})
}

func loadContractDocument(t *testing.T) contractapi.Document {
	t.Helper()

	document, err := contractapi.LoadRepositoryDocument()
	if err != nil {
		t.Fatalf("load repository openapi document: %v", err)
	}

	return document
}

func serviceBaseURL(t *testing.T, service *testenv.Service) string {
	t.Helper()

	return "http://" + service.Address(t, "8008")
}

func performHTTPRequest(t *testing.T, method, rawURL string, body []byte, headers map[string]string) *http.Response {
	t.Helper()

	request, err := http.NewRequest(method, rawURL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request %s %s: %v", method, rawURL, err)
	}

	for key, value := range headers {
		request.Header.Set(key, value)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("perform request %s %s: %v", method, rawURL, err)
	}

	return response
}

func requireResponseMatchesContract(t *testing.T, document contractapi.Document, path, method string, response *http.Response, body []byte) {
	t.Helper()

	operation, err := document.Operation(path, method)
	if err != nil {
		t.Fatalf("resolve %s %s operation: %v", method, path, err)
	}

	contractResponse, ok := operation.Response(strconv.Itoa(response.StatusCode))
	if !ok {
		t.Fatalf("%s %s missing response %d in contract", method, path, response.StatusCode)
	}

	if len(contractResponse.Content) == 0 {
		return
	}

	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("parse response content type for %s %s: %v", method, path, err)
	}

	mediaType, ok := contractResponse.Content[contentType]
	if !ok {
		t.Fatalf("%s %s missing contract content type %s", method, path, contentType)
	}

	if mediaType.Schema == nil {
		return
	}

	schema, err := document.ResolveSchema(mediaType.Schema)
	if err != nil {
		t.Fatalf("resolve response schema for %s %s: %v", method, path, err)
	}

	switch contentType {
	case "application/json":
		var value any
		if err := json.Unmarshal(body, &value); err != nil {
			t.Fatalf("decode json body for %s %s: %v", method, path, err)
		}
		requireSchemaAllowsValue(t, document, schema, value)
	case "application/yaml":
		requireSchemaAllowsValue(t, document, schema, strings.TrimSpace(string(body)))
	case "text/plain":
		requireSchemaAllowsValue(t, document, schema, strings.TrimSpace(string(body)))
	default:
		t.Fatalf("unsupported content type %s for %s %s", contentType, method, path)
	}
}

func requireRequestMatchesContract(t *testing.T, document contractapi.Document, path, method, contentType string, body []byte) {
	t.Helper()

	operation, err := document.Operation(path, method)
	if err != nil {
		t.Fatalf("resolve %s %s operation: %v", method, path, err)
	}

	schema, err := document.ResolveRequestSchema(operation, contentType)
	if err != nil {
		t.Fatalf("resolve request schema for %s %s: %v", method, path, err)
	}

	if schema == nil {
		t.Fatalf("%s %s has no request schema for %s", method, path, contentType)
	}

	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		t.Fatalf("decode request body for %s %s: %v", method, path, err)
	}

	requireSchemaAllowsValue(t, document, schema, value)
}

func requireSchemaAllowsValue(t *testing.T, document contractapi.Document, schema *contractapi.Schema, value any) {
	t.Helper()

	resolved, err := document.ResolveSchema(schema)
	if err != nil {
		t.Fatalf("resolve nested schema: %v", err)
	}

	if resolved == nil {
		t.Fatal("expected non-nil schema")
	}

	if len(resolved.Enum) > 0 {
		text, ok := value.(string)
		if !ok {
			t.Fatalf("expected enum string value, got %T", value)
		}

		for _, candidate := range resolved.Enum {
			if text == candidate {
				goto typecheck
			}
		}

		t.Fatalf("value %q is not allowed by enum %v", text, resolved.Enum)
	}

typecheck:
	switch resolved.Type {
	case "object":
		objectValue, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("expected object value, got %T", value)
		}

		for _, required := range resolved.Required {
			if _, ok := objectValue[required]; !ok {
				t.Fatalf("missing required property %q in %#v", required, objectValue)
			}
		}

		for key, propertyValue := range objectValue {
			propertySchema, ok := resolved.Property(key)
			if ok {
				requireSchemaAllowsValue(t, document, propertySchema, propertyValue)
				continue
			}

			allowed, ok := resolved.AdditionalProperties.(bool)
			if !ok || !allowed {
				t.Fatalf("unexpected property %q in %#v", key, objectValue)
			}
		}
	case "array":
		arrayValue, ok := value.([]any)
		if !ok {
			t.Fatalf("expected array value, got %T", value)
		}

		if len(resolved.PrefixItems) > 0 {
			if len(arrayValue) != len(resolved.PrefixItems) {
				t.Fatalf("unexpected tuple length: got %d want %d", len(arrayValue), len(resolved.PrefixItems))
			}

			for index, itemSchema := range resolved.PrefixItems {
				requireSchemaAllowsValue(t, document, itemSchema, arrayValue[index])
			}
			return
		}

		for _, item := range arrayValue {
			requireSchemaAllowsValue(t, document, resolved.Items, item)
		}
	case "string":
		if _, ok := value.(string); !ok {
			t.Fatalf("expected string value, got %T", value)
		}
	case "integer":
		number, ok := value.(float64)
		if !ok {
			t.Fatalf("expected integer value, got %T", value)
		}
		if number != float64(int64(number)) {
			t.Fatalf("expected integral number, got %v", number)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			t.Fatalf("expected boolean value, got %T", value)
		}
	default:
		if resolved.Type == "" && resolved.Ref == "" {
			t.Fatalf("unsupported schema with empty type: %+v", resolved)
		}
	}
}
