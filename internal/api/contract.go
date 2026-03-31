package api

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	ErrPathNotFound   = errors.New("openapi path not found")
	ErrMethodNotFound = errors.New("openapi method not found")
	ErrSchemaNotFound = errors.New("openapi schema not found")
)

// Document captures the subset of the OpenAPI document that PACMAN tests assert.
type Document struct {
	OpenAPI    string     `yaml:"openapi"`
	Paths      PathMap    `yaml:"paths"`
	Components Components `yaml:"components"`
}

// Components captures reusable OpenAPI components used by the contract tests.
type Components struct {
	Parameters      map[string]Parameter      `yaml:"parameters"`
	Responses       map[string]Response       `yaml:"responses"`
	Schemas         map[string]*Schema        `yaml:"schemas"`
	SecuritySchemes map[string]SecurityScheme `yaml:"securitySchemes"`
}

// PathMap stores path items keyed by literal path.
type PathMap map[string]PathItem

// PathItem contains the supported HTTP methods for one OpenAPI path item.
type PathItem struct {
	Get    *Operation `yaml:"get"`
	Post   *Operation `yaml:"post"`
	Put    *Operation `yaml:"put"`
	Patch  *Operation `yaml:"patch"`
	Delete *Operation `yaml:"delete"`
	Head   *Operation `yaml:"head"`
}

// Operation captures the subset of OpenAPI operations used by compatibility tests.
type Operation struct {
	Summary     string                 `yaml:"summary"`
	Description string                 `yaml:"description"`
	OperationID string                 `yaml:"operationId"`
	Security    []map[string][]string  `yaml:"security"`
	Parameters  []Parameter            `yaml:"parameters"`
	RequestBody *RequestBody           `yaml:"requestBody"`
	Responses   map[string]Response    `yaml:"responses"`
	Extensions  map[string]interface{} `yaml:",inline"`
}

// Parameter captures an OpenAPI parameter or component reference.
type Parameter struct {
	Ref         string                 `yaml:"$ref,omitempty"`
	Name        string                 `yaml:"name,omitempty"`
	In          string                 `yaml:"in,omitempty"`
	Required    bool                   `yaml:"required,omitempty"`
	Description string                 `yaml:"description,omitempty"`
	Schema      *Schema                `yaml:"schema,omitempty"`
	Extensions  map[string]interface{} `yaml:",inline"`
}

// RequestBody captures the media-type keyed request body schema.
type RequestBody struct {
	Required bool                 `yaml:"required"`
	Content  map[string]MediaType `yaml:"content"`
}

// Response captures a response description or a component reference.
type Response struct {
	Ref         string               `yaml:"$ref,omitempty"`
	Description string               `yaml:"description,omitempty"`
	Content     map[string]MediaType `yaml:"content,omitempty"`
}

// MediaType captures the schema for one content type.
type MediaType struct {
	Schema *Schema `yaml:"schema"`
}

// SecurityScheme captures the top-level scheme metadata needed by tests.
type SecurityScheme struct {
	Type   string `yaml:"type"`
	Scheme string `yaml:"scheme,omitempty"`
}

// Schema captures the subset of JSON Schema features used in docs/openapi.yaml.
type Schema struct {
	Ref                  string                 `yaml:"$ref,omitempty"`
	Type                 string                 `yaml:"type,omitempty"`
	Format               string                 `yaml:"format,omitempty"`
	Required             []string               `yaml:"required,omitempty"`
	Properties           map[string]*Schema     `yaml:"properties,omitempty"`
	Items                *Schema                `yaml:"items,omitempty"`
	PrefixItems          []*Schema              `yaml:"prefixItems,omitempty"`
	Enum                 []string               `yaml:"enum,omitempty"`
	AdditionalProperties interface{}            `yaml:"additionalProperties,omitempty"`
	Extensions           map[string]interface{} `yaml:",inline"`
}

// LoadDocument loads and parses an OpenAPI document from disk.
func LoadDocument(path string) (Document, error) {
	payload, err := loadResolvedDocument(path)
	if err != nil {
		return Document{}, err
	}

	return decodeDocument(payload)
}

// LoadRepositoryDocument loads the repository OpenAPI contract from docs/openapi.yaml.
func LoadRepositoryDocument() (Document, error) {
	_, currentFile, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(currentFile), "..", "..", "docs", "openapi.yaml")
	return LoadDocument(path)
}

// Operation looks up one HTTP method under the given path.
func (document Document) Operation(path, method string) (*Operation, error) {
	item, ok := document.Paths[path]
	if !ok {
		return nil, ErrPathNotFound
	}

	operation := item.method(method)
	if operation == nil {
		return nil, ErrMethodNotFound
	}

	return operation, nil
}

func (item PathItem) method(method string) *Operation {
	switch strings.ToLower(strings.TrimSpace(method)) {
	case "get":
		return item.Get
	case "post":
		return item.Post
	case "put":
		return item.Put
	case "patch":
		return item.Patch
	case "delete":
		return item.Delete
	case "head":
		return item.Head
	default:
		return nil
	}
}

// Schema resolves a named component schema.
func (document Document) Schema(name string) (*Schema, error) {
	schema, ok := document.Components.Schemas[name]
	if !ok {
		return nil, ErrSchemaNotFound
	}

	return schema, nil
}

// ResolveParameter resolves either an inline parameter or a parameter component reference.
func (document Document) ResolveParameter(parameter Parameter) (Parameter, error) {
	if parameter.Ref == "" {
		return parameter, nil
	}

	name, err := componentName(parameter.Ref, "#/components/parameters/")
	if err != nil {
		return Parameter{}, err
	}

	resolved, ok := document.Components.Parameters[name]
	if !ok {
		return Parameter{}, fmt.Errorf("resolve parameter %q: %w", name, ErrPathNotFound)
	}

	return resolved, nil
}

// ResolveSchema resolves either an inline schema or a schema component reference.
func (document Document) ResolveSchema(schema *Schema) (*Schema, error) {
	if schema == nil || schema.Ref == "" {
		return schema, nil
	}

	name, err := componentName(schema.Ref, "#/components/schemas/")
	if err != nil {
		return nil, err
	}

	return document.Schema(name)
}

// ResolveRequestSchema resolves the request-body schema for the given content type.
func (document Document) ResolveRequestSchema(operation *Operation, contentType string) (*Schema, error) {
	if operation == nil || operation.RequestBody == nil {
		return nil, nil
	}

	mediaType, ok := operation.RequestBody.Content[contentType]
	if !ok {
		return nil, nil
	}

	return document.ResolveSchema(mediaType.Schema)
}

// Parameter resolves a named parameter from the operation.
func (document Document) Parameter(operation *Operation, name string) (*Parameter, error) {
	if operation == nil {
		return nil, nil
	}

	for _, candidate := range operation.Parameters {
		resolved, err := document.ResolveParameter(candidate)
		if err != nil {
			return nil, err
		}

		if resolved.Name == name {
			return &resolved, nil
		}
	}

	return nil, nil
}

// Response reports whether the operation exposes the given status code.
func (operation *Operation) Response(code string) (Response, bool) {
	if operation == nil {
		return Response{}, false
	}

	response, ok := operation.Responses[code]
	return response, ok
}

// SecurityExplicitlyDisabled reports whether the operation declares `security: []`.
func (operation *Operation) SecurityExplicitlyDisabled() bool {
	return operation != nil && operation.Security != nil && len(operation.Security) == 0
}

// RequiresSecurityScheme reports whether the operation lists the given security scheme.
func (operation *Operation) RequiresSecurityScheme(name string) bool {
	if operation == nil {
		return false
	}

	for _, requirement := range operation.Security {
		if _, ok := requirement[name]; ok {
			return true
		}
	}

	return false
}

// ExtensionBool reads a boolean-valued vendor extension.
func (operation *Operation) ExtensionBool(name string) bool {
	if operation == nil {
		return false
	}

	value, ok := operation.Extensions[name]
	if !ok {
		return false
	}

	enabled, ok := value.(bool)
	return ok && enabled
}

// ExtensionString reads a string-valued vendor extension.
func (operation *Operation) ExtensionString(name string) string {
	if operation == nil {
		return ""
	}

	value, ok := operation.Extensions[name]
	if !ok {
		return ""
	}

	text, ok := value.(string)
	if !ok {
		return ""
	}

	return text
}

// Requires reports whether the schema marks the named property as required.
func (schema *Schema) Requires(name string) bool {
	if schema == nil {
		return false
	}

	for _, required := range schema.Required {
		if required == name {
			return true
		}
	}

	return false
}

// Property resolves a named schema property.
func (schema *Schema) Property(name string) (*Schema, bool) {
	if schema == nil || schema.Properties == nil {
		return nil, false
	}

	property, ok := schema.Properties[name]
	return property, ok
}

func componentName(ref, prefix string) (string, error) {
	if !strings.HasPrefix(ref, prefix) {
		return "", fmt.Errorf("unsupported component ref %q", ref)
	}

	return strings.TrimPrefix(ref, prefix), nil
}

func loadResolvedDocument(path string) ([]byte, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read openapi document: %w", err)
	}

	var raw map[string]any
	if err := yaml.Unmarshal(payload, &raw); err != nil {
		return nil, fmt.Errorf("decode openapi document: %w", err)
	}

	if err := resolveTopLevelExternalRefs(path, raw); err != nil {
		return nil, fmt.Errorf("resolve openapi document refs: %w", err)
	}

	resolved, err := yaml.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("encode openapi document: %w", err)
	}

	return resolved, nil
}

func decodeDocument(payload []byte) (Document, error) {
	var document Document
	if err := yaml.Unmarshal(payload, &document); err != nil {
		return Document{}, fmt.Errorf("decode openapi document: %w", err)
	}

	return document, nil
}

func resolveTopLevelExternalRefs(rootPath string, document map[string]any) error {
	if err := resolveObjectRefs(rootPath, document, "paths"); err != nil {
		return err
	}

	for _, componentSection := range []string{"parameters", "responses", "securitySchemes", "schemas"} {
		if err := resolveObjectRefs(rootPath, document, "components", componentSection); err != nil {
			return err
		}
	}

	return nil
}

func resolveObjectRefs(rootPath string, document map[string]any, keys ...string) error {
	target, ok := lookupMap(document, keys...)
	if !ok {
		return nil
	}

	for name, value := range target {
		resolved, err := resolveExternalRef(rootPath, value)
		if err != nil {
			return fmt.Errorf("%s %q: %w", strings.Join(keys, "."), name, err)
		}
		target[name] = resolved
	}

	return nil
}

func resolveExternalRef(rootPath string, value any) (any, error) {
	node, ok := value.(map[string]any)
	if !ok {
		return value, nil
	}

	ref, ok := node["$ref"].(string)
	if !ok || strings.HasPrefix(ref, "#") {
		return value, nil
	}

	refPath, pointer := splitRef(ref)
	fragmentPath := filepath.Join(filepath.Dir(rootPath), refPath)

	payload, err := os.ReadFile(fragmentPath)
	if err != nil {
		return nil, fmt.Errorf("read ref %q: %w", ref, err)
	}

	var fragment any
	if err := yaml.Unmarshal(payload, &fragment); err != nil {
		return nil, fmt.Errorf("decode ref %q: %w", ref, err)
	}

	resolved, err := resolvePointer(fragment, pointer)
	if err != nil {
		return nil, fmt.Errorf("resolve ref %q: %w", ref, err)
	}

	return resolved, nil
}

func splitRef(ref string) (path string, pointer string) {
	path, pointer, found := strings.Cut(ref, "#")
	if !found {
		return ref, ""
	}

	return path, pointer
}

func resolvePointer(value any, pointer string) (any, error) {
	if pointer == "" {
		return value, nil
	}

	if !strings.HasPrefix(pointer, "/") {
		return nil, fmt.Errorf("unsupported ref fragment %q", pointer)
	}

	current := value
	for _, token := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")

		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[token]
			if !ok {
				return nil, fmt.Errorf("pointer segment %q not found", token)
			}
			current = next
		case []any:
			index, err := strconv.Atoi(token)
			if err != nil {
				return nil, fmt.Errorf("pointer segment %q is not a valid array index", token)
			}
			if index < 0 || index >= len(typed) {
				return nil, fmt.Errorf("pointer index %d out of range", index)
			}
			current = typed[index]
		default:
			return nil, fmt.Errorf("pointer segment %q cannot be resolved from %T", token, current)
		}
	}

	return current, nil
}

func lookupMap(document map[string]any, keys ...string) (map[string]any, bool) {
	current := document
	for _, key := range keys {
		value, ok := current[key]
		if !ok {
			return nil, false
		}

		next, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		current = next
	}

	return current, true
}
