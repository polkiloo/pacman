package api

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDocumentErrorsAndHelpers(t *testing.T) {
	t.Parallel()

	if _, err := LoadDocument(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("expected missing openapi file error")
	}

	dir := t.TempDir()
	invalidPath := filepath.Join(dir, "invalid.yaml")
	if err := os.WriteFile(invalidPath, []byte("openapi: ["), 0o600); err != nil {
		t.Fatalf("write invalid openapi file: %v", err)
	}

	if _, err := LoadDocument(invalidPath); err == nil {
		t.Fatal("expected invalid yaml error")
	}

	document := Document{
		Paths: PathMap{
			"/test": {
				Get: &Operation{
					Security:   []map[string][]string{},
					Extensions: map[string]interface{}{"x-enabled": true, "x-string": "yes"},
					Parameters: []Parameter{
						{Name: "inline", In: "query"},
						{Ref: "#/components/parameters/Lag"},
					},
					RequestBody: &RequestBody{
						Required: true,
						Content: map[string]MediaType{
							"application/json": {Schema: &Schema{Ref: "#/components/schemas/Body"}},
						},
					},
					Responses: map[string]Response{
						"200": {Description: "ok"},
					},
				},
				Post:   &Operation{},
				Put:    &Operation{},
				Patch:  &Operation{},
				Delete: &Operation{},
				Head:   &Operation{},
			},
		},
		Components: Components{
			Parameters: map[string]Parameter{
				"Lag": {Name: "lag", In: "query"},
			},
			Schemas: map[string]*Schema{
				"Body": {
					Type:     "object",
					Required: []string{"candidate"},
					Properties: map[string]*Schema{
						"candidate": {Type: "string"},
					},
				},
			},
			SecuritySchemes: map[string]SecurityScheme{
				"bearerAuth": {Type: "http", Scheme: "bearer"},
			},
		},
	}

	if _, err := document.Operation("/missing", "get"); !errors.Is(err, ErrPathNotFound) {
		t.Fatalf("expected missing path error, got %v", err)
	}

	for _, method := range []string{"get", "post", "put", "patch", "delete", "head"} {
		if _, err := document.Operation("/test", method); err != nil {
			t.Fatalf("resolve %s operation: %v", method, err)
		}
	}

	if _, err := document.Operation("/test", "trace"); !errors.Is(err, ErrMethodNotFound) {
		t.Fatalf("expected missing method error, got %v", err)
	}

	if _, err := document.Schema("Missing"); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("expected missing schema error, got %v", err)
	}

	getOperation, err := document.Operation("/test", "get")
	if err != nil {
		t.Fatalf("resolve get operation: %v", err)
	}

	if !getOperation.SecurityExplicitlyDisabled() {
		t.Fatal("expected explicit security disable")
	}

	if !getOperation.ExtensionBool("x-enabled") {
		t.Fatal("expected enabled extension")
	}

	if getOperation.ExtensionBool("x-string") {
		t.Fatal("expected non-boolean extension lookup to be false")
	}

	resolvedParameter, err := document.ResolveParameter(Parameter{Ref: "#/components/parameters/Lag"})
	if err != nil {
		t.Fatalf("resolve parameter component: %v", err)
	}

	if resolvedParameter.Name != "lag" {
		t.Fatalf("unexpected resolved parameter: %+v", resolvedParameter)
	}

	inlineParameter, err := document.ResolveParameter(Parameter{Name: "candidate", In: "query"})
	if err != nil {
		t.Fatalf("resolve inline parameter: %v", err)
	}

	if inlineParameter.Name != "candidate" {
		t.Fatalf("unexpected inline parameter: %+v", inlineParameter)
	}

	if _, err := document.ResolveParameter(Parameter{Ref: "#/components/parameters/Missing"}); err == nil {
		t.Fatal("expected missing parameter component error")
	}

	if _, err := document.ResolveParameter(Parameter{Ref: "#/components/bogus/Lag"}); err == nil {
		t.Fatal("expected unsupported parameter ref error")
	}

	parameter, err := document.Parameter(getOperation, "lag")
	if err != nil {
		t.Fatalf("resolve operation parameter: %v", err)
	}

	if parameter == nil || parameter.Name != "lag" {
		t.Fatalf("unexpected parameter lookup result: %+v", parameter)
	}

	parameter, err = document.Parameter(getOperation, "missing")
	if err != nil {
		t.Fatalf("resolve missing operation parameter: %v", err)
	}

	if parameter != nil {
		t.Fatalf("expected missing parameter lookup to return nil, got %+v", parameter)
	}

	parameter, err = document.Parameter(nil, "lag")
	if err != nil {
		t.Fatalf("resolve nil operation parameter: %v", err)
	}

	if parameter != nil {
		t.Fatalf("expected nil operation parameter lookup to return nil, got %+v", parameter)
	}

	_, err = document.Parameter(&Operation{
		Parameters: []Parameter{
			{Ref: "#/components/parameters/Missing"},
		},
	}, "lag")
	if err == nil {
		t.Fatal("expected operation parameter lookup to surface bad parameter refs")
	}

	requestSchema, err := document.ResolveRequestSchema(getOperation, "application/json")
	if err != nil {
		t.Fatalf("resolve request schema: %v", err)
	}

	if requestSchema == nil || !requestSchema.Requires("candidate") {
		t.Fatalf("expected request schema to require candidate, got %+v", requestSchema)
	}

	property, ok := requestSchema.Property("candidate")
	if !ok || property.Type != "string" {
		t.Fatalf("unexpected request schema property: %+v", property)
	}

	if property, ok := requestSchema.Property("missing"); ok || property != nil {
		t.Fatalf("expected missing property lookup to be nil, got %+v", property)
	}

	if schema, err := document.ResolveRequestSchema(getOperation, "application/xml"); err != nil || schema != nil {
		t.Fatalf("expected missing media-type schema to be nil, got schema=%+v err=%v", schema, err)
	}

	if schema, err := document.ResolveRequestSchema(&Operation{}, "application/json"); err != nil || schema != nil {
		t.Fatalf("expected nil request schema for empty operation, got schema=%+v err=%v", schema, err)
	}

	if schema, err := document.ResolveSchema(nil); err != nil || schema != nil {
		t.Fatalf("expected nil schema resolution to be nil, got schema=%+v err=%v", schema, err)
	}

	if _, err := document.ResolveSchema(&Schema{Ref: "#/components/schemas/Missing"}); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("expected missing schema error, got %v", err)
	}

	if _, err := document.ResolveSchema(&Schema{Ref: "#/components/bogus/Body"}); err == nil {
		t.Fatal("expected unsupported schema ref error")
	}

	if response, ok := getOperation.Response("200"); !ok || response.Description != "ok" {
		t.Fatalf("unexpected response lookup result: ok=%v response=%+v", ok, response)
	}

	var nilOperation *Operation
	if _, ok := nilOperation.Response("200"); ok {
		t.Fatal("expected nil operation response lookup to fail")
	}

	if nilOperation.SecurityExplicitlyDisabled() {
		t.Fatal("expected nil operation security disable lookup to be false")
	}

	if nilOperation.RequiresSecurityScheme("bearerAuth") {
		t.Fatal("expected nil operation security lookup to be false")
	}

	if nilOperation.ExtensionBool("x-enabled") {
		t.Fatal("expected nil operation extension lookup to be false")
	}

	securedOperation := &Operation{
		Security: []map[string][]string{
			{"bearerAuth": {}},
			{"mutualTLS": {}},
		},
	}

	if !securedOperation.RequiresSecurityScheme("bearerAuth") {
		t.Fatal("expected bearer auth requirement to be present")
	}

	if securedOperation.RequiresSecurityScheme("missing") {
		t.Fatal("expected missing security requirement to be false")
	}

	var nilSchema *Schema
	if nilSchema.Requires("candidate") {
		t.Fatal("expected nil schema requires lookup to be false")
	}

	if (&Schema{Required: []string{"candidate"}}).Requires("missing") {
		t.Fatal("expected missing required field lookup to be false")
	}

	if property, ok := nilSchema.Property("candidate"); ok || property != nil {
		t.Fatalf("expected nil schema property lookup to fail, got %+v", property)
	}
}

func TestLoadDocumentResolvesExternalRefs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	root := `openapi: 3.1.0
paths:
  /health:
    $ref: './paths.yaml#/~1health'
components:
  schemas:
    Tuple:
      $ref: './schemas.yaml#/Tuple'
`
	if err := os.WriteFile(filepath.Join(dir, "openapi.yaml"), []byte(root), 0o600); err != nil {
		t.Fatalf("write root spec: %v", err)
	}

	paths := `/health:
  get:
    summary: Split health probe
    responses:
      '200':
        description: ok
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Tuple'
`
	if err := os.WriteFile(filepath.Join(dir, "paths.yaml"), []byte(paths), 0o600); err != nil {
		t.Fatalf("write paths fragment: %v", err)
	}

	schemas := `Tuple:
  type: array
  prefixItems:
    - type: string
    - type: integer
`
	if err := os.WriteFile(filepath.Join(dir, "schemas.yaml"), []byte(schemas), 0o600); err != nil {
		t.Fatalf("write schemas fragment: %v", err)
	}

	document, err := LoadDocument(filepath.Join(dir, "openapi.yaml"))
	if err != nil {
		t.Fatalf("load split openapi document: %v", err)
	}

	operation, err := document.Operation("/health", "get")
	if err != nil {
		t.Fatalf("resolve split health operation: %v", err)
	}

	if operation.Summary != "Split health probe" {
		t.Fatalf("unexpected split operation summary: %q", operation.Summary)
	}

	response, ok := operation.Response("200")
	if !ok {
		t.Fatal("expected split operation 200 response")
	}

	if got := response.Content["application/json"].Schema.Ref; got != "#/components/schemas/Tuple" {
		t.Fatalf("unexpected split response schema ref: %q", got)
	}

	tuple, err := document.Schema("Tuple")
	if err != nil {
		t.Fatalf("resolve split tuple schema: %v", err)
	}

	if len(tuple.PrefixItems) != 2 {
		t.Fatalf("expected split tuple schema prefixItems to resolve, got %d", len(tuple.PrefixItems))
	}
}

func TestLoadDocumentExternalRefErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	missingRefRoot := `openapi: 3.1.0
paths:
  /health:
    $ref: './missing.yaml#/~1health'
`
	missingRefPath := filepath.Join(dir, "missing-ref.yaml")
	if err := os.WriteFile(missingRefPath, []byte(missingRefRoot), 0o600); err != nil {
		t.Fatalf("write missing-ref root spec: %v", err)
	}

	if _, err := LoadDocument(missingRefPath); err == nil {
		t.Fatal("expected missing external ref error")
	}

	badPointerRoot := `openapi: 3.1.0
paths:
  /health:
    $ref: './paths.yaml#/~1missing'
`
	badPointerPath := filepath.Join(dir, "bad-pointer.yaml")
	if err := os.WriteFile(badPointerPath, []byte(badPointerRoot), 0o600); err != nil {
		t.Fatalf("write bad-pointer root spec: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir, "paths.yaml"), []byte(`/health: {}`+"\n"), 0o600); err != nil {
		t.Fatalf("write bad-pointer fragment: %v", err)
	}

	if _, err := LoadDocument(badPointerPath); err == nil {
		t.Fatal("expected bad external ref pointer error")
	}
}

func TestRepositoryDocumentPatroniProbeCompatibility(t *testing.T) {
	t.Parallel()

	document, err := LoadRepositoryDocument()
	if err != nil {
		t.Fatalf("load repository openapi document: %v", err)
	}

	testCases := []struct {
		path               string
		requiredParams     []string
		expectIgnoredTags  bool
		expectFreeFormTags bool
		expectHumanLag     bool
	}{
		{path: "/health"},
		{path: "/liveness"},
		{path: "/readiness", requiredParams: []string{"lag", "mode"}, expectHumanLag: true},
		{path: "/primary", expectIgnoredTags: true},
		{path: "/replica", requiredParams: []string{"lag", "replication_state"}, expectFreeFormTags: true, expectHumanLag: true},
		{path: "/", expectIgnoredTags: true},
		{path: "/read-write", expectIgnoredTags: true},
		{path: "/leader", expectIgnoredTags: true},
		{path: "/standby-leader", expectIgnoredTags: true},
		{path: "/standby_leader", expectIgnoredTags: true},
		{path: "/read-only", requiredParams: []string{"lag"}, expectFreeFormTags: true, expectHumanLag: true},
		{path: "/synchronous"},
		{path: "/sync"},
		{path: "/read-only-sync"},
		{path: "/quorum"},
		{path: "/read-only-quorum"},
		{path: "/asynchronous", requiredParams: []string{"lag"}, expectHumanLag: true},
		{path: "/async", requiredParams: []string{"lag"}, expectHumanLag: true},
	}

	for _, testCase := range testCases {
		operation, err := document.Operation(testCase.path, "get")
		if err != nil {
			t.Fatalf("resolve %s get operation: %v", testCase.path, err)
		}

		if !operation.ExtensionBool("x-patroni-compatible") {
			t.Fatalf("%s is missing Patroni compatibility flag", testCase.path)
		}

		if !operation.ExtensionBool("x-patroni-head-options-supported") {
			t.Fatalf("%s is missing head/options compatibility flag", testCase.path)
		}

		if !operation.SecurityExplicitlyDisabled() {
			t.Fatalf("%s should allow unauthenticated probe access", testCase.path)
		}

		if _, ok := operation.Response("200"); !ok {
			t.Fatalf("%s is missing 200 response", testCase.path)
		}

		if _, ok := operation.Response("503"); !ok {
			t.Fatalf("%s is missing 503 response", testCase.path)
		}

		for _, parameterName := range testCase.requiredParams {
			parameter, err := document.Parameter(operation, parameterName)
			if err != nil {
				t.Fatalf("resolve %s parameter on %s: %v", parameterName, testCase.path, err)
			}
			if parameter == nil {
				t.Fatalf("%s is missing %s parameter", testCase.path, parameterName)
			}
		}

		if got := operation.ExtensionBool("x-patroni-ignores-tag-filters"); got != testCase.expectIgnoredTags {
			t.Fatalf("%s unexpected ignore-tags flag: got %v want %v", testCase.path, got, testCase.expectIgnoredTags)
		}

		if got := operation.ExtensionBool("x-patroni-free-form-tag-filters"); got != testCase.expectFreeFormTags {
			t.Fatalf("%s unexpected free-form-tags flag: got %v want %v", testCase.path, got, testCase.expectFreeFormTags)
		}

		if got := operation.ExtensionBool("x-patroni-human-readable-lag"); got != testCase.expectHumanLag {
			t.Fatalf("%s unexpected human-lag flag: got %v want %v", testCase.path, got, testCase.expectHumanLag)
		}
	}
}

func TestRepositoryDocumentPatroniMonitoringAndAdminCompatibility(t *testing.T) {
	t.Parallel()

	document, err := LoadRepositoryDocument()
	if err != nil {
		t.Fatalf("load repository openapi document: %v", err)
	}

	monitoringCases := []struct {
		path        string
		schemaRef   string
		contentType string
	}{
		{path: "/patroni", schemaRef: "#/components/schemas/PatroniNodeStatus", contentType: "application/json"},
		{path: "/metrics", contentType: "text/plain"},
		{path: "/cluster", schemaRef: "#/components/schemas/PatroniClusterStatus", contentType: "application/json"},
		{path: "/history", schemaRef: "#/components/schemas/PatroniHistory", contentType: "application/json"},
	}

	for _, testCase := range monitoringCases {
		operation, err := document.Operation(testCase.path, "get")
		if err != nil {
			t.Fatalf("resolve %s get operation: %v", testCase.path, err)
		}

		if !operation.ExtensionBool("x-patroni-compatible") {
			t.Fatalf("%s is missing Patroni compatibility flag", testCase.path)
		}

		if !operation.SecurityExplicitlyDisabled() {
			t.Fatalf("%s should allow unauthenticated read access", testCase.path)
		}

		response, ok := operation.Response("200")
		if !ok {
			t.Fatalf("%s is missing 200 response", testCase.path)
		}

		mediaType, ok := response.Content[testCase.contentType]
		if !ok {
			t.Fatalf("%s is missing %s content", testCase.path, testCase.contentType)
		}

		if testCase.schemaRef != "" && mediaType.Schema.Ref != testCase.schemaRef {
			t.Fatalf("%s unexpected schema ref: got %q want %q", testCase.path, mediaType.Schema.Ref, testCase.schemaRef)
		}
	}

	securedPaths := []struct {
		path   string
		method string
	}{
		{path: "/config", method: "get"},
		{path: "/config", method: "patch"},
		{path: "/config", method: "put"},
		{path: "/switchover", method: "post"},
		{path: "/switchover", method: "delete"},
		{path: "/failover", method: "post"},
		{path: "/restart", method: "post"},
		{path: "/restart", method: "delete"},
		{path: "/reload", method: "post"},
		{path: "/reinitialize", method: "post"},
		{path: "/api/v1/cluster", method: "get"},
		{path: "/api/v1/operations/switchover", method: "post"},
		{path: "/api/v1/operations/failover", method: "post"},
	}

	for _, testCase := range securedPaths {
		operation, err := document.Operation(testCase.path, testCase.method)
		if err != nil {
			t.Fatalf("resolve %s %s operation: %v", testCase.method, testCase.path, err)
		}

		if !operation.RequiresSecurityScheme("bearerAuth") || !operation.RequiresSecurityScheme("mutualTLS") {
			t.Fatalf("%s %s should require bearer and mutualTLS", testCase.method, testCase.path)
		}
	}

	configPatch, err := document.Operation("/config", "patch")
	if err != nil {
		t.Fatalf("resolve /config patch: %v", err)
	}

	if !configPatch.ExtensionBool("x-patroni-null-removes-fields") {
		t.Fatal("/config patch is missing null-removes-fields compatibility flag")
	}

	configPut, err := document.Operation("/config", "put")
	if err != nil {
		t.Fatalf("resolve /config put: %v", err)
	}

	if !configPut.ExtensionBool("x-patroni-full-rewrite") {
		t.Fatal("/config put is missing full-rewrite compatibility flag")
	}

	switchover, err := document.Operation("/switchover", "post")
	if err != nil {
		t.Fatalf("resolve /switchover post: %v", err)
	}

	switchoverSchema, err := document.ResolveRequestSchema(switchover, "application/json")
	if err != nil {
		t.Fatalf("resolve switchover request schema: %v", err)
	}

	for _, required := range []string{"leader"} {
		if !switchoverSchema.Requires(required) {
			t.Fatalf("switchover schema should require %s", required)
		}
	}

	for _, property := range []string{"candidate", "scheduled_at"} {
		if _, ok := switchoverSchema.Property(property); !ok {
			t.Fatalf("switchover schema is missing %s", property)
		}
	}

	for _, code := range []string{"200", "202", "400", "412", "503"} {
		if _, ok := switchover.Response(code); !ok {
			t.Fatalf("/switchover post is missing %s response", code)
		}
	}

	failover, err := document.Operation("/failover", "post")
	if err != nil {
		t.Fatalf("resolve /failover post: %v", err)
	}

	if !failover.ExtensionBool("x-pacman-safety-gates") {
		t.Fatal("/failover post is missing PACMAN safety-gates marker")
	}

	failoverSchema, err := document.ResolveRequestSchema(failover, "application/json")
	if err != nil {
		t.Fatalf("resolve failover request schema: %v", err)
	}

	if !failoverSchema.Requires("candidate") {
		t.Fatal("failover schema should require candidate")
	}

	if _, ok := failoverSchema.Property("leader"); !ok {
		t.Fatal("failover schema is missing leader compatibility property")
	}

	for _, code := range []string{"200", "400", "412", "503"} {
		if _, ok := failover.Response(code); !ok {
			t.Fatalf("/failover post is missing %s response", code)
		}
	}

	restart, err := document.Operation("/restart", "post")
	if err != nil {
		t.Fatalf("resolve /restart post: %v", err)
	}

	restartSchema, err := document.ResolveRequestSchema(restart, "application/json")
	if err != nil {
		t.Fatalf("resolve restart request schema: %v", err)
	}

	for _, property := range []string{"restart_pending", "role", "postgres_version", "timeout", "schedule"} {
		if _, ok := restartSchema.Property(property); !ok {
			t.Fatalf("restart schema is missing %s", property)
		}
	}

	reinitialize, err := document.Operation("/reinitialize", "post")
	if err != nil {
		t.Fatalf("resolve /reinitialize post: %v", err)
	}

	reinitializeSchema, err := document.ResolveRequestSchema(reinitialize, "application/json")
	if err != nil {
		t.Fatalf("resolve reinitialize request schema: %v", err)
	}

	for _, property := range []string{"force", "from-leader"} {
		if _, ok := reinitializeSchema.Property(property); !ok {
			t.Fatalf("reinitialize schema is missing %s", property)
		}
	}

	reload, err := document.Operation("/reload", "post")
	if err != nil {
		t.Fatalf("resolve /reload post: %v", err)
	}

	if reload.RequestBody != nil {
		t.Fatalf("expected /reload post to have no request body, got %+v", reload.RequestBody)
	}
}

func TestRepositoryDocumentPatroniSchemas(t *testing.T) {
	t.Parallel()

	document, err := LoadRepositoryDocument()
	if err != nil {
		t.Fatalf("load repository openapi document: %v", err)
	}

	nodeStatus, err := document.Schema("PatroniNodeStatus")
	if err != nil {
		t.Fatalf("resolve PatroniNodeStatus schema: %v", err)
	}

	for _, required := range []string{"state", "role", "patroni"} {
		if !nodeStatus.Requires(required) {
			t.Fatalf("PatroniNodeStatus should require %s", required)
		}
	}

	for _, property := range []string{"pending_restart", "pending_restart_reason", "xlog", "replication"} {
		if _, ok := nodeStatus.Property(property); !ok {
			t.Fatalf("PatroniNodeStatus is missing %s", property)
		}
	}

	dynamicConfig, err := document.Schema("PatroniDynamicConfig")
	if err != nil {
		t.Fatalf("resolve PatroniDynamicConfig schema: %v", err)
	}

	for _, property := range []string{"ttl", "loop_wait", "retry_timeout", "maximum_lag_on_failover", "postgresql"} {
		if _, ok := dynamicConfig.Property(property); !ok {
			t.Fatalf("PatroniDynamicConfig is missing %s", property)
		}
	}

	history, err := document.Schema("PatroniHistoryLine")
	if err != nil {
		t.Fatalf("resolve PatroniHistoryLine schema: %v", err)
	}

	if len(history.PrefixItems) != 4 {
		t.Fatalf("expected PatroniHistoryLine to contain four tuple items, got %d", len(history.PrefixItems))
	}

	patroniRole, err := document.Schema("PatroniRole")
	if err != nil {
		t.Fatalf("resolve PatroniRole schema: %v", err)
	}

	if len(patroniRole.Enum) == 0 {
		t.Fatal("expected PatroniRole enum values")
	}

	memberRole, err := document.Schema("PatroniMemberRole")
	if err != nil {
		t.Fatalf("resolve PatroniMemberRole schema: %v", err)
	}

	if len(memberRole.Enum) == 0 {
		t.Fatal("expected PatroniMemberRole enum values")
	}

	if _, ok := document.Components.SecuritySchemes["bearerAuth"]; !ok {
		t.Fatal("expected bearerAuth security scheme")
	}

	if _, ok := document.Components.SecuritySchemes["mutualTLS"]; !ok {
		t.Fatal("expected mutualTLS security scheme")
	}

	lagParameter, ok := document.Components.Parameters["PatroniLagQuery"]
	if !ok {
		t.Fatal("expected PatroniLagQuery component parameter")
	}

	if lagParameter.Name != "lag" || lagParameter.Schema == nil || lagParameter.Schema.Type != "string" {
		t.Fatalf("unexpected PatroniLagQuery definition: %+v", lagParameter)
	}
}
