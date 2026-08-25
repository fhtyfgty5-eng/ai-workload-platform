package httpapi

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type openAPIDocument struct {
	OpenAPI    string                          `yaml:"openapi"`
	Paths      map[string]map[string]operation `yaml:"paths"`
	Components struct {
		Schemas         map[string]yaml.Node `yaml:"schemas"`
		Responses       map[string]yaml.Node `yaml:"responses"`
		Parameters      map[string]yaml.Node `yaml:"parameters"`
		SecuritySchemes map[string]yaml.Node `yaml:"securitySchemes"`
	} `yaml:"components"`
}

type operation struct {
	Parameters  []yaml.Node           `yaml:"parameters"`
	RequestBody yaml.Node             `yaml:"requestBody"`
	Responses   map[string]yaml.Node  `yaml:"responses"`
	Security    []map[string][]string `yaml:"security"`
}

type operationExpectation struct {
	Path     string
	Method   string
	Success  string
	Schema   string
	Security string
}

var implementedOperations = []operationExpectation{
	{Path: "/health/live", Method: "get", Success: "200", Schema: "HealthResponse"},
	{Path: "/health/ready", Method: "get", Success: "200", Schema: "HealthResponse"},
	{Path: "/api/v1/workflows", Method: "get", Success: "200", Schema: "WorkflowPage", Security: "bearerAuth"},
	{Path: "/api/v1/workflows", Method: "post", Success: "201", Schema: "DefinitionRef", Security: "bearerAuth"},
	{Path: "/api/v1/workflows/{workflow-id}", Method: "get", Success: "200", Schema: "WorkflowSummary", Security: "bearerAuth"},
	{Path: "/api/v1/workflows/{workflow-id}/versions", Method: "get", Success: "200", Schema: "VersionPage", Security: "bearerAuth"},
	{Path: "/api/v1/workflows/{workflow-id}/versions", Method: "post", Success: "201", Schema: "DefinitionRef", Security: "bearerAuth"},
	{Path: "/api/v1/workflows/{workflow-id}/versions/{version}", Method: "get", Success: "200", Schema: "WorkflowDefinition", Security: "bearerAuth"},
	{Path: "/api/v1/workflows/{workflow-id}/versions/{version}/runs", Method: "post", Success: "202", Schema: "StartRunResponse", Security: "bearerAuth"},
	{Path: "/api/v1/runs", Method: "get", Success: "200", Schema: "RunPage", Security: "bearerAuth"},
	{Path: "/api/v1/runs/{run-id}", Method: "get", Success: "200", Schema: "RunSummary", Security: "bearerAuth"},
	{Path: "/api/v1/runs/{run-id}/tasks", Method: "get", Success: "200", Schema: "TaskPage", Security: "bearerAuth"},
	{Path: "/api/v1/runs/{run-id}/tasks/{task-key}", Method: "get", Success: "200", Schema: "TaskDetail", Security: "bearerAuth"},
	{Path: "/api/v1/runs/{run-id}/events", Method: "get", Success: "200", Schema: "EventPage", Security: "bearerAuth"},
	{Path: "/api/v1/runs/{run-id}/cancel", Method: "post", Success: "200", Schema: "CancelRunResponse", Security: "bearerAuth"},
	{Path: "/api/v1/workers/register", Method: "post", Success: "201", Schema: "RegisterWorkerResponse", Security: "workerBootstrapAuth"},
	{Path: "/api/v1/workers/{worker-id}/claims", Method: "post", Success: "200", Schema: "ClaimResponse", Security: "workerSessionAuth"},
	{Path: "/api/v1/workers/{worker-id}/heartbeat", Method: "post", Success: "200", Schema: "HeartbeatResponse", Security: "workerSessionAuth"},
	{Path: "/api/v1/workers/{worker-id}/leases/{dispatch-id}/complete", Method: "post", Success: "200", Schema: "CompleteResponse", Security: "workerSessionAuth"},
	{Path: "/api/v1/workers/{worker-id}/drain", Method: "post", Success: "200", Schema: "WorkerSummary", Security: "workerSessionAuth"},
	{Path: "/api/v1/workers", Method: "get", Success: "200", Schema: "WorkerPage", Security: "bearerAuth"},
	{Path: "/api/v1/workers/{worker-id}", Method: "get", Success: "200", Schema: "WorkerSummary", Security: "bearerAuth"},
}

func TestOpenAPI31DocumentsEveryImplementedOperationAndSuccessSchema(t *testing.T) {
	document, _ := loadOpenAPI(t)
	if document.OpenAPI != "3.1.0" {
		t.Fatalf("openapi = %q, want 3.1.0", document.OpenAPI)
	}
	wantOperations := make(map[string]struct{}, len(implementedOperations))
	for _, expected := range implementedOperations {
		key := expected.Method + " " + expected.Path
		wantOperations[key] = struct{}{}
		path, ok := document.Paths[expected.Path]
		if !ok {
			t.Errorf("OpenAPI missing path %s", expected.Path)
			continue
		}
		operation, ok := path[expected.Method]
		if !ok {
			t.Errorf("OpenAPI missing operation %s", key)
			continue
		}
		got := responseSchemaRef(t, document, operation.Responses[expected.Success])
		want := "#/components/schemas/" + expected.Schema
		if got != want {
			t.Errorf("%s success schema = %q, want %q", key, got, want)
		}
	}
	for path, methods := range document.Paths {
		for method := range methods {
			key := strings.ToLower(method) + " " + path
			if _, ok := wantOperations[key]; !ok {
				t.Errorf("OpenAPI documents unimplemented operation %s", key)
			}
		}
	}
}

func TestOpenAPIOperationsHaveSecurityErrorsParametersAndResolvableRefs(t *testing.T) {
	document, root := loadOpenAPI(t)
	for _, expected := range implementedOperations {
		operation := document.Paths[expected.Path][expected.Method]
		if expected.Security != "" {
			if !hasSecurity(operation.Security, expected.Security) {
				t.Errorf("%s %s missing %s", expected.Method, expected.Path, expected.Security)
			}
			if _, ok := document.Components.SecuritySchemes[expected.Security]; !ok {
				t.Errorf("%s %s references missing security scheme %s", expected.Method, expected.Path, expected.Security)
			}
			for _, status := range []string{"401", "500", "503"} {
				if responseSchemaRef(t, document, operation.Responses[status]) != "#/components/schemas/ErrorEnvelope" {
					t.Errorf("%s %s response %s must use ErrorEnvelope", expected.Method, expected.Path, status)
				}
			}
		}
		for status, response := range operation.Responses {
			resolved := resolveResponse(t, document, response)
			if scalar(mappingValue(resolved, "description")) == "" {
				t.Errorf("%s %s response %s has no description", expected.Method, expected.Path, status)
			}
			media := mappingValue(mappingValue(resolved, "content"), "application/json")
			if media.Kind != 0 && mappingValue(media, "schema").Kind == 0 {
				t.Errorf("%s %s response %s declares application/json without a schema", expected.Method, expected.Path, status)
			}
		}
	}
	runList := document.Paths["/api/v1/runs"]["get"]
	parameterNames := operationParameterNames(t, document, runList)
	for _, name := range []string{"cursor", "limit", "workflow_id", "status"} {
		if _, ok := parameterNames[name]; !ok {
			t.Errorf("GET /api/v1/runs missing parameter %q", name)
		}
	}

	refs := make(map[string]struct{})
	collectLocalRefs(root, refs)
	for ref := range refs {
		if resolveLocalRef(root, ref) == nil {
			t.Errorf("unresolved local reference %q", ref)
		}
	}
}

func TestOpenAPITaskDefinitionDocumentsStructuredInput(t *testing.T) {
	document, _ := loadOpenAPI(t)
	task := document.Components.Schemas["TaskDefinition"]
	input := mappingValue(mappingValue(task, "properties"), "input")
	if scalar(mappingValue(input, "type")) != "object" || scalar(mappingValue(input, "additionalProperties")) != "true" {
		t.Fatalf("TaskDefinition.input schema = %v, want object with arbitrary JSON properties", input)
	}
}

func TestOpenAPIStatusEnumsMatchImplementedProtocols(t *testing.T) {
	document, _ := loadOpenAPI(t)
	taskStatuses := sequenceValues(mappingValue(document.Components.Schemas["TaskStatus"], "enum"))
	if _, ok := taskStatuses["queued"]; !ok {
		t.Errorf("TaskStatus enum = %v, missing queued", taskStatuses)
	}

	completeRequest := document.Components.Schemas["CompleteRequest"]
	result := mappingValue(mappingValue(completeRequest, "properties"), "result")
	kind := mappingValue(mappingValue(result, "properties"), "kind")
	resultKinds := sequenceValues(mappingValue(kind, "enum"))
	wantKinds := map[string]struct{}{
		"success": {}, "temporary_failure": {}, "permanent_failure": {},
	}
	if len(resultKinds) != len(wantKinds) {
		t.Fatalf("CompleteRequest result kinds = %v, want %v", resultKinds, wantKinds)
	}
	for want := range wantKinds {
		if _, ok := resultKinds[want]; !ok {
			t.Fatalf("CompleteRequest result kinds = %v, missing %q", resultKinds, want)
		}
	}
}

func loadOpenAPI(t *testing.T) (openAPIDocument, *yaml.Node) {
	t.Helper()
	body, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var document openAPIDocument
	if err := yaml.Unmarshal(body, &document); err != nil {
		t.Fatalf("parse typed OpenAPI: %v", err)
	}
	var root yaml.Node
	if err := yaml.Unmarshal(body, &root); err != nil {
		t.Fatalf("parse OpenAPI node tree: %v", err)
	}
	return document, &root
}

func responseSchemaRef(t *testing.T, document openAPIDocument, response yaml.Node) string {
	t.Helper()
	if response.Kind == 0 {
		return ""
	}
	response = resolveResponse(t, document, response)
	content := mappingValue(response, "content")
	media := mappingValue(content, "application/json")
	schema := mappingValue(media, "schema")
	return scalar(mappingValue(schema, "$ref"))
}

func resolveResponse(t *testing.T, document openAPIDocument, response yaml.Node) yaml.Node {
	t.Helper()
	ref := scalar(mappingValue(response, "$ref"))
	if ref == "" {
		return response
	}
	const prefix = "#/components/responses/"
	if !strings.HasPrefix(ref, prefix) {
		t.Fatalf("response has unsupported ref %q", ref)
	}
	resolved, ok := document.Components.Responses[strings.TrimPrefix(ref, prefix)]
	if !ok {
		t.Fatalf("response ref %q does not exist", ref)
	}
	return resolved
}

func operationParameterNames(t *testing.T, document openAPIDocument, operation operation) map[string]struct{} {
	t.Helper()
	names := make(map[string]struct{})
	for _, parameter := range operation.Parameters {
		if ref := scalar(mappingValue(parameter, "$ref")); ref != "" {
			const prefix = "#/components/parameters/"
			if !strings.HasPrefix(ref, prefix) {
				t.Fatalf("unsupported parameter ref %q", ref)
			}
			parameter = document.Components.Parameters[strings.TrimPrefix(ref, prefix)]
		}
		names[scalar(mappingValue(parameter, "name"))] = struct{}{}
	}
	return names
}

func hasSecurity(security []map[string][]string, scheme string) bool {
	for _, requirement := range security {
		if _, ok := requirement[scheme]; ok {
			return true
		}
	}
	return false
}

func mappingValue(node yaml.Node, key string) yaml.Node {
	if node.Kind == yaml.DocumentNode && len(node.Content) == 1 {
		node = *node.Content[0]
	}
	if node.Kind != yaml.MappingNode {
		return yaml.Node{}
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Value == key {
			return *node.Content[index+1]
		}
	}
	return yaml.Node{}
}

func scalar(node yaml.Node) string {
	if node.Kind != yaml.ScalarNode {
		return ""
	}
	return node.Value
}

func sequenceValues(node yaml.Node) map[string]struct{} {
	values := make(map[string]struct{})
	if node.Kind != yaml.SequenceNode {
		return values
	}
	for _, item := range node.Content {
		values[item.Value] = struct{}{}
	}
	return values
}

func collectLocalRefs(node *yaml.Node, refs map[string]struct{}) {
	if node == nil {
		return
	}
	if node.Kind == yaml.MappingNode {
		for index := 0; index+1 < len(node.Content); index += 2 {
			if node.Content[index].Value == "$ref" && strings.HasPrefix(node.Content[index+1].Value, "#/") {
				refs[node.Content[index+1].Value] = struct{}{}
			}
		}
	}
	for _, child := range node.Content {
		collectLocalRefs(child, refs)
	}
}

func resolveLocalRef(root *yaml.Node, ref string) *yaml.Node {
	if !strings.HasPrefix(ref, "#/") {
		return nil
	}
	current := root
	if current.Kind == yaml.DocumentNode && len(current.Content) == 1 {
		current = current.Content[0]
	}
	for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
		if current.Kind != yaml.MappingNode {
			return nil
		}
		var next *yaml.Node
		for index := 0; index+1 < len(current.Content); index += 2 {
			if current.Content[index].Value == part {
				next = current.Content[index+1]
				break
			}
		}
		if next == nil {
			return nil
		}
		current = next
	}
	return current
}
