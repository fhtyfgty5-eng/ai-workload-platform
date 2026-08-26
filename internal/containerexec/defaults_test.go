package containerexec

import (
	"testing"
	"time"
)

func TestDefaultActionSpecsUseOneApprovedImageAndFixedActions(t *testing.T) {
	specs, limits := DefaultActionSpecs("local/action@sha256:abc")
	if len(specs) < 3 {
		t.Fatalf("DefaultActionSpecs() returned %d actions, want at least 3", len(specs))
	}
	if limits.Timeout <= 0 {
		t.Fatal("default registry timeout must be positive")
	}
	for _, spec := range specs {
		if spec.Image != "local/action@sha256:abc" || len(spec.Entrypoint) == 0 {
			t.Fatalf("action is not bound to the approved image and entrypoint: %+v", spec)
		}
		if spec.Network != NetworkNone || spec.OutputLimitBytes <= 0 {
			t.Fatalf("action has unsafe defaults: %+v", spec)
		}
	}
}

func TestDefaultActionSpecsHaveStableInputSchemas(t *testing.T) {
	specs, _ := DefaultActionSpecs("local/action@sha256:abc")
	byName := make(map[string]ActionSpec, len(specs))
	for _, spec := range specs {
		byName[spec.Name] = spec
	}
	if byName["document.normalize"].InputSchema["source"] != "string" {
		t.Fatalf("normalize schema = %+v", byName["document.normalize"].InputSchema)
	}
	if byName["resource.cpu-burn"].InputSchema["milliseconds"] != "number" {
		t.Fatalf("cpu-burn schema = %+v", byName["resource.cpu-burn"].InputSchema)
	}
	if byName["document.summarize"].Limits.Timeout < time.Second {
		t.Fatalf("summarize timeout = %s", byName["document.summarize"].Limits.Timeout)
	}
}
