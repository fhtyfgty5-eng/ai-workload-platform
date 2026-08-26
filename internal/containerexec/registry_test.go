package containerexec

import (
	"testing"
	"time"
)

func TestRegistryRejectsUnboundedAction(t *testing.T) {
	_, err := NewRegistry([]ActionSpec{{
		Name: "document.normalize", Image: "example/normalize:1.0", Entrypoint: []string{"/app/normalize"},
		Limits: ResourceLimits{CPU: 1000, MemoryBytes: 64 << 20, EphemeralStorageBytes: 1 << 20, PidsLimit: 32, Timeout: time.Second},
	}}, ResourceLimits{CPU: 500, MemoryBytes: 32 << 20, EphemeralStorageBytes: 1 << 20, PidsLimit: 16, Timeout: time.Second})
	if err == nil {
		t.Fatal("NewRegistry() error = nil, want resource limit rejection")
	}
}

func TestRegistryResolvesFixedActionAndRejectsUnknown(t *testing.T) {
	registry, err := NewRegistry([]ActionSpec{{
		Name: "document.normalize", Image: "example/normalize@sha256:abc", Entrypoint: []string{"/app/normalize"},
		Limits: ResourceLimits{CPU: 100, MemoryBytes: 16 << 20, EphemeralStorageBytes: 1 << 20, PidsLimit: 16, Timeout: time.Second},
	}}, ResourceLimits{CPU: 500, MemoryBytes: 32 << 20, EphemeralStorageBytes: 2 << 20, PidsLimit: 32, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	if _, ok := registry.Resolve("document.normalize"); !ok {
		t.Fatal("Resolve() did not find registered action")
	}
	if _, ok := registry.Resolve("sh -c id"); ok {
		t.Fatal("Resolve() accepted unknown shell action")
	}
}
