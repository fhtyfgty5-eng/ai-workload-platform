package containerexec

import (
	"testing"
)

func TestBuildDockerConfigKeepsSecurityDefaults(t *testing.T) {
	spec := ContainerSpec{
		Image: "example/normalize@sha256:abc", Entrypoint: []string{"/app/normalize"}, Arguments: []string{"{}"},
		User: "65532:65532", ReadOnlyRootFS: true, NoNewPrivileges: true, NetworkMode: "none", CapDrop: []string{"ALL"},
		CPU: 100, MemoryBytes: 16 << 20, EphemeralStorageBytes: 1 << 20, PidsLimit: 16,
	}
	config, host := buildDockerConfig(spec)
	if config.User != spec.User || !host.ReadonlyRootfs || host.Privileged || host.NetworkMode != "none" || len(host.Binds) != 0 {
		t.Fatalf("Docker config lost security defaults: config=%+v host=%+v", config, host)
	}
	if host.PidsLimit == nil || *host.PidsLimit != spec.PidsLimit || host.Memory != spec.MemoryBytes || host.NanoCPUs == 0 {
		t.Fatalf("Docker resource limits = %+v, want %+v", host, spec)
	}
}
