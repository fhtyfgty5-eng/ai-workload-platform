package containerexec

import "time"

// DefaultActionSpecs 返回仓库示例使用的固定动作集合和平台总资源上限。
// image 必须由本地构建脚本生成带 digest 的镜像引用；调用方不能把请求中的镜像传入这里。
func DefaultActionSpecs(image string) ([]ActionSpec, ResourceLimits) {
	limits := ResourceLimits{
		CPU:                   1000,
		MemoryBytes:           256 << 20,
		EphemeralStorageBytes: 32 << 20,
		PidsLimit:             64,
		Timeout:               5 * time.Minute,
	}
	entrypoint := []string{"/action"}
	digest := image
	if image == "workload-action:local" {
		digest = "sha256:local"
	}
	return []ActionSpec{
		{
			Name: "document.normalize", Image: image, ImageDigest: digest, Entrypoint: entrypoint,
			InputSchema: InputSchema{"source": "string"},
			Limits:      ResourceLimits{CPU: 250, MemoryBytes: 64 << 20, EphemeralStorageBytes: 8 << 20, PidsLimit: 32, Timeout: 30 * time.Second},
			Network:     NetworkNone, OutputLimitBytes: 64 << 10,
		},
		{
			Name: "document.summarize", Image: image, ImageDigest: digest, Entrypoint: entrypoint,
			InputSchema: InputSchema{"source": "string", "max_words": "number"},
			Limits:      ResourceLimits{CPU: 250, MemoryBytes: 64 << 20, EphemeralStorageBytes: 8 << 20, PidsLimit: 32, Timeout: time.Minute},
			Network:     NetworkNone, OutputLimitBytes: 64 << 10,
		},
		{
			Name: "resource.cpu-burn", Image: image, ImageDigest: digest, Entrypoint: entrypoint,
			InputSchema: InputSchema{"milliseconds": "number"},
			Limits:      ResourceLimits{CPU: 500, MemoryBytes: 64 << 20, EphemeralStorageBytes: 8 << 20, PidsLimit: 32, Timeout: time.Minute},
			Network:     NetworkNone, OutputLimitBytes: 4 << 10,
		},
		{
			Name: "resource.memory-burn", Image: image, ImageDigest: digest, Entrypoint: entrypoint,
			InputSchema: InputSchema{"megabytes": "number"},
			Limits:      ResourceLimits{CPU: 500, MemoryBytes: 64 << 20, EphemeralStorageBytes: 8 << 20, PidsLimit: 32, Timeout: time.Minute},
			Network:     NetworkNone, OutputLimitBytes: 4 << 10,
		},
		{
			Name: "resource.output-burn", Image: image, ImageDigest: digest, Entrypoint: entrypoint,
			InputSchema: InputSchema{"bytes": "number"},
			Limits:      ResourceLimits{CPU: 250, MemoryBytes: 64 << 20, EphemeralStorageBytes: 8 << 20, PidsLimit: 32, Timeout: 30 * time.Second},
			Network:     NetworkNone, OutputLimitBytes: 64 << 10,
		},
	}, limits
}
