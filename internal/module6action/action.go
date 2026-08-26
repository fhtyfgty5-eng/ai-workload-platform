// Package module6action 实现模块 6 镜像内置的少量确定性示例动作。
package module6action

import (
	"fmt"
	"strings"
	"time"
)

// Run 只解释固定动作名和结构化参数，不执行 Shell、脚本或用户提供的程序。
func Run(name string, input map[string]any) (string, error) {
	switch name {
	case "document.normalize":
		source, err := stringInput(input, "source")
		if err != nil {
			return "", err
		}
		return strings.Join(strings.Fields(source), " "), nil
	case "document.summarize":
		source, err := stringInput(input, "source")
		if err != nil {
			return "", err
		}
		maxWords := 50
		if value, ok := input["max_words"]; ok {
			number, ok := value.(float64)
			if !ok || number < 1 || number > 1000 {
				return "", fmt.Errorf("max_words must be between 1 and 1000")
			}
			maxWords = int(number)
		}
		words := strings.Fields(source)
		if len(words) > maxWords {
			words = words[:maxWords]
		}
		return strings.Join(words, " "), nil
	case "resource.cpu-burn":
		milliseconds, err := boundedNumber(input, "milliseconds", 1, 60_000)
		if err != nil {
			return "", err
		}
		deadline := time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
		var value uint64
		for time.Now().Before(deadline) {
			value = value*1664525 + 1013904223
		}
		return fmt.Sprintf("cpu-burn:%d", value), nil
	case "resource.memory-burn":
		megabytes, err := boundedNumber(input, "megabytes", 1, 64)
		if err != nil {
			return "", err
		}
		buffer := make([]byte, megabytes*1024*1024)
		for index := range buffer {
			buffer[index] = byte(index)
		}
		return fmt.Sprintf("memory-burn:%d", len(buffer)), nil
	case "resource.output-burn":
		bytes, err := boundedNumber(input, "bytes", 1, 1<<20)
		if err != nil {
			return "", err
		}
		return strings.Repeat("x", bytes), nil
	default:
		return "", fmt.Errorf("action %q is not registered", name)
	}
}

func stringInput(input map[string]any, key string) (string, error) {
	value, ok := input[key].(string)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s must be a non-empty string", key)
	}
	return value, nil
}

func boundedNumber(input map[string]any, key string, min, max int) (int, error) {
	value, ok := input[key].(float64)
	if !ok || value < float64(min) || value > float64(max) {
		return 0, fmt.Errorf("%s must be between %d and %d", key, min, max)
	}
	return int(value), nil
}
