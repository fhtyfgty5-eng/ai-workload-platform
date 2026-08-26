// workload-action 是模块 6 固定任务镜像的唯一入口。
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/fhtyfgty5-eng/ai-workload-platform/internal/module6action"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: workload-action <action> <json-input>")
		os.Exit(2)
	}
	input := make(map[string]any)
	if err := json.Unmarshal([]byte(os.Args[2]), &input); err != nil {
		fmt.Fprintf(os.Stderr, "invalid JSON input: %v\n", err)
		os.Exit(2)
	}
	output, err := module6action.Run(os.Args[1], input)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	fmt.Println(output)
}
