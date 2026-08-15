#!/usr/bin/env bash
set -uo pipefail

if [[ $# -gt 1 ]]; then
  printf 'usage: %s [repository-root]\n' "$0" >&2
  exit 2
fi

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=${1:-$(cd "$SCRIPT_DIR/.." && pwd)}
if ! ROOT=$(cd "$ROOT" 2>/dev/null && pwd); then
  printf 'FAIL repository root does not exist: %s\n' "$ROOT" >&2
  exit 1
fi

failures=0
fail() {
  printf 'FAIL %s\n' "$1" >&2
  failures=$((failures + 1))
}

required_files=(
  "README.md"
  "项目状态.md"
  "docs/新窗口启动协议.md"
  "docs/产品/产品说明.md"
  "docs/架构/架构基线.md"
  "docs/学习/学习资料规范.md"
  "docs/学习/CSDN发布模板.md"
  "docs/学习/文章/模块0-工作流-DAG-状态机与可靠执行基础.md"
  "docs/学习/文章/模块1-单机可靠工作流内核设计与验证基础.md"
  "docs/计划/项目路线图.md"
  "docs/计划/模块0详细计划.md"
  "docs/计划/模块1需求与设计.md"
  "docs/决策/ADR-0001-项目范围.md"
  "docs/决策/ADR-0002-产品定位与模块路线调整.md"
)

for relative_path in "${required_files[@]}"; do
  if [[ ! -f "$ROOT/$relative_path" ]]; then
    fail "missing required file: $relative_path"
  fi
done
if [[ $failures -eq 0 ]]; then
  printf 'PASS required_files (%d)\n' "${#required_files[@]}"
fi

if command -v rg >/dev/null 2>&1; then
  conflict_output=$(rg -n '^(<<<<<<<|=======|>>>>>>>)' "$ROOT/README.md" "$ROOT/项目状态.md" "$ROOT/docs" 2>/dev/null || true)
  if [[ -n "$conflict_output" ]]; then
    printf '%s\n' "$conflict_output" >&2
    fail 'conflict markers'
  else
    printf 'PASS conflict_markers\n'
  fi

  local_path_output=$(rg -n --glob '*.md' '(/Users/[^/[:space:]]+/|/home/[^/[:space:]]+/|[A-Za-z]:\\Users\\[^\\[:space:]]+\\)' "$ROOT/README.md" "$ROOT/项目状态.md" "$ROOT/docs" 2>/dev/null || true)
  if [[ -n "$local_path_output" ]]; then
    printf '%s\n' "$local_path_output" >&2
    fail 'local absolute paths in Markdown'
  else
    printf 'PASS local_absolute_paths\n'
  fi
else
  fail 'rg is required for conflict marker and local path checks'
fi

if python3 - "$ROOT" <<'PY'
from pathlib import Path
import re
import sys
from urllib.parse import unquote

root = Path(sys.argv[1]).resolve()
pattern = re.compile(r"\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
errors = []
checked = 0

for markdown_file in root.rglob("*.md"):
    if ".git" in markdown_file.parts:
        continue
    for line_number, line in enumerate(markdown_file.read_text(encoding="utf-8").splitlines(), 1):
        if line.endswith((" ", "\t")):
            errors.append(f"{markdown_file}:{line_number}: trailing whitespace")
        for raw_target in pattern.findall(line):
            if raw_target.startswith(("#", "http://", "https://", "mailto:")):
                continue
            target = unquote(raw_target.split("#", 1)[0])
            if not target:
                continue
            checked += 1
            target_path = (markdown_file.parent / target).resolve()
            try:
                target_path.relative_to(root)
            except ValueError:
                errors.append(f"{markdown_file}:{line_number}: target outside repository: {raw_target}")
                continue
            if not target_path.exists():
                errors.append(f"{markdown_file}:{line_number}: missing local link: {raw_target}")

if errors:
    print("\n".join(errors), file=sys.stderr)
    sys.exit(1)
print(f"PASS local_links ({checked})")
print("PASS markdown_trailing_whitespace")
PY
then
  :
else
  failures=$((failures + 1))
fi

if git -C "$ROOT" diff --check; then
  printf 'PASS git_diff_check\n'
else
  fail 'git diff --check'
fi

if [[ $failures -eq 0 ]]; then
  printf 'all document checks passed\n'
  exit 0
fi

printf '%d document check(s) failed\n' "$failures" >&2
exit 1
