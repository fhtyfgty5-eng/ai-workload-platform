#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VERIFY="$REPO_ROOT/scripts/verify-docs.sh"
FIXTURE=$(mktemp -d)
trap 'rm -rf "$FIXTURE"' EXIT

prepare_fixture() {
  rm -rf "$FIXTURE"
  mkdir -p "$FIXTURE"
  cp "$REPO_ROOT/README.md" "$FIXTURE/README.md"
  cp "$REPO_ROOT/项目状态.md" "$FIXTURE/项目状态.md"
  cp -R "$REPO_ROOT/docs" "$FIXTURE/docs"
  git -C "$FIXTURE" init -q
  git -C "$FIXTURE" config user.email test@example.invalid
  git -C "$FIXTURE" config user.name docs-test
  git -C "$FIXTURE" add .
  git -C "$FIXTURE" commit -qm baseline
}

expect_success() {
  local name=$1
  if "$VERIFY" "$FIXTURE" >/dev/null; then
    printf 'PASS %s\n' "$name"
  else
    printf 'FAIL %s: expected success\n' "$name" >&2
    exit 1
  fi
}

expect_failure() {
  local name=$1
  if "$VERIFY" "$FIXTURE" >/dev/null 2>&1; then
    printf 'FAIL %s: expected failure\n' "$name" >&2
    exit 1
  else
    printf 'PASS %s\n' "$name"
  fi
}

prepare_fixture
expect_success baseline

rm "$FIXTURE/README.md"
expect_failure missing_required_file

prepare_fixture
rm "$FIXTURE/docs/计划/模块1需求与设计.md"
expect_failure missing_current_module_design

prepare_fixture
rm "$FIXTURE/docs/学习/文章/模块1-单机可靠工作流内核设计与验证基础.md"
expect_failure missing_current_module_learning_article

prepare_fixture
printf '\n[坏链接](docs/不存在.md)\n' >> "$FIXTURE/README.md"
expect_failure broken_local_link

prepare_fixture
printf '\n<<<<<<< conflict\n' >> "$FIXTURE/README.md"
expect_failure conflict_marker

prepare_fixture
printf '\n本机目录：/Users/example/private-project\n' >> "$FIXTURE/README.md"
expect_failure local_absolute_path

prepare_fixture
printf '未跟踪文档包含尾随空白  \n' > "$FIXTURE/docs/未跟踪文档.md"
expect_failure untracked_markdown_trailing_whitespace

printf '8/8 scenarios passed\n'
