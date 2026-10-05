#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pulse-sink-harness-test.XXXXXX")"
trap 'rm -rf "$TEMP_DIR"' EXIT

COUNT_SENTINEL="$TEMP_DIR/count-injected"
CRASHES_SENTINEL="$TEMP_DIR/crashes-injected"
COUNT_PAYLOAD="1\"; touch $COUNT_SENTINEL; #"
CRASHES_PAYLOAD="3\"; touch $CRASHES_SENTINEL; #"
rendered="$(cd "$ROOT_DIR" && make -n verify-sink COUNT="$COUNT_PAYLOAD" CRASHES="$CRASHES_PAYLOAD")"
[[ "$rendered" == './scripts/verify-sink.sh' ]] || {
  printf 'Make target did not ignore custom variables: %s\n' "$rendered" >&2
  exit 1
}
[[ "$rendered" != *"$COUNT_PAYLOAD"* && "$rendered" != *"$CRASHES_PAYLOAD"* ]] || {
  printf 'untrusted Make variable appeared in the recipe: %s\n' "$rendered" >&2
  exit 1
}
[[ ! -e "$COUNT_SENTINEL" && ! -e "$CRASHES_SENTINEL" ]] || {
  printf 'Make dry run executed injected shell content\n' >&2
  exit 1
}

MANIFEST_FIXTURE="$TEMP_DIR/manifest-fixture"
mkdir -p "$MANIFEST_FIXTURE/src" "$MANIFEST_FIXTURE/ignored"
git -C "$MANIFEST_FIXTURE" init -q
git -C "$MANIFEST_FIXTURE" config user.name 'Harness Test'
git -C "$MANIFEST_FIXTURE" config user.email 'harness-test@example.invalid'
printf 'ignored/*\n' >"$MANIFEST_FIXTURE/.gitignore"
printf 'tracked source\n' >"$MANIFEST_FIXTURE/src/tracked.rs"
printf 'deleted tracked source\n' >"$MANIFEST_FIXTURE/src/deleted.rs"
git -C "$MANIFEST_FIXTURE" add .gitignore src/tracked.rs src/deleted.rs
printf 'untracked source\n' >"$MANIFEST_FIXTURE/src/untracked.rs"
printf 'IGNORED_SECRET_NEVER_MANIFEST\n' >"$MANIFEST_FIXTURE/ignored/cache.bin"
ln -s ../ignored/cache.bin "$MANIFEST_FIXTURE/src/tracked-secret-link"
git -C "$MANIFEST_FIXTURE" add src/tracked-secret-link
ln -s ../ignored/cache.bin "$MANIFEST_FIXTURE/src/untracked-secret-link"
ln -s ../missing/secret "$MANIFEST_FIXTURE/src/broken-link"
rm "$MANIFEST_FIXTURE/src/deleted.rs"
MANIFEST_ONE="$TEMP_DIR/source-manifest-one.jsonl"
MANIFEST_TWO="$TEMP_DIR/source-manifest-two.jsonl"
if "$ROOT_DIR/scripts/lib/source-manifest.sh" "$MANIFEST_FIXTURE" "$MANIFEST_ONE" "$TEMP_DIR"; then
  printf 'expected manifest generation to report a missing tracked file\n' >&2
  exit 1
fi
if "$ROOT_DIR/scripts/lib/source-manifest.sh" "$MANIFEST_FIXTURE" "$MANIFEST_TWO" "$TEMP_DIR"; then
  printf 'expected repeated manifest generation to report a missing tracked file\n' >&2
  exit 1
fi
cmp -s "$MANIFEST_ONE" "$MANIFEST_TWO" || {
  printf 'source manifest output is not deterministic\n' >&2
  exit 1
}
jq -se --arg path 'src/tracked.rs' 'any(.[]; .path == $path and (.sha256 | test("^[0-9a-f]{64}$")))' "$MANIFEST_ONE" >/dev/null || {
  printf 'manifest omitted the tracked source hash\n' >&2
  exit 1
}
jq -se --arg path 'src/untracked.rs' 'any(.[]; .path == $path and (.sha256 | test("^[0-9a-f]{64}$")))' "$MANIFEST_ONE" >/dev/null || {
  printf 'manifest omitted the non-ignored untracked source hash\n' >&2
  exit 1
}
jq -se --arg path 'ignored/cache.bin' 'all(.[]; .path != $path)' "$MANIFEST_ONE" >/dev/null || {
  printf 'manifest included an ignored file\n' >&2
  exit 1
}
jq -se --arg path 'src/deleted.rs' 'any(.[]; .path == $path and .missing == true)' "$MANIFEST_ONE" >/dev/null || {
  printf 'manifest did not explicitly record the missing tracked file\n' >&2
  exit 1
}
LINK_TARGET_HASH="$(printf 'symlink\0../ignored/cache.bin' | shasum -a 256 | awk '{print $1}')"
for link_path in src/tracked-secret-link src/untracked-secret-link; do
  jq -se --arg path "$link_path" --arg hash "$LINK_TARGET_HASH" \
    'any(.[]; .path == $path and .type == "symlink" and .target == "../ignored/cache.bin" and .target_sha256 == $hash)' \
    "$MANIFEST_ONE" >/dev/null || {
    printf 'manifest did not hash the symlink target representation for %s\n' "$link_path" >&2
    exit 1
  }
done
jq -se --arg hash "$(printf 'symlink\0../missing/secret' | shasum -a 256 | awk '{print $1}')" \
  'any(.[]; .path == "src/broken-link" and .type == "symlink" and .target == "../missing/secret" and .target_sha256 == $hash)' \
  "$MANIFEST_ONE" >/dev/null || {
  printf 'manifest did not record the broken symlink without dereferencing it\n' >&2
  exit 1
}
if grep -F 'IGNORED_SECRET_NEVER_MANIFEST' "$MANIFEST_ONE" >/dev/null; then
  printf 'manifest exposed the ignored symlink target contents\n' >&2
  exit 1
fi
SECRET_CONTENT_HASH="$(shasum -a 256 "$MANIFEST_FIXTURE/ignored/cache.bin" | awk '{print $1}')"
jq -se --arg hash "$SECRET_CONTENT_HASH" 'all(.[]; .sha256 != $hash)' "$MANIFEST_ONE" >/dev/null || {
  printf 'manifest hashed ignored symlink target contents\n' >&2
  exit 1
}

if "$ROOT_DIR/scripts/verify-sink.sh" "1; touch $TEMP_DIR/invalid-count-injected" 3 \
  >"$TEMP_DIR/invalid-count.log" 2>&1; then
  printf 'expected invalid count to fail before service checks\n' >&2
  exit 1
fi
[[ ! -e "$TEMP_DIR/invalid-count-injected" ]] || {
  printf 'invalid count input executed as shell code\n' >&2
  exit 1
}
if "$ROOT_DIR/scripts/verify-sink.sh" 1 "3; touch $TEMP_DIR/invalid-crashes-injected" \
  >"$TEMP_DIR/invalid-crashes.log" 2>&1; then
  printf 'expected invalid crash count to fail before service checks\n' >&2
  exit 1
fi
[[ ! -e "$TEMP_DIR/invalid-crashes-injected" ]] || {
  printf 'invalid crash count input executed as shell code\n' >&2
  exit 1
}

printf 'sink harness argument checks passed\n'
