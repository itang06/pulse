#!/usr/bin/env bash
set -euo pipefail

if (($# != 3)); then
  printf 'usage: source-manifest.sh REPOSITORY OUTPUT SCRATCH_DIR\n' >&2
  exit 2
fi

repository="$1"
output="$2"
scratch_dir="$3"
mkdir -p "$scratch_dir"
raw_paths="$scratch_dir/source-manifest-paths.raw"
sorted_paths="$scratch_dir/source-manifest-paths.sorted"

git -C "$repository" ls-files --cached --others --exclude-standard -z >"$raw_paths"
LC_ALL=C perl -e '
  local $/;
  my $data = <>;
  my @paths = grep { length $_ } split(/\0/, $data);
  print "$_\0" for sort { $a cmp $b } @paths;
' "$raw_paths" >"$sorted_paths"

: >"$output"
missing=0
unsupported=0
while IFS= read -r -d '' path; do
  file="$repository/$path"
  if [[ -L "$file" ]]; then
    if ! target="$(readlink "$file")"; then
      jq -cn --arg path "$path" '{path:$path,type:"unsupported_symlink"}' >>"$output"
      unsupported=1
      continue
    fi
    checksum="$(printf 'symlink\0%s' "$target" | shasum -a 256 | awk '{print $1}')"
    jq -cn --arg path "$path" --arg target "$target" --arg target_sha256 "$checksum" \
      '{path:$path,type:"symlink",target:$target,target_sha256:$target_sha256}' >>"$output"
  elif [[ -f "$file" ]]; then
    checksum="$(shasum -a 256 "$file" | awk '{print $1}')"
    jq -cn --arg path "$path" --arg sha256 "$checksum" '{path:$path,type:"file",sha256:$sha256}' >>"$output"
  elif [[ -e "$file" ]]; then
    jq -cn --arg path "$path" '{path:$path,type:"unsupported"}' >>"$output"
    unsupported=1
  else
    jq -cn --arg path "$path" '{path:$path,type:"missing",missing:true}' >>"$output"
    missing=1
  fi
done <"$sorted_paths"

if ((missing)); then
  printf 'source manifest contains missing paths; see %s\n' "$output" >&2
fi
if ((unsupported)); then
  printf 'source manifest contains unsupported filesystem entries; see %s\n' "$output" >&2
fi
if ((missing || unsupported)); then
  exit 1
fi
