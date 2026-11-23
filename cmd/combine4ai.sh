#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'Usage: %s [directory] [output-file]\n' "$0" >&2
  printf 'Defaults: directory=., output=concatenated_text_files.txt\n' >&2
}

if [[ ${1:-} == '-h' || ${1:-} == '--help' ]]; then
  usage
  exit 0
fi

root=${1:-.}
output=${2:-concatenated_text_files.txt}

if [[ ! -d $root ]]; then
  printf 'Error: not a directory: %s\n' "$root" >&2
  exit 1
fi

root=$(cd -- "$root" && pwd -P)
out_dir=$(dirname -- "$output")
out_name=$(basename -- "$output")
mkdir -p -- "$out_dir"
out_dir=$(cd -- "$out_dir" && pwd -P)
output="$out_dir/$out_name"

# Truncate the destination before walking, and skip it if it is inside the tree.
: > "$output"

while IFS= read -r -d '' file_path; do
  [[ $file_path == "$output" ]] && continue

  # `file` identifies text encodings and empty files; binary files are skipped.
  description=$(file -b -- "$file_path" 2>/dev/null || true)
  case $description in
    *text*|empty) ;;
    *) continue ;;
  esac

  {
    printf '%s\n' "$file_path"
    cat -- "$file_path"
    printf '\n\n'
  } >> "$output"
done < <(find "$root" -type f -print0)

printf 'Wrote concatenated text files to %s\n' "$output"
