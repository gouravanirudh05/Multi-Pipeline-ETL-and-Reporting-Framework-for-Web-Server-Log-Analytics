parse_paths() {
  local raw="$1"
  if [[ "$raw" == \[* ]]; then
    printf '%s\n' "$raw" \
      | sed -e 's/^\[//' -e 's/\]$//' -e 's/","/\n/g' -e 's/^"//' -e 's/"$//'
  else
    printf '%s\n' "$raw"
  fi
}
mapfile -t LOG_FILES < <(parse_paths '["data/access_log_Jul95","data/access_log_Aug95"]')
for file in "${LOG_FILES[@]}"; do
    echo "FILE: $file"
done
