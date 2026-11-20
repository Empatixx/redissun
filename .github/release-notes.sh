#!/usr/bin/env bash
# Prints the notes of the next release: one heading per pull request merged since the latest release,
# followed by the "## Changelog" section of its description. Set SINCE to start from another time.
set -euo pipefail

since=${SINCE:-$(gh release view --json publishedAt --jq .publishedAt 2>/dev/null || echo "1970-01-01T00:00:00Z")}

gh pr list --state merged --base main --search "merged:>$since" --limit 200 --json number,title,body --jq '
  def changes:
    (. // "") | gsub("\r"; "") | split("\n")
    | reduce .[] as $line ({on: false, out: []};
        if ($line | test("^## Changelog\\s*$")) then .on = true
        elif ($line | test("^## ")) then .on = false
        elif .on and ($line | test("\\S")) then .out += [$line]
        else . end)
    | .out | join("\n");
  sort_by(.number)[]
  | (.body | changes) as $changes
  | "### \(.title) (#\(.number))\n" + (if $changes == "" then "" else "\n" + $changes + "\n" end)
' </dev/null
