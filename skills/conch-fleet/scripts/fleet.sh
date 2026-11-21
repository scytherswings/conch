#!/bin/sh
# Read the conch fleet's live inventory: GET /fleet on the arbiter.
#
#   CONCH_ARBITER=https://conch-lease.example.workers.dev \
#   CONCH_AUTH_SECRET=... scripts/fleet.sh
#
# The read is observation only — it claims, renews, and releases nothing — so it
# is safe against a production arbiter. Prints the arbiter's JSON verbatim; pipe
# it to jq for a table. If the `conch` binary is installed, prefer
# `conch fleet --arbiter <url> --json`, which is the same read with nicer output.
set -eu

: "${CONCH_ARBITER:?set CONCH_ARBITER to your arbiter base URL, e.g. https://conch-lease.example.workers.dev}"

# The secret may come inline or from a file, mirroring the supervisor's own
# arbiter.secret_env / arbiter.secret_file rules so there is one story about
# where it lives.
secret="${CONCH_AUTH_SECRET:-}"
if [ -z "$secret" ] && [ -n "${CONCH_AUTH_SECRET_FILE:-}" ] && [ -f "$CONCH_AUTH_SECRET_FILE" ]; then
  secret=$(cat "$CONCH_AUTH_SECRET_FILE")
fi
: "${secret:?set CONCH_AUTH_SECRET, or CONCH_AUTH_SECRET_FILE to a readable file}"

# Accept a bare base URL or a URL that already points at the endpoint.
url="${CONCH_ARBITER%/}"
case "$url" in
  */fleet) ;;
  *) url="$url/fleet" ;;
esac

exec curl -fsS -H "X-Conch-Auth: $secret" "$url"
