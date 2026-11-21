#!/usr/bin/env bash
# Runs internal/dynarbiter's integration suite (integration_test.go, guarded
# behind DYNAMO_LOCAL_ENDPOINT) against a throwaway amazon/dynamodb-local
# container. The tests create and drop their own tables inside the
# container's one lifetime, so this script's only jobs are: get the
# container up, point the tests at it, tear it down no matter what happened.
set -euo pipefail

PORT=8000
CONTAINER_NAME="conch-dynamo-local-test-$$-$(date +%s)"

if ! command -v docker >/dev/null 2>&1; then
    echo "dynamo-local-test.sh: docker is not on PATH" >&2
    exit 1
fi

if lsof -iTCP:"${PORT}" -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "dynamo-local-test.sh: port ${PORT} is already in use — stop whatever is bound to it and re-run" >&2
    exit 1
fi

cleanup() {
    docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "dynamo-local-test.sh: starting ${CONTAINER_NAME} on :${PORT}"
docker run -d --name "${CONTAINER_NAME}" -p "${PORT}:8000" amazon/dynamodb-local >/dev/null

echo "dynamo-local-test.sh: waiting for DynamoDB Local to accept connections"
deadline=$((SECONDS + 30))
until curl -s -o /dev/null "http://localhost:${PORT}"; do
    if [ "${SECONDS}" -ge "${deadline}" ]; then
        echo "dynamo-local-test.sh: DynamoDB Local did not come up within 30s" >&2
        docker logs "${CONTAINER_NAME}" >&2 || true
        exit 1
    fi
    sleep 0.5
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
module_root="$(cd "${script_dir}/.." && pwd)"

echo "dynamo-local-test.sh: running the integration suite"
(
    cd "${module_root}"
    DYNAMO_LOCAL_ENDPOINT="http://localhost:${PORT}" \
        go test -run Integration -count=1 -v ./internal/dynarbiter/
)

echo "dynamo-local-test.sh: PASS"
