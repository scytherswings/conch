#!/usr/bin/env bash
#
# Bootstraps the DynamoDB arbiter backend (design spec §9): one table, one
# scoped IAM user, and — only the first time it is missing — one access key
# pair printed to stdout for pasting into each box's aws.env. Run once from
# a workstation carrying your OWN high-access AWS credentials (whatever the
# ambient AWS CLI environment already resolves — a profile, an SSO session,
# env vars); this script never reads, writes, or stores those credentials
# anywhere itself, and the access key it creates belongs to a narrowly
# scoped runtime user, not to you.
#
# Every step below is check-then-act: describe/get before create, so a
# second run repairs a partial bootstrap instead of duplicating or erroring
# on state that already exists. Nothing here ever touches the fleet item's
# live svc/host maps if they already exist — see step 3.
#
# Usage:
#   deploy/dynamo/bootstrap.sh [REGION] [TABLE]
#
#   REGION defaults to us-west-2, TABLE defaults to conch. Both are
#   positional so the common case is `deploy/dynamo/bootstrap.sh` with no
#   arguments at all.
#
# Requires: aws CLI v2, credentials with dynamodb:CreateTable/DescribeTable/
# PutItem/GetItem and iam:CreatePolicy/GetPolicy/CreateUser/GetUser/
# AttachUserPolicy/CreateAccessKey/ListAccessKeys/GetCallerIdentity on this
# account. That admin-level access is used interactively, right here, and
# never persisted.

set -euo pipefail

REGION="${1:-us-west-2}"
TABLE="${2:-conch}"

# The IAM policy/user names are fixed, not parameterized: one runtime
# identity per AWS account, shared by every conch host talking to this
# table, matching how the credentials file is documented in the design spec
# (spec §9) and the config's arbiter.credentials_file field.
POLICY_NAME="conch-arbiter"
USER_NAME="conch-arbiter"

log() {
	printf '==> %s\n' "$1" >&2
}

# ---------------------------------------------------------------------------
# Step 1: the table itself — pk (S) as the sole hash key, provisioned
# capacity (design spec §5 explicitly rejects PAY_PER_REQUEST: on-demand
# mode is not part of the DynamoDB always-free tier, and the free tier's
# 25/25 provisioned RCU/WCU is exactly what this backend is designed to fit
# inside). No sort key: every item this backend ever writes (`fleet`,
# `meta`) is addressed by pk alone.
# ---------------------------------------------------------------------------
if aws dynamodb describe-table --region "$REGION" --table-name "$TABLE" >/dev/null 2>&1; then
	log "table $TABLE already exists in $REGION"
else
	log "creating table $TABLE in $REGION (pk S HASH, provisioned 25/25)"
	aws dynamodb create-table \
		--region "$REGION" \
		--table-name "$TABLE" \
		--attribute-definitions AttributeName=pk,AttributeType=S \
		--key-schema AttributeName=pk,KeyType=HASH \
		--provisioned-throughput ReadCapacityUnits=25,WriteCapacityUnits=25 \
		>/dev/null
fi
# Outside the if/else on purpose: a re-run of this script after an earlier
# run was interrupted mid-creation finds the table already present (the
# describe-table branch above) but possibly still CREATING, not yet
# ACTIVE — skipping the wait in that case would send step 2's get-item
# straight into a raw, unexplained AWS error. `wait table-exists` polls
# until ACTIVE and returns immediately as a no-op if the table already is.
log "confirming $TABLE is ACTIVE (no-op if it already is)"
aws dynamodb wait table-exists --region "$REGION" --table-name "$TABLE"

TABLE_ARN="$(aws dynamodb describe-table --region "$REGION" --table-name "$TABLE" \
	--query 'Table.TableArn' --output text)"

# ---------------------------------------------------------------------------
# Step 2: the meta item — schema version, written once, read by nothing yet
# (a future fail-fast startup check is spec §9's stated reason for it
# existing at all). Plain existence check: this item is never live
# application state the way the fleet item is, so there is no CAS to guard
# here, only a "don't bother re-writing it" short-circuit.
# ---------------------------------------------------------------------------
META_ITEM="$(aws dynamodb get-item --region "$REGION" --table-name "$TABLE" \
	--key '{"pk":{"S":"meta"}}' --query 'Item' --output text)"
if [ "$META_ITEM" = "None" ]; then
	log "writing meta item (schema=1)"
	aws dynamodb put-item --region "$REGION" --table-name "$TABLE" \
		--item '{"pk":{"S":"meta"},"schema":{"N":"1"}}' \
		>/dev/null
else
	log "meta item already present, leaving it alone"
fi

# ---------------------------------------------------------------------------
# Step 3: the fleet item — belt-and-braces only. internal/dynarbiter's own
# client seeds this item lazily on its first poll (svc/host maps via
# if_not_exists, unconditioned, idempotent by construction — see
# client.go's seed() and expr.go's buildSeedFleet/buildSeedHost), so this
# step exists purely so a bootstrap run before any host has ever polled
# leaves the table in the same state a first poll would. The
# ConditionExpression is the one thing standing between this line and
# clobbering a fleet that has been running for a year: if the item already
# exists — seeded by a client, or by an earlier bootstrap run — the
# condition fails, the CLI reports a ConditionalCheckFailedException, and
# this script treats that as success. Any OTHER failure is fatal, because
# it means something is actually wrong (bad credentials, wrong region,
# throttling), not that the fleet item is merely already there.
# ---------------------------------------------------------------------------
log "conditionally seeding fleet item (attribute_not_exists(pk) — never overwrites live state)"
set +e
PUT_OUTPUT="$(aws dynamodb put-item --region "$REGION" --table-name "$TABLE" \
	--item '{"pk":{"S":"fleet"},"svc":{"M":{}},"host":{"M":{}}}' \
	--condition-expression 'attribute_not_exists(pk)' 2>&1)"
PUT_RC=$?
set -e
if [ "$PUT_RC" -ne 0 ]; then
	if printf '%s' "$PUT_OUTPUT" | grep -q "ConditionalCheckFailedException"; then
		log "fleet item already exists; left untouched"
	else
		printf '%s\n' "$PUT_OUTPUT" >&2
		exit 1
	fi
fi

# ---------------------------------------------------------------------------
# Step 4: the IAM policy — exactly the two actions the runtime client ever
# calls (internal/dynarbiter/client.go's DDB interface is GetItem and
# UpdateItem, nothing else), scoped to this one table's ARN. Bootstrap
# itself does its own writes with YOUR ambient credentials, never this
# policy's — the runtime user this policy attaches to cannot create tables,
# read other tables, or touch IAM.
# ---------------------------------------------------------------------------
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"

if aws iam get-policy --policy-arn "$POLICY_ARN" >/dev/null 2>&1; then
	log "IAM policy $POLICY_NAME already exists"
else
	log "creating IAM policy $POLICY_NAME (GetItem+UpdateItem on $TABLE_ARN only)"
	POLICY_DOCUMENT="$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ConchArbiterRuntime",
      "Effect": "Allow",
      "Action": ["dynamodb:GetItem", "dynamodb:UpdateItem"],
      "Resource": "${TABLE_ARN}"
    }
  ]
}
JSON
)"
	aws iam create-policy \
		--policy-name "$POLICY_NAME" \
		--description "conch's runtime access to its one DynamoDB arbiter table: GetItem+UpdateItem only, nothing else" \
		--policy-document "$POLICY_DOCUMENT" \
		>/dev/null
fi

# ---------------------------------------------------------------------------
# Step 5: the IAM user and its policy attachment. AttachUserPolicy is
# idempotent on AWS's side (re-attaching an already-attached policy is a
# no-op, not an error), so it runs unconditionally rather than needing its
# own existence check.
# ---------------------------------------------------------------------------
if aws iam get-user --user-name "$USER_NAME" >/dev/null 2>&1; then
	log "IAM user $USER_NAME already exists"
else
	log "creating IAM user $USER_NAME"
	aws iam create-user --user-name "$USER_NAME" >/dev/null
fi

aws iam attach-user-policy --user-name "$USER_NAME" --policy-arn "$POLICY_ARN"
log "policy $POLICY_NAME attached to $USER_NAME"

# ---------------------------------------------------------------------------
# Step 6: the access key — created, and printed, ONLY if this user has none.
# AWS caps a user at two access keys; a bootstrap re-run against a user that
# already has one must not silently mint a second, unlabeled key nobody
# asked for. The key pair is printed to stdout and nowhere else — this
# script does not write it to a file, does not log it, and holds no copy of
# it once the process exits.
# ---------------------------------------------------------------------------
EXISTING_KEYS="$(aws iam list-access-keys --user-name "$USER_NAME" \
	--query 'length(AccessKeyMetadata)' --output text)"

if [ "$EXISTING_KEYS" -eq 0 ]; then
	log "creating access key for $USER_NAME"
	# One API call, both fields read from its one response — calling
	# create-access-key twice to fetch id then secret would mint two keys.
	#
	# The assignment is split in two on purpose: `set -e` does NOT trip on a
	# failing command substitution when it sits inline inside another
	# command's argument (e.g. `read ... <<<"$(cmd)"` — the exit status
	# `errexit` sees there is `read`'s, not `cmd`'s, so a failed `aws` call
	# would silently print empty AWS_ACCESS_KEY_ID=/AWS_SECRET_ACCESS_KEY=
	# lines and "bootstrap complete" right after). Assigning the
	# substitution to a plain variable first makes ITS exit status the
	# assignment's own, which `set -e` does honor.
	KEYPAIR="$(aws iam create-access-key \
		--user-name "$USER_NAME" \
		--query '[AccessKey.AccessKeyId, AccessKey.SecretAccessKey]' \
		--output text)"
	IFS=$'\t' read -r ACCESS_KEY_ID SECRET_ACCESS_KEY <<<"$KEYPAIR"
	cat <<EOF

# paste into /etc/conch/hooks/aws.env on each box, chmod 600
AWS_ACCESS_KEY_ID=${ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY=${SECRET_ACCESS_KEY}
EOF
else
	log "$USER_NAME already has $EXISTING_KEYS access key(s); not creating another (AWS allows at most 2 per user) — reuse the existing pair from wherever it was first pasted, or delete one key first if you need a fresh pair"
fi

log "bootstrap complete: table=$TABLE region=$REGION policy=$POLICY_NAME user=$USER_NAME"
