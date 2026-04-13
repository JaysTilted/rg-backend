#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-jay@23.88.127.9}"
REMOTE_CONTAINER="${REMOTE_CONTAINER:-supabase-supabase-db-1}"
REMOTE_PORT="${REMOTE_PORT:-5432}"
LOCAL_PORT="${LOCAL_PORT:-65432}"
REMOTE_NETWORK="${REMOTE_NETWORK:-supabase_supabase-internal}"
MODE="${1:-foreground}"

get_remote_ip() {
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no "$REMOTE_HOST" \
    "docker inspect ${REMOTE_CONTAINER} | python3 -c 'import sys, json; data=json.load(sys.stdin)[0][\"NetworkSettings\"][\"Networks\"]; pref=\"${REMOTE_NETWORK}\"; print(data.get(pref, {}).get(\"IPAddress\") or next(iter(data.values())).get(\"IPAddress\", \"\"))'"
}

REMOTE_IP="$(get_remote_ip)"
if [[ -z "${REMOTE_IP}" ]]; then
  echo "Unable to resolve remote DB container IP for ${REMOTE_CONTAINER}" >&2
  exit 1
fi

echo "Forwarding localhost:${LOCAL_PORT} -> ${REMOTE_IP}:${REMOTE_PORT} via ${REMOTE_HOST}"

SSH_ARGS=(
  -o BatchMode=yes
  -o StrictHostKeyChecking=no
  -o ExitOnForwardFailure=yes
  -g
  -L "0.0.0.0:${LOCAL_PORT}:${REMOTE_IP}:${REMOTE_PORT}"
  "${REMOTE_HOST}"
  -N
)

if [[ "${MODE}" == "background" ]]; then
  ssh -f "${SSH_ARGS[@]}"
  echo "Started tunnel in background."
else
  exec ssh "${SSH_ARGS[@]}"
fi
