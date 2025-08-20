#!/bin/bash
set -euo pipefail

# --- Configuration ---
METABASE_URL="http://localhost:3000"
ADMIN_EMAIL="admin@example.com"
ADMIN_PASSWORD="AdminPass123!"
SITE_NAME="GA4 Analytics Dashboard"
DB_NAME="GA4 Analytics"
MAX_RETRIES=30
RETRY_INTERVAL=5
COOKIE_JAR="/tmp/metabase_cookie.jar"

# Try these hosts for the Metabase DB connection, in order
DB_HOST_CANDIDATES=("postgres" "host.docker.internal")

# --- Helpers ---
log() { echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"; }

wait_for_metabase() {
  log "Waiting for Metabase to be ready..."
  local n=0
  until [[ "$(curl -s -o /dev/null -w ''%{http_code}'' "$METABASE_URL/api/session/properties")" == "200" ]]; do
    ((n++))
    if (( n >= MAX_RETRIES )); then
      log "ERROR: Metabase did not become ready in time."
      exit 1
    fi
    log "Metabase not ready yet... (Attempt $n/$MAX_RETRIES)"
    sleep "$RETRY_INTERVAL"
  done
  log "✅ Metabase API is ready."
}

wait_for_postgres() {
  log "Waiting for PostgreSQL to accept connections..."
  local pg_container
  pg_container="$(docker ps --filter "name=postgres" --format '{{.Names}}' | head -n1 || true)"

  local n=0
  if [[ -n "$pg_container" ]]; then
    while ! docker exec "$pg_container" pg_isready -U postgres -h localhost -q >/dev/null 2>&1; do
      ((n++))
      if (( n >= MAX_RETRIES )); then
        log "WARN: pg_isready failed; falling back to host TCP check on localhost:5432"
        break
      fi
      log "Postgres not ready via pg_isready... (Attempt $n/$MAX_RETRIES)"
      sleep "$RETRY_INTERVAL"
    done
    if docker exec "$pg_container" pg_isready -U postgres -h localhost -q >/dev/null 2>&1; then
      log "✅ PostgreSQL is ready (pg_isready)."
      return
    fi
  fi

  n=0
  while ! (echo > /dev/tcp/localhost/5432) >/dev/null 2>&1; do
    ((n++))
    if (( n >= MAX_RETRIES )); then
      log "ERROR: PostgreSQL port 5432 not reachable on host."
      exit 1
    fi
    log "Postgres not reachable on localhost:5432... (Attempt $n/$MAX_RETRIES)"
    sleep "$RETRY_INTERVAL"
  done
  log "✅ PostgreSQL reachable on host:5432."
}

get_session_cookie_and_token() {
  rm -f "$COOKIE_JAR" || true
  local resp token
  resp="$(curl -s -c "$COOKIE_JAR" -X POST \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$ADMIN_EMAIL\",\"password\":\"$ADMIN_PASSWORD\"}" \
    "$METABASE_URL/api/session")"
  token="$(echo "$resp" | jq -r '.id // empty' | tr -d '\r\n')"
  echo "$token"
}

create_admin_if_needed() {
  local token setup_token resp
  token="$(get_session_cookie_and_token)"
  if [[ -n "$token" ]]; then
    echo "$token"
    return
  fi

  setup_token="$(curl -s "$METABASE_URL/api/session/properties" | jq -r '."setup-token" // empty')"
  if [[ -z "$setup_token" || "$setup_token" == "null" ]]; then
    log "ERROR: No setup token and login failed. Metabase likely initialized with different creds."
    exit 1
  fi

  log "Creating admin user via setup token..."
  resp="$(jq -n --arg token "$setup_token" --arg email "$ADMIN_EMAIL" --arg pw "$ADMIN_PASSWORD" --arg site "$SITE_NAME" \
    '{token:$token,user:{first_name:"Admin",last_name:"User",email:$email,password:$pw},prefs:{allow_tracking:false,site_name:$site}}' \
    | curl -s -c "$COOKIE_JAR" -X POST -H "Content-Type: application/json" -d @- "$METABASE_URL/api/setup")"

  token="$(echo "$resp" | jq -r '.id // empty' | tr -d '\r\n')"
  if [[ -z "$token" ]]; then
    log "ERROR: Setup failed: $resp"
    exit 1
  fi
  echo "$token"
}

check_session() {
  local token="$1" http
  http=$(curl -s -b "$COOKIE_JAR" -o /tmp/mb_user_current.json -w "%{http_code}" \
    "$METABASE_URL/api/user/current")
  if [[ "$http" != "200" ]]; then
    log "ERROR: /api/user/current returned HTTP $http"
    log "Body: $(cat /tmp/mb_user_current.json)"
    return 1
  fi
  if ! jq empty </tmp/mb_user_current.json >/dev/null 2>&1; then
    log "ERROR: Non-JSON from /api/user/current: $(cat /tmp/mb_user_current.json)"
    return 1
  fi
  log "Session OK for user: $(jq -r '.email' </tmp/mb_user_current.json)"
  return 0
}

db_exists() {
  local http
  http=$(curl -s -b "$COOKIE_JAR" -o /tmp/mb_databases.json -w "%{http_code}" \
    -H "Accept: application/json" \
    "$METABASE_URL/api/database")
  if [[ "$http" != "200" ]]; then
    log "WARN: /api/database returned HTTP $http"
    log "Body: $(cat /tmp/mb_databases.json)"
    echo ""
    return 0
  fi
  if ! jq empty </tmp/mb_databases.json >/dev/null 2>&1; then
    log "WARN: Non-JSON from /api/database: $(cat /tmp/mb_databases.json)"
    echo ""
    return 0
  fi
  jq -r --arg name "$DB_NAME" '.data[]? | select(.name==$name) | .id // empty' </tmp/mb_databases.json
}

try_create_db() {
  local host="$1"
  log "Creating DB '$DB_NAME' with host '$host'..."
  jq -n --arg name "$DB_NAME" --arg host "$host" \
    '{engine:"postgres",name:$name,details:{host:$host,port:5432,dbname:"postgres",user:"postgres",password:"postgres",ssl:false},is_full_sync:true,auto_run_queries:true}' \
    >/tmp/mb_db_payload.json

  local http
  http=$(curl -s -b "$COOKIE_JAR" -o /tmp/mb_create_db.json -w "%{http_code}" \
    -X POST \
    -H "Content-Type: application/json" \
    -d @/tmp/mb_db_payload.json \
    "$METABASE_URL/api/database")

  if [[ "$http" != "200" && "$http" != "201" ]]; then
    log "Create DB returned HTTP $http"
    log "Body: $(cat /tmp/mb_create_db.json)"
    return 1
  fi
  if ! jq empty </tmp/mb_create_db.json >/dev/null 2>&1; then
    log "Create DB returned non-JSON: $(cat /tmp/mb_create_db.json)"
    return 1
  fi
  local id
  id=$(jq -r '.id // empty' </tmp/mb_create_db.json)
  if [[ -n "$id" ]]; then
    log "✅ Created DB (ID: $id) using host '$host'."
    return 0
  fi
  log "Create DB JSON did not contain id: $(cat /tmp/mb_create_db.json)"
  return 1
}

main() {
  log "Starting Metabase setup..."
  wait_for_metabase
  wait_for_postgres

  local token existing_id
  token="$(create_admin_if_needed)"
  log "Got session token."

  if ! check_session "$token"; then
    log "ERROR: Session invalid; aborting."
    exit 1
  fi

  existing_id="$(db_exists)"
  if [[ -n "$existing_id" ]]; then
    log "✅ Database '$DB_NAME' already exists (ID: $existing_id)."
    log "🎉 Metabase setup is complete."
    exit 0
  fi

  for host in "${DB_HOST_CANDIDATES[@]}"; do
    if try_create_db "$host"; then
      log "🎉 Metabase setup is complete."
      exit 0
    fi
  done

  log "ERROR: Could not create DB connection via any host (${DB_HOST_CANDIDATES[*]}). See logs above."
  exit 1
}

main