#!/usr/bin/env bash

set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
EMAIL="${EMAIL:-zubair@example.com}"
NAME="${NAME:-Zubair Ahmad}"
ORG="${ORG:-DQ Labs}"
ADMIN_EMAIL="${ADMIN_EMAIL:-admin.dataqtx@gmail.com}"
CREDENTIAL="${CREDENTIAL:-}"
CSV_PATH="${CSV_PATH:-}"
COLUMN="${COLUMN:-}"
FILE_ID_2="${FILE_ID_2:-}"

TOKEN_FILE=".api_token"
ADMIN_TOKEN_FILE=".api_admin_token"
USER_ID_FILE=".api_user_id"
FILE_ID_FILE=".api_file_id"

json_field() {
  local field="$1"
  local payload
  payload="$(cat)"
  JSON_PAYLOAD="$payload" python3 - "$field" <<'PY'
import json
import os
import sys

field = sys.argv[1]
data = json.loads(os.environ["JSON_PAYLOAD"])
value = data
for part in field.split("."):
    if isinstance(value, dict):
        value = value.get(part)
    else:
        value = None
        break
if value is None:
    sys.exit(1)
if isinstance(value, (dict, list)):
    print(json.dumps(value))
else:
    print(value)
PY
}

save_value() {
  local path="$1"
  local value="$2"
  printf '%s' "$value" > "$path"
}

load_value() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo ""
    return
  fi
  cat "$path"
}

token_header() {
  local token
  token="$(load_value "$TOKEN_FILE")"
  if [[ -z "$token" ]]; then
    echo "Session token not found. Run './test_api.sh google-auth' first." >&2
    exit 1
  fi
  printf 'Authorization: Bearer %s' "$token"
}

admin_token_header() {
  local token
  token="$(load_value "$ADMIN_TOKEN_FILE")"
  if [[ -z "$token" ]]; then
    echo "Admin session token not found. Run './test_api.sh admin-login' first." >&2
    exit 1
  fi
  printf 'Authorization: Bearer %s' "$token"
}

user_id_value() {
  local user_id
  user_id="$(load_value "$USER_ID_FILE")"
  if [[ -z "$user_id" ]]; then
    echo "User ID not found. Run './test_api.sh google-auth' first." >&2
    exit 1
  fi
  printf '%s' "$user_id"
}

file_id_value() {
  local file_id
  file_id="$(load_value "$FILE_ID_FILE")"
  if [[ -z "$file_id" ]]; then
    echo "File ID not found. Run './test_api.sh upload' first." >&2
    exit 1
  fi
  printf '%s' "$file_id"
}

run_step() {
  local label="$1"
  shift
  echo "== $label =="
  "$@"
  echo
}

print_usage() {
  cat <<'EOF'
Usage: ./test_api.sh <command>

Commands:
  health
  admin-me
  admin-users
  all-apis
  logout
  admin-logout
  full-smoke-test
  token
  me
  get-user
  update-user
  upload
  files
  file
  cleaning-report
  user-files
  analyze
  insights
  correlations
  distribution
  outliers
  spikes
  trends
  compare
  compare-get
  compare-insights
  report
  report-compare
  history
  delete-file

Environment variables you can override:
  BASE_URL, ORG, CSV_PATH, COLUMN, FILE_ID_2
EOF
}

cmd="${1:-}"

case "$cmd" in
  health)
    curl -sS "$BASE_URL/health"
    ;;

  logout)
    response="$(curl -sS -X POST "$BASE_URL/api/auth/logout" \
      -H "$(token_header)")"
    echo "$response"
    rm -f "$TOKEN_FILE"
    ;;

  admin-logout)
    response="$(curl -sS -X POST "$BASE_URL/api/admin/logout" \
      -H "$(admin_token_header)")"
    echo "$response"
    rm -f "$ADMIN_TOKEN_FILE"
    ;;

  full-smoke-test)
    if [[ -z "$CSV_PATH" ]]; then
      echo "Set CSV_PATH before running full-smoke-test. Example: CSV_PATH=/full/path/file.csv ./test_api.sh full-smoke-test" >&2
      exit 1
    fi
    run_step "health" "$0" health
    run_step "me" "$0" me
    run_step "update-user" env ORG="$ORG" "$0" update-user
    run_step "upload" env CSV_PATH="$CSV_PATH" "$0" upload
    run_step "analyze" "$0" analyze
    run_step "report" "$0" report
    ;;

  all-apis)
    run_step "health" "$0" health
    run_step "admin-me" "$0" admin-me
    run_step "admin-users" "$0" admin-users
    run_step "me" "$0" me
    run_step "get-user" "$0" get-user
    run_step "update-user" env ORG="$ORG" "$0" update-user

    if [[ -n "$CSV_PATH" ]]; then
      run_step "upload" env CSV_PATH="$CSV_PATH" "$0" upload
      run_step "files" "$0" files
      run_step "file" "$0" file
      run_step "cleaning-report" "$0" cleaning-report
      run_step "user-files" "$0" user-files
      run_step "analyze" "$0" analyze
      run_step "insights" "$0" insights
      run_step "correlations" "$0" correlations
      run_step "report" "$0" report
      run_step "history" "$0" history

      if [[ -n "$COLUMN" ]]; then
        run_step "distribution" env COLUMN="$COLUMN" "$0" distribution
        run_step "outliers" env COLUMN="$COLUMN" "$0" outliers
        run_step "spikes" env COLUMN="$COLUMN" "$0" spikes
        run_step "trends" env COLUMN="$COLUMN" "$0" trends
      else
        echo "Skipping column-specific APIs. Set COLUMN to run distribution/outliers/spikes/trends."
        echo
      fi

      if [[ -n "$FILE_ID_2" ]]; then
        run_step "compare" env FILE_ID_2="$FILE_ID_2" "$0" compare
        run_step "compare-get" env FILE_ID_2="$FILE_ID_2" "$0" compare-get
        run_step "compare-insights" env FILE_ID_2="$FILE_ID_2" "$0" compare-insights
        run_step "report-compare" env FILE_ID_2="$FILE_ID_2" "$0" report-compare
      else
        echo "Skipping compare APIs. Set FILE_ID_2 to run compare/compare-get/compare-insights/report-compare."
        echo
      fi

      run_step "delete-file" "$0" delete-file
    else
      echo "Skipping file-dependent APIs. Set CSV_PATH to run upload/files/analysis/report endpoints."
      echo
    fi

    run_step "logout" "$0" logout
    run_step "admin-logout" "$0" admin-logout
    ;;

  token)
    load_value "$TOKEN_FILE"
    ;;

  me)
    curl -sS "$BASE_URL/api/auth/me" \
      -H "$(token_header)"
    ;;

  admin-me)
    curl -sS "$BASE_URL/api/admin/me" \
      -H "$(admin_token_header)"
    ;;

  get-user)
    user_id="$(user_id_value)"
    curl -sS "$BASE_URL/api/users/user/$user_id" \
      -H "$(token_header)"
    ;;

  admin-users)
    curl -sS "$BASE_URL/api/admin/users" \
      -H "$(admin_token_header)"
    ;;

  update-user)
    user_id="$(user_id_value)"
    curl -sS -X PUT "$BASE_URL/api/users/user/$user_id" \
      -H "$(token_header)" \
      -H "Content-Type: application/json" \
      -d "{
        \"organization\": \"$ORG\"
      }"
    ;;

  upload)
    if [[ -z "$CSV_PATH" ]]; then
      echo "Set CSV_PATH before running upload. Example: CSV_PATH=/full/path/file.csv ./test_api.sh upload" >&2
      exit 1
    fi
    response="$(curl -sS -X POST "$BASE_URL/api/upload" \
      -H "$(token_header)" \
      -F "file=@$CSV_PATH")"
    echo "$response"
    file_id="$(printf '%s' "$response" | json_field file_id || true)"
    if [[ -n "$file_id" ]]; then
      save_value "$FILE_ID_FILE" "$file_id"
    fi
    ;;

  files)
    curl -sS "$BASE_URL/api/files" \
      -H "$(token_header)"
    ;;

  file)
    file_id="$(file_id_value)"
    curl -sS "$BASE_URL/api/files/$file_id" \
      -H "$(token_header)"
    ;;

  cleaning-report)
    file_id="$(file_id_value)"
    curl -sS "$BASE_URL/api/files/$file_id/cleaning-report" \
      -H "$(token_header)"
    ;;

  user-files)
    user_id="$(user_id_value)"
    curl -sS "$BASE_URL/api/users/user/$user_id/files" \
      -H "$(token_header)"
    ;;

  analyze)
    file_id="$(file_id_value)"
    curl -sS -X POST "$BASE_URL/api/analyze/$file_id" \
      -H "$(token_header)"
    ;;

  insights)
    file_id="$(file_id_value)"
    curl -sS "$BASE_URL/api/analyze/$file_id/insights" \
      -H "$(token_header)"
    ;;

  correlations)
    file_id="$(file_id_value)"
    curl -sS "$BASE_URL/api/analyze/$file_id/correlations" \
      -H "$(token_header)"
    ;;

  distribution)
    file_id="$(file_id_value)"
    if [[ -z "$COLUMN" ]]; then
      echo "Set COLUMN before running distribution. Example: COLUMN=temperature ./test_api.sh distribution" >&2
      exit 1
    fi
    curl -sS "$BASE_URL/api/analyze/$file_id/distribution/$COLUMN" \
      -H "$(token_header)"
    ;;

  outliers)
    file_id="$(file_id_value)"
    if [[ -z "$COLUMN" ]]; then
      echo "Set COLUMN before running outliers. Example: COLUMN=temperature ./test_api.sh outliers" >&2
      exit 1
    fi
    curl -sS "$BASE_URL/api/analyze/$file_id/outliers/$COLUMN?method=iqr" \
      -H "$(token_header)"
    ;;

  spikes)
    file_id="$(file_id_value)"
    if [[ -z "$COLUMN" ]]; then
      echo "Set COLUMN before running spikes. Example: COLUMN=temperature ./test_api.sh spikes" >&2
      exit 1
    fi
    curl -sS "$BASE_URL/api/analyze/$file_id/spikes/$COLUMN?threshold=2.0" \
      -H "$(token_header)"
    ;;

  trends)
    file_id="$(file_id_value)"
    if [[ -z "$COLUMN" ]]; then
      echo "Set COLUMN before running trends. Example: COLUMN=temperature ./test_api.sh trends" >&2
      exit 1
    fi
    curl -sS "$BASE_URL/api/analyze/$file_id/trends/$COLUMN" \
      -H "$(token_header)"
    ;;

  compare)
    file_id="$(file_id_value)"
    if [[ -z "$FILE_ID_2" ]]; then
      echo "Set FILE_ID_2 before running compare. Example: FILE_ID_2=<second-file-id> ./test_api.sh compare" >&2
      exit 1
    fi
    curl -sS -X POST "$BASE_URL/api/compare" \
      -H "$(token_header)" \
      -H "Content-Type: application/json" \
      -d "{
        \"file_id_1\": \"$file_id\",
        \"file_id_2\": \"$FILE_ID_2\"
      }"
    ;;

  compare-get)
    file_id="$(file_id_value)"
    if [[ -z "$FILE_ID_2" ]]; then
      echo "Set FILE_ID_2 before running compare-get. Example: FILE_ID_2=<second-file-id> ./test_api.sh compare-get" >&2
      exit 1
    fi
    curl -sS "$BASE_URL/api/compare/$file_id/$FILE_ID_2" \
      -H "$(token_header)"
    ;;

  compare-insights)
    file_id="$(file_id_value)"
    if [[ -z "$FILE_ID_2" ]]; then
      echo "Set FILE_ID_2 before running compare-insights. Example: FILE_ID_2=<second-file-id> ./test_api.sh compare-insights" >&2
      exit 1
    fi
    curl -sS -X POST "$BASE_URL/api/compare/insights-only" \
      -H "$(token_header)" \
      -H "Content-Type: application/json" \
      -d "{
        \"file_id_1\": \"$file_id\",
        \"file_id_2\": \"$FILE_ID_2\"
      }"
    ;;

  report)
    file_id="$(file_id_value)"
    curl -sS -X POST "$BASE_URL/api/report/$file_id" \
      -H "$(token_header)"
    ;;

  report-compare)
    file_id="$(file_id_value)"
    if [[ -z "$FILE_ID_2" ]]; then
      echo "Set FILE_ID_2 before running report-compare. Example: FILE_ID_2=<second-file-id> ./test_api.sh report-compare" >&2
      exit 1
    fi
    curl -sS -X POST "$BASE_URL/api/report/compare/$file_id/$FILE_ID_2" \
      -H "$(token_header)"
    ;;

  history)
    user_id="$(user_id_value)"
    curl -sS "$BASE_URL/api/users/user/$user_id/analysis-history" \
      -H "$(token_header)"
    ;;

  delete-file)
    file_id="$(file_id_value)"
    curl -sS -X DELETE "$BASE_URL/api/files/$file_id" \
      -H "$(token_header)"
    ;;

  ""|-h|--help|help)
    print_usage
    ;;

  *)
    echo "Unknown command: $cmd" >&2
    print_usage
    exit 1
    ;;
esac
