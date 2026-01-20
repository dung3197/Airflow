#!/usr/bin/env bash
set -euo pipefail

# =========================
# CONFIGURATION
# =========================
CONTAINER_NAME="airflow-scheduler"
LOG_PREFIX="[airflow-healthcheck]"

# =========================
# FUNCTIONS
# =========================
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') ${LOG_PREFIX} $1"
}

container_exists() {
  docker inspect "$CONTAINER_NAME" >/dev/null 2>&1
}

container_health() {
  docker inspect \
    --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
    "$CONTAINER_NAME"
}

restart_container() {
  log "Restarting container: ${CONTAINER_NAME}"
  docker restart "$CONTAINER_NAME" >/dev/null
  log "Container restarted successfully"
}

# =========================
# MAIN
# =========================
if ! container_exists; then
  log "ERROR: Container '${CONTAINER_NAME}' does not exist"
  exit 1
fi

HEALTH_STATUS="$(container_health)"

case "$HEALTH_STATUS" in
  unhealthy)
    log "Container is UNHEALTHY"
    restart_container
    ;;
  healthy)
    log "Container is healthy"
    ;;
  none)
    log "WARNING: Container has no healthcheck defined"
    ;;
  *)
    log "INFO: Container health status = ${HEALTH_STATUS}"
    ;;
esac
