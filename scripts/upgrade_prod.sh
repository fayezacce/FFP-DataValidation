#!/usr/bin/env bash
# =============================================================================
# FFP Data Validation Platform — Production Upgrade Script
# Author: Fayez Ahmed, Assistant Programmer, DG Food
# =============================================================================
# Upgrades from commit 0cfa198 ("Fix: Restrict columns in Download All Invalid PDF")
# to the current latest code.
#
# Usage (on prod server, from the repo root):
#   chmod +x scripts/upgrade_prod.sh
#   sudo bash scripts/upgrade_prod.sh
#
# Prerequisites:
#   - Docker + Docker Compose v2 installed
#   - .env file beside docker-compose.prod.yml (see .env.production template)
#   - The DB backup is already taken (ffp-db-05042026.tar.gz provided)
# =============================================================================

set -euo pipefail

# ── Config ──────────────────────────────────────────────────────────────────
COMPOSE_FILE="docker-compose.prod.yml"
BACKEND_SERVICE="backend"
DB_SERVICE="db"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="./db_backups"
LOG_FILE="./upgrade_${TIMESTAMP}.log"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*" | tee -a "$LOG_FILE"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*" | tee -a "$LOG_FILE"; }
error() { echo -e "${RED}[ERROR]${NC} $*" | tee -a "$LOG_FILE"; exit 1; }

# ── Argument Parsing ────────────────────────────────────────────────────────
RESTORE_FILE=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --restore)
      RESTORE_FILE="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

info "=== FFP Platform Upgrade — $(date) ==="

# ── Step 0: Pre-flight checks ────────────────────────────────────────────────
info "--- Step 0: Pre-flight checks ---"

command -v docker  >/dev/null 2>&1 || error "Docker is not installed"
command -v git     >/dev/null 2>&1 || error "Git is not installed"
[[ -f ".env" ]]   || error ".env file missing. Copy .env.production to .env and fill in secrets."
[[ -f "$COMPOSE_FILE" ]] || error "$COMPOSE_FILE not found. Run from repo root."

# Check the DB is currently running
docker compose -f "$COMPOSE_FILE" ps "$DB_SERVICE" | grep -q "running\|healthy" \
  || error "Database container MUST be running to perform upgrade/restore. Run 'docker compose -f $COMPOSE_FILE up -d db' first."

source .env  # load POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

# ── Step 0.5: Optional Restore ──────────────────────────────────────────────
if [[ -n "$RESTORE_FILE" ]]; then
    if [[ ! -f "$RESTORE_FILE" ]]; then
        error "Restore file not found: $RESTORE_FILE"
    fi
    info "--- Step 0.5: Restoring database from $RESTORE_FILE ---"
    warn "This will DROP all existing data in the public schema!"
    
    # Stop backend to prevent locks
    docker compose -f "$COMPOSE_FILE" stop "$BACKEND_SERVICE" frontend || true
    
    info "Cleaning public schema..."
    docker compose -f "$COMPOSE_FILE" exec -T "$DB_SERVICE" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO public;"
    
    info "Running restore (this may take a while for large files)..."
    if [[ "$RESTORE_FILE" == *.gz ]]; then
        gunzip -c "$RESTORE_FILE" | docker compose -f "$COMPOSE_FILE" exec -T "$DB_SERVICE" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
    else
        cat "$RESTORE_FILE" | docker compose -f "$COMPOSE_FILE" exec -T "$DB_SERVICE" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
    fi
    info "Restore complete."
fi

# ── Step 1: Create safety DB snapshot ───────────────────────────────────────
info "--- Step 1: Creating pre-upgrade DB snapshot ---"
mkdir -p "$BACKUP_DIR"

SNAPSHOT_FILE="${BACKUP_DIR}/pre_upgrade_${TIMESTAMP}.tar.gz"

docker compose -f "$COMPOSE_FILE" exec -T "$DB_SERVICE" \
  pg_dump -U "${POSTGRES_USER:-ffp_admin}" \
          -d "${POSTGRES_DB:-ffp_validator}" \
          -F tar \
  | gzip > "$SNAPSHOT_FILE" \
  && info "Snapshot saved → $SNAPSHOT_FILE" \
  || warn "Snapshot failed — continuing. You still have the provided backup."

# ── Step 2: Git pull ─────────────────────────────────────────────────────────
info "--- Step 2: Pulling latest code ---"
git pull --ff-only origin master \
  || error "Git pull failed. Resolve conflicts then re-run."

# ── Step 3: Build new Docker images (no cache to pick up new pip packages) ──
info "--- Step 3: Building Docker images (this may take a few minutes) ---"
docker compose -f "$COMPOSE_FILE" build --no-cache "$BACKEND_SERVICE" \
  || error "Docker build failed."

info "Building frontend..."
docker compose -f "$COMPOSE_FILE" build --no-cache frontend \
  || error "Frontend build failed."

# ── Step 4: Start services with zero-downtime restart ───────────────────────
info "--- Step 4: Rolling restart of services ---"

# Start pgbouncer first (new service — may not exist yet)
info "Starting PgBouncer connection pooler..."
docker compose -f "$COMPOSE_FILE" up -d pgbouncer \
  && sleep 5 \
  || warn "PgBouncer failed to start — backend will be in degraded mode."

# Restart backend (migrate_schema runs automatically on startup)
info "Restarting backend (schema migrations run automatically)..."
docker compose -f "$COMPOSE_FILE" up -d --no-deps "$BACKEND_SERVICE"
sleep 15 # wait for migration to complete

# ── Step 5: Exclusive Data Backfill ──────────────────────────────────────────
info "--- Step 5: Exclusive Data Backfill ---"
warn "Stopping app services temporarily for exclusive DB access..."
docker compose -f "$COMPOSE_FILE" stop "$BACKEND_SERVICE" frontend

BACKEND_CONTAINER=$(docker compose -f "$COMPOSE_FILE" ps -q "$BACKEND_SERVICE")
# We use a temporary container to run the backfill scripts while services are stopped
info "Running backfill scripts in standalone mode..."

run_task() {
    local desc="$1"
    local cmd="$2"
    info "Running: $desc"
    docker run --rm --network ffp-datavalidation_app_network \
      -e DATABASE_URL="postgres://${POSTGRES_USER:-ffp_admin}:${POSTGRES_PASSWORD}@db:5432/${POSTGRES_DB:-ffp_validator}" \
      ffp-datavalidation-backend python "$cmd" \
      && info "  ✓ $desc completed" \
      || warn "  ✗ $desc failed — review logs"
}

# 5a. Geo ID backfill
run_task "Geo ID backfill" "backfill_geo_ids.py"

# 5b. Mobile column extraction
run_task "Mobile number backfill" "app/scripts/backfill_columns.py"

# 5c. Summary stats sync
run_task "Summary stats sync" "app/scripts/sync_all_stats.py"

# 5d. Repair Stale Geo IDs
run_task "Repair stale geo IDs" "app/scripts/repair_geo_ids_prod.py"

# ── Step 6: Restart Services ───────────────────────────────────────────────
info "--- Step 6: Restarting Services ---"
docker compose -f "$COMPOSE_FILE" up -d "$BACKEND_SERVICE" frontend
sleep 10

sleep 5
HEALTH=$(curl -sf http://localhost:8000/health | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['status'])" 2>/dev/null || echo "unreachable")
DB_STATUS=$(curl -sf http://localhost:8000/health | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['db'])" 2>/dev/null || echo "unreachable")

echo ""
echo "  ╔════════════════════════════════════╗"
echo "  ║        Upgrade Summary              ║"
echo "  ╠════════════════════════════════════╣"
echo "  ║  Backend : $HEALTH"
echo "  ║  Database: $DB_STATUS"
echo "  ╚════════════════════════════════════╝"
echo ""

if [[ "$HEALTH" == "ok" && "$DB_STATUS" == "ok" ]]; then
  info "=== Upgrade completed successfully ==="
  info "Snapshot saved at: $SNAPSHOT_FILE"
  info "Full log: $LOG_FILE"
else
  error "=== Upgrade completed with issues — review log: $LOG_FILE ==="
fi

info "To tail live logs: docker compose -f $COMPOSE_FILE logs -f backend"
