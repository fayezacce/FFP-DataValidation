# Production deployment, CI/CD, and zero-downtime migration guide for FFP Data Validation

# ══════════════════════════════════════════════════════════════════════════════
# PART A — ONE-TIME SERVER SETUP (new server)
# ══════════════════════════════════════════════════════════════════════════════

# 1. Install Docker + Docker Compose
curl -fsSL https://get.docker.com | bash
sudo usermod -aG docker $USER && newgrp docker
sudo apt install docker-compose-plugin -y

# 2. Create deploy directory
sudo mkdir -p /opt/ffp-datavalidation
sudo chown $USER:$USER /opt/ffp-datavalidation
cd /opt/ffp-datavalidation

# 3. Copy required files from repo (or git clone)
git clone https://github.com/fayezacce/FFP-DataValidation.git .
# OR copy just these files:
#   docker-compose.prod.yml
#   nginx.conf
#   .env   (created from .env.example)

# 4. Create .env from template
cp .env.example .env
nano .env
# Fill in:
#   POSTGRES_PASSWORD=<strong-random-password>
#   JWT_SECRET=<run: python -c "import secrets; print(secrets.token_hex(64))">
#   ALLOWED_ORIGINS=http://YOUR_SERVER_IP,https://YOUR_DOMAIN

# 5. Create db_backups directory
mkdir -p db_backups

# 6. Log in to GitHub Container Registry (GHCR) so Docker can pull private images
echo $GITHUB_PAT | docker login ghcr.io -u fayezacce --password-stdin

# 7. Pull images and start
docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d

# 8. Verify
docker compose -f docker-compose.prod.yml ps
curl http://localhost/api/health   # should return {"status":"ok","db":"ok",...}

# ══════════════════════════════════════════════════════════════════════════════
# PART B — GITHUB ACTIONS CI/CD SECRETS (set in GitHub repo Settings → Secrets)
# ══════════════════════════════════════════════════════════════════════════════

# Required secrets:
#   SERVER_HOST       — IP address or hostname of production server
#   SERVER_USER       — SSH username (e.g. ubuntu, ec2-user, or your user)
#   SERVER_SSH_KEY    — Private SSH key (paste contents of ~/.ssh/id_rsa)
#   SERVER_PORT       — SSH port (optional, defaults to 22)
#   DEPLOY_PATH       — /opt/ffp-datavalidation

# How to generate an SSH key pair if you don't have one:
ssh-keygen -t ed25519 -C "github-actions-deploy" -f ~/.ssh/ffp_deploy
# Add public key to server authorized_keys:
cat ~/.ssh/ffp_deploy.pub >> ~/.ssh/authorized_keys
# Add private key to GitHub:
cat ~/.ssh/ffp_deploy   # paste this into SERVER_SSH_KEY secret

# ══════════════════════════════════════════════════════════════════════════════
# PART C — ZERO-DOWNTIME MIGRATION (existing prod with 1M records)
# ══════════════════════════════════════════════════════════════════════════════

# ─── STEP 1: MANDATORY — full DB backup before anything else ──────────────────
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="/opt/ffp-datavalidation/db_backups/pre_migration_${TIMESTAMP}.sql.gz"

# Run backup directly from the running container
docker exec $(docker ps -qf "name=db") \
  pg_dump -U fayez ffp_validator | gzip > $BACKUP_FILE

# Verify backup is not empty
ls -lh $BACKUP_FILE   # should be several MB, not 0 bytes

# ─── STEP 2: Test new images against a staging DB ─────────────────────────────
# Option A: Restore backup to a temp container and test
docker run --rm \
  -e POSTGRES_USER=fayez -e POSTGRES_PASSWORD=testpass -e POSTGRES_DB=ffp_test \
  -v /tmp/testdb:/var/lib/postgresql/data \
  --name test_db -d postgres:15-alpine

# Wait a moment for DB to start
sleep 5

# Restore backup
gunzip -c $BACKUP_FILE | docker exec -i test_db \
  psql -U fayez ffp_test

# Start test backend against restored DB
docker run --rm --network host \
  -e DATABASE_URL=postgresql://fayez:testpass@localhost:5432/ffp_test \
  -e JWT_SECRET=test_secret_only \
  -e ALLOWED_ORIGINS=http://localhost \
  ghcr.io/fayezacce/ffp-datavalidation/backend:latest \
  uvicorn app.main:app --host 0.0.0.0 --port 8001

# Test the new /health endpoint
curl http://localhost:8001/health

# Clean up test
docker stop test_db && docker rm test_db

# ─── STEP 3: Pull new images on prod (while old version still serves traffic) ──
cd /opt/ffp-datavalidation
docker compose -f docker-compose.prod.yml pull

# ─── STEP 4: Confirm schema migrations are SAFE ───────────────────────────────
# Our migrate_schema() adds columns with IF NOT EXISTS — fully backwards compatible.
# No destructive ALTER TABLE or DROP TABLE exists in this release.
# ✅ SAFE to proceed without downtime.

# ─── STEP 5: Rolling restart (< 5 second interruption) ───────────────────────
docker compose -f docker-compose.prod.yml up -d --remove-orphans

# Docker will:
# 1. Start new backend container (runs migrate_schema on startup)
# 2. Health check passes → nginx switches traffic to new container
# 3. Old container stops

# ─── STEP 6: Verify production is healthy ─────────────────────────────────────
curl http://localhost/api/health
docker compose -f docker-compose.prod.yml ps
docker compose -f docker-compose.prod.yml logs backend --tail=50

# ─── STEP 7: Quick smoke tests ────────────────────────────────────────────────
# Statistics page should load (was crashing before fix)
curl -I http://localhost/api/statistics   # should be 200 or 401 (not 500)

# Check record count is unchanged
docker exec $(docker ps -qf "name=db") \
  psql -U fayez ffp_validator -c "SELECT COUNT(*) FROM valid_records;"

# ─── STEP 8: ROLLBACK if needed (keep previous image tag for this) ─────────────
# Replace 'latest' with the prior image SHA tag if something goes wrong:
# Edit docker-compose.prod.yml and set:
#   image: ghcr.io/fayezacce/ffp-datavalidation/backend:<previous-sha>
# Then:
docker compose -f docker-compose.prod.yml up -d --remove-orphans

# Previous SHA can be found via:
# GitHub Actions → build-and-push job → "Build and push Backend image" step output

# ══════════════════════════════════════════════════════════════════════════════
# PART D — ONGOING OPERATIONS
# ══════════════════════════════════════════════════════════════════════════════

# View logs
docker compose -f docker-compose.prod.yml logs -f backend

# Manual backup on demand
docker exec $(docker ps -qf "name=db") \
  pg_dump -U fayez ffp_validator | gzip > db_backups/manual_$(date +%Y%m%d).sql.gz

# Restore from backup
gunzip -c db_backups/FILENAME.sql.gz | \
  docker exec -i $(docker ps -qf "name=db") psql -U fayez ffp_validator

# Scale backend workers (edit compose or run):
docker compose -f docker-compose.prod.yml up -d --scale backend=2

# Check backup list
ls -lhtr db_backups/

# Emergency deploy without waiting for CI (skip tests)
# Go to GitHub → Actions → "FF Validator CI/CD" → Run workflow → deploy_only=true
