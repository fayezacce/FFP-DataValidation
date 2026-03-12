# FFP Data Validation Platform

A government-grade data validation system for the **Food Friendly Program (FFP)** under the Ministry of Food, Bangladesh. Validates NID (National ID), Date of Birth, and beneficiary records at upazila/district level.

## Architecture

```
┌───────────────────────────────────────────────────────────┐
│                    Outer Nginx (SSL)                      │
│               (your existing reverse proxy)               │
└──────────────────────┬────────────────────────────────────┘
                       │ HTTP :80
┌──────────────────────▼────────────────────────────────────┐
│              Docker Compose Stack                         │
│                                                           │
│  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌───────────┐ │
│  │  Nginx   │→ │ Frontend │  │ Backend  │  │ PostgreSQL│ │
│  │ (proxy)  │→ │ Next.js  │  │ FastAPI  │  │   15.6    │ │
│  │ :3000    │  │ :3001    │  │ :8000    │  │  :5432    │ │
│  └─────────┘  └──────────┘  └──────────┘  └───────────┘ │
│                                ┌──────────┐              │
│                                │ DB Backup│              │
│                                │  (daily) │              │
│                                └──────────┘              │
└───────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer    | Technology                |
|----------|--------------------------|
| Frontend | Next.js 15, React 19     |
| Backend  | FastAPI (Python 3.13)    |
| Database | PostgreSQL 15.6-alpine   |
| Proxy    | Nginx 1.25-alpine        |
| Deploy   | Docker Compose, GHCR CI/CD |

## Quick Start (Development)

```bash
# Clone
git clone https://github.com/fayezacce/FFP-DataValidation.git
cd FFP-DataValidation

# Create .env from template
cp .env.example .env
# Edit .env — set JWT_SECRET (min 64 chars), POSTGRES_PASSWORD, ALLOWED_ORIGINS

# Start all services
docker compose up --build
```

The app will be available at `http://localhost`.

## Production Deployment

### Prerequisites
- Docker & Docker Compose installed on the server
- An outer Nginx handling SSL termination
- GitHub Container Registry (GHCR) access for CI/CD

### Step 1: Environment Setup

Copy `.env.production` to your server as `.env` and fill in real values:

```bash
scp .env.production user@server:/path/to/FFP-DataValidation/.env
```

**Critical variables to change:**
| Variable           | Requirement                                      |
|--------------------|--------------------------------------------------|
| `JWT_SECRET`       | Min 64 chars. Generate: `python -c "import secrets; print(secrets.token_hex(64))"` |
| `POSTGRES_PASSWORD`| Strong, unique password (40+ chars recommended)  |
| `ALLOWED_ORIGINS`  | Your exact production domain (e.g., `https://ffp.mofood.gov.bd`) |

### Step 2: Deploy

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

### Step 3: Verify

```bash
# Check all containers are healthy
docker compose -f docker-compose.prod.yml ps

# Check backend health
curl http://localhost/api/health
```

### Step 4: Change Default Admin Password

> ⚠️ **CRITICAL:** The default admin account (`admin`/`admin123`) MUST be changed immediately. The system will lock out data uploads if the default password persists with data present.

Login to the admin panel and change the password via **Admin → Users**.

## Outer Nginx Configuration

Your existing SSL-terminating Nginx must forward these headers:

```nginx
location /ffp/ {
    proxy_pass http://app-server:80/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto https;

    # Recommended: HSTS
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;
}
```

## Production Migration (from master branch)

If migrating from the old `master` branch with an existing ~400K record database:

### Pre-Migration Backup
```bash
# On the production server, take a backup FIRST
docker exec ffp_db pg_dump -U <POSTGRES_USER> -Fc ffp_validator > "pre_migrate_$(date +%Y%m%d_%H%M%S).dump"
```

### Migration Steps
```bash
# 1. Pull the latest code
git pull origin master

# 2. Stop the current stack
docker compose -f docker-compose.prod.yml down

# 3. Create .env from the new template (if not already done)
cp .env.production .env
# Edit .env with your actual values

# 4. Start the updated stack
docker compose -f docker-compose.prod.yml up -d --build

# 5. The backend will automatically:
#    - Create the new 'invalid_records' table
#    - Add 'last_upload_*' columns to 'summary_stats'
#    - Existing data remains untouched (additive-only migration)

# 6. Verify data integrity
docker exec ffp_db psql -U <POSTGRES_USER> -d ffp_validator \
  -c "SELECT relname, reltuples::bigint FROM pg_stat_user_tables ORDER BY reltuples DESC;"
```

### Rollback (if needed)
```bash
docker compose -f docker-compose.prod.yml down
docker compose -f docker-compose.prod.yml up -d db
docker exec -i ffp_db pg_restore -U <POSTGRES_USER> -d ffp_validator \
  --clean --no-owner < pre_migrate_TIMESTAMP.dump
docker compose -f docker-compose.prod.yml up -d
```

## API Key Note

After this update, **all existing API keys will stop working** because they are now stored as SHA-256 hashes instead of plaintext. Users must regenerate their API keys via the Admin panel.

## Security Features

- JWT authentication with mandatory strong secret (64+ char)
- API keys hashed (SHA-256) before storage
- Path traversal protection on file uploads
- Rate limiting on login (5 attempts/minute)
- CSP headers (no unsafe-eval)
- Auto-lockout if default admin password is active with data
- Role-based access control (RBAC)
- Audit logging for all actions
- Automated daily database backups

## Project Structure

```
FFP-DataValidation/
├── backend/
│   ├── app/
│   │   ├── main.py          # FastAPI app, validation endpoints
│   │   ├── auth.py           # JWT + API key authentication
│   │   ├── auth_routes.py    # Login, user management routes
│   │   ├── admin_routes.py   # Admin panel API routes
│   │   ├── models.py         # SQLAlchemy models
│   │   ├── rbac.py           # Role-based access control
│   │   ├── audit.py          # Audit trail logging
│   │   ├── validator.py      # NID/DOB validation logic
│   │   └── pdf_generator.py  # Report generation
│   ├── tests/                # Unit & integration tests
│   ├── requirements.txt      # Pinned Python dependencies
│   └── Dockerfile
├── frontend/
│   └── src/app/              # Next.js pages and components
├── docker-compose.yml        # Development
├── docker-compose.prod.yml   # Production
├── nginx.conf                # Internal reverse proxy config
├── .env.example              # Environment template
├── .env.production           # Production env template
└── MIGRATION_GUIDE.md        # Database migration details
```

## License

Internal use — Ministry of Food, Government of Bangladesh.
