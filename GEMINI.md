# GEMINI.md - FFP Data Validation Platform

> Project-specific AI configuration for the FFP DataValidation workspace.

---

## Project Overview

**FFP Data Validator** is a nationwide data validation platform for Bangladesh's Food Friendly Program.
Stack: FastAPI (Python) + Next.js 14 (TypeScript) + PostgreSQL, deployed via Docker Compose.

**Version**: 2.0.0 (Modular Architecture)

---

## Agent Routing

| Domain | Agent | Key Skills |
|--------|-------|------------|
| Backend (FastAPI, validation, DB) | `ffp-backend` | ffp-data-model, ffp-validation-engine |
| Frontend (Next.js, UI, dashboard) | `ffp-frontend` | ffp-data-model |
| DevOps (Docker, deploy, nginx) | `ffp-devops` | ffp-deployment |

---

## Architecture (v2.0)

### Backend Modules
```
backend/app/
├── main.py              — App factory, lifespan, migrations, middleware (~310 lines)
├── upload_routes.py     — /validate, /preview
├── export_routes.py     — Live exports, ZIP, recheck, trailing zeros, downloads
├── statistics_routes.py — Dashboard stats, deletion, manual updates, audit logs
├── search_routes.py     — NID/DOB/name search, record deletion
├── batch_routes.py      — Upload history, batch deletion, batch file listings
├── task_routes.py       — Background task polling and cleanup
├── sync_routes.py       — Multi-instance sync, IBAS NID verify
├── geo_routes.py        — Geo hierarchy, location guessing, password change
├── auth_routes.py       — Login, user CRUD, API key management
├── admin_routes.py      — System config, maintenance, geo cleanup
├── audit_routes.py      — Audit logs, API usage reporting [NEW]
├── validator.py         — Pure NID/DOB validation engine
├── pdf_generator.py     — PDF report generation with Nikosh font
├── stats_utils.py       — SummaryStats refresh from truth tables
├── bd_geo.py           — Geographic data matching
├── auth.py             — JWT auth, password hashing
├── rbac.py             — Permission checker
├── audit.py            — Audit logging
├── models.py           — SQLAlchemy ORM models
└── database.py         — DB engine, session factory
```

### Frontend Pages
```
frontend/src/app/
├── page.tsx             — Upload page (~49KB)
├── statistics/page.tsx  — Statistics dashboard (~62KB)
├── admin/page.tsx       — Admin panel (~86KB)
├── search/page.tsx      — Search page (~24KB)
├── login/page.tsx       — Login page
└── components/          — Modular components (StatsTable, AnalyticsCharts, etc.)
```

### Infrastructure
```
Docker Compose → nginx (:3000) → Frontend / Backend
                              → PgBouncer → PostgreSQL
SSL terminated by upstream nginx (not this stack)
```

---

## Code Rules

### Python (Backend)
- Use `datetime.now(timezone.utc)` — NEVER `datetime.utcnow()`
- Use `logging.getLogger("ffp")` — NEVER `print()`
- New routes go in dedicated `*_routes.py` files — NEVER in `main.py`
- Type hints on all functions, docstrings on all endpoints
- Parameterized SQL queries only — never f-strings with user input

### TypeScript (Frontend)
- API calls use `/api/` prefix (nginx proxied)
- JWT in localStorage, every fetch includes `Authorization: Bearer`
- Handle 401 (redirect to login) and 503 (security lockout)
- Bengali number formatting: `.toLocaleString('en-IN')`

---

## Security

- JWT_SECRET ≥ 64 chars (enforced at startup)
- Default admin password lockout when data exists
- CORS: `https://ffp-valid.dgfood.gov.bd`
- `.agent/` excluded from git and Docker

---

## Production Hardening Status

### ✅ Completed
- Phase 1: Security (CORS fix, credential exclusion)
- Phase 2: Backend modularization (Refactored main.py into 9+ route modules)
- Phase 3: Test coverage expansion (Validator + all API integration tests)
- Phase 4: Frontend modularization (Extracted tables, cards, and admin tabs)
- Phase 5: Production Hardening (Multi-tenancy, Async Upload Queue, Analytics, Audit, Bangla UI, Mobile Responsive)
- Phase 6: Production Upgrade Verification (Successful dry-run with production DB restore and automated backfills)

### 🔲 Remaining
- None. System is ready for production deployment.

