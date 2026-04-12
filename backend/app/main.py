"""
FFP Data Validator API
Author: Fayez Ahmed

Slim app factory — all route handlers live in dedicated modules.
"""
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session
from sqlalchemy import text
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
import os
import json
import time as _time
import logging

from .database import engine, Base, get_db, SessionLocal
from .models import (
    User, SystemConfig, SummaryStats, ValidRecord, InvalidRecord,
    Division, District, Upazila, Permission, RolePermission,
    UploadedFile, UploadBatch,
)
from .auth import (
    get_current_user, verify_password, hash_password, limiter,
)

logger = logging.getLogger("ffp")


# ─────────────────────────────────────────────────────────────────────────────
# APP LIFESPAN — startup/shutdown
# ─────────────────────────────────────────────────────────────────────────────


async def lifespan(app: FastAPI):
    os.makedirs("downloads", exist_ok=True)
    os.makedirs("uploads", exist_ok=True)

    # Enable extensions and create tables
    db_init = SessionLocal()
    try:
        db_init.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        db_init.commit()
    except Exception as e:
        logger.warning(f"pg_trgm extension: {e}")
        db_init.rollback()
    finally:
        db_init.close()

    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        migrate_schema(db)
        _migrate_json_to_db(db)

        # Seed default admin if no users exist
        if db.query(User).count() == 0:
            admin_user = User(username="admin", hashed_password=hash_password("admin123"), role="admin")
            db.add(admin_user)
            db.commit()
            logger.warning("Default admin user created: admin / admin123 — CHANGE IT IMMEDIATELY!")
        else:
            admin = db.query(User).filter(User.username == "admin").first()
            if admin and verify_password("admin123", admin.hashed_password):
                if db.query(ValidRecord).count() > 0:
                    app.state.security_lockout = True
                    logger.critical("SECURITY LOCKOUT — default admin password is active AND data exists. Upload endpoints disabled.")
                else:
                    logger.warning("Default admin password 'admin123' is still active!")

        _seed_geo_data_if_empty(db)
        _seed_permissions_if_empty(db)

        from . import bd_geo
        bd_geo.load_geo_data_from_db(db)
    finally:
        db.close()

    yield


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA MIGRATIONS
# ─────────────────────────────────────────────────────────────────────────────


def migrate_schema(db: Session):
    """Safely add any missing columns to DB tables."""
    new_summary_columns = [
        ("last_upload_total", "INTEGER DEFAULT 0"),
        ("last_upload_valid", "INTEGER DEFAULT 0"),
        ("last_upload_invalid", "INTEGER DEFAULT 0"),
        ("last_upload_new", "INTEGER DEFAULT 0"),
        ("last_upload_updated", "INTEGER DEFAULT 0"),
        ("last_upload_duplicate", "INTEGER DEFAULT 0"),
        ("excel_valid_url", "VARCHAR"),
        ("excel_invalid_url", "VARCHAR"),
        ("pdf_invalid_url", "VARCHAR"),
    ]
    for col_name, col_def in new_summary_columns:
        try:
            db.execute(text(f"ALTER TABLE summary_stats ADD COLUMN IF NOT EXISTS {col_name} {col_def}"))
            db.commit()
        except Exception:
            db.rollback()

    new_user_columns = [
        ("api_key_last_used", "TIMESTAMP"),
        ("api_rate_limit", "INTEGER DEFAULT 60"),
        ("api_total_limit", "INTEGER"),
        ("api_usage_count", "INTEGER DEFAULT 0"),
        ("api_ip_whitelist", "VARCHAR"),
        ("division_access", "VARCHAR"),
        ("district_access", "VARCHAR"),
        ("upazila_access", "VARCHAR"),
    ]
    for col_name, col_def in new_user_columns:
        try:
            db.execute(text(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {col_name} {col_def}"))
            db.commit()
        except Exception:
            db.rollback()

    for table in ["valid_records", "invalid_records"]:
        try:
            db.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS card_no VARCHAR"))
            db.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS division_id INTEGER"))
            db.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS district_id INTEGER"))
            db.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS upazila_id INTEGER"))
            db.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS batch_id INTEGER"))
            db.commit()
            idx_name = f"ix_{table}_geo_names"
            db.execute(text(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} (division, district, upazila)"))
            
            # CRITICAL PERFORMANCE INDEX for statistics dashboard
            idx_upz_name = f"ix_{table}_upazila_id"
            db.execute(text(f"CREATE INDEX IF NOT EXISTS {idx_upz_name} ON {table} (upazila_id)"))
            
            db.commit()
        except Exception:
            db.rollback()

    # New UploadBatch URL columns
    new_batch_columns = [
        ("valid_url", "VARCHAR"),
        ("invalid_url", "VARCHAR"),
        ("pdf_url", "VARCHAR"),
        ("pdf_invalid_url", "VARCHAR"),
    ]
    for col_name, col_def in new_batch_columns:
        try:
            db.execute(text(f"ALTER TABLE upload_batches ADD COLUMN IF NOT EXISTS {col_name} {col_def}"))
            db.commit()
        except Exception:
            db.rollback()

    # Legacy URL prefix fix (/api/downloads/ -> /api/export/download/)
    # This ensures that even if local files are missing, the URLs point to the correct current REST endpoint.
    try:
        # Summary Stats
        db.execute(text("""
            UPDATE summary_stats 
            SET excel_valid_url = REPLACE(excel_valid_url, '/api/downloads/', '/api/export/download/'),
                excel_invalid_url = REPLACE(excel_invalid_url, '/api/downloads/', '/api/export/download/'),
                pdf_invalid_url = REPLACE(pdf_invalid_url, '/api/downloads/', '/api/export/download/'),
                excel_url = REPLACE(excel_url, '/api/downloads/', '/api/export/download/'),
                pdf_url = REPLACE(pdf_url, '/api/downloads/', '/api/export/download/')
            WHERE excel_valid_url LIKE '/api/downloads/%' 
               OR excel_invalid_url LIKE '/api/downloads/%'
               OR pdf_invalid_url LIKE '/api/downloads/%'
               OR excel_url LIKE '/api/downloads/%'
               OR pdf_url LIKE '/api/downloads/%'
        """))
        # Upload Batches
        db.execute(text("""
            UPDATE upload_batches 
            SET valid_url = REPLACE(valid_url, '/api/downloads/', '/api/export/download/'),
                invalid_url = REPLACE(invalid_url, '/api/downloads/', '/api/export/download/'),
                pdf_url = REPLACE(pdf_url, '/api/downloads/', '/api/export/download/'),
                pdf_invalid_url = REPLACE(pdf_invalid_url, '/api/downloads/', '/api/export/download/')
            WHERE valid_url LIKE '/api/downloads/%'
               OR invalid_url LIKE '/api/downloads/%'
               OR pdf_url LIKE '/api/downloads/%'
               OR pdf_invalid_url LIKE '/api/downloads/%'
        """))
        db.commit()
        logger.info("Migrated legacy URL prefixes in database.")
    except Exception as e:
        logger.error(f"Error migrating legacy URLs: {e}")
        db.rollback()

    invalid_cols = [("card_no", "VARCHAR"), ("master_serial", "VARCHAR"), ("mobile", "VARCHAR")]
    for col_name, col_def in invalid_cols:
        try:
            db.execute(text(f"ALTER TABLE invalid_records ADD COLUMN IF NOT EXISTS {col_name} {col_def}"))
            db.commit()
        except Exception:
            db.rollback()

    try:
        Base.metadata.create_all(bind=engine)
    except Exception:
        pass

    try:
        db.execute(text("ALTER TABLE upazilas ADD COLUMN IF NOT EXISTS quota INTEGER DEFAULT 0"))
        db.commit()
    except Exception:
        db.rollback()

    try:
        db.execute(text("ALTER TABLE valid_records ADD COLUMN IF NOT EXISTS batch_id INTEGER"))
        db.execute(text("ALTER TABLE valid_records ADD COLUMN IF NOT EXISTS mobile VARCHAR"))
        db.commit()
    except Exception:
        db.rollback()

    try:
        db.execute(text("ALTER TABLE invalid_records ADD COLUMN IF NOT EXISTS batch_id INTEGER"))
        db.commit()
    except Exception:
        db.rollback()

    geo_id_cols = [("division_id", "INTEGER"), ("district_id", "INTEGER"), ("upazila_id", "INTEGER")]
    for table in ["summary_stats", "valid_records", "invalid_records", "upload_batches"]:
        for col_name, col_def in geo_id_cols:
            try:
                db.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_def}"))
                db.commit()
            except Exception:
                db.rollback()

    # Preserve original excel column headers per upazila and per batch
    for table in ["summary_stats", "upload_batches"]:
        try:
            db.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS column_headers JSON"))
            db.commit()
            logger.info(f"Migrated: added column_headers to {table}")
        except Exception:
            db.rollback()


# ─────────────────────────────────────────────────────────────────────────────
# SEEDING
# ─────────────────────────────────────────────────────────────────────────────


def _seed_geo_data_if_empty(db: Session):
    """Seed divisions/districts/upazilas from geo_data.json if empty."""
    geo_file_path = os.path.join(os.path.dirname(__file__), "geo_data.json")
    if not os.path.exists(geo_file_path):
        logger.info("geo_data.json not found. Skipping seeding.")
        return

    try:
        if db.query(Division).count() == 0:
            with open(geo_file_path, "r", encoding="utf-8") as f:
                divisions_data = json.load(f)

            div_count = dist_count = upz_count = 0
            for div in divisions_data:
                div_name = div["name"]
                db.add(Division(name=div_name, is_active=True))
                div_count += 1
                for dist in div["districts"]:
                    dist_name = dist["name"]
                    db.add(District(division_name=div_name, name=dist_name, is_active=True))
                    dist_count += 1
                    for upz_name in dist["upazilas"]:
                        db.add(Upazila(division_name=div_name, district_name=dist_name, name=upz_name, is_active=True))
                        upz_count += 1
            db.commit()
            logger.info(f"Seeded {div_count} divisions, {dist_count} districts, {upz_count} upazilas.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error seeding geo data: {e}")


def _seed_permissions_if_empty(db: Session):
    """Seed permissions and role_permissions if empty."""
    try:
        if db.query(Permission).count() == 0:
            perms = [
                ("upload_data", "Can upload and validate Excel files"),
                ("view_stats", "Can view statistics and dashboard"),
                ("view_geo", "Can view geographical hierarchy"),
                ("view_admin", "Can view administrative settings"),
                ("manage_users", "Can create/edit users"),
                ("manage_geo", "Can edit geographical data"),
            ]
            for name, desc in perms:
                db.add(Permission(name=name, description=desc))

            role_map = {
                "admin": [p[0] for p in perms],
                "uploader": ["upload_data"],
                "viewer": ["view_stats", "view_geo"],
            }
            for role, pnames in role_map.items():
                for pname in pnames:
                    db.add(RolePermission(role=role, permission_name=pname))
            db.commit()
            logger.info("Seeded permissions and role mappings.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error seeding permissions: {e}")


def _migrate_json_to_db(db: Session):
    """One-time migration from legacy JSON stats file to DB."""
    from datetime import datetime

    stats_file = os.path.join("downloads", "validation_stats.json")
    if os.path.exists(stats_file):
        try:
            with open(stats_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                entries = data.get("entries", {})
                for key, val in entries.items():
                    existing = db.query(SummaryStats).filter(
                        SummaryStats.district == val["district"],
                        SummaryStats.upazila == val["upazila"],
                    ).first()
                    if not existing:
                        created_at = datetime.fromisoformat(val["created_at"].rstrip("Z"))
                        updated_at = datetime.fromisoformat(val["updated_at"].rstrip("Z"))
                        summary = SummaryStats(
                            division=val["division"],
                            district=val["district"],
                            upazila=val["upazila"],
                            total=val["total"],
                            valid=val["valid"],
                            invalid=val["invalid"],
                            version=val.get("version", 1),
                            filename=val.get("filename", ""),
                            pdf_url=val.get("pdf_url", ""),
                            excel_url=val.get("excel_url", ""),
                            excel_valid_url=val.get("excel_valid_url", ""),
                            excel_invalid_url=val.get("excel_invalid_url", ""),
                            created_at=created_at,
                            updated_at=updated_at,
                        )
                        db.add(summary)
                db.commit()
        except Exception as e:
            logger.error(f"Migration error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# APP FACTORY
# ─────────────────────────────────────────────────────────────────────────────

docs_url = None if os.environ.get("DISABLE_DOCS", "true").lower() == "true" else "/docs"
app = FastAPI(title="FFP Data Validator API", docs_url=docs_url, redoc_url=None, lifespan=lifespan, strict_slashes=False)


# ── Middleware ────────────────────────────────────────────────────────────────

from .audit import log_api_usage


@app.middleware("http")
async def audit_api_usage_middleware(request: Request, call_next):
    start_time = _time.time()
    response = await call_next(request)
    process_time = (_time.time() - start_time) * 1000

    user = getattr(request.state, "user", None)
    user_id = username = None
    if user:
        user_id = getattr(request.state, "user_id", None)
        username = getattr(request.state, "username", None)

    skip_paths = {"/health", "/favicon.ico", "/api/health"}
    if request.url.path in skip_paths:
        return response

    db = SessionLocal()
    try:
        log_api_usage(
            db=db,
            user_id=user_id,
            username=username,
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            ip_address=request.client.host if request.client else "unknown",
            latency_ms=process_time,
        )
    except Exception:
        pass
    finally:
        db.close()

    return response


# ── CORS ──────────────────────────────────────────────────────────────────────

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "http://localhost,http://localhost:3000")
_allowed_origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)


# ── Rate limiting ─────────────────────────────────────────────────────────────

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# ── Health ────────────────────────────────────────────────────────────────────

_APP_START = _time.time()


@app.get("/health", include_in_schema=False)
async def health_check(db: Session = Depends(get_db)):
    """Lightweight liveness + DB probe. No auth required."""
    try:
        db.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception as exc:
        db_status = f"error: {exc}"
    return {
        "status": "ok" if db_status == "ok" else "degraded",
        "db": db_status,
        "uptime_seconds": round(_time.time() - _APP_START),
        "version": "2.0.0",
    }


# ── Include All Route Modules ────────────────────────────────────────────────

from . import auth_routes
from . import admin_routes
from . import upload_routes
from . import export_routes
from . import statistics_routes
from . import search_routes
from . import batch_routes
from . import task_routes
from . import sync_routes
from . import geo_routes
from . import audit_routes

# API Routes v2.0 (Modular)
app.include_router(auth_routes.router, prefix="/auth")
app.include_router(admin_routes.router, prefix="/admin")
app.include_router(upload_routes.router, prefix="/upload")
app.include_router(export_routes.router, prefix="/export")
app.include_router(statistics_routes.router, prefix="/statistics")
app.include_router(search_routes.router, prefix="")  # search/nid at root /search, /nid
app.include_router(batch_routes.router, prefix="/batches")
app.include_router(task_routes.router, prefix="/tasks", tags=["tasks"])
app.include_router(sync_routes.router, prefix="/sync", tags=["sync"])
app.include_router(geo_routes.router, prefix="/geo", tags=["geo"])
app.include_router(audit_routes.router, prefix="/audit", tags=["audit"])


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
