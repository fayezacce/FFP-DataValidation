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
    # Adding a retry loop for DB startup resilience
    import time
    from sqlalchemy.exc import OperationalError
    
    max_retries = 10
    retry_delay = 2
    for attempt in range(max_retries):
        try:
            db_init = SessionLocal()
            try:
                # Use a Postgres session-level advisory lock to prevent multiple Uvicorn workers
                # from racing to create schema and seed data concurrently, which causes UniqueViolations.
                db_init.execute(text("SELECT pg_advisory_lock(42424242)"))
                db_init.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                db_init.commit()
                
                # Clean up orphan sequences left by previously interrupted create_all()
                try:
                    with engine.connect() as conn:
                        res_tables = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'"))
                        existing_tables = {row[0] for row in res_tables}
                        res_seqs = conn.execute(text("SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema='public'"))
                        existing_seqs = [row[0] for row in res_seqs]
                        for seq_name in existing_seqs:
                            if seq_name.endswith("_id_seq"):
                                table_name = seq_name[:-7]
                                if table_name not in existing_tables:
                                    logger.warning(f"Found orphan sequence '{seq_name}' for missing table '{table_name}'. Dropping...")
                                    conn.execute(text(f"DROP SEQUENCE IF EXISTS {seq_name} CASCADE"))
                        conn.commit()
                except Exception as e:
                    logger.error(f"Orphan sequence cleanup failed: {e}")

                Base.metadata.create_all(bind=engine)
                
                migrate_schema(db_init)
                _migrate_json_to_db(db_init)
                
                # Seed default admin if no users exist
                if db_init.query(User).count() == 0:
                    admin_user = User(username="admin", hashed_password=hash_password("admin123"), role="admin")
                    db_init.add(admin_user)
                    db_init.commit()
                    logger.warning("Default admin user created: admin / admin123 — CHANGE IT IMMEDIATELY!")
                else:
                    admin = db_init.query(User).filter(User.username == "admin").first()
                    if admin and verify_password("admin123", admin.hashed_password):
                        if db_init.query(ValidRecord).count() > 0:
                            app.state.security_lockout = True
                            logger.critical("SECURITY LOCKOUT — default admin password is active AND data exists. Upload endpoints disabled.")
                        else:
                            logger.warning("Default admin password 'admin123' is still active!")

                _seed_geo_data_if_empty(db_init)
                _seed_permissions_if_empty(db_init)
                _sync_geo_aliases(db_init)
                _sync_header_aliases(db_init)

                from . import bd_geo
                bd_geo.load_geo_data_from_db(db_init)
            finally:
                db_init.execute(text("SELECT pg_advisory_unlock(42424242)"))
                db_init.commit()
                db_init.close()
            
            logger.info("Database initialized successfully.")
            break
        except OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Database connection failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Could not connect to database after {max_retries} attempts. Exiting.")
                raise e
        except Exception as e:
            logger.error(f"Unexpected error during application startup: {e}")
            raise e

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
            
            # Composite index for stats recalculation queries (district, upazila)
            idx_dist_upz = f"ix_{table}_district_upazila"
            db.execute(text(f"CREATE INDEX IF NOT EXISTS {idx_dist_upz} ON {table} (district, upazila)"))
            
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

    # ── NEW COLUMN: column_headers on summary_stats and upload_batches ──
    for table in ["summary_stats", "upload_batches"]:
        try:
            db.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS column_headers JSON"))
            db.commit()
        except Exception:
            db.rollback()

    # ── MISSING INDEXES on existing tables ──────────────────────────────
    # create_all() does NOT add indexes to tables that already exist in the DB.
    # We must explicitly create them with IF NOT EXISTS.
    existing_table_indexes = [
        # valid_records indexes
        ("ix_valid_record_name",             "valid_records",  "(name)"),
        ("ix_valid_record_batch",            "valid_records",  "(upload_batch)"),
        ("ix_valid_record_batch_id",         "valid_records",  "(batch_id)"),
        ("ix_valid_upazila_nid",             "valid_records",  "(upazila_id, nid)"),
        ("ix_valid_records_card_no",         "valid_records",  "(card_no)"),
        ("ix_valid_records_mobile",          "valid_records",  "(mobile)"),
        # invalid_records indexes
        ("ix_invalid_record_batch",          "invalid_records", "(upload_batch)"),
        ("ix_invalid_record_batch_id",       "invalid_records", "(batch_id)"),
        ("ix_invalid_upazila_nid",           "invalid_records", "(upazila_id, nid)"),
        ("ix_invalid_records_card_no",       "invalid_records", "(card_no)"),
        ("ix_invalid_records_mobile",        "invalid_records", "(mobile)"),
        ("ix_invalid_records_master_serial", "invalid_records", "(master_serial)"),
        # summary_stats indexes
        ("ix_summary_stats_sorting",         "summary_stats",  "(division, district, upazila)"),
    ]
    for idx_name, table, cols in existing_table_indexes:
        try:
            db.execute(text(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} {cols}"))
            db.commit()
        except Exception:
            db.rollback()

    # GiST trigram indexes for fast global search (require pg_trgm extension)
    trgm_indexes = [
        ("ix_valid_record_nid_trgm",  "valid_records",  "nid",  "gist_trgm_ops"),
        ("ix_valid_record_name_trgm", "valid_records",  "name", "gist_trgm_ops"),
    ]
    for idx_name, table, col, ops in trgm_indexes:
        try:
            db.execute(text(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} USING gist ({col} {ops})"))
            db.commit()
        except Exception as e:
            logger.warning(f"Trgm index {idx_name} skipped: {e}")
            db.rollback()

    # Handle GeoAlias constraint migration (from global unique to composite unique)
    try:
        # Postgres often names unique constraints as {table}_{col}_key
        db.execute(text("ALTER TABLE geo_aliases DROP CONSTRAINT IF EXISTS geo_aliases_alias_name_key"))
        # Drop index created by unique=True
        db.execute(text("DROP INDEX IF EXISTS ix_geo_aliases_alias_name"))
        # Create new composite unique constraint
        db.execute(text("ALTER TABLE geo_aliases ADD CONSTRAINT _alias_target_uc UNIQUE (alias_name, target_type, target_id)"))
        db.commit()
        logger.info("Migrated geo_aliases to composite unique constraint.")
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
                "uploader": ["upload_data", "view_stats", "view_geo"],
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


def _sync_geo_aliases(db: Session):
    """Sync hardcoded aliases from bd_geo.py into the geo_aliases table."""
    from .models import GeoAlias, District, Division, SystemConfig
    try:
        # OPTIMIZATION: Check if sync has already been completed persistently
        sync_flag = db.query(SystemConfig).filter(SystemConfig.key == "geo_aliases_synced_v1").first()
        if sync_flag and sync_flag.value == "true":
            return

        # Pre-defined aliases from system defaults logic
        dist_aliases = {
            "Cumilla": ["Comilla", "Kumilla"],
            "Chattogram": ["Chittagong", "CTG"],
            "Coxsbazar": ["Cox's Bazar", "Coxs Bazar"],
            "Khagrachhari": ["Khagrachari"],
            "Bogura": ["Bogra"],
            "Chapainawabganj": ["Chapai Nawabganj", "Chapai"],
            "Jashore": ["Jessore"],
            "Jhenaidah": ["Jhenaidaha"],
            "Jhalokati": ["Jhalokathi", "Jhalakathi"],
            "Barishal": ["Barisal"],
            "Moulvibazar": ["Maulvibazar"],
            "Netrakona": ["Netrokona"],
        }
        
        div_aliases = {
            "Barishal": ["Barisal"],
            "Chattogram": ["Chittagong"]
        }

        created_count = 0
        updated_count = 0
        
        # 1. Sync District Aliases
        for target_name, aliases in dist_aliases.items():
            district = db.query(District).filter(District.name.ilike(target_name)).first()
            
            if district:
                for alias_name in aliases:
                    # Case-insensitive check for existing alias for this specific target
                    existing = db.query(GeoAlias).filter(
                        GeoAlias.target_type == "district",
                        GeoAlias.target_id == district.id,
                        GeoAlias.alias_name.ilike(alias_name)
                    ).first()
                    
                    if not existing:
                        db.add(GeoAlias(
                            target_type="district",
                            target_id=district.id,
                            alias_name=alias_name
                        ))
                        created_count += 1
                        
        # 2. Sync Division Aliases
        for target_name, aliases in div_aliases.items():
            division = db.query(Division).filter(Division.name.ilike(target_name)).first()
                
            if division:
                for alias_name in aliases:
                    existing = db.query(GeoAlias).filter(
                        GeoAlias.target_type == "division",
                        GeoAlias.target_id == division.id,
                        GeoAlias.alias_name.ilike(alias_name)
                    ).first()
                    
                    if not existing:
                        db.add(GeoAlias(
                            target_type="division",
                            target_id=division.id,
                            alias_name=alias_name
                        ))
                        created_count += 1
        
        if created_count > 0:
            db.commit()
            logger.info(f"Synchronized {created_count} geographic aliases from system defaults.")
        
        # Mark as completed
        if not sync_flag:
            db.add(SystemConfig(key="geo_aliases_synced_v1", value="true", description="Initial geo aliases seed completed"))
            db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error syncing geo aliases: {e}")


def _sync_header_aliases(db: Session):
    """Sync hardcoded aliases from header_mapping.json into the header_aliases table."""
    from .models import HeaderAlias, SystemConfig
    try:
        # OPTIMIZATION: Check if sync has already been completed persistently
        sync_flag = db.query(SystemConfig).filter(SystemConfig.key == "header_aliases_synced_v1").first()
        if sync_flag and sync_flag.value == "true":
            return

        mapping_path = os.path.join(os.path.dirname(__file__), "header_mapping.json")
        if os.path.exists(mapping_path):
            with open(mapping_path, "r", encoding="utf-8") as f:
                mapping = json.load(f)
            
            created_count = 0
            # Get all existing headers in one go for efficiency
            existing_headers = {h[0] for h in db.query(HeaderAlias.original_header).all()}
            
            for original_header, canonical_key in mapping.items():
                if original_header not in existing_headers:
                    db.add(HeaderAlias(
                        original_header=original_header,
                        canonical_key=canonical_key
                    ))
                    existing_headers.add(original_header) 
                    created_count += 1
            
            if created_count > 0:
                db.commit()
                logger.info(f"Synchronized {created_count} new header aliases from JSON mapping to database.")
            
            # Mark as completed
            if not sync_flag:
                db.add(SystemConfig(key="header_aliases_synced_v1", value="true", description="Initial header alias seed completed"))
                db.commit()
            else:
                logger.info("Header aliases are already up-to-date with JSON mapping.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error syncing header aliases from JSON: {e}")


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
from . import alias_routes

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
app.include_router(alias_routes.router)  # prefix is defined inside the router


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
