"""
FFP Data Validator API
Author: Fayez Ahmed, Assistant Programmer, DG Food

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
import re
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
            try:
                with engine.connect() as conn:
                    # Advisory lock at the session level to synchronize all workers
                    conn.execute(text("SELECT pg_advisory_lock(42424242)"))
                    
                    try:
                        # 1. Extensions
                        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                        conn.commit()
                        
                        # 2. Cleanup orphan sequences
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

                        # 3. Create all tables (idempotent, but now synchronized)
                        Base.metadata.create_all(bind=conn)
                        conn.commit()
                        
                        # 4. Migrations & Seeding (using a Session bound to this connection)
                        with Session(bind=conn) as sess:
                            migrate_schema(sess)
                            
                            # Seed default admin
                            if sess.query(User).count() == 0:
                                admin_user = User(username="admin", hashed_password=hash_password("admin123"), role="admin")
                                sess.add(admin_user)
                                sess.commit()
                                logger.warning("Default admin user created: admin / admin123 — CHANGE IT IMMEDIATELY!")
                            else:
                                admin = sess.query(User).filter(User.username == "admin").first()
                                if admin and verify_password("admin123", admin.hashed_password):
                                    if sess.query(ValidRecord).count() > 0:
                                        app.state.security_lockout = True
                                        logger.critical("SECURITY LOCKOUT — default admin password is active AND data exists.")
                                    else:
                                        logger.warning("Default admin password 'admin123' is still active!")

                            _seed_geo_data_if_empty(sess)
                            _seed_permissions_if_empty(sess)
                            _sync_geo_aliases(sess)
                            _sync_header_aliases(sess)
                            from . import bd_geo
                            bd_geo.load_geo_data_from_db(sess)
                            sess.commit()
                    finally:
                        # Rollback any aborted transaction before releasing lock
                        # (migration steps catch their own exceptions, but some may slip through)
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        try:
                            conn.execute(text("SELECT pg_advisory_unlock(42424242)"))
                            conn.commit()
                        except Exception as unlock_err:
                            logger.warning(f"Advisory lock unlock failed (non-fatal): {unlock_err}")
            except Exception as e:
                logger.error(f"Database initialization failed: {e}")
                raise e
            
            logger.info("Database initialized successfully.")
            break
        except (OperationalError, Exception) as e:
            if attempt < max_retries - 1:
                logger.warning(f"Database connection failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Could not connect to database after {max_retries} attempts or critical error. Exiting.")
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
        # Create new composite unique constraint — idempotent via pg_constraint check
        db.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = '_alias_target_uc'
                      AND conrelid = 'geo_aliases'::regclass
                ) THEN
                    ALTER TABLE geo_aliases
                        ADD CONSTRAINT _alias_target_uc UNIQUE (alias_name, target_type, target_id);
                END IF;
            END
            $$;
        """))
        db.commit()
        logger.info("Migrated geo_aliases to composite unique constraint.")
    except Exception:
        db.rollback()

    # ── Beneficiary management columns on valid_records and invalid_records ──
    beneficiary_cols = [
        ("father_husband_name", "VARCHAR"),
        ("name_bn",            "VARCHAR"),
        ("name_en",            "VARCHAR"),
        ("ward",               "VARCHAR"),
        ("union_name",         "VARCHAR"),
        ("dealer_id",          "INTEGER"),
        ("verification_status","VARCHAR DEFAULT 'unverified'"),
        ("verified_by_id",     "INTEGER"),
        ("verified_by",        "VARCHAR"),
        ("verified_at",        "TIMESTAMP"),
        # ── Standard Canonical Fields: promoted from data JSON ──
        ("occupation",         "VARCHAR"),
        ("gender",             "VARCHAR"),
        ("religion",           "VARCHAR"),
        ("address",            "VARCHAR"),
        ("spouse_name",        "VARCHAR"),
        ("spouse_nid",         "VARCHAR"),
        ("spouse_dob",         "VARCHAR"),
    ]
    for table in ["valid_records", "invalid_records"]:
        for col_name, col_def in beneficiary_cols:
            # Note: invalid_records doesn't need verification status logically, 
            # but we add it for schema consistency so shared code works easily.
            try:
                db.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_def}"))
                db.commit()
            except Exception:
                db.rollback()

    # Indexes for new management columns
    mgmt_indexes = [
        ("ix_valid_record_father",        "valid_records",   "(father_husband_name)"),
        ("ix_valid_record_verification",  "valid_records",   "(verification_status)"),
        ("ix_valid_record_dealer",        "valid_records",   "(dealer_id)"),
        ("ix_invalid_record_father",      "invalid_records", "(father_husband_name)"),
        ("ix_invalid_record_dealer",      "invalid_records", "(dealer_id)"),
        ("ix_dealers_nid",                "dealers",         "(nid)"),
        ("ix_dealers_upazila_id",         "dealers",         "(upazila_id)"),
        # ── New canonical column indexes ──
        ("ix_valid_gender",               "valid_records",   "(gender)"),
        ("ix_valid_address",              "valid_records",   "(address)"),
        ("ix_valid_occupation",           "valid_records",   "(occupation)"),
        ("ix_valid_spouse_nid",           "valid_records",   "(spouse_nid)"),
        ("ix_invalid_gender",             "invalid_records", "(gender)"),
        ("ix_invalid_spouse_nid",         "invalid_records", "(spouse_nid)"),
    ]
    for idx_name, table, cols in mgmt_indexes:
        try:
            db.execute(text(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} {cols}"))
            db.commit()
        except Exception:
            db.rollback()


# ─────────────────────────────────────────────────────────────────────────────
# SEEDING
# ─────────────────────────────────────────────────────────────────────────────


def _backfill_canonical_columns(db: Session, task_id: str = None):
    """
    Admin maintenance Task 6: blazing-fast SQL-native backfill of all 7 new
    canonical columns (occupation, gender, religion, address, spouse_*)
    from the data JSONB into dedicated typed columns.

    Prerequisite: Task 4 (Normalize JSON Keys) must have run first so
    canonical keys already exist inside the data JSON blob.

    Strategy:
    - Pure SQL: no Python JSON parsing, all work in Postgres JSONB engine
    - 20,000-row chunks per transaction (proven migrate_dealers pattern)
    - COALESCE(col, new_val) never overwrites manually-edited data
    - Idempotent: WHERE clause skips already-backfilled rows
    """
    from .models import BackgroundTask

    def _update_task(msg: str, pct: int, status: str = "running"):
        if not task_id:
            return
        try:
            t = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
            if t:
                t.status, t.progress, t.message = status, pct, msg
                db.commit()
        except Exception:
            db.rollback()

    def _is_cancelled() -> bool:
        if not task_id:
            return False
        try:
            db.expire_all()
            t = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
            return t is None or t.status == "error"
        except Exception:
            return False

    CHUNK = 20_000
    UPDATE_SQL = """
        UPDATE {table} SET
            occupation  = COALESCE(occupation,  NULLIF(TRIM(data->>'occupation'),  '')),
            gender      = COALESCE(gender,      NULLIF(TRIM(data->>'gender'),      '')),
            religion    = COALESCE(religion,    NULLIF(TRIM(data->>'religion'),    '')),
            address     = COALESCE(address,     NULLIF(TRIM(data->>'address'),     '')),
            spouse_name = COALESCE(spouse_name, NULLIF(TRIM(data->>'spouse_name'), '')),
            spouse_nid  = COALESCE(spouse_nid,  NULLIF(TRIM(data->>'spouse_nid'),  '')),
            spouse_dob  = COALESCE(spouse_dob,  NULLIF(TRIM(data->>'spouse_dob'),  ''))
        WHERE id IN (
            SELECT id FROM {table}
            WHERE data IS NOT NULL
              AND (occupation IS NULL OR gender IS NULL)
              AND id > :last_id
            ORDER BY id ASC
            LIMIT :lim
        )
        RETURNING id
    """
    try:
        _update_task("Computing row counts...", 1)
        total_valid = db.execute(text(
            "SELECT count(*) FROM valid_records WHERE data IS NOT NULL AND (occupation IS NULL OR gender IS NULL)"
        )).scalar() or 0
        total_invalid = db.execute(text(
            "SELECT count(*) FROM invalid_records WHERE data IS NOT NULL AND (occupation IS NULL OR gender IS NULL)"
        )).scalar() or 0
        logger.info(f"Canonical backfill scope: {total_valid:,} valid + {total_invalid:,} invalid rows")
        if total_valid + total_invalid == 0:
            _update_task("All columns already backfilled. No action needed.", 100, "completed")
            return
        # ── PHASE 1: valid_records ──
        processed, last_id = 0, 0
        _update_task(f"Phase 1/2: Backfilling {total_valid:,} valid records...", 5)
        while True:
            if _is_cancelled():
                return
            result = db.execute(text(UPDATE_SQL.format(table="valid_records")), {"last_id": last_id, "lim": CHUNK})
            db.commit()
            ids = result.fetchall()
            if not ids:
                break
            last_id = max(r[0] for r in ids)
            processed += len(ids)
            pct = 5 + int((processed / max(total_valid, 1)) * 44)
            _update_task(f"Phase 1/2: valid records ({processed:,}/{total_valid:,})", min(pct, 49))
        # ── PHASE 2: invalid_records ──
        inv_processed, last_id = 0, 0
        _update_task(f"Phase 2/2: Backfilling {total_invalid:,} invalid records...", 50)
        while True:
            if _is_cancelled():
                return
            result = db.execute(text(UPDATE_SQL.format(table="invalid_records")), {"last_id": last_id, "lim": CHUNK})
            db.commit()
            ids = result.fetchall()
            if not ids:
                break
            last_id = max(r[0] for r in ids)
            inv_processed += len(ids)
            pct = 50 + int((inv_processed / max(total_invalid, 1)) * 48)
            _update_task(f"Phase 2/2: invalid records ({inv_processed:,}/{total_invalid:,})", min(pct, 98))
        msg = f"Done! {processed:,} valid + {inv_processed:,} invalid rows backfilled."
        logger.info(f"Canonical backfill complete: {msg}")
        _update_task(msg, 100, "completed")
    except Exception as e:
        db.rollback()
        logger.error(f"Canonical backfill failed: {e}")
        if task_id:
            try:
                t = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
                if t:
                    t.status, t.error_details = "error", str(e)
                    db.commit()
            except Exception:
                pass


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
                ("upload_data",    "Can upload and validate Excel files"),
                ("view_stats",     "Can view statistics and dashboard"),
                ("view_geo",       "Can view geographical hierarchy"),
                ("view_admin",     "Can view administrative settings"),
                ("manage_users",   "Can create/edit users"),
                ("manage_geo",     "Can edit geographical data"),
                ("manage_records", "Can edit, add, and verify beneficiary records"),
            ]
            for name, desc in perms:
                db.add(Permission(name=name, description=desc))

            role_map = {
                "admin":    [p[0] for p in perms],
                "uploader": ["upload_data", "view_stats", "view_geo", "manage_records"],
                "viewer":   ["view_stats", "view_geo"],
            }
            for role, pnames in role_map.items():
                for pname in pnames:
                    db.add(RolePermission(role=role, permission_name=pname))
            db.commit()
            logger.info("Seeded permissions and role mappings.")
        else:
            # Additive: ensure manage_records exists for existing deployments
            existing_names = {p[0] for p in db.query(Permission.name).all()}
            if "manage_records" not in existing_names:
                db.add(Permission(name="manage_records", description="Can edit, add, and verify beneficiary records"))
                for role in ["admin", "uploader"]:
                    db.add(RolePermission(role=role, permission_name="manage_records"))
                db.commit()
                logger.info("Added manage_records permission to existing deployment.")
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
        mapping_path = os.path.join(os.path.dirname(__file__), "header_mapping.json")
        if os.path.exists(mapping_path):
            with open(mapping_path, "r", encoding="utf-8") as f:
                mapping = json.load(f)
            
            created_count = 0
            updated_count = 0
            
            # Get all existing headers for quick lookup
            existing = {h.original_header: h for h in db.query(HeaderAlias).all()}
            
            for original_header, canonical_key in mapping.items():
                if original_header in existing:
                    # Update if changed
                    if existing[original_header].canonical_key != canonical_key:
                        existing[original_header].canonical_key = canonical_key
                        updated_count += 1
                else:
                    # Add new
                    db.add(HeaderAlias(
                        original_header=original_header,
                        canonical_key=canonical_key
                    ))
                    created_count += 1
            
            if created_count > 0 or updated_count > 0:
                db.commit()
                logger.info(f"Header aliases sync: {created_count} added, {updated_count} updated.")
            
            # We don't rely on the flag anymore to allow updates
            # (or we can just keep it for first-run logic but we want updates to work)
    except Exception as e:
        db.rollback()
        logger.error(f"Error syncing header aliases from JSON: {e}")


def _normalize_json_keys(db, task_id: str = None):
    """
    Background admin task: Normalizes JSON keys across all valid_records and invalid_records.
    Reads all header aliases from the DB, and injects canonical keys into data json
    if their original counterparts exist. Preserves original keys.
    """
    import json
    from .models import HeaderAlias, BackgroundTask, SystemConfig

    def _update_task(msg: str, pct: int, status: str = "running"):
        if not task_id:
            return
        try:
            t = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
            if t:
                t.status = status
                t.progress = pct
                t.message = msg
                db.commit()
        except Exception:
            db.rollback()

    def _is_cancelled() -> bool:
        if not task_id:
            return False
        try:
            db.expire_all()
            t = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
            return t is None or t.status == "error"
        except Exception:
            return False

    try:
        flag = db.query(SystemConfig).filter(SystemConfig.key == "json_keys_normalized_v1").first()
        if flag and flag.value == "true":
            _update_task("Keys already normalized. No further action needed.", 100, "completed")
            return

        _update_task("Loading header aliases from database...", 1)
        
        # Build mapping: original -> canonical (whitespace-agnostic)
        aliases = db.query(HeaderAlias).all()
        alias_map = {re.sub(r'\s+', ' ', a.original_header).strip(): a.canonical_key for a in aliases if a.original_header.strip()}
        
        logger.info(f"Loaded {len(alias_map)} header aliases for normalization.")

        _update_task("Computing row counts...", 2)
        total_valid = db.execute(text("SELECT count(*) FROM valid_records WHERE data IS NOT NULL")).scalar() or 0
        total_invalid = db.execute(text("SELECT count(*) FROM invalid_records WHERE data IS NOT NULL")).scalar() or 0
        total_records = total_valid + total_invalid
        
        if total_records == 0:
            _update_task("No records to normalize.", 100, "completed")
            return

        CHUNK_SIZE = 10000
        processed = 0

        # Process valid_records
        last_id = 0
        _update_task(f"Normalizing valid records (0/{total_valid:,})...", 5)
        while True:
            if _is_cancelled():
                logger.info("Normalization cancelled by user.")
                return

            rows = db.execute(text("""
                SELECT id, data FROM valid_records 
                WHERE data IS NOT NULL AND id > :last_id 
                ORDER BY id ASC LIMIT :lim
            """), {"last_id": last_id, "lim": CHUNK_SIZE}).fetchall()

            if not rows:
                break

            updates = []
            for r in rows:
                if not r.data or not isinstance(r.data, dict):
                    continue
                
                d = r.data
                changed = False
                for k, v in list(d.items()):
                    if not k: continue
                    # Whitespace-agnostic cleaning to match aliases with newlines/tabs
                    clean_k = re.sub(r'\s+', ' ', k).strip()
                    if clean_k in alias_map:
                        canonical = alias_map[clean_k]
                        # only add if canonical not already present
                        if canonical not in d:
                            d[canonical] = v
                            changed = True

                if changed:
                    updates.append({"id": r.id, "data": json.dumps(d)})
            
            if updates:
                db.execute(text("UPDATE valid_records SET data = CAST(:data AS JSONB) WHERE id = :id"), updates)
                db.commit()

            last_id = max(r.id for r in rows)
            processed += len(rows)
            pct = 5 + int((processed / total_records) * 90)
            _update_task(f"Normalizing valid records... ({processed:,}/{total_valid:,})", min(pct, 95))
            logger.info(f"Normalized {processed}/{total_records} records")

        # Process invalid_records
        last_id = 0
        while True:
            if _is_cancelled():
                logger.info("Normalization cancelled by user.")
                return

            rows = db.execute(text("""
                SELECT id, data FROM invalid_records 
                WHERE data IS NOT NULL AND id > :last_id 
                ORDER BY id ASC LIMIT :lim
            """), {"last_id": last_id, "lim": CHUNK_SIZE}).fetchall()

            if not rows:
                break

            updates = []
            for r in rows:
                if not r.data or not isinstance(r.data, dict):
                    continue
                
                d = r.data
                changed = False
                for k, v in list(d.items()):
                    if not k: continue
                    # Whitespace-agnostic cleaning to match aliases with newlines/tabs
                    clean_k = re.sub(r'\s+', ' ', k).strip()
                    if clean_k in alias_map:
                        canonical = alias_map[clean_k]
                        if canonical not in d:
                            d[canonical] = v
                            changed = True

                if changed:
                    updates.append({"id": r.id, "data": json.dumps(d)})
            
            if updates:
                db.execute(text("UPDATE invalid_records SET data = CAST(:data AS JSONB) WHERE id = :id"), updates)
                db.commit()

            last_id = max(r.id for r in rows)
            # note: 'processed' includes valid + invalid for total progress
            processed += len(rows)
            # count specifically for message
            inv_count = processed - total_valid
            pct = 5 + int((processed / total_records) * 90)
            _update_task(f"Normalizing invalid records... ({inv_count:,}/{total_invalid:,})", min(pct, 95))

        # Mark done
        existing_flag = db.query(SystemConfig).filter(SystemConfig.key == "json_keys_normalized_v1").first()
        if existing_flag:
            existing_flag.value = "true"
        else:
            db.add(SystemConfig(key="json_keys_normalized_v1", value="true", description="Data JSON canonical keys injected"))
        db.commit()

        _update_task(f"Success! Normalized {total_records:,} records.", 100, "completed")

    except Exception as e:
        db.rollback()
        logger.error(f"JSON normalization failed: {e}")
        if task_id:
            try:
                t = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
                if t:
                    t.status = "error"
                    t.error_details = str(e)
                    db.commit()
            except Exception:
                pass


def _migrate_dealers_from_json(db: Session, task_id: str = None):
    """
    Background admin task: blazing-fast JSONB set-based backfill.
    Relies entirely on CANONICAL keys injected by _normalize_json_keys.
    Extracts dealer info + promoted columns from the data JSON inside PostgreSQL.
    """
    from .models import SystemConfig, Dealer, BackgroundTask

    def _update_task(msg: str, pct: int, status: str = "running"):
        if not task_id:
            return
        try:
            t = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
            if t:
                t.status = status
                t.progress = pct
                t.message = msg
                db.commit()
        except Exception:
            db.rollback()

    def _is_cancelled() -> bool:
        if not task_id:
            return False
        try:
            db.expire_all()
            t = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
            return t is None or t.status == "error"
        except Exception:
            return False

    try:
        # Note: lockout check removed in v2.0 to allow 'Force Refresh' via Admin UI
        # flag = db.query(SystemConfig).filter(SystemConfig.key == "dealers_migrated_v1").first()
        # if flag and flag.value == "true":
        #    ...

        logger.info("Starting SQL-native dealer migration (Canonical fast-path)...")
        _update_task("Computing row counts...", 1)

        # Treat empty string ('') as NULL to allow backfilling incomplete records
        total_valid   = db.execute(text("SELECT count(*) FROM valid_records WHERE (name_bn IS NULL OR name_bn = '') AND data IS NOT NULL")).scalar() or 0
        total_invalid = db.execute(text("SELECT count(*) FROM invalid_records WHERE (name_bn IS NULL OR name_bn = '') AND data IS NOT NULL")).scalar() or 0
        total_records = total_valid + total_invalid
        logger.info(f"Migration scope: {total_valid} valid + {total_invalid} invalid = {total_records} rows")

        # ── PHASE 1: Backfill promoted columns on valid_records (Canonical Only) ──
        _update_task(f"Phase 1/3: Backfilling {total_valid:,} valid records...", 5)
        CHUNK = 20_000
        processed = 0
        last_id = 0

        while True:
            if _is_cancelled():
                logger.info("Migration cancelled by user.")
                return

            result = db.execute(text("""
                UPDATE valid_records SET
                    name_bn = COALESCE(NULLIF(TRIM(data->>'name_bn'), ''), NULLIF(TRIM(data->>'name_bn_en'), '')),
                    name_en = COALESCE(NULLIF(TRIM(data->>'name_en'), ''), NULLIF(TRIM(data->>'name_en_en'), '')),
                    father_husband_name = COALESCE(father_husband_name, NULLIF(TRIM(data->>'father_husband_name'), ''), NULLIF(TRIM(data->>'spouse_name'), '')),
                    ward = COALESCE(ward, NULLIF(TRIM(data->>'ward'), '')),
                    union_name = COALESCE(union_name, NULLIF(TRIM(data->>'union_name'), ''))
                WHERE id IN (
                    SELECT id FROM valid_records
                    WHERE (name_bn IS NULL OR name_bn = '') AND data IS NOT NULL AND id > :last_id
                    ORDER BY id ASC LIMIT :lim
                )
                RETURNING id
            """), {"last_id": last_id, "lim": CHUNK})
            db.commit()

            updated_ids = result.fetchall()
            if not updated_ids:
                break

            last_id = max(r[0] for r in updated_ids)
            processed += len(updated_ids)
            pct = 5 + int((processed / max(total_records, 1)) * 40)
            _update_task(f"Phase 1/3: Backfilling valid records... ({processed:,}/{total_valid:,})", min(pct, 44))

        # ── PHASE 2: Create dealers + link back in one set-based pass (Canonical) ──
        _update_task("Phase 2/3: Extracting & inserting NEW dealers from JSON data...", 45)

        db.execute(text("""
            INSERT INTO dealers (nid, name, mobile, upazila, district, division,
                                 upazila_id, district_id, division_id, created_at)
            SELECT DISTINCT ON (nid_val, upazila_id)
                nid_val AS nid,
                COALESCE(NULLIF(name_val, ''), 'Unknown') AS name,
                mobile_val AS mobile,
                upazila, district, division,
                upazila_id, district_id, division_id,
                NOW()
            FROM (
                SELECT
                    NULLIF(TRIM(data->>'dealer_nid'), '') AS nid_val,
                    TRIM(data->>'dealer_name') AS name_val,
                    NULLIF(TRIM(data->>'dealer_mobile'), '') AS mobile_val,
                    upazila, district, division,
                    upazila_id, district_id, division_id
                FROM valid_records
                WHERE upazila_id IS NOT NULL AND data IS NOT NULL
            ) sub
            WHERE nid_val IS NOT NULL
            ON CONFLICT (nid, upazila_id) DO NOTHING
        """))
        db.commit()
        
        _update_task("Phase 2/3: Linking dealers to records (matching by Geo + NID)...", 70)

        db.execute(text("""
            UPDATE valid_records vr
            SET dealer_id = d.id
            FROM dealers d
            WHERE vr.dealer_id IS NULL
              AND d.upazila_id = vr.upazila_id
              AND d.nid = NULLIF(TRIM(vr.data->>'dealer_nid'), '')
        """))
        db.commit()
        
        _update_task("Phase 2/3: Dealer link complete.", 80)

        # ── PHASE 3: Backfill invalid_records (Canonical Only) ──────────────────────
        _update_task(f"Phase 3/3: Backfilling {total_invalid:,} invalid records...", 82)
        inv_processed = 0
        last_inv_id = 0

        while True:
            if _is_cancelled():
                logger.info("Migration cancelled by user.")
                return

            result = db.execute(text("""
                UPDATE invalid_records SET
                    name_bn = COALESCE(NULLIF(TRIM(data->>'name_bn'), ''), NULLIF(TRIM(data->>'name_bn_en'), '')),
                    name_en = COALESCE(NULLIF(TRIM(data->>'name_en'), ''), NULLIF(TRIM(data->>'name_en_en'), '')),
                    father_husband_name = COALESCE(father_husband_name, NULLIF(TRIM(data->>'father_husband_name'), ''), NULLIF(TRIM(data->>'spouse_name'), '')),
                    ward = COALESCE(ward, NULLIF(TRIM(data->>'ward'), '')),
                    union_name = COALESCE(union_name, NULLIF(TRIM(data->>'union_name'), ''))
                WHERE id IN (
                    SELECT id FROM invalid_records
                    WHERE (name_bn IS NULL OR name_bn = '') AND data IS NOT NULL AND id > :last_id
                    ORDER BY id ASC LIMIT :lim
                )
                RETURNING id
            """), {"last_id": last_inv_id, "lim": CHUNK})
            db.commit()

            updated_ids = result.fetchall()
            if not updated_ids:
                break

            last_inv_id = max(r[0] for r in updated_ids)
            inv_processed += len(updated_ids)
            pct = 82 + int((inv_processed / max(total_invalid, 1)) * 16)
            _update_task(f"Phase 3/3: Backfilling invalid records... ({inv_processed:,}/{total_invalid:,})", min(pct, 98))

        # ── Mark complete ──────────────────────────────────────────────────────────
        existing_flag = db.query(SystemConfig).filter(SystemConfig.key == "dealers_migrated_v1").first()
        if existing_flag:
            existing_flag.value = "true"
        else:
            db.add(SystemConfig(
                key="dealers_migrated_v1",
                value="true",
                description="One-time dealer extraction and column backfill completed"
            ))
        db.commit()

        summary = f"Done! {processed:,} valid + {inv_processed:,} invalid rows backfilled."
        logger.info(f"Migration complete: {summary}")
        _update_task(summary, 100, "completed")

    except Exception as e:
        db.rollback()
        logger.error(f"Dealer migration failed: {e}")
        if task_id:
            try:
                t = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
                if t:
                    t.status = "error"
                    t.error_details = str(e)
                    db.commit()
            except Exception:
                db.rollback()

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
from . import records_routes

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
app.include_router(alias_routes.router)  # prefix defined inside router
app.include_router(records_routes.router, prefix="/records", tags=["records"])


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
