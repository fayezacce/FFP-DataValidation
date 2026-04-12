#!/usr/bin/env python3
"""
FFP Platform — Pre-Upgrade Schema Verification
================================================
Run this BEFORE the upgrade to verify:
1. Which DB tables/columns already exist in production
2. Which new columns/tables will be added by migrate_schema()
3. Which backfills are needed

Usage inside the backend container (connected to prod or restored backup):
  python app/scripts/verify_upgrade.py

Or locally with DATABASE_URL set:
  DATABASE_URL=postgresql://... python app/scripts/verify_upgrade.py
"""

import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger("verify_upgrade")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    DB_USER = os.environ.get("POSTGRES_USER", "ffp_admin")
    DB_PASS = os.environ.get("POSTGRES_PASSWORD", "")
    DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
    DB_NAME = os.environ.get("POSTGRES_DB", "ffp_validator")
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

from sqlalchemy import create_engine, text

engine = create_engine(DATABASE_URL)


def column_exists(conn, table: str, column: str) -> bool:
    res = conn.execute(text("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = :tbl AND column_name = :col
    """), {"tbl": table, "col": column})
    return res.fetchone() is not None


def table_exists(conn, table: str) -> bool:
    res = conn.execute(text("""
        SELECT 1 FROM information_schema.tables
        WHERE table_name = :tbl
    """), {"tbl": table})
    return res.fetchone() is not None


def index_exists(conn, index_name: str) -> bool:
    res = conn.execute(text("""
        SELECT 1 FROM pg_indexes WHERE indexname = :idx
    """), {"idx": index_name})
    return res.fetchone() is not None


def count_rows(conn, table: str, where: str = "1=1") -> int:
    res = conn.execute(text(f"SELECT COUNT(*) FROM {table} WHERE {where}"))
    return res.scalar()


def ok(s):  return f"\033[92m✓  {s}\033[0m"
def warn(s): return f"\033[93m⚠  {s}\033[0m"
def err(s):  return f"\033[91m✗  {s}\033[0m"


def main():
    print("\n" + "=" * 64)
    print("  FFP Platform — Pre-Upgrade Schema Verification")
    print("=" * 64 + "\n")

    with engine.connect() as conn:

        # ── 1. New Tables ────────────────────────────────────────────
        print("[ New Tables Required by v2.0 ]")
        new_tables = [
            ("background_tasks",  "Tracks async export/zip tasks"),
            ("export_history",    "Audit trail for CSV exports"),
            ("export_tracking",   "Tracks last export timestamp per scope"),
        ]
        for tbl, desc in new_tables:
            if table_exists(conn, tbl):
                print(ok(f"{tbl:<25} already exists"))
            else:
                print(warn(f"{tbl:<25} MISSING — will be created on startup  ({desc})"))
        print()

        # ── 2. New Columns ───────────────────────────────────────────
        print("[ New Columns Required by v2.0 ]")
        mutations = [
            ("summary_stats",  "division_id",       "INTEGER — geo ID index"),
            ("summary_stats",  "district_id",       "INTEGER — geo ID index"),
            ("summary_stats",  "upazila_id",        "INTEGER — geo ID index"),
            ("summary_stats",  "column_headers",    "JSON — original excel headers"),
            ("summary_stats",  "last_upload_total", "INTEGER — per-upload snapshot"),
            ("summary_stats",  "last_upload_valid", "INTEGER — per-upload snapshot"),
            ("summary_stats",  "last_upload_invalid","INTEGER — per-upload snapshot"),
            ("summary_stats",  "last_upload_new",   "INTEGER — per-upload snapshot"),
            ("summary_stats",  "last_upload_updated","INTEGER — per-upload snapshot"),
            ("summary_stats",  "excel_valid_url",   "VARCHAR — valid excel URL"),
            ("summary_stats",  "excel_invalid_url", "VARCHAR — invalid excel URL"),
            ("summary_stats",  "pdf_invalid_url",   "VARCHAR — invalid PDF URL"),
            ("upload_batches", "division_id",       "INTEGER — geo ID index"),
            ("upload_batches", "district_id",       "INTEGER — geo ID index"),
            ("upload_batches", "upazila_id",        "INTEGER — geo ID index"),
            ("upload_batches", "valid_url",         "VARCHAR — valid excel URL"),
            ("upload_batches", "invalid_url",       "VARCHAR — invalid excel URL"),
            ("upload_batches", "pdf_url",           "VARCHAR — PDF report URL"),
            ("upload_batches", "pdf_invalid_url",   "VARCHAR — invalid PDF URL"),
            ("upload_batches", "column_headers",    "JSON — original excel headers"),
            ("valid_records",  "division_id",       "INTEGER — geo ID for fast lookup"),
            ("valid_records",  "district_id",       "INTEGER — geo ID for fast lookup"),
            ("valid_records",  "upazila_id",        "INTEGER — CRITICAL for export performance"),
            ("valid_records",  "batch_id",          "INTEGER — links to upload_batches"),
            ("valid_records",  "mobile",            "VARCHAR — secondary identifier"),
            ("invalid_records","division_id",       "INTEGER — geo ID for fast lookup"),
            ("invalid_records","district_id",       "INTEGER — geo ID for fast lookup"),
            ("invalid_records","upazila_id",        "INTEGER — CRITICAL for export performance"),
            ("invalid_records","batch_id",          "INTEGER — links to upload_batches"),
            ("invalid_records","card_no",           "VARCHAR — card number"),
            ("invalid_records","master_serial",     "VARCHAR — serial number"),
            ("invalid_records","mobile",            "VARCHAR — mobile number"),
            ("upazilas",       "quota",             "INTEGER DEFAULT 0 — beneficiary quota"),
            ("users",          "api_key_last_used", "TIMESTAMP — API key tracking"),
            ("users",          "division_access",   "VARCHAR — RBAC geo restriction"),
            ("users",          "district_access",   "VARCHAR — RBAC geo restriction"),
            ("users",          "upazila_access",    "VARCHAR — RBAC geo restriction"),
        ]

        missing_cols = []
        for tbl, col, desc in mutations:
            exists = column_exists(conn, tbl, col)
            if exists:
                print(ok(f"{tbl}.{col:<22} exists"))
            else:
                print(warn(f"{tbl}.{col:<22} MISSING — migrate_schema() will ADD  ({desc})"))
                missing_cols.append((tbl, col))
        print()

        # ── 3. Indexes ───────────────────────────────────────────────
        print("[ Performance Indexes ]")
        indexes = [
            ("ix_valid_records_upazila_id",   "CRITICAL for export speed"),
            ("ix_valid_records_geo_names",    "Composite division/district/upazila"),
            ("ix_invalid_records_upazila_id", "CRITICAL for export speed"),
            ("ix_invalid_records_geo_names",  "Composite division/district/upazila"),
            ("ix_valid_upazila_nid",          "Upazila+NID compound lookup"),
            ("ix_invalid_upazila_nid",        "Upazila+NID compound lookup"),
        ]
        for idx, desc in indexes:
            if index_exists(conn, idx):
                print(ok(f"{idx:<40} exists"))
            else:
                print(warn(f"{idx:<40} MISSING — will be created  ({desc})"))
        print()

        # ── 4. Backfill Needs ────────────────────────────────────────
        print("[ Backfill Requirements ]")

        valid_total  = count_rows(conn, "valid_records")
        invalid_total = count_rows(conn, "invalid_records")

        v_missing_geo = count_rows(conn, "valid_records",   "upazila_id IS NULL") \
            if column_exists(conn, "valid_records", "upazila_id") else "N/A (column missing — will be 100% after upgrade)"
        i_missing_geo = count_rows(conn, "invalid_records", "upazila_id IS NULL") \
            if column_exists(conn, "invalid_records", "upazila_id") else "N/A (column missing — will be 100% after upgrade)"
        s_missing_geo = count_rows(conn, "summary_stats",   "upazila_id IS NULL") \
            if column_exists(conn, "summary_stats", "upazila_id") else "N/A (column missing — will be 100% after upgrade)"
        b_missing_geo = count_rows(conn, "upload_batches",  "upazila_id IS NULL") \
            if column_exists(conn, "upload_batches", "upazila_id") else "N/A (column missing — will be 100% after upgrade)"

        v_missing_mobile = count_rows(conn, "valid_records",   "mobile IS NULL OR mobile = ''") \
            if column_exists(conn, "valid_records", "mobile") else "N/A (column missing)"

        i_missing_mobile = count_rows(conn, "invalid_records", "mobile IS NULL OR mobile = ''") \
            if column_exists(conn, "invalid_records", "mobile") else "N/A (column missing)"

        print(f"  Valid records total         : {valid_total:,}")
        print(f"  Invalid records total       : {invalid_total:,}")
        print()
        def bf(label, count):
            if isinstance(count, str):
                print(warn(f"  {label:<40}: {count}"))
            elif count == 0:
                print(ok(f"  {label:<40}: 0 (already done)"))
            else:
                print(warn(f"  {label:<40}: {count:,} rows  ← backfill needed"))

        bf("valid_records  missing upazila_id", v_missing_geo)
        bf("invalid_records missing upazila_id", i_missing_geo)
        bf("summary_stats  missing upazila_id", s_missing_geo)
        bf("upload_batches  missing upazila_id", b_missing_geo)
        bf("valid_records  missing mobile",     v_missing_mobile)
        bf("invalid_records missing mobile",    i_missing_mobile)
        print()

        # ── 5. Summary ───────────────────────────────────────────────
        print("[ Summary ]")
        if not missing_cols and v_missing_geo == 0 and i_missing_geo == 0:
            print(ok("Schema is up-to-date. No migrations needed."))
        else:
            print(warn(f"{len(missing_cols)} columns missing — migrate_schema() will add them on startup."))
            print(warn("Run backfill scripts after deploy to populate geo IDs and mobile numbers."))

        print("\n  Recommended post-deploy order:")
        print("  1. docker compose -f docker-compose.prod.yml up -d  (migrate_schema runs automatically)")
        print("  2. python backfill_geo_ids.py                       (populate division/district/upazila IDs)")
        print("  3. python app/scripts/backfill_columns.py           (populate mobile from data JSON)")
        print("  4. python app/scripts/sync_all_stats.py             (recalculate summary stats)")
        print()


if __name__ == "__main__":
    main()
