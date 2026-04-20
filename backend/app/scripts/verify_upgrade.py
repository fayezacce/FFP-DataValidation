"""
Post-upgrade verification script.
Run inside Docker: python /app/app/scripts/verify_upgrade.py

Checks ALL tables, columns, and indexes that the latest code requires.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from database import SessionLocal
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

from sqlalchemy import text

EXPECTED_TABLES = [
    "users", "system_configs", "remote_instances",
    "divisions", "districts", "upazilas",
    "geo_aliases", "header_aliases",
    "summary_stats", "upload_batches", "valid_records", "invalid_records",
    "uploaded_files", "permissions", "role_permissions",
    "audit_logs", "api_usage_logs",
    "trailing_zero_whitelist", "background_tasks",
    "export_history", "export_tracking",
]

EXPECTED_COLUMNS = {
    "summary_stats": [
        "id", "division", "district", "upazila", "total", "valid", "invalid",
        "division_id", "district_id", "upazila_id",
        "last_upload_total", "last_upload_valid", "last_upload_invalid",
        "last_upload_new", "last_upload_updated", "last_upload_duplicate",
        "version", "filename", "pdf_url", "excel_url",
        "excel_valid_url", "excel_invalid_url", "pdf_invalid_url",
        "column_headers", "created_at", "updated_at",
    ],
    "valid_records": [
        "id", "nid", "dob", "name", "division", "district", "upazila",
        "division_id", "district_id", "upazila_id",
        "source_file", "upload_batch", "batch_id",
        "card_no", "mobile", "data",
        "created_at", "updated_at",
    ],
    "invalid_records": [
        "id", "nid", "dob", "name", "division", "district", "upazila",
        "division_id", "district_id", "upazila_id",
        "source_file", "upload_batch", "batch_id",
        "master_serial", "card_no", "mobile", "error_message", "data",
        "created_at", "updated_at",
    ],
    "users": [
        "id", "username", "hashed_password", "role", "is_active", "api_key",
        "api_key_last_used", "api_rate_limit", "api_total_limit",
        "api_usage_count", "api_ip_whitelist",
        "division_access", "district_access", "upazila_access",
        "created_at",
    ],
    "upload_batches": [
        "id", "filename", "original_name", "uploader_id", "username",
        "division", "district", "upazila",
        "total_rows", "valid_count", "invalid_count",
        "division_id", "district_id", "upazila_id",
        "new_records", "updated_records",
        "valid_url", "invalid_url", "pdf_url", "pdf_invalid_url",
        "status", "column_headers", "created_at",
    ],
}

EXPECTED_INDEXES = [
    # valid_records
    "ix_valid_records_nid", "ix_valid_records_dob", "ix_nid_dob",
    "ix_valid_record_name", "ix_valid_record_batch", "ix_valid_record_batch_id",
    "ix_valid_upazila_nid", "ix_valid_records_geo_names", "ix_valid_records_upazila_id",
    "ix_valid_records_district_upazila",
    # invalid_records
    "ix_invalid_records_geo_names", "ix_invalid_records_upazila_id",
    "ix_invalid_records_district_upazila",
    "ix_invalid_record_batch", "ix_invalid_record_batch_id",
    # summary_stats
    "ix_district_upazila", "ix_summary_stats_sorting",
]


def verify():
    db = SessionLocal()
    errors = []
    warnings = []

    print("=" * 60)
    print("FFP UPGRADE VERIFICATION")
    print("=" * 60)

    # 1. Check tables
    print("\n🔍 Checking tables...")
    result = db.execute(text(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    ))
    existing_tables = {row[0] for row in result}

    for table in EXPECTED_TABLES:
        if table in existing_tables:
            print(f"  ✅ {table}")
        else:
            errors.append(f"MISSING TABLE: {table}")
            print(f"  ❌ {table} — MISSING!")

    # 2. Check columns
    print("\n🔍 Checking columns on key tables...")
    for table, expected_cols in EXPECTED_COLUMNS.items():
        if table not in existing_tables:
            errors.append(f"Cannot check columns — table {table} missing")
            continue

        result = db.execute(text(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
        ))
        existing_cols = {row[0] for row in result}

        missing = [c for c in expected_cols if c not in existing_cols]
        if missing:
            errors.append(f"TABLE {table}: missing columns: {', '.join(missing)}")
            print(f"  ❌ {table}: missing {missing}")
        else:
            print(f"  ✅ {table}: all {len(expected_cols)} columns present")

    # 3. Check indexes
    print("\n🔍 Checking indexes...")
    result = db.execute(text(
        "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
    ))
    existing_indexes = {row[0] for row in result}

    for idx in EXPECTED_INDEXES:
        if idx in existing_indexes:
            print(f"  ✅ {idx}")
        else:
            warnings.append(f"MISSING INDEX: {idx}")
            print(f"  ⚠️  {idx} — MISSING (will be created on next restart)")

    # 4. Check pg_trgm extension
    print("\n🔍 Checking extensions...")
    result = db.execute(text("SELECT extname FROM pg_extension WHERE extname = 'pg_trgm'"))
    if result.fetchone():
        print("  ✅ pg_trgm extension")
    else:
        warnings.append("pg_trgm extension not installed")
        print("  ⚠️  pg_trgm extension — MISSING")

    # 5. Check geo data seeded
    print("\n🔍 Checking seeded data...")
    for table, label in [("divisions", "Divisions"), ("districts", "Districts"), ("upazilas", "Upazilas")]:
        if table in existing_tables:
            result = db.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            if count > 0:
                print(f"  ✅ {label}: {count} rows")
            else:
                warnings.append(f"No {label} seeded")
                print(f"  ⚠️  {label}: 0 rows — will be seeded on startup")
        else:
            print(f"  ⏭️  {label}: table not created yet")

    # 6. Check data counts
    print("\n📊 Data counts:")
    for table in ["valid_records", "invalid_records", "summary_stats", "users", "upload_batches"]:
        if table in existing_tables:
            result = db.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            print(f"  {table}: {count:,} rows")

    # Summary
    print("\n" + "=" * 60)
    if errors:
        print(f"❌ VERIFICATION FAILED — {len(errors)} errors, {len(warnings)} warnings")
        for e in errors:
            print(f"   ERROR: {e}")
        for w in warnings:
            print(f"   WARN:  {w}")
        db.close()
        sys.exit(1)
    elif warnings:
        print(f"⚠️  PASSED WITH {len(warnings)} WARNINGS")
        for w in warnings:
            print(f"   WARN:  {w}")
    else:
        print("✅ ALL CHECKS PASSED!")
    print("=" * 60)

    db.close()


if __name__ == "__main__":
    verify()
