#!/usr/bin/env python3
"""
Test migrate_schema against the prod backup (ffp_prod_test DB).
Run inside the backend container.
"""
import os
import sys
sys.path.insert(0, '/app')

os.environ['DATABASE_URL'] = 'postgresql://fayez:fayez_secret@db:5432/ffp_prod_test'

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from app.models import Base

eng = create_engine(os.environ['DATABASE_URL'])

# 1. Create any new tables that don't exist yet (background_tasks, export_history, export_tracking)
print("Step 1: Creating missing tables via Base.metadata.create_all...")
Base.metadata.create_all(bind=eng)
print("  Done.")

# 2. Run migrate_schema (all ALTER TABLE IF NOT EXISTS operations)
print("Step 2: Running migrate_schema()...")
Session = sessionmaker(bind=eng)
db = Session()
try:
    from app.main import migrate_schema
    migrate_schema(db)
    print("  Done — migrate_schema() completed successfully.")
finally:
    db.close()

# 3. Quick verification: check the critical new columns exist
print("Step 3: Verifying critical columns were added...")
with eng.connect() as conn:
    checks = [
        ("valid_records", "upazila_id"),
        ("valid_records", "mobile"),
        ("invalid_records", "upazila_id"),
        ("summary_stats", "column_headers"),
        ("upload_batches", "column_headers"),
    ]
    all_ok = True
    for tbl, col in checks:
        res = conn.execute(text("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name = :t AND column_name = :c
        """), {"t": tbl, "c": col})
        found = res.fetchone() is not None
        status = "✓" if found else "✗"
        print(f"  {status}  {tbl}.{col}")
        if not found:
            all_ok = False

if all_ok:
    print("\n✓ Migration simulation PASSED — all critical columns are present.")
else:
    print("\n✗ Migration simulation FAILED — some columns are still missing.")
    sys.exit(1)
