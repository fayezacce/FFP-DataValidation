"""
Synchronizes all stats across FFP platform
Author: Fayez Ahmed, Assistant Programmer, DG Food
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import os
import sys

# Add /app (the container root — parent of the 'app' package) to sys.path
# so that 'from app.database import ...' resolves whether this script is run
# as 'python /app/app/scripts/sync_all_stats.py' or as a module.
_script_dir = os.path.dirname(os.path.abspath(__file__))          # .../scripts
_app_pkg    = os.path.dirname(_script_dir)                         # .../app
_container_root = os.path.dirname(_app_pkg)                        # /app
for _p in [_container_root, _app_pkg]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from app.database import SessionLocal
    from app.models import SummaryStats
    from app.stats_utils import refresh_summary_stats
    print("Imports successful.")
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

def sync_all():
    db = SessionLocal()
    try:
        print("Starting full statistics synchronization...")
        all_stats = db.query(SummaryStats).all()
        total = len(all_stats)
        print(f"Found {total} upazilas in SummaryStats.")
        
        for idx, stat in enumerate(all_stats):
            print(f"[{idx+1}/{total}] Syncing {stat.district} -> {stat.upazila}...", end="\r")
            refresh_summary_stats(db, stat.division, stat.district, stat.upazila)
            
        print(f"\nSuccessfully synchronized {total} upazilas.")
        
        # Check for "Orphaned" records that are in Valid/Invalid but NOT in SummaryStats
        print("Checking for orphaned records missing from SummaryStats...")
        from app.models import ValidRecord, InvalidRecord
        from sqlalchemy import func
        
        valid_groups = db.query(ValidRecord.division, ValidRecord.district, ValidRecord.upazila).group_by(ValidRecord.division, ValidRecord.district, ValidRecord.upazila).all()
        invalid_groups = db.query(InvalidRecord.division, InvalidRecord.district, InvalidRecord.upazila).group_by(InvalidRecord.division, InvalidRecord.district, InvalidRecord.upazila).all()
        
        all_groups = set(valid_groups) | set(invalid_groups)
        
        new_summaries = 0
        for div, dist, upz in all_groups:
            # Check if exists in SummaryStats
            exists = db.query(SummaryStats).filter(SummaryStats.district == dist, SummaryStats.upazila == upz).first()
            if not exists:
                print(f"Found missing SummaryStats entry for {dist} -> {upz}. Creating...")
                refresh_summary_stats(db, div, dist, upz)
                new_summaries += 1
        
        if new_summaries > 0:
            print(f"Created {new_summaries} missing SummaryStats entries.")
        else:
            print("No missing SummaryStats entries found.")

    except Exception as e:
        print(f"\nError during sync: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    sync_all()
