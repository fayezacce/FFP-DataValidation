import os
import sys
from sqlalchemy.orm import Session
from datetime import datetime
from difflib import get_close_matches

# Add parent directory to sys.path to import app modules
sys.path.append(os.path.join(os.getcwd(), ".."))

from app.database import SessionLocal, engine
from app.models import SummaryStats, Upazila, ValidRecord, InvalidRecord, UploadBatch

def _normalize(s: str) -> str:
    import re
    if not s: return ""
    return re.sub(r'[\s\-]+', ' ', s.lower().strip())

def _super_normalize(s: str) -> str:
    """Removes all non-alphanumeric for a deeper match."""
    import re
    if not s: return ""
    return re.sub(r'[^a-zA-Z0-9]', '', s.lower().strip())

def migrate_mismatched_stats(db: Session, dry_run=True):
    print(f"{'DRY RUN' if dry_run else 'EXECUTING'} - Merging mismatched Upazila stats into official records...")
    
    # 1. Load official Upazilas
    upazilas = db.query(Upazila).all()
    official_map = {} # (district_norm, upazila_norm) -> official_upazila_object
    district_upazilas = {} # district_id_norm -> list of upazila_norms
    official_districts = {} # normalized_district_name (with aliases) -> official_district_name
    
    # Official district names
    dists = sorted(list({u.district_name for u in upazilas}))
    for d in dists:
        norm = _normalize(d)
        official_districts[norm] = d
        # Hardcoded aliases from bd_geo.py
        aliases = []
        if norm == "comilla": aliases = ["cumilla", "kumilla"]
        elif norm == "cumilla": aliases = ["comilla", "kumilla"] # Handle if DB has Cumilla
        elif "cox" in norm: aliases = ["cox's bazar", "coxs bazar", "coxsbazar"]
        elif "khagrach" in norm: aliases = ["khagrachhari", "khagrachari"]
        elif "chattogram" in norm: aliases = ["chittagong", "ctg"]
        elif "bogura" in norm: aliases = ["bogra"]
        elif "jashore" in norm: aliases = ["jessore"]
        elif "barishal" in norm: aliases = ["barisal"]
        
        for alias in aliases:
            official_districts[_normalize(alias)] = d
            official_districts[_super_normalize(alias)] = d

        # Also add super-normalized version
        official_districts[_super_normalize(d)] = d

    for u in upazilas:
        dist_official = u.district_name
        upz_norm = _normalize(u.name)
        official_map[(dist_official, upz_norm)] = u
        if dist_official not in district_upazilas:
            district_upazilas[dist_official] = []
        district_upazilas[dist_official].append(upz_norm)

    # 2. Get all SummaryStats
    all_stats = db.query(SummaryStats).all()
    
    for s in all_stats:
        # Try to resolve the district first
        d_norm = _normalize(s.district)
        d_super = _super_normalize(s.district)
        
        target_dist = official_districts.get(d_norm) or official_districts.get(d_super)
        
        if not target_dist:
            print(f"  [SKIPPING] District '{s.district}' could not be resolved.")
            continue
            
        upz_norm = _normalize(s.upazila)
        
        # Is this an official record?
        if (s.district == target_dist) and ((target_dist, upz_norm) in official_map):
            continue
            
        # It's a mismatch! Find the closest official record in the SAME district
        matches = get_close_matches(upz_norm, district_upazilas[target_dist], n=1, cutoff=0.7)
        if not matches:
            print(f"  [SKIPPING] No close match for '{s.upazila}' in district '{target_dist}'.")
            continue
            
        official_upz = official_map[(target_dist, matches[0])]
        
        print(f"  [MATCH] '{s.upazila}' -> '{official_upz.name}' (in {target_dist})")
        
        # 3. Find if the official upazila already has a SummaryStats record
        official_stats = db.query(SummaryStats).filter(
            SummaryStats.district == official_upz.district_name,
            SummaryStats.upazila == official_upz.name
        ).first()
        
        if not official_stats:
            if not dry_run:
                print(f"    -> Updating this record set it to official spelling.")
                s.upazila = official_upz.name
                s.district = official_upz.district_name
                s.division = official_upz.division_name
            else:
                print(f"    -> Would update this record to official spelling.")
        else:
            # MERGE!
            if not dry_run:
                print(f"    -> Merging counts (T:{s.total}, V:{s.valid}, I:{s.invalid}) into official stats (ID:{official_stats.id}).")
                official_stats.total += s.total
                official_stats.valid += s.valid
                official_stats.invalid += s.invalid
                # Update records and batches to point to the official name
                db.query(ValidRecord).filter(ValidRecord.district == s.district, ValidRecord.upazila == s.upazila).update({
                    "district": official_upz.district_name,
                    "upazila": official_upz.name,
                    "division": official_upz.division_name
                }, synchronize_session=False)
                
                db.query(InvalidRecord).filter(InvalidRecord.district == s.district, InvalidRecord.upazila == s.upazila).update({
                    "district": official_upz.district_name,
                    "upazila": official_upz.name,
                    "division": official_upz.division_name
                }, synchronize_session=False)
                
                db.query(UploadBatch).filter(UploadBatch.district == s.district, UploadBatch.upazila == s.upazila).update({
                    "district": official_upz.district_name,
                    "upazila": official_upz.name,
                    "division": official_upz.division_name
                }, synchronize_session=False)
                
                # Delete the mismatched record
                db.delete(s)
            else:
                print(f"    -> Would merge counts and update all related records to '{official_upz.name}'.")

    if not dry_run:
        db.commit()
        print("MIGRATION COMPLETED SUCCESSFULLY.")
    else:
        print("DRY RUN COMPLETED. No changes were made.")

if __name__ == "__main__":
    db = SessionLocal()
    try:
        # Run dry run first if any arg is present
        is_dry = len(sys.argv) < 2 or sys.argv[1] != "--execute"
        migrate_mismatched_stats(db, dry_run=is_dry)
    finally:
        db.close()
