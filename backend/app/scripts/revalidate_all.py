"""
Re-validate all invalid records against current system configuration.
If a record is now valid (e.g. after increasing trailing_zero_limit), 
it is moved back to the valid_records table.
"""
import sys
import os
from datetime import datetime, timezone

# Add app directory to path
_script_dir = os.path.dirname(os.path.abspath(__file__))          # .../scripts
_app_pkg    = os.path.dirname(_script_dir)                         # .../app
_container_root = os.path.dirname(_app_pkg)                        # /app
for _p in [_container_root, _app_pkg]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from app.database import SessionLocal
from app.models import SystemConfig, InvalidRecord, ValidRecord, TrailingZeroWhitelist
from app.validator import validate_nid
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ffp.revalidate")

def revalidate_all():
    db = SessionLocal()
    try:
        # 1. Fetch current config
        tz_limit_conf = db.query(SystemConfig).filter(SystemConfig.key == "trailing_zero_limit").first()
        if tz_limit_conf and tz_limit_conf.value and tz_limit_conf.value.strip().isdigit():
            tz_limit = int(tz_limit_conf.value.strip())
        else:
            tz_limit = 2
            
        tz_whitelist_records = db.query(TrailingZeroWhitelist.nid).all()
        tz_whitelist = {r[0] for r in tz_whitelist_records}
        
        logger.info(f"Starting re-validation with tz_limit={tz_limit} and {len(tz_whitelist)} whitelisted NIDs")
        
        # 2. Fetch all invalid records
        invalid_recs = db.query(InvalidRecord).all()
        logger.info(f"Found {len(invalid_recs)} invalid records to check")
        
        moved_count = 0
        seen_nids = set()
        
        for inv in invalid_recs:
            nid = str(inv.nid or "").strip()
            if not nid:
                continue
                
            dob = str(inv.dob or "").strip()
            dob_year = dob[:4] if len(dob) >= 4 and dob[:4].isdigit() else None
            
            # Re-run full validation
            _, status, message = validate_nid(nid, dob_year, tz_limit, tz_whitelist)
            
            if status == "success":
                # Check for duplicates before moving
                existing_in_db = db.query(ValidRecord).filter(ValidRecord.nid == nid).first()
                if existing_in_db or nid in seen_nids:
                    logger.warning(f"Record {inv.id} (NID: {nid}) is now valid but already exists in valid_records or current batch. Deleting duplicate from invalid_records.")
                    db.delete(inv)
                    moved_count += 1
                else:
                    logger.info(f"Record {inv.id} (NID: {nid}) is now VALID. Moving...")
                    # Create valid record
                    valid = ValidRecord(
                        nid=inv.nid,
                        dob=inv.dob,
                        name=inv.name,
                        division=inv.division,
                        district=inv.district,
                        upazila=inv.upazila,
                        division_id=inv.division_id,
                        district_id=inv.district_id,
                        upazila_id=inv.upazila_id,
                        source_file=inv.source_file,
                        upload_batch=inv.upload_batch,
                        batch_id=inv.batch_id,
                        card_no=inv.card_no,
                        mobile=inv.mobile,
                        data=inv.data,
                        father_husband_name=inv.father_husband_name,
                        name_bn=inv.name_bn,
                        name_en=inv.name_en,
                        ward=inv.ward,
                        union_name=inv.union_name,
                        occupation=inv.occupation,
                        gender=inv.gender,
                        religion=inv.religion,
                        address=inv.address,
                        spouse_name=inv.spouse_name,
                        spouse_nid=inv.spouse_nid,
                        spouse_dob=inv.spouse_dob,
                        verification_status="unverified",
                        created_at=inv.created_at,
                        updated_at=datetime.now(timezone.utc)
                    )
                    db.add(valid)
                    db.delete(inv)
                    seen_nids.add(nid)
                    moved_count += 1
                
                if moved_count % 100 == 0:
                    db.commit()
                    logger.info(f"Committed {moved_count} records so far...")
        
        db.commit()
        logger.info(f"Re-validation complete. Processed {moved_count} records.")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Re-validation failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    revalidate_all()
