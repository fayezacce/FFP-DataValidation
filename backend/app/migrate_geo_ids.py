import sys
import os
from sqlalchemy import func
from app.database import SessionLocal
from app.models import ValidRecord, InvalidRecord, Upazila, SummaryStats, Division, District, UploadBatch

def migrate():
    db = SessionLocal()
    try:
        print("Starting Geo ID Migration...")

        # 1. Load Geo Maps (using cleaned names)
        divisions = { d.name.lower().strip(): d.id for d in db.query(Division).all() }
        districts = { d.name.lower().strip(): d.id for d in db.query(District).all() }
        # (dist_name.lower(), upz_name.lower()) -> upazila_id
        upazilas = { (u.district_name.lower().strip(), u.name.lower().strip()): u.id for u in db.query(Upazila).all() }

        # 2. Process Tables
        # We iterate over unique (division, district, upazila) combinations found in each table
        # Since we cleaned the data in Phase 1, there are only ~450 distinct groups.
        # This makes the update very fast even for millions of records.
        
        for model in [ValidRecord, InvalidRecord, SummaryStats, UploadBatch]:
            print(f"--- Migrating {model.__tablename__} ---")
            
            geo_groups = db.query(model.division, model.district, model.upazila).group_by(model.division, model.district, model.upazila).all()
            print(f"Found {len(geo_groups)} unique geo combinations.")
            
            for div_n, dist_n, upz_n in geo_groups:
                d_key = dist_n.lower().strip() if dist_n else ""
                u_key = upz_n.lower().strip() if upz_n else ""
                div_key = div_n.lower().strip() if div_n else ""
                
                div_id = divisions.get(div_key)
                dist_id = districts.get(d_key)
                upz_id = upazilas.get((d_key, u_key))
                
                if div_id or dist_id or upz_id:
                    # Update all rows matching this name combination
                    count = db.query(model).filter(
                        model.division == div_n,
                        model.district == dist_n,
                        model.upazila == upz_n
                    ).update({
                        model.division_id: div_id,
                        model.district_id: dist_id,
                        model.upazila_id: upz_id
                    }, synchronize_session=False)
                    print(f"  Matched ({div_n}, {dist_n}, {upz_n}) -> IDs ({div_id}, {dist_id}, {upz_id}). Updated {count} rows.")
                    db.commit()
                else:
                    print(f"  WARNING: Could not resolve IDs for ({div_n}, {dist_n}, {upz_n})")
            
            print(f"Migration for {model.__tablename__} complete.\n")

    except Exception as e:
        db.rollback()
        print(f"Error during migration: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    migrate()
