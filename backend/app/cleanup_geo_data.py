import sys
import os
from sqlalchemy import func
from app.database import SessionLocal
from app.models import ValidRecord, InvalidRecord, Upazila, SummaryStats, Division, District

def cleanup():
    db = SessionLocal()
    try:
        print("Starting Robust Geo Data Cleanup...")

        # 1. Map all official upazilas
        official_upz = db.query(Upazila).all()
        # Map: (dist_lower, upz_lower) -> (division, district, upazila)
        geo_map = { (u.district_name.lower().strip(), u.name.lower().strip()): (u.division_name, u.district_name, u.name) for u in official_upz }
        
        # Also create a reverse map for fuzzy matching (just by upazila name if unique)
        upz_only_map = {}
        for u in official_upz:
            u_name = u.name.lower().strip()
            if u_name not in upz_only_map:
                upz_only_map[u_name] = []
            upz_only_map[u_name].append((u.division_name, u.district_name, u.name))

        # 2. Cleanup ValidRecord and InvalidRecord
        for model in [ValidRecord, InvalidRecord]:
            print(f"Processing {model.__tablename__}...")
            # Fetch all unique (division, district, upazila) groups
            groups = db.query(model.division, model.district, model.upazila).group_by(model.division, model.district, model.upazila).all()
            
            for div, dist, upz in groups:
                if not upz: continue
                d_key = dist.lower().strip() if dist else ""
                u_key = upz.lower().strip()
                
                # Direct match check
                match = geo_map.get((d_key, u_key))
                
                # Spelling correction fallback
                if not match:
                    if u_key == "phulchhari": u_key = "phulchari"
                    if u_key == "chaugachha": u_key = "chaugacha"
                    match = geo_map.get((d_key, u_key))
                
                # Upazila-only fallback (if unique across Bangladesh)
                if not match:
                    possible = upz_only_map.get(u_key, [])
                    if len(possible) == 1:
                        match = possible[0]

                if match:
                    new_div, new_dist, new_upz = match
                    if (div != new_div or dist != new_dist or upz != new_upz):
                        print(f"Correcting: ({div}, {dist}, {upz}) -> ({new_div}, {new_dist}, {new_upz})")
                        db.query(model).filter(model.division == div, model.district == dist, model.upazila == upz).update({
                            model.division: new_div,
                            model.district: new_dist,
                            model.upazila: new_upz
                        }, synchronize_session=False)
                else:
                    # Final attempt: strip only
                    db.query(model).filter(model.division == div, model.district == dist, model.upazila == upz).update({
                        model.division: div.strip() if div else div,
                        model.district: dist.strip() if dist else dist,
                        model.upazila: upz.strip()
                    }, synchronize_session=False)

        db.commit()

        # 3. Recalculate SummaryStats
        print("Recalculating SummaryStats...")
        db.query(SummaryStats).delete()
        db.commit()

        # Aggregate counts - grouping by (district, upazila) to ensure unique stats entries
        # We'll take the division from the first group encountered
        valid_counts = db.query(
            ValidRecord.division, ValidRecord.district, ValidRecord.upazila, func.count(ValidRecord.id)
        ).group_by(ValidRecord.division, ValidRecord.district, ValidRecord.upazila).all()

        invalid_counts = db.query(
            InvalidRecord.division, InvalidRecord.district, InvalidRecord.upazila, func.count(InvalidRecord.id)
        ).group_by(InvalidRecord.division, InvalidRecord.district, InvalidRecord.upazila).all()

        stats_map = {} # (district, upazila) -> {division, valid, invalid}
        for div, dist, upz, count in valid_counts:
            key = (dist, upz)
            if key not in stats_map: stats_map[key] = {"division": div, "valid": 0, "invalid": 0}
            stats_map[key]["valid"] += count

        for div, dist, upz, count in invalid_counts:
            key = (dist, upz)
            if key not in stats_map: stats_map[key] = {"division": div, "valid": 0, "invalid": 0}
            stats_map[key]["invalid"] += count

        print(f"Inserting {len(stats_map)} unique upazila stats...")
        for (dist, upz), counts in stats_map.items():
            db.add(SummaryStats(
                division=counts["division"],
                district=dist,
                upazila=upz,
                valid=counts["valid"],
                invalid=counts["invalid"],
                total=counts["valid"] + counts["invalid"]
            ))

        db.commit()
        print("Cleanup and Recalculation complete!")

    except Exception as e:
        db.rollback()
        print(f"Error during cleanup: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    cleanup()
