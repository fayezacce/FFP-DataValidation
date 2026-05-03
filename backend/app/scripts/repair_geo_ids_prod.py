import os
import sys
from sqlalchemy import text

# Add the current directory to sys.path so we can import .database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from database import SessionLocal
    print("Imports successful.")
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

def repair_geo_ids():
    db = SessionLocal()
    try:
        print("Starting Geo ID repair...")
        tables = ["valid_records", "invalid_records", "summary_stats", "upload_batches"]
        report = {}

        for tbl in tables:
            print(f"Repairing IDs for {tbl}...")
            
            # Re-assign upazila_id, district_id, division_id for every row by name match.
            # Uses a single SET-based UPDATE with a lateral join — no Python loops.
            fix_sql = text(f"""
                UPDATE {tbl} t
                SET
                    upazila_id  = u.id,
                    district_id = d.id,
                    division_id = dv.id
                FROM upazilas u
                JOIN districts d  ON d.name  = u.district_name
                JOIN divisions dv ON dv.name  = d.division_name
                WHERE LOWER(TRIM(u.name))          = LOWER(TRIM(t.upazila))
                  AND LOWER(TRIM(u.district_name)) = LOWER(TRIM(t.district))
                  AND (
                        t.upazila_id  IS DISTINCT FROM u.id
                     OR t.district_id IS DISTINCT FROM d.id
                     OR t.division_id IS DISTINCT FROM dv.id
                  )
            """)
            result = db.execute(fix_sql)
            report[tbl] = result.rowcount
            db.commit()
            print(f"  ✓ {result.rowcount} rows repaired in {tbl}.")

        total = sum(report.values())
        print(f"\nSuccessfully repaired {total} rows with stale or missing geo IDs across {len(tables)} tables.")

        # ── Auto-refresh SummaryStats from truth tables ──────────────────────
        print("\nRefreshing SummaryStats from truth tables...")
        
        recalc_sql = text("""
            WITH counts AS (
                SELECT 
                    norm_district, norm_upazila,
                    SUM(valid_cnt) as live_valid,
                    SUM(invalid_cnt) as live_invalid
                FROM (
                    SELECT LOWER(TRIM(district)) as norm_district, LOWER(TRIM(upazila)) as norm_upazila, COUNT(*) as valid_cnt, 0 as invalid_cnt 
                    FROM valid_records GROUP BY norm_district, norm_upazila
                    UNION ALL
                    SELECT LOWER(TRIM(district)) as norm_district, LOWER(TRIM(upazila)) as norm_upazila, 0 as valid_cnt, COUNT(*) as invalid_cnt 
                    FROM invalid_records GROUP BY norm_district, norm_upazila
                ) combined
                GROUP BY norm_district, norm_upazila
            )
            UPDATE summary_stats s
            SET valid = COALESCE(c.live_valid, 0),
                invalid = COALESCE(c.live_invalid, 0),
                total = COALESCE(c.live_valid, 0) + COALESCE(c.live_invalid, 0),
                updated_at = NOW()
            FROM counts c
            WHERE LOWER(TRIM(s.district)) = c.norm_district
              AND LOWER(TRIM(s.upazila))  = c.norm_upazila
        """)
        stats_result = db.execute(recalc_sql)
        print(f"  ✓ {stats_result.rowcount} SummaryStats entries refreshed.")

        # Zero out ghost entries
        zero_sql = text("""
            UPDATE summary_stats s
            SET valid = 0, invalid = 0, total = 0, updated_at = NOW()
            WHERE NOT EXISTS (
                SELECT 1 FROM valid_records v
                WHERE LOWER(TRIM(v.district)) = LOWER(TRIM(s.district))
                  AND LOWER(TRIM(v.upazila))  = LOWER(TRIM(s.upazila))
            )
            AND NOT EXISTS (
                SELECT 1 FROM invalid_records i
                WHERE LOWER(TRIM(i.district)) = LOWER(TRIM(s.district))
                  AND LOWER(TRIM(i.upazila))  = LOWER(TRIM(s.upazila))
            )
            AND (s.valid > 0 OR s.invalid > 0)
        """)
        ghost_result = db.execute(zero_sql)
        if ghost_result.rowcount > 0:
            print(f"  ✓ {ghost_result.rowcount} ghost SummaryStats entries zeroed out.")
        else:
            print("  ✓ No ghost entries found.")
        
        db.commit()
        print("\nAll done — data is consistent.")

    except Exception as e:
        print(f"\nError during geo id repair: {e}")
        db.rollback()
        sys.exit(1)
    finally:
        db.close()

if __name__ == "__main__":
    repair_geo_ids()
