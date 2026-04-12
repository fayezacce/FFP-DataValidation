
import os
import sys
import logging
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger("backfill_geo")

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    DB_USER = os.environ.get("POSTGRES_USER", "ffp_admin")
    DB_PASS = os.environ.get("POSTGRES_PASSWORD", "")
    DB_HOST = os.environ.get("POSTGRES_HOST", "db")
    DB_NAME = os.environ.get("POSTGRES_DB", "ffp_validator")
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


def backfill_geo_ids():
    db = Session()
    try:
        logger.info("Loading geographical maps from database...")

        # Load ID maps
        divs  = {r[0].lower().strip(): r[1] for r in db.execute(text("SELECT name, id FROM divisions")).fetchall()}
        dists = {r[0].lower().strip(): r[1] for r in db.execute(text("SELECT name, id FROM districts")).fetchall()}
        upzs  = {}
        for r in db.execute(text("SELECT district_name, name, id FROM upazilas")).fetchall():
            upzs[(r[0].lower().strip(), r[1].lower().strip())] = r[2]

        tables = ["summary_stats", "valid_records", "invalid_records", "upload_batches"]

        for table in tables:
            logger.info(f"Processing table: {table}")

            count = db.execute(text(
                f"SELECT COUNT(*) FROM {table} WHERE division_id IS NULL OR district_id IS NULL OR upazila_id IS NULL"
            )).scalar()

            if count == 0:
                logger.info(f"  No rows to fix in {table}")
                continue

            logger.info(f"  Found {count:,} rows to fix — using bulk SET-based updates...")

            # Get distinct geo combos that need updating
            rows = db.execute(text(
                f"SELECT DISTINCT division, district, upazila FROM {table} "
                f"WHERE division_id IS NULL OR district_id IS NULL OR upazila_id IS NULL"
            )).fetchall()

            total_updated = 0
            for div_name, dist_name, upz_name in rows:
                if not dist_name or not upz_name:
                    continue

                div_id  = divs.get(div_name.lower().strip()) if div_name else None
                dist_id = dists.get(dist_name.lower().strip()) if dist_name else None
                upz_id  = upzs.get((dist_name.lower().strip(), upz_name.lower().strip()))

                if not upz_id:
                    logger.warning(f"  No upazila ID for '{dist_name}' / '{upz_name}'")
                    continue

                # Retry loop for deadlock resilience
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        result = db.execute(text(
                            f"UPDATE {table} "
                            f"SET division_id  = COALESCE(division_id,  :div_id), "
                            f"    district_id  = COALESCE(district_id,  :dist_id), "
                            f"    upazila_id   = COALESCE(upazila_id,   :upz_id) "
                            f"WHERE division = :div AND district = :dist AND upazila = :upz "
                            f"AND (division_id IS NULL OR district_id IS NULL OR upazila_id IS NULL)"
                        ), {"div_id": div_id, "dist_id": dist_id, "upz_id": upz_id, "div": div_name, "dist": dist_name, "upz": upz_name})
                        total_updated += result.rowcount
                        break # Success
                    except Exception as e:
                        if "deadlock" in str(e).lower() and attempt < max_retries - 1:
                            import time
                            wait_time = (attempt + 1) * 2
                            logger.warning(f"  ⚠ Deadlock detected for '{upz_name}'. Retrying in {wait_time}s... (Attempt {attempt+1}/{max_retries})")
                            db.rollback() # Rollback to clear the deadlock
                            time.sleep(wait_time)
                        else:
                            raise e

            db.commit()
            logger.info(f"  Done: {table} — Updated {total_updated:,} rows")

        logger.info("All geo ID backfills complete.")

    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    backfill_geo_ids()
