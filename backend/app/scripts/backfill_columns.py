import os
import sys
import time
import logging
import multiprocessing
from datetime import datetime, timezone
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker

# Add app directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Database configuration from environment
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    DB_USER = os.environ.get("POSTGRES_USER", "fayez")
    DB_PASS = os.environ.get("POSTGRES_PASSWORD", "fayez_secret")
    DB_HOST = os.environ.get("POSTGRES_HOST", "db")
    DB_NAME = os.environ.get("POSTGRES_DB", "ffp_validator")
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(processName)s] %(message)s'
)
logger = logging.getLogger("backfill")

# Optimization Parameters
CHUNK_SIZE = 50000
WORKER_COUNT = min(multiprocessing.cpu_count(), 4)

def normalize_mobile(mobile):
    """Clean mobile numbers: standardize digits and trim."""
    if not mobile:
        return ""
    mobile = str(mobile).strip()
    # Replace Bengali digits if any
    bengali_to_english = {
        '০': '0', '১': '1', '২': '2', '৩': '3', '৪': '4',
        '৫': '5', '৬': '6', '৭': '7', '৮': '8', '৯': '9'
    }
    for bn, en in bengali_to_english.items():
        mobile = mobile.replace(bn, en)
    return mobile

def process_chunk(start_id, end_id, table_name):
    """Worker function to process a range of IDs."""
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    
    try:
        # Fetch rows that need backfilling
        # We only backfill if 'mobile' is currently null or empty
        select_sql = text(f"""
            SELECT id, data FROM {table_name}
            WHERE id >= :start AND id < :end
              AND (mobile IS NULL OR mobile = '')
        """)
        
        rows = db.execute(select_sql, {"start": start_id, "end": end_id}).fetchall()
        
        if not rows:
            return 0

        updates = []
        for row in rows:
            data = row.data if isinstance(row.data, dict) else {}
            # Standardizing mobile from JSON blob
            raw_mobile = data.get("mobile") or data.get("Mobile") or ""
            clean_mobile = normalize_mobile(raw_mobile)
            
            updates.append({
                "id": row.id,
                "mobile": clean_mobile
            })

        if updates:
            # Batch update
            update_sql = text(f"UPDATE {table_name} SET mobile = :mobile WHERE id = :id")
            db.execute(update_sql, updates)
            db.commit()
            
        return len(updates)
    except Exception as e:
        logger.error(f"Error processing chunk {start_id}-{end_id}: {str(e)}")
        db.rollback()
        return -1
    finally:
        db.close()

def run_backfill(table_name="valid_records"):
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT MIN(id), MAX(id) FROM {table_name}"))
        min_id, max_id = res.fetchone()
    
    if min_id is None:
        logger.info(f"Table {table_name} is empty. Skipping.")
        return

    logger.info(f"Starting backfill for {table_name}. ID range: {min_id} - {max_id}")
    
    ranges = []
    current = min_id
    while current <= max_id:
        ranges.append((current, current + CHUNK_SIZE))
        current += CHUNK_SIZE

    total_updated = 0
    with multiprocessing.Pool(processes=WORKER_COUNT) as pool:
        results = [pool.apply_async(process_chunk, (r[0], r[1], table_name)) for r in ranges]
        
        for i, res in enumerate(results):
            val = res.get()
            if val > 0:
                total_updated += val
            
            if (i + 1) % 10 == 0 or (i + 1) == len(results):
                progress = (i + 1) / len(results) * 100
                logger.info(f"Progress: {progress:.1f}% | Processed {i+1}/{len(ranges)} chunks | Total updated: {total_updated}")

    logger.info(f"Completed backfill for {table_name}. Total records updated: {total_updated}")

if __name__ == "__main__":
    start_time = time.time()
    
    # Backfill Valid Records
    run_backfill("valid_records")
    
    # Backfill Invalid Records
    run_backfill("invalid_records")
    
    duration = time.time() - start_time
    logger.info(f"All backfill tasks completed in {duration:.2f} seconds.")
