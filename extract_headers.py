import sys
import os

print("Starting extraction...")
sys.path.append(os.path.join(os.path.dirname(__file__), "backend"))

try:
    from app.database import SessionLocal
    from app.models import ValidRecord
    from sqlalchemy import text
except Exception as e:
    print(f"Failed to import: {e}")
    sys.exit(1)

try:
    db = SessionLocal()
    print("DB connection created")
    
    # Check JSON/JSONB using Postgres jsonb_object_keys or json_object_keys
    print("Executing query...")
    try:
        # Assuming JSON column
        result = db.execute(text('''
            SELECT key, count(*) 
            FROM valid_records, json_object_keys(data) AS key 
            GROUP BY key 
            ORDER BY count DESC
        ''')).fetchall()
        print("Used json_object_keys")
    except Exception as e1:
        db.rollback()
        print(f"json_object_keys failed: {e1}, trying jsonb_object_keys")
        result = db.execute(text('''
            SELECT key, count(*) 
            FROM valid_records, jsonb_object_keys(data) AS key 
            GROUP BY key 
            ORDER BY count DESC
        ''')).fetchall()
    
    for r in result:
        print(f"{r.key}: {r.count}")
        
except Exception as e:
    print(f"Database error: {e}")
