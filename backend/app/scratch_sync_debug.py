import sys
import os
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import District, Division, GeoAlias
from app.main import _sync_geo_aliases

def test_sync():
    db = SessionLocal()
    try:
        print("Starting manual sync...")
        _sync_geo_aliases(db)
        print("Manual sync finished.")
        
        # Check results
        aliases = db.query(GeoAlias).all()
        print(f"Total aliases in DB: {len(aliases)}")
        for a in aliases:
            print(f"- {a.target_type}: {a.alias_name} mapping to ID {a.target_id}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    test_sync()
