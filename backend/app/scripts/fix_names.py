import sys
import os

# Add the parent directory to sys.path to allow importing from 'app'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import ValidRecord, InvalidRecord
import pandas as pd
import json

def fix_names():
    db: Session = SessionLocal()
    try:
        # 1. Fix ValidRecords in chunks
        print("Counting ValidRecords with name='Unknown'...")
        total_valid = db.query(ValidRecord).filter(ValidRecord.name == 'Unknown').count()
        print(f"Found {total_valid} ValidRecords to fix.")
        
        chunk_size = 2000
        for offset in range(0, total_valid, chunk_size):
            records = db.query(ValidRecord).filter(ValidRecord.name == 'Unknown').limit(chunk_size).all()
            if not records:
                break
                
            count = 0
            for record in records:
                if not record.data:
                    continue
                
                data = record.data
                name = None
                for key in data.keys():
                    k_norm = str(key).strip()
                    if "নাম" in k_norm or "Name" in k_norm:
                        if k_norm in ["উপকারভোগীর নাম(বাংলা)", "উপকারভোগীর নাম", "উপকার ভোগীর নাম", "নাম", "Name", "Beneficiary Name"]:
                            name = data[key]
                            break
                
                if name and str(name).strip() != "Unknown":
                    record.name = str(name).strip()
                    count += 1
            
            db.commit()
            print(f"Processed batch {offset//chunk_size + 1}, fixed {count} names in this batch.")

        # 2. Fix InvalidRecords in chunks
        print("\nCounting InvalidRecords with name='Unknown'...")
        total_invalid = db.query(InvalidRecord).filter(InvalidRecord.name == 'Unknown').count()
        print(f"Found {total_invalid} InvalidRecords to fix.")
        
        for offset in range(0, total_invalid, chunk_size):
            records = db.query(InvalidRecord).filter(InvalidRecord.name == 'Unknown').limit(chunk_size).all()
            if not records:
                break
                
            count = 0
            for record in records:
                if not record.data:
                    continue
                
                data = record.data
                name = None
                for key in data.keys():
                    k_norm = str(key).strip()
                    if "নাম" in k_norm or "Name" in k_norm:
                        if k_norm in ["উপকারভোগীর নাম(বাংলা)", "উপকারভোগীর নাম", "উপকার ভোগীর নাম", "নাম", "Name", "Beneficiary Name"]:
                            name = data[key]
                            break
                
                if name and str(name).strip() != "Unknown":
                    record.name = str(name).strip()
                    count += 1
            
            db.commit()
            print(f"Processed batch {offset//chunk_size + 1}, fixed {count} names in this batch.")

    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    fix_names()
