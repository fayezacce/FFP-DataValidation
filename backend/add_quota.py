import os
from sqlalchemy import text
from app.database import SessionLocal

def add_quota_column():
    db = SessionLocal()
    try:
        # Check if column exists
        result = db.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='upazilas' AND column_name='quota';
        """)).fetchone()
        
        if not result:
            print("Adding quota column to upazilas table...")
            db.execute(text("ALTER TABLE upazilas ADD COLUMN quota INTEGER DEFAULT 0;"))
            db.commit()
            print("Column added successfully.")
        else:
            print("Column 'quota' already exists.")
            
    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    add_quota_column()
