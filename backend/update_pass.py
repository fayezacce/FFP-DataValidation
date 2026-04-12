
from app.database import SessionLocal
from app.models import User
from app.auth import hash_password

db = SessionLocal()
try:
    user = db.query(User).filter(User.username == 'fayez').first()
    if user:
        user.hashed_password = hash_password('qucwow-zijsyk-Xuhsy5')
        db.commit()
        print("SUCCESS: Password updated for user fayez")
    else:
        print("ERROR: User fayez not found")
except Exception as e:
    db.rollback()
    print(f"ERROR: {e}")
finally:
    db.close()
