from app.database import SessionLocal
from app.models import User
from app.auth import hash_password

db = SessionLocal()
try:
    existing = db.query(User).filter(User.username == "testadmin").first()
    hashed = hash_password("admin123")
    if existing:
        existing.hashed_password = hashed
    else:
        new_admin = User(username="testadmin", hashed_password=hashed, role="admin", is_active=True)
        db.add(new_admin)
    db.commit()
    print("Admin seeded successfully.")
except Exception as e:
    print(f"Error: {e}")
finally:
    db.close()
