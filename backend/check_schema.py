from sqlalchemy import text
from app.database import SessionLocal

db = SessionLocal()

# Check upazilas columns
r1 = db.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'upazilas' ORDER BY ordinal_position"))
print("=== upazilas columns ===")
for r in r1:
    print(f"  {r[0]}")

# Check districts columns
r2 = db.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'districts' ORDER BY ordinal_position"))
print("=== districts columns ===")
for r in r2:
    print(f"  {r[0]}")

# Check valid_records id columns
r3 = db.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'valid_records' AND column_name LIKE '%_id' ORDER BY ordinal_position"))
print("=== valid_records _id columns ===")
for r in r3:
    print(f"  {r[0]}")

# Check if run_cleanup ever completed successfully
from app.models import BackgroundTask
tasks = db.query(BackgroundTask).filter(BackgroundTask.task_name == "geo_cleanup").order_by(BackgroundTask.created_at.desc()).limit(5).all()
print("\n=== Recent geo_cleanup tasks ===")
for t in tasks:
    print(f"  {t.created_at} | status={t.status} | msg={t.message} | err={t.error_details}")

if not tasks:
    print("  No geo_cleanup tasks found - cleanup was NEVER run!")

db.close()
