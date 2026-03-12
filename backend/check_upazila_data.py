from app.database import SessionLocal
from app.models import Upazila, ValidRecord, InvalidRecord, SummaryStats, UploadBatch

def check():
    db = SessionLocal()
    try:
        # Check Upazila table
        u = db.query(Upazila).filter(Upazila.name.ilike('%Jagannathpur%')).first()
        print(f"--- Upazila Table ---")
        if u:
            print(f"Found: {u.name} (District: {u.district_name}, Division: {u.division_name}, ID: {u.id})")
        else:
            print("Not found in Upazila table (checked for '%Jagannathpur%')")
            sunamganj_ups = db.query(Upazila).filter(Upazila.district_name.ilike('%Sunamganj%')).all()
            print(f"Upazilas in Sunamganj: {[up.name for up in sunamganj_ups]}")

        # Check SummaryStats table
        s = db.query(SummaryStats).filter(SummaryStats.upazila.ilike('%Jagannathpur%')).first()
        s_alt = db.query(SummaryStats).filter(SummaryStats.upazila.ilike('%Jogonnathpur%')).first()
        print(f"\n--- SummaryStats Table ---")
        if s:
            print(f"Found (Jagannathpur): Valid={s.valid}, Invalid={s.invalid}, Total={s.total}")
        if s_alt:
            print(f"Found (Jogonnathpur): Valid={s_alt.valid}, Invalid={s_alt.invalid}, Total={s_alt.total}")
        if not s and not s_alt:
            print("No SummaryStats found for 'Jagannathpur' or 'Jogonnathpur'")

        # Check UploadBatch table
        batches = db.query(UploadBatch).filter(UploadBatch.upazila.ilike('%Jagannathpur%')).all()
        batches_alt = db.query(UploadBatch).filter(UploadBatch.upazila.ilike('%Jogonnathpur%')).all()
        print(f"\n--- UploadBatch Table ---")
        print(f"Batches for 'Jagannathpur': {len(batches)}")
        print(f"Batches for 'Jogonnathpur': {len(batches_alt)}")
        for b in batches + batches_alt:
            print(f"  BatchID: {b.id}, Filename: {b.filename}, Valid: {b.valid_count}, Invalid: {b.invalid_count}, Created: {b.created_at}")

        # Check ValidRecord table
        v_count = db.query(ValidRecord).filter(ValidRecord.upazila.ilike('%Jagannathpur%')).count()
        v_count_alt = db.query(ValidRecord).filter(ValidRecord.upazila.ilike('%Jogonnathpur%')).count()
        print(f"\n--- ValidRecord Table ---")
        print(f"Count for 'Jagannathpur': {v_count}")
        print(f"Count for 'Jogonnathpur': {v_count_alt}")

        # Check InvalidRecord table
        i_count = db.query(InvalidRecord).filter(InvalidRecord.upazila.ilike('%Jagannathpur%')).count()
        i_count_alt = db.query(InvalidRecord).filter(InvalidRecord.upazila.ilike('%Jogonnathpur%')).count()
        print(f"\n--- InvalidRecord Table ---")
        print(f"Count for 'Jagannathpur': {i_count}")
        print(f"Count for 'Jogonnathpur': {i_count_alt}")

    finally:
        db.close()

if __name__ == "__main__":
    check()
