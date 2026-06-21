import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app.database import SessionLocal
from app.export_routes import get_live_records_df, _prepare_export_df, _build_export_filename, save_live_excel_nikosh
from app.validator import ensure_dob_format
from app.models import SummaryStats

output_dir = os.path.abspath(os.path.join(os.getcwd(), "downloads", "approved_xlsx", "sylhet"))
os.makedirs(output_dir, exist_ok=True)
print("Output directory:", output_dir)

cols = [
    "division", "district", "upazila", "union_name", "serial_no", "name_bn", "name_en",
    "father_husband_name", "dob", "occupation", "address", "ward", "nid_17", "nid_10",
    "mobile", "gender", "religion", "spouse_name", "spouse_nid", "spouse_dob"
]

with SessionLocal() as db:
    entries = db.query(SummaryStats).filter(SummaryStats.division == "Sylhet").order_by(
        SummaryStats.district, SummaryStats.upazila
    ).all()
    print("Sylhet upazilas:", len(entries))
    total_written = 0

    for idx, entry in enumerate(entries, 1):
        kwargs = {"is_invalid": False, "keep_data": True}
        if entry.upazila_id:
            kwargs["upazila_id"] = entry.upazila_id
        else:
            kwargs.update({"division": entry.division, "district": entry.district, "upazila": entry.upazila})
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        df = get_live_records_df(db, **kwargs)
        if df is None or df.empty:
            print(f"{idx}/{len(entries)} skipped {entry.district}/{entry.upazila} (no rows)")
            continue

        df.columns = df.columns.str.lower()
        export_df = _prepare_export_df(df, cols)
        export_df = ensure_dob_format(export_df)

        filename = _build_export_filename(
            {"division": entry.division, "district": entry.district, "upazila": entry.upazila},
            "{district}_{upazila}_Approved_NID_NotVerified_FFP_List.xlsx",
            mode="valid",
        )
        out_path = os.path.join(output_dir, filename)
        save_live_excel_nikosh(export_df, out_path, "Valid Records", is_valid=True)
        total_written += 1
        print(f"{idx}/{len(entries)} wrote {filename}")

    print("Total files written:", total_written)
