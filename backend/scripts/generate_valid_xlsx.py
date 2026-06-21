import os
from app.database import SessionLocal
from app.export_routes import get_live_records_df, _prepare_export_df, _safe_filename, save_live_excel_nikosh
from app.validator import ensure_dob_format
from app.models import SummaryStats


target = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'downloads', 'approved_xlsx'))
os.makedirs(target, exist_ok=True)
print('Export target:', target)

with SessionLocal() as db:
    entries = db.query(SummaryStats).filter(SummaryStats.valid > 0).order_by(SummaryStats.division, SummaryStats.district, SummaryStats.upazila).all()
    print('Upazilas with valid records:', len(entries))
    cols = [
        'division', 'district', 'upazila', 'union_name', 'serial_no', 'name_bn', 'name_en',
        'father_husband_name', 'dob', 'occupation', 'address', 'ward', 'nid_17', 'nid_10',
        'mobile', 'gender', 'religion', 'spouse_name', 'spouse_nid', 'spouse_dob'
    ]
    count = 0
    for idx, e in enumerate(entries, 1):
        kwargs = {'is_invalid': False, 'keep_data': True}
        if e.upazila_id:
            kwargs['upazila_id'] = e.upazila_id
        else:
            kwargs.update({'division': e.division, 'district': e.district, 'upazila': e.upazila})
        df = get_live_records_df(db, **kwargs)
        if df is None or df.empty:
            print(f'{idx}/{len(entries)} skipped empty {e.division}/{e.district}/{e.upazila}')
            continue
        df.columns = df.columns.str.lower()
        export_df = _prepare_export_df(df, cols)
        export_df = ensure_dob_format(export_df)
        file_name = _safe_filename(f"{e.district}_{e.upazila}_Approved_NID_NotVerified_FFP_List.xlsx")
        out_path = os.path.join(target, file_name)
        save_live_excel_nikosh(export_df, out_path, 'Valid Records', is_valid=True)
        count += 1
        if idx % 10 == 0:
            print(f'{idx}/{len(entries)} processed, {count} files written so far')
    print('Finished. Total files written:', count)
    print('Export folder:', target)
