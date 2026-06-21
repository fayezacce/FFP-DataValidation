import os
from app.database import SessionLocal
from app.export_routes import get_live_records_df, _prepare_export_df, _build_export_filename, save_live_excel_nikosh
from app.validator import ensure_dob_format
from app.models import SummaryStats

OUTPUT_DIR = os.environ.get('OUTPUT_DIR') or os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'downloads', 'approved_xlsx'))
FILENAME_TEMPLATE = os.environ.get('FILENAME_TEMPLATE', '{district}_{upazila}_Approved_NID_NotVerified_FFP_List.xlsx')
EXPORT_COLUMNS = [
    'division', 'district', 'upazila', 'union_name', 'serial_no', 'name_bn', 'name_en',
    'father_husband_name', 'dob', 'occupation', 'address', 'ward', 'nid_17', 'nid_10',
    'mobile', 'gender', 'religion', 'spouse_name', 'spouse_nid', 'spouse_dob'
]


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print('Output directory:', OUTPUT_DIR)

    db = SessionLocal()
    try:
        entries = db.query(SummaryStats).filter(SummaryStats.valid > 0).order_by(
            SummaryStats.division, SummaryStats.district, SummaryStats.upazila
        ).all()
        print('Upazilas with valid records:', len(entries))

        total_written = 0
        for idx, entry in enumerate(entries, 1):
            kwargs = {'is_invalid': False, 'keep_data': True}
            if entry.upazila_id:
                kwargs['upazila_id'] = entry.upazila_id
            else:
                kwargs.update({'division': entry.division, 'district': entry.district, 'upazila': entry.upazila})

            df = get_live_records_df(db, **kwargs)
            if df is None or df.empty:
                print(f'{idx}/{len(entries)} skipped {entry.division}/{entry.district}/{entry.upazila} (no rows)')
                continue

            df.columns = df.columns.str.lower()
            export_df = _prepare_export_df(df, EXPORT_COLUMNS)
            export_df = ensure_dob_format(export_df)

            filename = _build_export_filename(
                {
                    'division': entry.division,
                    'district': entry.district,
                    'upazila': entry.upazila,
                },
                FILENAME_TEMPLATE,
                mode='valid',
            )
            out_path = os.path.join(OUTPUT_DIR, filename)
            save_live_excel_nikosh(export_df, out_path, 'Valid Records', is_valid=True)

            total_written += 1
            if idx % 10 == 0 or idx == len(entries):
                print(f'{idx}/{len(entries)} processed, {total_written} files written')

        print('Total files written:', total_written)
    finally:
        db.close()


if __name__ == '__main__':
    main()
