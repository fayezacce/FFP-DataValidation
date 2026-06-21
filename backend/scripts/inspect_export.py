from app.database import SessionLocal
from app.export_routes import get_live_records_df
from app.models import SummaryStats


def main():
    db = SessionLocal()
    try:
        entry = db.query(SummaryStats).filter(SummaryStats.valid > 0).first()
        if not entry:
            print('No valid summary stats found')
            return
        print(entry.division, entry.district, entry.upazila, entry.upazila_id)
        df = get_live_records_df(db, division=entry.division, district=entry.district, upazila=entry.upazila, is_invalid=False, upazila_id=entry.upazila_id)
        print('type', type(df), 'shape', None if df is None else df.shape)
        if df is not None:
            print('columns', list(df.columns))
            print('row0', df.iloc[0:1].to_dict(orient='records'))
            print('dtypes', df.dtypes.to_dict())
    finally:
        db.close()


if __name__ == '__main__':
    main()
