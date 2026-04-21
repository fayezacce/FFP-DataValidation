import sys
sys.path.insert(0, '/app')
from app.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

# 1. Counts
total = db.execute(text('SELECT count(*) FROM valid_records')).scalar()
with_data = db.execute(text('SELECT count(*) FROM valid_records WHERE data IS NOT NULL')).scalar()
no_dealer = db.execute(text('SELECT count(*) FROM valid_records WHERE dealer_id IS NULL')).scalar()
print(f'Total valid records: {total}')
print(f'With data JSON:      {with_data}')
print(f'No dealer_id:        {no_dealer}')

# 2. Check known dealer key variants
print()
print('--- Dealer key counts in data JSON ---')
for key in ['dealer_nid', 'Dealer_NID', 'dealer_name', 'Dealer_Name', 'dealer_mobile', 'Dealer_Mobile']:
    count = db.execute(text(
        "SELECT count(*) FROM valid_records WHERE data->>:k IS NOT NULL AND TRIM(data->>:k) != ''"
    ), {'k': key}).scalar()
    print(f'  [{key}]: {count} records')

# 3. Check dealers table
dealer_count = db.execute(text('SELECT count(*) FROM dealers')).scalar()
print(f'\nDealers table rows: {dealer_count}')

# 4. Show full keys for first 3 records
print()
print('--- All JSON keys in first 3 records ---')
rows = db.execute(text('SELECT id, data FROM valid_records WHERE data IS NOT NULL LIMIT 3')).fetchall()
for row in rows:
    d = row.data if isinstance(row.data, dict) else {}
    print(f'\n  Record ID {row.id}:')
    for k, v in d.items():
        print(f'    {repr(k)}: {repr(str(v)[:80])}')

# 5. Check if there are any Bangla dealer keys
print()
print('--- Bangla key search ---')
bangla_keys = ['\u09a1\u09bf\u09b2\u09be\u09b0 \u098f\u09a8\u09be\u0987\u09a1\u09bf', '\u09a1\u09bf\u09b2\u09be\u09b0\u09c7\u09b0 \u09a8\u09be\u09ae', '\u09a1\u09bf\u09b2\u09be\u09b0 \u09ae\u09cb\u09ac\u09be\u0987\u09b2']
for key in bangla_keys:
    count = db.execute(text(
        "SELECT count(*) FROM valid_records WHERE data->>:k IS NOT NULL AND TRIM(data->>:k) != ''"
    ), {'k': key}).scalar()
    print(f'  [{key}]: {count} records')

db.close()
print('\nDone.')
