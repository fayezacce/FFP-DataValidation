import sys; sys.path.append('/')
from app.database import SessionLocal
from app.models import ValidRecord
from collections import Counter
import json

db = SessionLocal()
print('Starting strict yield_per scan...')
c = Counter()
try:
    # yield_per forces a true server-side cursor in Postgres + SQLAlchemy
    query = db.query(ValidRecord.data).yield_per(10000)
    
    for i, r in enumerate(query):
        if r.data:
            c.update(r.data.keys())
        if i % 100000 == 0:
            print(f'Scanned {i} rows natively...', flush=True)

    print('\n--- EXACT FULL DB UNIQUE HEADERS ---', flush=True)
    with open('/app/full_db_headers.txt', 'w', encoding='utf-8') as f:
        for k, v in c.most_common():
            print(f'{v:5d} : {k}', flush=True)
            f.write(f'{v:5d} : {k}\n')
            
    print('Done writing.')
except Exception as e:
    print('Failed:', e)
