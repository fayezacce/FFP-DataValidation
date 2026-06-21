import os
from sqlalchemy import create_engine, text

db_url = os.environ.get('DATABASE_URL')
print('DATABASE_URL:', db_url)
if not db_url:
    raise SystemExit('DATABASE_URL is not set')
engine = create_engine(db_url)
with engine.connect() as conn:
    result = conn.execute(text('select count(1) from valid_records'))
    print('valid_records count:', result.scalar())
    result = conn.execute(text('select count(1) from summary_stats'))
    print('summary_stats count:', result.scalar())
