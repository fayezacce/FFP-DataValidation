import psycopg2
conn = psycopg2.connect('host=localhost dbname=ffp_standard user=fayez password=postgres')
cur = conn.cursor()
cur.execute("SELECT gender, name_bn, address FROM live_records WHERE district='Barguna' AND upazila='Amtali' LIMIT 10")
rows = cur.fetchall()
for r in rows:
    for v in r:
        if v:
            s = str(v)[:80]
            has_mojibake = ('à' in repr(v)[:100] or 'ç' in repr(v)[:100] or '¦' in repr(v)[:100])
            print(repr(s)[:100], '<<< MOJIBAKE' if has_mojibake else 'clean')
    print('---')
