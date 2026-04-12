import pandas as pd
import glob
from collections import Counter

headers = Counter()
files = glob.glob('/app/uploads/*.xls*')

for f in files:
    try:
        # Read Excel headers
        df = pd.read_excel(f, nrows=0)
        # Process and normalize column names very slightly to avoid exact matches
        for col in df.columns:
            # Clean up whitespace and exact string matches
            c = str(col).strip()
            if 'Unnamed' not in c and c:
                headers[c] += 1
    except Exception as e:
        print(f"Skipped {f}: {e}")

out_path = '/app/headers_out.txt'
with open(out_path, 'w', encoding='utf-8') as outfile:
    outfile.write("Header variations in 23 Excel files currently inside backend uploaded_files:\n\n")
    for k, v in headers.most_common():
        outfile.write(f"{v:3d} : {k}\n")

print(f"Successfully wrote {len(headers)} unique headers to {out_path}.")
