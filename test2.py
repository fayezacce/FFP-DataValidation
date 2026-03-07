import pandas as pd
from app.validator import process_dataframe

df = pd.read_excel('test.xlsx', header=2) # header is on row 3

# we find the columns based on manual inspection or matching
dob_col = None
nid_col = None
for col in df.columns:
    if 'তারিখ' in str(col) or 'dob' in str(col).lower():
        dob_col = col
    if 'nid' in str(col).lower() or 'National ID' in str(col):
        nid_col = col
        
print(f"Total rows in df: {len(df)}")
if dob_col and nid_col:
    print(f"Using DOB: {dob_col}, NID: {nid_col}")
    res, stats = process_dataframe(df, dob_col, nid_col)
    print("Stats:", stats)
else:
    print("Did not find columns natively, these are the columns:", df.columns.tolist())
    
