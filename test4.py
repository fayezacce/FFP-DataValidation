import pandas as pd
from app.validator import process_dataframe

def main():
    df = pd.read_excel('test.xlsx', header=2)
    # Check what columns are present
    name_col = df.columns[2] # typically Name is around C (index 2)
    dob_col = df.columns[3]
    nid_col = df.columns[8]
    
    # User says "only calculate rows with information stored"
    # drop rows where Name, DOB, NID are all empty
    df_clean = df.dropna(subset=[name_col, dob_col, nid_col], how='all')
    
    res, stats = process_dataframe(df_clean, dob_col, nid_col)
    
    issues_df = res[res['Status'] == 'error']
    # Also find any rows that had DOB errors
    dob_issues = res[res['Cleaned_DOB'] == 'Invalid Date']
    
    print("---FINAL STATS---")
    print(f"Original Rows: {len(df)}")
    print(f"Cleaned Rows: {len(df_clean)}")
    print("Total:", stats['total_rows'])
    print("Issues:", stats['issues'])
    print("Converted:", stats['converted_nid'])
    print(f"DOB Invalid Count: {len(dob_issues)}")
    print("-----------------")

if __name__ == "__main__":
    main()
