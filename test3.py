import pandas as pd
from app.validator import process_dataframe

def main():
    df = pd.read_excel('test.xlsx', header=2)
    dob_col = df.columns[3]
    nid_col = df.columns[8]
    
    res, stats = process_dataframe(df, dob_col, nid_col)
    
    issues_df = res[res['Status'] == 'error']
    print(f"Total Issues Found: {len(issues_df)}")
    for idx, row in issues_df.iterrows():
        print(f"Row {idx+4}: DOB={row[dob_col]} (Cleaned: {row['Cleaned_DOB']}), NID={row[nid_col]} (Cleaned: {row['Cleaned_NID']}), Msg: {row['Message']}")

if __name__ == "__main__":
    main()
