import pandas as pd
import sys
import os

# add backend path to sys.path
sys.path.append(r"c:\Users\fayez\.gemini\antigravity\scratch\backend")

from app.validator import process_dataframe

def main():
    file_path = r"c:\Users\fayez\.gemini\antigravity\scratch\Cumilla_Brammanpara - DC Food Comilla (1).xlsx"
    df = pd.read_excel(file_path)
    # Print the columns to see what they are called
    print("Columns:", df.columns.tolist())
    
    # Let's assume the columns are named "Date of Birth" and "NID" or similar.
    # We will search for them dynamically if needed.
    dob_col = next((c for c in df.columns if "dob" in str(c).lower() or "birth" in str(c).lower()), None)
    nid_col = next((c for c in df.columns if "nid" in str(c).lower() or "national" in str(c).lower() or "voter" in str(c).lower() or "identity" in str(c).lower()), None)
    
    print(f"Using DOB: {dob_col}, NID: {nid_col}")
    
    if not dob_col or not nid_col:
        print("Could not find columns")
        return

    results, stats = process_dataframe(df, dob_col, nid_col)
    print("Stats:", stats)
    
    # We will also try the fix.
    
if __name__ == "__main__":
    main()
