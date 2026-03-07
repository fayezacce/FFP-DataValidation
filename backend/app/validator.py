import pandas as pd
import re
from datetime import datetime

# Bengali to English digits mapping
BENGALI_TO_ENGLISH = {
    '০': '0', '১': '1', '২': '2', '৩': '3', '৪': '4',
    '৫': '5', '৬': '6', '৭': '7', '৮': '8', '৯': '9'
}

def normalize_digits(text):
    if text is None:
        return ""
    text_str = str(text).strip()
    for ben, eng in BENGALI_TO_ENGLISH.items():
        text_str = text_str.replace(ben, eng)
    return text_str

def clean_dob(value) -> tuple[str, str]:
    """Returns (cleaned_date_str, year_str) or (None, None)"""
    if pd.isna(value) or value is None:
        return None, None
        
    val_str = normalize_digits(str(value))
    
    # Try different formats
    formats = [
        "%Y-%m-%d",      # 1990-12-31
        "%d/%m/%Y",      # 31/12/1990
        "%d-%m-%Y",      # 31-12-1990
        "%Y/%m/%d",      # 1990/12/31
        "%d-%b-%Y",      # 31-Dec-1990
        "%d-%m-%y"       # 31-12-90
    ]
    
    # Check if it's already a datetime object (common with pandas reading Excel)
    if isinstance(value, datetime) or isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d"), str(value.year)
        
    for fmt in formats:
        try:
            # Handle potential time components in the string
            date_only = val_str.split(' ')[0] 
            dt = datetime.strptime(date_only, fmt)
            return dt.strftime("%Y-%m-%d"), str(dt.year)
        except ValueError:
            continue
            
    # Maybe it's an Excel serial date as a string (e.g. "32874")
    try:
        if val_str.isdigit() and 10000 <= int(val_str) <= 60000:
            dt = pd.to_datetime('1899-12-30') + pd.to_timedelta(int(val_str), 'D')
            return dt.strftime("%Y-%m-%d"), str(dt.year)
    except Exception:
        pass
        
    return None, None

def validate_nid(nid_raw, dob_year) -> tuple[str, str, str]:
    """Returns (final_nid, status, message)"""
    if pd.isna(nid_raw) or nid_raw is None:
        return "", "error", "Missing NID"
        
    nid_str = normalize_digits(str(nid_raw)).replace(".0", "").strip()
    
    # Remove any non-digit characters
    nid_str = re.sub(r'\D', '', nid_str)
    
    if len(nid_str) == 10:
        return nid_str, "success", "Smart NID"
    elif len(nid_str) == 17:
        return nid_str, "success", "Standard NID"
    elif len(nid_str) == 13:
        if dob_year:
            new_nid = f"{dob_year}{nid_str}"
            return new_nid, "warning", "Converted 13 to 17 digits"
        else:
            return nid_str, "error", "13-digit NID but missing valid DOB year"
    else:
        return nid_str, "error", f"Invalid NID length: {len(nid_str)} digits"

def process_dataframe(df: pd.DataFrame, dob_col: str, nid_col: str, header_row: int = 1):
    """Processes DataFrame and adds cleaned cols, status, message, and Excel_Row."""
    results = df.copy()
    
    # Ensure columns exist
    if dob_col not in df.columns or nid_col not in df.columns:
        raise ValueError(f"Columns {dob_col} or {nid_col} not found in uploaded file.")
    
    # Prepare result columns
    results['Cleaned_DOB'] = None
    results['DOB_Year'] = None
    results['Cleaned_NID'] = None
    results['Status'] = None
    results['Message'] = None
    results['Excel_Row'] = None
    
    stats = {"total_rows": len(df), "issues": 0, "converted_nid": 0}
    
    for idx, row in df.iterrows():
        dob_raw = row[dob_col]
        nid_raw = row[nid_col]
        
        cleaned_dob, dob_year = clean_dob(dob_raw)
        final_nid, status, message = validate_nid(nid_raw, dob_year)
        
        results.at[idx, 'Cleaned_DOB'] = cleaned_dob if cleaned_dob else "Invalid Date"
        results.at[idx, 'DOB_Year'] = dob_year
        results.at[idx, 'Cleaned_NID'] = final_nid
        results.at[idx, 'Status'] = status
        results.at[idx, 'Message'] = message
        # df.index is 0-based relative to the sliced dataframe.
        # Original excel row = index + header_row + 1 (for 1-based row numbers)
        results.at[idx, 'Excel_Row'] = idx + header_row + 1
        
        if status == "error" or not cleaned_dob:
            stats["issues"] += 1
            if status != "error":
                results.at[idx, 'Status'] = "error"
                results.at[idx, 'Message'] = "Invalid DOB"
            elif not cleaned_dob:
                results.at[idx, 'Message'] += " and Invalid DOB"
            
        elif status == "warning":
            stats["converted_nid"] += 1
            
    # Find duplicate NIDs
    valid_nids_mask = results['Cleaned_NID'].notna() & (results['Cleaned_NID'] != '')
    duplicates_mask = results.duplicated(subset=['Cleaned_NID'], keep=False) & valid_nids_mask
    duplicate_indices = results[duplicates_mask].index

    for idx in duplicate_indices:
        current_status = results.at[idx, 'Status']
        current_msg = results.at[idx, 'Message']
        
        if current_status != "error":
            if current_status == "warning":
                stats["converted_nid"] -= 1
            stats["issues"] += 1
            results.at[idx, 'Status'] = "error"
            
        if "Duplicate NID" not in str(current_msg):
            results.at[idx, 'Message'] = f"{current_msg} and Duplicate NID" if current_msg and current_msg != "nan" else "Duplicate NID"

    # Drop intermediate columns if desired
    results = results.drop(columns=['DOB_Year'])
    return results, stats
