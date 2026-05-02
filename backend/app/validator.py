import pandas as pd
import re
import unicodedata
from datetime import datetime

# Bengali to English digits mapping
BENGALI_TO_ENGLISH = {
    '০': '0', '১': '1', '২': '2', '৩': '3', '৪': '4',
    '৫': '5', '৬': '6', '৭': '7', '৮': '8', '৯': '9'
}

import json
import os

# Load definitive header mapping from DB dump inside the local file
# This is now handled dynamically out of the DB in the route handler.

_BEN_TRANS = str.maketrans(BENGALI_TO_ENGLISH)

def normalize_digits(text):
    if text is None:
        return ""
    return str(text).strip().translate(_BEN_TRANS)

def normalize_col(text):
    """Normalize column names for fuzzy matching using NFKC normalization and slugification."""
    if not text:
        return ""
    # Normalize unicode (handles 'য়' vs 'য়' etc)
    text = unicodedata.normalize('NFKC', str(text))
    return re.sub(r'\W+', '', text.strip().lower())

def clean_dob(value) -> tuple[str, str]:
    """Returns (cleaned_date_str, year_str) or (None, None)"""
    if pd.isna(value) or value is None:
        return None, None
        
    # Check if it's already a datetime object (common with pandas reading Excel)
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.strftime("%Y-%m-%d"), str(value.year)
        
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
    
    date_only = val_str.split(' ')[0]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_only, fmt)
            return dt.strftime("%Y-%m-%d"), str(dt.year)
        except ValueError:
            continue
            
    # Maybe it's an Excel serial date as a string (e.g. "32874" or "32874.0")
    try:
        clean_str = val_str[:-2] if val_str.endswith('.0') else val_str
        if clean_str.isdigit() and 10000 <= int(clean_str) <= 60000:
            dt = pd.to_datetime('1899-12-30') + pd.to_timedelta(int(clean_str), 'D')
            return dt.strftime("%Y-%m-%d"), str(dt.year)
    except Exception:
        pass
        
    return None, None

_NON_DIGIT = re.compile(r'\D')

def check_fake_nid(nid: str, tz_limit: int = 0, tz_whitelist: set = None) -> tuple[bool, str]:
    """Detect suspicious NID patterns. Returns (is_suspicious, reason).
    Called AFTER length validation passes (nid is already digits-only)."""
    if not nid or len(nid) < 10:
        return False, ""

    # All zeros (e.g. 0000000000)
    if all(c == '0' for c in nid):
        return True, "All-zero NID"

    # All same digit (e.g. 1111111111)
    if len(set(nid)) == 1:
        return True, "All-same-digit NID"

    # Trust whitelisted NIDs fully (skip all further fraud checks)
    if tz_whitelist and nid in tz_whitelist:
        return False, ""

    # Trailing zero check for 17-digit NIDs only
    if len(nid) == 17:
        if tz_whitelist and nid in tz_whitelist:
            pass  # Whitelisted, skip trailing zero check
        elif tz_limit > 0:
            # Count how many trailing zeros the NID has
            trailing_zeros = len(nid) - len(nid.rstrip('0'))
            if trailing_zeros >= tz_limit:
                if tz_limit == 2:
                    return True, "Trailing double-zero NID"
                else:
                    return True, f"Trailing {tz_limit}+ zero NID"

    # Ascending sequential run of 7+ consecutive digits (e.g. 1234567)
    for i in range(len(nid) - 6):
        chunk = nid[i:i+7]
        if all(int(chunk[j+1]) == int(chunk[j]) + 1 for j in range(6)):
            return True, "Sequential ascending digit pattern detected"

    # Descending sequential run of 7+
    for i in range(len(nid) - 6):
        chunk = nid[i:i+7]
        if all(int(chunk[j+1]) == int(chunk[j]) - 1 for j in range(6)):
            return True, "Sequential descending digit pattern detected"

    return False, ""


def validate_nid(nid_raw, dob_year, tz_limit: int = 0, tz_whitelist: set = None) -> tuple[str, str, str]:
    """Returns (final_nid, status, message)"""
    if pd.isna(nid_raw) or nid_raw is None:
        return "", "error", "Missing NID"

    nid_str = normalize_digits(str(nid_raw)).replace(".0", "").strip()
    nid_str = _NON_DIGIT.sub('', nid_str)

    nid_len = len(nid_str)
    if nid_len == 10:
        final_nid, status, message = nid_str, "success", "Smart NID"
    elif nid_len == 17:
        final_nid, status, message = nid_str, "success", "Standard NID"
    elif nid_len == 13:
        if dob_year:
            new_nid = f"{dob_year}{nid_str}"
            final_nid, status, message = new_nid, "warning", "Converted 13 to 17 digits"
        else:
            return nid_str, "error", "13-digit NID but missing valid DOB year"
    else:
        return nid_str, "error", f"Invalid NID length: {nid_len} digits"

    # Fraud pattern check — only on structurally valid NIDs
    is_fake, fake_reason = check_fake_nid(final_nid, tz_limit, tz_whitelist)
    if is_fake:
        return final_nid, "error", fake_reason

    return final_nid, status, message

def resolve_column_name(target: str, available_cols: list) -> str:
    """
    Finds the best match for target in available_cols.
    1. Exact match
    2. Case-insensitive/trimmed match
    3. Slug match (respecting duplicate indices if present)
    """
    if target in available_cols:
        return target
    
    available_list = [str(c) for c in available_cols]
    target_str = str(target)
    
    # 1. Exact match (already checked above but for safety)
    if target_str in available_list:
        return available_cols[available_list.index(target_str)]
        
    # 2. Trimmed Case-insensitive
    t_clean = target_str.strip().lower()
    for i, c in enumerate(available_list):
        if c.strip().lower() == t_clean:
            return available_cols[i]
            
    # 3. Bengali Mapping (Semantic)
    semantic_map = {
        "name": ["উপকারভোগীর নাম", "উপকার ভোগীর নাম", "উপকারভোগীর নাম (বাংলা)", "নাম", "উপকারভোগীরনামবাংলাএনআইডি", "beneficiary name", "name"],
        "nid": ["জাতীয় পরিচয় পত্র নম্বর", "জাতীয় পরিচয় পত্র নম্বর", "জাতীয় পরিচয়পত্র নম্বর", "জাতীয়পরিচয়পত্রনম্বর", "এনআইডি", "nid number", "nid"],
        "dob": ["date of birth", "dob", "জন্মতারিখ", "জন্ম তারিখ", "জম্ম তারিখ", "জম্মতারিখ", "জন্মতারিখএনআইডি"],
        "mobile": ["মোবাইল নং", "মোবাইল নম্বর", "মোবাইল নম্বর এনআইডি", "মোবাইল নং (নিজ নামে)", "mobile number", "mobile no"]
    }
    
    t_clean = normalize_col(target_str)
    for key, variants in semantic_map.items():
        if t_clean == key or t_clean in [normalize_col(v) for v in variants]:
            # If the target itself is a semantic key or variant, 
            # check if ANY of the variants exist in available_cols
            for v in variants:
                v_slug = normalize_col(v)
                for i, col in enumerate(available_list):
                    if normalize_col(col) == v_slug:
                        return available_cols[i]

    # 4. Slug matching
    def get_index_suffix(s):
        # Extract .1 or (2)
        m = re.search(r'[\.\s\(]+(\d+)[\)]*$', s)
        return m.group(1) if m else None
        
    target_slug = normalize_col(target_str)
    target_idx = get_index_suffix(target_str)
    
    # First try: Slug + Index must match
    for i, c in enumerate(available_list):
        if normalize_col(c) == target_slug and get_index_suffix(c) == target_idx:
            return available_cols[i]
            
    # Second try: Just Slug (takes first occurrence)
    for i, c in enumerate(available_list):
        if normalize_col(c) == target_slug:
            return available_cols[i]
            
    return None


def ensure_dob_format(df: pd.DataFrame, primary_dob_col: str = None) -> pd.DataFrame:
    """
    Finds all DOB-related columns and ensures they are in YYYY-MM-DD format.
    Scans for columns explicitly requested, named 'dob', 'spouse_dob', or containing 'birth' or 'জন্ম'.
    """
    dob_keywords = ['dob', 'জন্ম', 'birth']
    normalized_keywords = [normalize_col(k) for k in dob_keywords if normalize_col(k)]
    
    df = df.copy()
    
    for col in df.columns:
        col_str = str(col)
        col_norm = normalize_col(col_str)
        
        is_target = False
        if primary_dob_col and col_str.strip() == str(primary_dob_col).strip():
            is_target = True
        elif any(kw in col_str.lower() for kw in dob_keywords):
            is_target = True
        elif any(kw in col_norm for kw in normalized_keywords):
            is_target = True
            
        if is_target:
            # Apply cleaning to every row in this column
            def _clean_val(v):
                if pd.isna(v) or v is None:
                    return ""
                cleaned, _ = clean_dob(v)
                return cleaned if cleaned else str(v)
            
            df[col] = df[col].apply(_clean_val)
            
    return df

def process_dataframe(df: pd.DataFrame, dob_col: str, nid_col: str, header_row: int = 1, tz_limit: int = 0, tz_whitelist: set = None, header_mapping: dict = None):
    """Processes DataFrame and adds cleaned cols, status, message, and Excel_Row."""
    if header_mapping is None:
        header_mapping = {}
        
    # NATIVE NORMALIZATION: Replace all columns with canonical mapped names where possible
    new_cols = []
    for c in df.columns:
        c_clean = str(c).strip()
        new_cols.append(header_mapping.get(c_clean, c_clean))
    df.columns = new_cols
    
    results = df.copy()
    
    # Resolve columns using fuzzy logic, now checking the canonical names first
    actual_dob_col = resolve_column_name("dob", df.columns.tolist()) or resolve_column_name(dob_col, df.columns.tolist())
    
    # Prioritize specific Bengali NID column name
    actual_nid_col = resolve_column_name("nid_number", df.columns.tolist()) or resolve_column_name("জাতীয় পরিচয় পত্র নম্বর", df.columns.tolist()) or resolve_column_name(nid_col, df.columns.tolist())
    
    # Tracking fields (optional)
    name_col = resolve_column_name("name_bn", df.columns.tolist()) or resolve_column_name("Name", df.columns.tolist())
    
    card_col = resolve_column_name("card_no", df.columns.tolist()) or resolve_column_name("Card No", df.columns.tolist())
    serial_col = resolve_column_name("master_serial", df.columns.tolist()) or resolve_column_name("স্মারক নং", df.columns.tolist()) or resolve_column_name("Serial", df.columns.tolist())
    mobile_col = resolve_column_name("mobile", df.columns.tolist()) or resolve_column_name("Mobile Number", df.columns.tolist())

    
    # Ensure mandatory columns exist
    if not actual_dob_col or not actual_nid_col:
        available = ", ".join([str(c) for c in df.columns[:20]])
        raise ValueError(f"Column mismatch. Expected DOB: '{dob_col}', NID: '{nid_col}'. Available in file: {available}")
    
    dob_col = actual_dob_col
    nid_col = actual_nid_col
    
    n = len(df)
    cleaned_dobs = [None] * n
    dob_years = [None] * n
    cleaned_nids = [None] * n
    extracted_names = [None] * n
    card_nos = [None] * n
    master_serials = [None] * n
    mobiles = [None] * n
    statuses = [None] * n
    messages = [None] * n
    excel_rows = [None] * n
    
    stats = {"total_rows": n, "issues": 0, "converted_nid": 0}
    
    def _safe_get_loc(col_name):
        loc = df.columns.get_loc(col_name)
        if isinstance(loc, int):
            return loc
        if isinstance(loc, slice):
            return loc.start
        import numpy as np
        loc_arr = np.asarray(loc)
        if loc_arr.dtype == bool or loc_arr.dtype == np.bool_:
            return int(loc_arr.argmax())
        return int(loc_arr[0])

    dob_col_idx = _safe_get_loc(dob_col)
    nid_col_idx = _safe_get_loc(nid_col)
    name_col_idx = _safe_get_loc(name_col) if name_col else -1
    card_col_idx = _safe_get_loc(card_col) if card_col else -1
    serial_col_idx = _safe_get_loc(serial_col) if serial_col else -1
    mobile_col_idx = _safe_get_loc(mobile_col) if mobile_col else -1
    
    for i, tup in enumerate(df.itertuples(index=True)):
        idx = tup[0]  # original df index
        dob_raw = tup[dob_col_idx + 1]  # +1 because Index is tup[0]
        nid_raw = tup[nid_col_idx + 1]
        
        cleaned_dob, dob_year = clean_dob(dob_raw)
        final_nid, status, message = validate_nid(nid_raw, dob_year, tz_limit, tz_whitelist)
        
        cleaned_dobs[i] = cleaned_dob if cleaned_dob else "Invalid Date"
        dob_years[i] = dob_year
        cleaned_nids[i] = final_nid
        
        # Extract name if found
        if name_col_idx != -1:
            val = tup[name_col_idx + 1]
            extracted_names[i] = str(val).strip() if not pd.isna(val) else "Unknown"
        else:
            extracted_names[i] = "Unknown"
        
        # Extract tracking fields
        if card_col_idx != -1:
            val = tup[card_col_idx + 1]
            card_nos[i] = normalize_digits(str(val)).replace(".0", "").strip() if not pd.isna(val) else ""
        if serial_col_idx != -1:
            val = tup[serial_col_idx + 1]
            master_serials[i] = normalize_digits(str(val)).replace(".0", "").strip() if not pd.isna(val) else ""
        if mobile_col_idx != -1:
            val = tup[mobile_col_idx + 1]
            mobiles[i] = normalize_digits(str(val)).replace(".0", "").strip() if not pd.isna(val) else ""

        statuses[i] = status
        messages[i] = message
        excel_rows[i] = idx + header_row + 1
        
        if status == "error" or not cleaned_dob:
            stats["issues"] += 1
            if status != "error":
                statuses[i] = "error"
                messages[i] = "Invalid DOB"
            elif not cleaned_dob:
                messages[i] = message + " and Invalid DOB"
        elif status == "warning":
            stats["converted_nid"] += 1
    
    results['Cleaned_DOB'] = cleaned_dobs
    results['Cleaned_NID'] = cleaned_nids
    results['Extracted_Name'] = extracted_names
    results['Card_No'] = card_nos
    results['Master_Serial'] = master_serials
    results['Mobile'] = mobiles
    results['Status'] = statuses
    results['Message'] = messages
    results['Excel_Row'] = excel_rows
    
    # Find duplicate NIDs (vectorized)
    valid_nids_mask = results['Cleaned_NID'].notna() & (results['Cleaned_NID'] != '')
    duplicates_mask = results.duplicated(subset=['Cleaned_NID'], keep=False) & valid_nids_mask
    
    dup_idx = results.index[duplicates_mask]
    for idx in dup_idx:
        pos = results.index.get_loc(idx)
        current_status = statuses[pos]
        current_msg = messages[pos]
        
        if current_status != "error":
            if current_status == "warning":
                stats["converted_nid"] -= 1
            stats["issues"] += 1
            statuses[pos] = "error"
            
        if "Duplicate NID" not in str(current_msg):
            messages[pos] = f"{current_msg} and Duplicate NID" if current_msg and current_msg != "nan" else "Duplicate NID"

    results['Status'] = statuses
    results['Message'] = messages

    return results, stats
