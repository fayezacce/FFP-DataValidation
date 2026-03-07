import pytest
import pandas as pd
from app.validator import normalize_digits, clean_dob, validate_nid, process_dataframe

def test_normalize_digits():
    assert normalize_digits("১২৩৪৫৬৭৮৯০") == "1234567890"
    assert normalize_digits("abc") == "abc"
    assert normalize_digits(None) == ""
    assert normalize_digits("০১৯") == "019"

def test_clean_dob():
    # YYYY-MM-DD
    assert clean_dob("1990-12-31") == ("1990-12-31", "1990")
    # DD/MM/YYYY
    assert clean_dob("31/12/1990") == ("1990-12-31", "1990")
    # Bengali digits
    assert clean_dob("৩১-১২-১৯৯০") == ("1990-12-31", "1990")
    # Invalid
    assert clean_dob("invalid date") == (None, None)
    assert clean_dob(None) == (None, None)

def test_validate_nid():
    # 10 digits - Smart NID
    assert validate_nid("1234567890", "1990") == ("1234567890", "success", "Smart NID")
    
    # 17 digits - Standard NID
    assert validate_nid("19901234567890123", "1990") == ("19901234567890123", "success", "Standard NID")
    
    # 13 digits with DOB year - Prefix the year
    assert validate_nid("1234567890123", "1990") == ("19901234567890123", "warning", "Converted 13 to 17 digits")
    
    # 13 digits without DOB year - Error
    assert validate_nid("1234567890123", None) == ("1234567890123", "error", "13-digit NID but missing valid DOB year")
    
    # Invalid length
    assert validate_nid("12345", "1990")[1] == "error"
    assert validate_nid("123456789012345678", "1990")[1] == "error"
    
def test_process_dataframe():
    data = {
        "DOB_Col": ["1990-01-01", "৩১/১২/১৯৮০", "Invalid", "1995-05-05", "2000-01-01"],
        "NID_Col": ["1234567890", "1234567890123", "1234567890123", "12345678901234567", "123"]
    }
    df = pd.DataFrame(data)
    
    result_df, stats = process_dataframe(df, "DOB_Col", "NID_Col")
    
    assert stats["total_rows"] == 5
    assert stats["converted_nid"] == 1  # 2nd row (13 digits + valid DOB)
    assert stats["issues"] == 2         # 3rd row (13 digits + no DOB), 5th row (invalid length)
    
    # Check 13 to 17 conversion
    assert result_df.loc[1, "Cleaned_NID"] == "19801234567890123"
    assert result_df.loc[1, "Status"] == "warning"
