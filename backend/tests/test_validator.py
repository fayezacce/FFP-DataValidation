import pytest
import pandas as pd
from app.validator import normalize_digits, clean_dob, validate_nid, process_dataframe

def test_normalize_digits_extended():
    assert normalize_digits("১২৩৪") == "1234"
    assert normalize_digits(" 5678 ") == "5678"
    assert normalize_digits("০১৯২৪") == "01924"
    assert normalize_digits("") == ""

def test_clean_dob_formats():
    # Various valid formats
    assert clean_dob("1990-01-01")[1] == "1990"
    assert clean_dob("01/01/1980")[1] == "1980"
    assert clean_dob("15-05-2000")[1] == "2000"
    assert clean_dob("2023/12/31")[1] == "2023"
    assert clean_dob("01-Jan-1995")[1] == "1995"
    
    # Excel serial (if it happens to be passed as string)
    # 44197 -> 2021-01-01
    assert clean_dob("44197")[1] == "2021"

def test_validate_nid_cases():
    # Success cases
    assert validate_nid("1234567890", "1990")[1] == "success"
    assert validate_nid("19901234567890123", "1990")[1] == "success"
    
    # Conversion case
    nid13 = "1234567890123"
    res_nid, status, msg = validate_nid(nid13, "1985")
    assert status == "warning"
    assert res_nid == "1985" + nid13
    
    # Error cases
    assert validate_nid("123", "2000")[1] == "error"
    assert validate_nid(nid13, None)[1] == "error"
    assert validate_nid(None, "2000")[1] == "error"

def test_process_dataframe_complex():
    df = pd.DataFrame({
        "DOB": ["1990-01-01", "1985-05-05", "invalid"],
        "NID": ["1234567890", "1234567890123", "0987654321"]
    })
    
    res, stats = process_dataframe(df, "DOB", "NID")
    assert stats["total_rows"] == 3
    assert stats["converted_nid"] == 1
    assert stats["issues"] == 1 # The invalid DOB one
    
    # Check if duplicate detection worked (optionally, if that's what we want)
    # The first and third have same NID, but third has invalid DOB.
    # In processor, duplicates are checked AFTER validation.
