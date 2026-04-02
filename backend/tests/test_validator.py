import pytest
import pandas as pd
from app.validator import (
    normalize_digits, clean_dob, validate_nid, 
    check_fake_nid, resolve_column_name, process_dataframe
)

def test_normalize_digits_extended():
    assert normalize_digits("১২৩৪") == "1234"
    assert normalize_digits(" 5678 ") == "5678"
    assert normalize_digits("০১৯২৪") == "01924"
    assert normalize_digits(" 0123-456 ") == "0123-456"
    assert normalize_digits(None) == ""

def test_clean_dob_formats():
    # Various valid formats
    assert clean_dob("1990-01-01")[1] == "1990"
    assert clean_dob("01/01/1980")[1] == "1980"
    assert clean_dob("15-05-2000")[1] == "2000"
    assert clean_dob("2023/12/31")[1] == "2023"
    assert clean_dob("01-Jan-1995")[1] == "1995"
    assert clean_dob("১২/১০/১৯৯২")[1] == "1992" # Bengali digits in DOB
    
    # Excel serial
    assert clean_dob("44197")[1] == "2021"
    assert clean_dob(44197)[1] == "2021"

def test_check_fake_nid():
    # All same digit
    is_fake, reason = check_fake_nid("1111111111")
    assert is_fake is True
    assert "All-same-digit" in reason
    
    # All zeros
    is_fake, reason = check_fake_nid("0000000000")
    assert is_fake is True
    assert "All-zero" in reason
    
    # Trailing double zero (17-digit)
    is_fake, reason = check_fake_nid("19901234567890100")
    assert is_fake is True
    assert "Trailing double-zero" in reason
    
    # Whitelisted trailing zero
    is_fake, reason = check_fake_nid("19901234567890100", tz_whitelist={"19901234567890100"})
    assert is_fake is False
    
    # Sequential ascending 7+
    is_fake, reason = check_fake_nid("1234567890")
    assert is_fake is True
    assert "Sequential ascending" in reason
    
    # Sequential descending 7+
    is_fake, reason = check_fake_nid("9876543210")
    assert is_fake is True
    assert "Sequential descending" in reason
    
    # Valid NID
    is_fake, reason = check_fake_nid("2738495061")
    assert is_fake is False

def test_validate_nid_cases():
    # Success cases
    assert validate_nid("2738495061", "1990")[1] == "success"
    assert validate_nid("19902738495061726", "1990")[1] == "success"
    
    # Conversion case
    nid13 = "2738495061726"
    res_nid, status, msg = validate_nid(nid13, "1985")
    assert status == "warning"
    assert res_nid == "1985" + nid13
    
    # Fraud pattern integration
    assert validate_nid("1111111111", "1990")[1] == "error"
    
    # Error cases
    assert validate_nid("123", "2000")[1] == "error"
    assert validate_nid(nid13, None)[1] == "error"
    assert validate_nid(None, "2000")[1] == "error"

def test_resolve_column_name():
    cols = ["উপকারভোগীর নাম", "জাতীয় পরিচয় পত্র নম্বর", "Date of Birth", "Mobile Number"]
    
    # Exact
    assert resolve_column_name("Date of Birth", cols) == "Date of Birth"
    
    # Case-insensitive/Trimmed
    assert resolve_column_name(" date of birth ", cols) == "Date of Birth"
    
    # Normalization handles the subtle character differences in Bengali
    assert resolve_column_name("জাতীয় পরিচয় পত্র নম্বর", cols) == "জাতীয় পরিচয় পত্র নম্বর"

def test_process_dataframe_complex():
    df = pd.DataFrame({
        "উপকারভোগীর নাম": ["Rahim", "Karim", "Sumi"],
        "Date of Birth": ["1990-01-01", "1985-05-05", "invalid"],
        "জাতীয় পরিচয় পত্র নম্বর": ["2738495061", "2738495061726", "9870654321"]
    })
    
    # NID Number is not in df, but process_dataframe has mapping for the Bengali one
    res, stats = process_dataframe(df, "Date of Birth", "NID Number")
    assert stats["total_rows"] == 3
    assert stats["converted_nid"] == 1 # Karim 13->17
    assert stats["issues"] == 1 # The invalid DOB one
    
    assert res.iloc[0]["Extracted_Name"] == "Rahim"
    assert res.iloc[1]["Cleaned_NID"].startswith("1985")
    assert res.iloc[2]["Status"] == "error"
