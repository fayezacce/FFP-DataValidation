import pytest
import pandas as pd
import io
from app.validator import process_dataframe

def test_full_processing_flow():
    # Simulating a small beneficiary dataset
    data = {
        "Beneficiary Name": ["Rahim", "Karim", "Sumi"],
        "Date of Birth": ["1990-01-01", "01/01/1985", "১২/১০/১৯৯২"],
        "NID Number": ["2738495061", "2738495061726", "19922738495061726"]
    }
    df = pd.DataFrame(data)
    
    # Process
    res_df, stats = process_dataframe(df, "Date of Birth", "NID Number")
    
    # Verify Stats
    assert stats["total_rows"] == 3
    assert stats["issues"] == 0
    assert stats["converted_nid"] == 1 # Karim (13 -> 17)
    
    # Verify Content
    assert res_df.iloc[0]["Status"] == "success"
    assert res_df.iloc[1]["Status"] == "warning"
    assert len(res_df.iloc[1]["Cleaned_NID"]) == 17
    assert res_df.iloc[1]["Cleaned_NID"].startswith("1985")
    assert res_df.iloc[2]["Status"] == "success"

def test_mismatched_columns():
    df = pd.DataFrame({"A": [1], "B": [2]})
    with pytest.raises(ValueError) as excinfo:
        process_dataframe(df, "DOB", "NID")
    assert "Column mismatch" in str(excinfo.value)
