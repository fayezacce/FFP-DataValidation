import pandas as pd
import json

file_path = "c:/FFP-DataValidation/Cumilla_Muradnagar - DC Food Comilla.xls"

print("\n--- PANDAS HEADERS (.XLS) ---")
for h in range(3):
    try:
        df = pd.read_excel(file_path, header=h, dtype=str)
        print(f"header={h}: columns={df.columns.tolist()}")
    except Exception as e:
        print(f"header={h} failed: {e}")
