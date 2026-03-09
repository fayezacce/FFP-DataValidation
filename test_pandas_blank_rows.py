import pandas as pd
import io

excel_bytes = io.BytesIO()
df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
# Write with 3 blank rows at top
with pd.ExcelWriter(excel_bytes, engine="openpyxl") as writer:
    df.to_excel(writer, startrow=3, index=False)

excel_bytes.seek(0)
# Read without specifying header
df1 = pd.read_excel(excel_bytes, header=None)
print("No header specified:\n", df1.head())
print("len of df1:", len(df1))

excel_bytes.seek(0)
# Read with header=3
df2 = pd.read_excel(excel_bytes, header=3)
print("\nheader=3:\n", df2.columns)
print("len of df2:", len(df2))

excel_bytes.seek(0)
# Read with header=0
df3 = pd.read_excel(excel_bytes, header=0)
print("\nheader=0:\n", df3.columns)
print("len of df3:", len(df3))
