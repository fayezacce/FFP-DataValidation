import pandas as pd

data = {
    "Name": ["Rahim", "Karim", "Ayesha", "Fatima"],
    "DOB": ["1990-01-01", "31/12/1980", "2000-05-05", "1995-10-10"],
    "NID": ["1234567890", "1234567890123", "১২৩৪৫৬৭৮৯০১২৩", "12345"]
}
df = pd.DataFrame(data)
df.to_excel("test_data.xlsx", index=False)
print("Created test_data.xlsx")
