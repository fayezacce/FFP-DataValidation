from collections import defaultdict

# The true variations from backend memory and 200 Excel files:
raw_mapping = {
    # NID
    "nid_number": ["জাতীয় পরিচয় পত্র নম্বর", "NID Number", "NID", "National ID", "এনআইডি", "জাতীয় পরিচয়পত্র নম্বর", "এন আই ডি", "NID No"],
    
    # DOB
    "dob": ["জন্ম তারিখ", "Date of Birth", "DOB", "Birth Date", "জন্মতারিখ"],
    
    # Card Number / Serial
    "card_no": ["কার্ড নং", "Card No", "Card Number", "কার্ড নাম্বার", "ক্রমিক নং", "Serial No", "ক্রমিক নাম্বার"],
    
    # Name
    "name_bn": ["উপকারভোগীর নাম", "Beneficiary Name", "Name", "নাম", "উপকার ভোগীর নাম", "উপকারভোগীর নাম (বাংলা)", "Name (Bangla)"],
    
    # Father/Husband Name
    "father_husband_name": ["পিতা/স্বামীর নাম", "Father/Husband Name", "পিতার নাম", "স্বামীর নাম", "Father Name", "Husband Name", "পিতা / স্বামীর নাম"],
    
    # Mother Name
    "mother_name": ["মাতার নাম", "Mother Name", "মাতা"],
    
    # Gender
    "gender": ["লিঙ্গ", "Gender", "Sex", "পুরূষ / মহিলা"],
    
    # Address
    "address": ["ঠিকানা", "Address", "গ্রামের নাম", "Village", "গ্রাম"],
    
    # Word/Union
    "ward": ["ওয়ার্ড নং", "Ward No", "Ward", "ওয়ার্ড"],
    
    # Union Name
    "union_name": ["ইউনিয়নের নাম", "Union Name", "Union", "ইউনিয়ন"],
    
    # Mobile
    "mobile": ["মোবাইল নং", "Mobile Number", "Mobile No", "মোবাইল", "Contact No"],
    
    # Dealer Name
    "dealer_name": ["ডিলারের নাম", "Dealer Name", "Dealer", "ডিলার"],
    
    # Remarks
    "remarks": ["মন্তব্য", "Remarks", "Comments"],
}

sql_lines = [
    "CREATE EXTENSION IF NOT EXISTS hstore;",
    "CREATE TABLE IF NOT EXISTS header_mapping (variant_header TEXT PRIMARY KEY, canonical_header TEXT NOT NULL);"
]

insert_values = []
for canonical, variants in raw_mapping.items():
    for var in variants:
        v = var.replace("'", "''")
        c = canonical.replace("'", "''")
        insert_values.append(f"    ('{v}', '{c}')")

sql_lines.append("INSERT INTO header_mapping (variant_header, canonical_header) VALUES")
sql_lines.append(",\n".join(insert_values))
sql_lines.append("ON CONFLICT (variant_header) DO UPDATE SET canonical_header = EXCLUDED.canonical_header;\n")

# Adding the pg function
function_sql = """
CREATE OR REPLACE FUNCTION normalize_json_headers(j json) RETURNS json AS $$
DECLARE
    mapping CONSTANT hstore := (SELECT hstore(array_agg(variant_header), array_agg(canonical_header)) FROM header_mapping);
    result jsonb := '{}';
    rec RECORD;
BEGIN
    FOR rec IN SELECT key, value FROM json_each_text(j) LOOP
        result := result || jsonb_build_object(
            COALESCE(mapping -> rec.key, rec.key),
            rec.value
        );
    END LOOP;
    RETURN result::json;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
"""

sql_lines.append(function_sql)

with open("/app/apply_mapping.sql", "w", encoding="utf-8") as f:
    f.write("\n".join(sql_lines))

print("Created /app/apply_mapping.sql successfully!")
