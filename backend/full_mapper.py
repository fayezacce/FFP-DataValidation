import json
import re

with open('c:\\FFP-DataValidation\\backend\\db_headers.txt', 'r', encoding='utf-8') as f:
    lines = f.read().splitlines()

headers = []
current_header = ''
for line in lines[2:]:
    if '|' in line:
        parts = line.split('|')
        part1 = parts[0]
        if part1.endswith('+'):
            current_header += part1[:-1].strip() + ' '
        else:
            current_header += part1.strip()
            current_header = re.sub(r'\s+', ' ', current_header).strip()
            if current_header and not current_header.startswith('Unnamed:') and current_header not in ('Cleaned_DOB', 'Cleaned_NID', 'Status', 'Message', 'Excel_Row', 'Extracted_Name', 'Eroor'):
                headers.append(current_header)
            current_header = ''
    else:
        if current_header:
            current_header += line.strip() + ' '

headers = list(set(headers))

canonical_names = {
    'card_no': ['কার্ড', 'card', 'ক্রমিক', 'ক্র:', 'ক্র.', 'ক্রঃ', 'Sl No', 'কাড'],
    'master_serial': ['স্মারক', 'মাস্টার', 'master'],
    'spouse_name': ['স্বামী/স্ত্রীর নাম', 'স্বামী/স্ত্রী নাম', 'স্বামী/ স্ত্রীর নাম', 'স্বামী-স্ত্রীর নাম', 'স্বামী-স্ত্রী নাম', 'স্বামী স্ত্রী নাম', 'স্বামী / স্ত্রী নাম', 'স্বামী/স্ত্রী/অভিভাবক'],
    'spouse_nid': ['স্বামী/স্ত্রী জাতীয় পরিচয়', 'স্বামী/স্ত্রী এনআইডি', 'স্বামী-স্ত্রী জাতীয়', 'স্বামী/ স্ত্রীর জাতীয়', 'স্বামী স্ত্রীর জাতীয়', 'স্বামী/স্ত্রীর এনআইডি', 'স্বামী/ স্ত্রীর এনআইডি'],
    'spouse_dob': ['স্বামী/স্ত্রী জন্ম', 'স্বামী-স্ত্রী জন্ম', 'স্বামী/ স্ত্রীর জন্ম', 'স্বামী স্ত্রীর জন্ম'],
    'father_husband_name': ['পিতা', 'স্বামী/অভিভাবক', 'পিতা/স্বামীর'],
    'name_en': ['ইংরেজি', 'ইংরেজী', 'english', 'ইংরেজীতে'],
    'name_bn': ['উপকারভোগী', 'উপকার ভোগী', 'উপকারঅেগী', 'উপকাোগী', 'উপকার ভোগি', 'ভোক্তার নাম', 'Name'],
    'dob': ['জন্ম', 'dob', 'জান্ম'],
    'occupation': ['পেশা'],
    'address': ['গ্রাম', 'ঠিকানা', 'address', 'village'],
    'ward': ['ওয়ার্ড', 'ওয়াড'],
    'union_name': ['ইউনিয়ন'],
    'nid_number': ['জাতীয়', 'এনআইডি', 'পরিচয়পত্র', 'nid', 'পরিচয়প্রত্র'],
    'dealer_name': ['ডিলারের নাম', 'ডিলার নাম', 'ডিলাররের নাম', 'ডিলার'],
    'dealer_nid': ['ডিলারের এনআইডি', 'ডিলারের জাতীয়', 'ডিলারের এইডি'],
    'dealer_mobile': ['ডিলারের মোবাইল', 'ডিলারের মোবাঃ'],
    'mobile': ['মোবাইল', 'mobile', 'Contact'],
    'religion': ['ধর্ম', 'ধম', 'religious'],
    'gender': ['লিঙ্গ', 'gender', 'sex', 'পুরূষ / মহিলা']
}

mapping_order = [
    'spouse_nid', 'spouse_dob', 'spouse_name',
    'dealer_nid', 'dealer_mobile', 'dealer_name',
    'master_serial', 'card_no', 
    'father_husband_name', 'name_en', 'name_bn', 
    'dob', 'occupation', 'address', 'ward', 'union_name', 
    'nid_number', 'mobile', 'religion', 'gender'
]

mapped = {}
unmapped = []

for h in headers:
    h_lower = h.lower()
    matched = False
    for can in mapping_order:
        if any(keyword.lower() in h_lower for keyword in canonical_names[can]):
            mapped[h] = can
            matched = True
            break
    if not matched:
        unmapped.append(h)

report = {can: [] for can in mapping_order}
for k, v in mapped.items():
    report[v].append(k)

print('--- MAPPING SUMMARY ---')
for can in mapping_order:
    print(f'{can}: {len(report[can])} variants')
print(f'UNMAPPED: {len(unmapped)} variants\n')

print('--- UNMAPPED VARIANTS ---')
for u in unmapped:
    print(u)
    
with open('c:\\FFP-DataValidation\\backend\\generated_mapping.json', 'w', encoding='utf-8') as f:
    json.dump(mapped, f, ensure_ascii=False, indent=2)
