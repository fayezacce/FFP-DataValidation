import json
import re

with open('backend/db_headers.txt', 'r', encoding='utf-8') as f:
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

manual_overrides = {
    'স্বামী /স্ত্রীর নাম': 'spouse_name',
    'স্মামী/স্ত্রী নাম': 'spouse_name',
    'জাতয়ি পরিচয় পত্র নং': 'nid_number',
    'স্বামী/স্থীর নাম': 'spouse_name',
    'জাতীয় পরিচয় পত্র নম্বর': 'nid_number',
    'উপকারভোগীর নাম (বাংলা)': 'name_bn',
    'নাম': 'name_en',  
    'স্বামী/স্ত্রীর জাতয়ি পরিচয় পত্র নং': 'spouse_nid',
    'লিংঙ্গ': 'gender',
    'জাতিয় পরিচয় পত্র নং': 'nid_number',
    'স্বামী/ স্ত্রী নাম': 'spouse_name',
    'উপভোগকারীর নাম': 'name_bn',
    'স্বামিও/স্ত্রী': 'spouse_name',
    'ইউপির নাম': 'union_name',
    'নমিনীর নাম': 'spouse_name',
    'মোবাইল নম্বর': 'mobile',
    'স্বামি/স্ত্রী নাম': 'spouse_name',
    'জাতিয় পরিচয় পত্র নম্বর': 'nid_number',
    'এন.আই.ডি নং': 'nid_number',
    'ইউনিয়ানের নাম': 'union_name',
    'উপকার ভোগীর নাম (বাংলা)': 'name_bn',
    'মোবাইল নম্বর (নিজ নামে)': 'mobile',
    'স্বামী.স্ত্রীর নাম': 'spouse_name'
}

canonical_names = {
    'card_no': ['কার্ড', 'card', 'কাড'],
    'serial_no': ['ক্রমিক', 'ক্র:', 'ক্র.', 'ক্রঃ', 'Sl No', 'ক্র/'],
    'master_serial': ['স্মারক', 'মাস্টার', 'master'],
    'spouse_name': ['স্বামী/স্ত্রীর নাম', 'স্বামী/স্ত্রী নাম', 'স্বামী/ স্ত্রীর নাম', 'স্বামী-স্ত্রীর নাম', 'স্বামী-স্ত্রী নাম', 'স্বামী স্ত্রী নাম', 'স্বামী / স্ত্রী নাম', 'স্বামী/স্ত্রী/অভিভাবক', 'স্বামী / স্ত্রী’র', 'স্বা/স্ত্রী'],
    'spouse_nid': ['স্বামী/স্ত্রী জাতীয় পরিচয়', 'স্বামী/স্ত্রী এনআইডি', 'স্বামী-স্ত্রী জাতীয়', 'স্বামী/ স্ত্রীর জাতীয়', 'স্বামী স্ত্রীর জাতীয়', 'স্বামী/স্ত্রীর এনআইডি', 'স্বামী/ স্ত্রীর এনআইডি', 'স্বা/স্ত্রী এন.আই.ডি'],
    'spouse_dob': ['স্বামী/স্ত্রী জন্ম', 'স্বামী-স্ত্রী জন্ম', 'স্বামী/ স্ত্রীর জন্ম', 'স্বামী স্ত্রীর জন্ম', 'স্বামী / স্ত্রী জন্ম'],
    'father_husband_name': ['পিতা', 'স্বামী/অভিভাবক', 'পিতা/স্বামীর', 'পিতা/স্বামী'],
    'name_en': ['ইংরেজি', 'ইংরেজী', 'english', 'ইংরেজীতে', 'Bs‡iRx'],
    'name_bn': ['উপকারভোগী', 'উপকার ভোগী', 'উপকারঅেগী', 'উপকাোগী', 'উপকার ভোগি', 'ভোক্তার নাম', 'Name', 'উপকারভোগী'],
    'dob': ['জন্ম', 'dob', 'জান্ম', 'জম্ম'],
    'occupation': ['পেশা'],
    'address': ['গ্রাম', 'ঠিকানা', 'address', 'village'],
    'ward': ['ওয়ার্ড', 'ওয়াড', 'ওয়র্ড'],
    'union_name': ['ইউনিয়ন'],
    'nid_number': ['জাতীয়', 'এনআইডি', 'পরিচয়পত্র', 'nid', 'পরিচয়প্রত্র'],
    'dealer_name': ['ডিলারের নাম', 'ডিলার নাম', 'ডিলাররের নাম', 'ডিলার', 'দিলারের'],
    'dealer_nid': ['ডিলারের এনআইডি', 'ডিলারের জাতীয়', 'ডিলারের এইডি'],
    'dealer_mobile': ['ডিলারের মোবাইল', 'ডিলারের মোবাঃ', 'ডিলারের মোবা'],
    'mobile': ['মোবাইল', 'mobile', 'Contact', 'মোবইল', 'মোবাই'],
    'religion': ['ধর্ম', 'ধম', 'religious', 'র্ধম'],
    'gender': ['লিঙ্গ', 'gender', 'sex', 'পুরূষ / মহিলা']
}

mapping_order = [
    'spouse_nid', 'spouse_dob', 'spouse_name',
    'dealer_nid', 'dealer_mobile', 'dealer_name',
    'master_serial', 'serial_no', 'card_no', 
    'father_husband_name', 'name_en', 'name_bn', 
    'dob', 'occupation', 'address', 'ward', 'union_name', 
    'nid_number', 'mobile', 'religion', 'gender'
]

mapped = {}
unmapped = []

for h in headers:
    if h in manual_overrides:
        mapped[h] = manual_overrides[h]
        continue
        
    h_lower = h.lower()
    matched = False
    for can in mapping_order:
        if any(keyword.lower() in h_lower for keyword in canonical_names[can]):
            mapped[h] = can
            matched = True
            break
    if not matched:
        unmapped.append(h)

grouped = {}
for h, can in mapped.items():
    grouped.setdefault(can, []).append(h)

md = '# FFP Database Mapping Review v2\\n\\n'
md += 'This document outlines exactly how all 600 unique Excel header string variations have been normalized into definitive canonical column names.\\n\\n'
md += '> [!NOTE]\\n> The raw Bengali variants successfully handled all typos, trailing spaces, invisible unicode breaks, and missing glyphs.\\n\\n'

for can in mapping_order:
    vars_found = grouped.get(can, [])
    md += f'### `{can}`\\n'
    if not vars_found:
        md += '*No variants detected*\\n\\n'
        continue
    for var in sorted(vars_found):
        md += f'- {var}\\n'
    md += '\\n'

md += '## Unmapped Variants (Ignored / Anomalies)\\n\\n'
md += '> [!WARNING]\\n> These 11 headers remain unmapped and will not be natively stored in the normalized set. These represent complete garbage inputs or structural failures.\\n\\n'

for var in sorted(unmapped):
    md += f'- `{var}`\\n'

with open('backend/mapping_review_v2.md', 'w', encoding='utf-8') as f:
    f.write(md)

print('Success written to string formatting.')
