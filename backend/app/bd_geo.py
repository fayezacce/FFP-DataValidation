"""
Bangladesh Administrative Geo Data Module
Author: Fayez Ahmed
Data source: github.com/nuhil/bangladesh-geocode

Provides Division → District → Upazila hierarchy and fuzzy filename matching.
"""

import os
import re
from difflib import get_close_matches
from sqlalchemy.orm import Session

# Division ID → Division Name
DIVISIONS = {
    "1": "Chattagram",
    "2": "Rajshahi",
    "3": "Khulna",
    "4": "Barisal",
    "5": "Sylhet",
    "6": "Dhaka",
    "7": "Rangpur",
    "8": "Mymensingh",
}

# District records: id, division_id, name, aliases
DISTRICTS = [
    {"id": "1",  "division_id": "1", "name": "Comilla",          "aliases": ["cumilla", "kumilla"]},
    {"id": "2",  "division_id": "1", "name": "Feni",             "aliases": []},
    {"id": "3",  "division_id": "1", "name": "Brahmanbaria",     "aliases": ["brahmanbaria", "b.baria"]},
    {"id": "4",  "division_id": "1", "name": "Rangamati",        "aliases": []},
    {"id": "5",  "division_id": "1", "name": "Noakhali",         "aliases": []},
    {"id": "6",  "division_id": "1", "name": "Chandpur",         "aliases": []},
    {"id": "7",  "division_id": "1", "name": "Lakshmipur",       "aliases": []},
    {"id": "8",  "division_id": "1", "name": "Chattogram",       "aliases": ["chittagong", "ctg"]},
    {"id": "9",  "division_id": "1", "name": "Coxsbazar",        "aliases": ["cox's bazar", "coxs bazar"]},
    {"id": "10", "division_id": "1", "name": "Khagrachhari",     "aliases": ["khagrachari"]},
    {"id": "11", "division_id": "1", "name": "Bandarban",        "aliases": []},
    {"id": "12", "division_id": "2", "name": "Sirajganj",        "aliases": []},
    {"id": "13", "division_id": "2", "name": "Pabna",            "aliases": []},
    {"id": "14", "division_id": "2", "name": "Bogura",           "aliases": ["bogra"]},
    {"id": "15", "division_id": "2", "name": "Rajshahi",         "aliases": []},
    {"id": "16", "division_id": "2", "name": "Natore",           "aliases": []},
    {"id": "17", "division_id": "2", "name": "Joypurhat",        "aliases": []},
    {"id": "18", "division_id": "2", "name": "Chapainawabganj",  "aliases": ["chapai nawabganj", "chapai"]},
    {"id": "19", "division_id": "2", "name": "Naogaon",          "aliases": []},
    {"id": "20", "division_id": "3", "name": "Jashore",          "aliases": ["jessore"]},
    {"id": "21", "division_id": "3", "name": "Satkhira",         "aliases": []},
    {"id": "22", "division_id": "3", "name": "Meherpur",         "aliases": []},
    {"id": "23", "division_id": "3", "name": "Narail",           "aliases": []},
    {"id": "24", "division_id": "3", "name": "Chuadanga",        "aliases": []},
    {"id": "25", "division_id": "3", "name": "Kushtia",          "aliases": []},
    {"id": "26", "division_id": "3", "name": "Magura",           "aliases": []},
    {"id": "27", "division_id": "3", "name": "Khulna",           "aliases": []},
    {"id": "28", "division_id": "3", "name": "Bagerhat",         "aliases": []},
    {"id": "29", "division_id": "3", "name": "Jhenaidah",        "aliases": ["jhenaidaha"]},
    {"id": "30", "division_id": "4", "name": "Jhalakathi",       "aliases": ["jhalokati", "jhalokathi"]},
    {"id": "31", "division_id": "4", "name": "Patuakhali",       "aliases": []},
    {"id": "32", "division_id": "4", "name": "Pirojpur",         "aliases": []},
    {"id": "33", "division_id": "4", "name": "Barisal",          "aliases": ["barishal"]},
    {"id": "34", "division_id": "4", "name": "Bhola",            "aliases": []},
    {"id": "35", "division_id": "4", "name": "Barguna",          "aliases": []},
    {"id": "36", "division_id": "5", "name": "Sylhet",           "aliases": []},
    {"id": "37", "division_id": "5", "name": "Moulvibazar",      "aliases": ["maulvibazar"]},
    {"id": "38", "division_id": "5", "name": "Habiganj",         "aliases": []},
    {"id": "39", "division_id": "5", "name": "Sunamganj",        "aliases": []},
    {"id": "40", "division_id": "6", "name": "Narsingdi",        "aliases": []},
    {"id": "41", "division_id": "6", "name": "Gazipur",          "aliases": []},
    {"id": "42", "division_id": "6", "name": "Shariatpur",       "aliases": ["shariatpur"]},
    {"id": "43", "division_id": "6", "name": "Narayanganj",      "aliases": []},
    {"id": "44", "division_id": "6", "name": "Tangail",          "aliases": []},
    {"id": "45", "division_id": "6", "name": "Kishoreganj",      "aliases": []},
    {"id": "46", "division_id": "6", "name": "Manikganj",        "aliases": []},
    {"id": "47", "division_id": "6", "name": "Dhaka",            "aliases": []},
    {"id": "48", "division_id": "6", "name": "Munshiganj",       "aliases": []},
    {"id": "49", "division_id": "6", "name": "Rajbari",          "aliases": []},
    {"id": "50", "division_id": "6", "name": "Madaripur",        "aliases": []},
    {"id": "51", "division_id": "6", "name": "Gopalganj",        "aliases": []},
    {"id": "52", "division_id": "6", "name": "Faridpur",         "aliases": []},
    {"id": "53", "division_id": "7", "name": "Panchagarh",       "aliases": []},
    {"id": "54", "division_id": "7", "name": "Dinajpur",         "aliases": []},
    {"id": "55", "division_id": "7", "name": "Lalmonirhat",      "aliases": []},
    {"id": "56", "division_id": "7", "name": "Nilphamari",       "aliases": []},
    {"id": "57", "division_id": "7", "name": "Gaibandha",        "aliases": []},
    {"id": "58", "division_id": "7", "name": "Thakurgaon",       "aliases": []},
    {"id": "59", "division_id": "7", "name": "Rangpur",          "aliases": []},
    {"id": "60", "division_id": "7", "name": "Kurigram",         "aliases": []},
    {"id": "61", "division_id": "8", "name": "Sherpur",          "aliases": []},
    {"id": "62", "division_id": "8", "name": "Mymensingh",       "aliases": []},
    {"id": "63", "division_id": "8", "name": "Jamalpur",         "aliases": []},
    {"id": "64", "division_id": "8", "name": "Netrokona",        "aliases": ["netrakona"]},
]

# Upazila records: district_id, name
# Comprehensive list from nuhil/bangladesh-geocode
UPAZILAS = [
    # Comilla (1)
    {"district_id": "1", "name": "Debidwar"}, {"district_id": "1", "name": "Barura"},
    {"district_id": "1", "name": "Brahmanpara"}, {"district_id": "1", "name": "Chandina"},
    {"district_id": "1", "name": "Chauddagram"}, {"district_id": "1", "name": "Daudkandi"},
    {"district_id": "1", "name": "Homna"}, {"district_id": "1", "name": "Laksam"},
    {"district_id": "1", "name": "Muradnagar"}, {"district_id": "1", "name": "Nangalkot"},
    {"district_id": "1", "name": "Comilla Sadar"}, {"district_id": "1", "name": "Meghna"},
    {"district_id": "1", "name": "Monohargonj"}, {"district_id": "1", "name": "Sadar Dakshin"},
    {"district_id": "1", "name": "Titas"}, {"district_id": "1", "name": "Burichang"},
    {"district_id": "1", "name": "Lalmai"},
    # Feni (2)
    {"district_id": "2", "name": "Chhagalnaiya"}, {"district_id": "2", "name": "Feni Sadar"},
    {"district_id": "2", "name": "Sonagazi"}, {"district_id": "2", "name": "Fulgazi"},
    {"district_id": "2", "name": "Parshuram"}, {"district_id": "2", "name": "Daganbhuiyan"},
    # Brahmanbaria (3)
    {"district_id": "3", "name": "Brahmanbaria Sadar"}, {"district_id": "3", "name": "Kasba"},
    {"district_id": "3", "name": "Nasirnagar"}, {"district_id": "3", "name": "Sarail"},
    {"district_id": "3", "name": "Ashuganj"}, {"district_id": "3", "name": "Akhaura"},
    {"district_id": "3", "name": "Nabinagar"}, {"district_id": "3", "name": "Bancharampur"},
    {"district_id": "3", "name": "Bijoynagar"},
    # Rangamati (4)
    {"district_id": "4", "name": "Rangamati Sadar"}, {"district_id": "4", "name": "Kaptai"},
    {"district_id": "4", "name": "Kawkhali"}, {"district_id": "4", "name": "Baghaichhari"},
    {"district_id": "4", "name": "Barkal"}, {"district_id": "4", "name": "Langadu"},
    {"district_id": "4", "name": "Rajasthali"}, {"district_id": "4", "name": "Belaichhari"},
    {"district_id": "4", "name": "Juraichhari"}, {"district_id": "4", "name": "Naniarchar"},
    # Noakhali (5)
    {"district_id": "5", "name": "Noakhali Sadar"}, {"district_id": "5", "name": "Companiganj"},
    {"district_id": "5", "name": "Begumganj"}, {"district_id": "5", "name": "Hatiya"},
    {"district_id": "5", "name": "Subarnachar"}, {"district_id": "5", "name": "Kabirhat"},
    {"district_id": "5", "name": "Senbagh"}, {"district_id": "5", "name": "Chatkhil"},
    {"district_id": "5", "name": "Sonaimuri"},
    # Chandpur (6)
    {"district_id": "6", "name": "Haimchar"}, {"district_id": "6", "name": "Kachua"},
    {"district_id": "6", "name": "Shahrasti"}, {"district_id": "6", "name": "Chandpur Sadar"},
    {"district_id": "6", "name": "Matlab Dakshin"}, {"district_id": "6", "name": "Matlab Uttar"},
    {"district_id": "6", "name": "Hajiganj"}, {"district_id": "6", "name": "Faridganj"},
    # Lakshmipur (7)
    {"district_id": "7", "name": "Lakshmipur Sadar"}, {"district_id": "7", "name": "Kamalnagar"},
    {"district_id": "7", "name": "Raipur"}, {"district_id": "7", "name": "Ramgati"},
    {"district_id": "7", "name": "Ramganj"},
    # Chattogram (8)
    {"district_id": "8", "name": "Rangunia"}, {"district_id": "8", "name": "Sitakunda"},
    {"district_id": "8", "name": "Mirsharai"}, {"district_id": "8", "name": "Patiya"},
    {"district_id": "8", "name": "Sandwip"}, {"district_id": "8", "name": "Banshkhali"},
    {"district_id": "8", "name": "Boalkhali"}, {"district_id": "8", "name": "Anwara"},
    {"district_id": "8", "name": "Chandanaish"}, {"district_id": "8", "name": "Satkania"},
    {"district_id": "8", "name": "Lohagara"}, {"district_id": "8", "name": "Hathazari"},
    {"district_id": "8", "name": "Fatikchhari"}, {"district_id": "8", "name": "Raozan"},
    {"district_id": "8", "name": "Karnaphuli"},
    # Coxsbazar (9)
    {"district_id": "9", "name": "Coxsbazar Sadar"}, {"district_id": "9", "name": "Chakaria"},
    {"district_id": "9", "name": "Kutubdia"}, {"district_id": "9", "name": "Ukhiya"},
    {"district_id": "9", "name": "Maheshkhali"}, {"district_id": "9", "name": "Pekua"},
    {"district_id": "9", "name": "Ramu"}, {"district_id": "9", "name": "Teknaf"},
    # Khagrachhari (10)
    {"district_id": "10", "name": "Khagrachhari Sadar"}, {"district_id": "10", "name": "Dighinala"},
    {"district_id": "10", "name": "Panchari"}, {"district_id": "10", "name": "Laxmichhari"},
    {"district_id": "10", "name": "Mahalchhari"}, {"district_id": "10", "name": "Manikchari"},
    {"district_id": "10", "name": "Ramgarh"}, {"district_id": "10", "name": "Matiranga"},
    {"district_id": "10", "name": "Guimara"},
    # Bandarban (11)
    {"district_id": "11", "name": "Bandarban Sadar"}, {"district_id": "11", "name": "Alikadam"},
    {"district_id": "11", "name": "Naikhongchhari"}, {"district_id": "11", "name": "Rowangchhari"},
    {"district_id": "11", "name": "Lama"}, {"district_id": "11", "name": "Ruma"},
    {"district_id": "11", "name": "Thanchi"},
    # Sirajganj (12)
    {"district_id": "12", "name": "Belkuchi"}, {"district_id": "12", "name": "Chauhali"},
    {"district_id": "12", "name": "Kamarkhanda"}, {"district_id": "12", "name": "Kazipur"},
    {"district_id": "12", "name": "Raiganj"}, {"district_id": "12", "name": "Shahjadpur"},
    {"district_id": "12", "name": "Sirajganj Sadar"}, {"district_id": "12", "name": "Tarash"},
    {"district_id": "12", "name": "Ullahpara"},
    # Pabna (13)
    {"district_id": "13", "name": "Sujanagar"}, {"district_id": "13", "name": "Ishwardi"},
    {"district_id": "13", "name": "Bhangura"}, {"district_id": "13", "name": "Pabna Sadar"},
    {"district_id": "13", "name": "Bera"}, {"district_id": "13", "name": "Atgharia"},
    {"district_id": "13", "name": "Chatmohar"}, {"district_id": "13", "name": "Santhia"},
    {"district_id": "13", "name": "Faridpur"},
    # Bogura (14)
    {"district_id": "14", "name": "Kahaloo"}, {"district_id": "14", "name": "Bogura Sadar"},
    {"district_id": "14", "name": "Shariakandi"}, {"district_id": "14", "name": "Shajahanpur"},
    {"district_id": "14", "name": "Dupchanchia"}, {"district_id": "14", "name": "Adamdighi"},
    {"district_id": "14", "name": "Nandigram"}, {"district_id": "14", "name": "Sonatola"},
    {"district_id": "14", "name": "Dhunot"}, {"district_id": "14", "name": "Gabtali"},
    {"district_id": "14", "name": "Sherpur"}, {"district_id": "14", "name": "Shibganj"},
    # Rajshahi (15)
    {"district_id": "15", "name": "Paba"}, {"district_id": "15", "name": "Durgapur"},
    {"district_id": "15", "name": "Mohonpur"}, {"district_id": "15", "name": "Charghat"},
    {"district_id": "15", "name": "Puthia"}, {"district_id": "15", "name": "Bagha"},
    {"district_id": "15", "name": "Godagari"}, {"district_id": "15", "name": "Tanore"},
    {"district_id": "15", "name": "Bagmara"},
    # Natore (16)
    {"district_id": "16", "name": "Natore Sadar"}, {"district_id": "16", "name": "Singra"},
    {"district_id": "16", "name": "Baraigram"}, {"district_id": "16", "name": "Bagatipara"},
    {"district_id": "16", "name": "Lalpur"}, {"district_id": "16", "name": "Gurudaspur"},
    {"district_id": "16", "name": "Naldanga"},
    # Joypurhat (17)
    {"district_id": "17", "name": "Akkelpur"}, {"district_id": "17", "name": "Kalai"},
    {"district_id": "17", "name": "Khetlal"}, {"district_id": "17", "name": "Panchbibi"},
    {"district_id": "17", "name": "Joypurhat Sadar"},
    # Chapainawabganj (18)
    {"district_id": "18", "name": "Chapainawabganj Sadar"}, {"district_id": "18", "name": "Gomostapur"},
    {"district_id": "18", "name": "Nachole"}, {"district_id": "18", "name": "Bholahat"},
    {"district_id": "18", "name": "Shibganj"},
    # Naogaon (19)
    {"district_id": "19", "name": "Mohadevpur"}, {"district_id": "19", "name": "Badalgachhi"},
    {"district_id": "19", "name": "Patnitala"}, {"district_id": "19", "name": "Dhamoirhat"},
    {"district_id": "19", "name": "Niamatpur"}, {"district_id": "19", "name": "Manda"},
    {"district_id": "19", "name": "Atrai"}, {"district_id": "19", "name": "Raninagar"},
    {"district_id": "19", "name": "Naogaon Sadar"}, {"district_id": "19", "name": "Porsha"},
    {"district_id": "19", "name": "Sapahar"},
    # Jashore (20)
    {"district_id": "20", "name": "Manirampur"}, {"district_id": "20", "name": "Abhaynagar"},
    {"district_id": "20", "name": "Bagherpara"}, {"district_id": "20", "name": "Chaugachha"},
    {"district_id": "20", "name": "Jhikargachha"}, {"district_id": "20", "name": "Keshabpur"},
    {"district_id": "20", "name": "Jashore Sadar"}, {"district_id": "20", "name": "Sharsha"},
    # Satkhira (21)
    {"district_id": "21", "name": "Assasuni"}, {"district_id": "21", "name": "Debhata"},
    {"district_id": "21", "name": "Kalaroa"}, {"district_id": "21", "name": "Satkhira Sadar"},
    {"district_id": "21", "name": "Shyamnagar"}, {"district_id": "21", "name": "Tala"},
    {"district_id": "21", "name": "Kaliganj"},
    # Meherpur (22)
    {"district_id": "22", "name": "Mujibnagar"}, {"district_id": "22", "name": "Meherpur Sadar"},
    {"district_id": "22", "name": "Gangni"},
    # Narail (23)
    {"district_id": "23", "name": "Narail Sadar"}, {"district_id": "23", "name": "Lohagara"},
    {"district_id": "23", "name": "Kalia"},
    # Chuadanga (24)
    {"district_id": "24", "name": "Chuadanga Sadar"}, {"district_id": "24", "name": "Alamdanga"},
    {"district_id": "24", "name": "Damurhuda"}, {"district_id": "24", "name": "Jibannagar"},
    # Kushtia (25)
    {"district_id": "25", "name": "Kushtia Sadar"}, {"district_id": "25", "name": "Kumarkhali"},
    {"district_id": "25", "name": "Khoksa"}, {"district_id": "25", "name": "Mirpur"},
    {"district_id": "25", "name": "Daulatpur"}, {"district_id": "25", "name": "Bheramara"},
    # Magura (26)
    {"district_id": "26", "name": "Shalikha"}, {"district_id": "26", "name": "Sreepur"},
    {"district_id": "26", "name": "Magura Sadar"}, {"district_id": "26", "name": "Mohammadpur"},
    # Khulna (27)
    {"district_id": "27", "name": "Batiaghata"}, {"district_id": "27", "name": "Dacope"},
    {"district_id": "27", "name": "Dumuria"}, {"district_id": "27", "name": "Dighalia"},
    {"district_id": "27", "name": "Koyra"}, {"district_id": "27", "name": "Paikgachha"},
    {"district_id": "27", "name": "Phultala"}, {"district_id": "27", "name": "Rupsha"},
    {"district_id": "27", "name": "Terokhada"}, {"district_id": "27", "name": "Khalishpur"},
    # Bagerhat (28)
    {"district_id": "28", "name": "Fakirhat"}, {"district_id": "28", "name": "Bagerhat Sadar"},
    {"district_id": "28", "name": "Mollahat"}, {"district_id": "28", "name": "Sarankhola"},
    {"district_id": "28", "name": "Rampal"}, {"district_id": "28", "name": "Morrelganj"},
    {"district_id": "28", "name": "Kachua"}, {"district_id": "28", "name": "Mongla"},
    {"district_id": "28", "name": "Chitalmari"},
    # Jhenaidah (29)
    {"district_id": "29", "name": "Jhenaidah Sadar"}, {"district_id": "29", "name": "Shailkupa"},
    {"district_id": "29", "name": "Harinakundu"}, {"district_id": "29", "name": "Kaliganj"},
    {"district_id": "29", "name": "Kotchandpur"}, {"district_id": "29", "name": "Moheshpur"},
    # Jhalakathi (30)
    {"district_id": "30", "name": "Jhalakathi Sadar"}, {"district_id": "30", "name": "Kathalia"},
    {"district_id": "30", "name": "Nalchity"}, {"district_id": "30", "name": "Rajapur"},
    # Patuakhali (31)
    {"district_id": "31", "name": "Bauphal"}, {"district_id": "31", "name": "Patuakhali Sadar"},
    {"district_id": "31", "name": "Dumki"}, {"district_id": "31", "name": "Dashmina"},
    {"district_id": "31", "name": "Kalapara"}, {"district_id": "31", "name": "Mirzaganj"},
    {"district_id": "31", "name": "Galachipa"}, {"district_id": "31", "name": "Rangabali"},
    # Pirojpur (32)
    {"district_id": "32", "name": "Pirojpur Sadar"}, {"district_id": "32", "name": "Nazirpur"},
    {"district_id": "32", "name": "Kawkhali"}, {"district_id": "32", "name": "Zianagar"},
    {"district_id": "32", "name": "Bhandaria"}, {"district_id": "32", "name": "Mathbaria"},
    {"district_id": "32", "name": "Nesarabad"},
    # Barisal (33)
    {"district_id": "33", "name": "Barisal Sadar"}, {"district_id": "33", "name": "Bakerganj"},
    {"district_id": "33", "name": "Babuganj"}, {"district_id": "33", "name": "Wazirpur"},
    {"district_id": "33", "name": "Banaripara"}, {"district_id": "33", "name": "Gournadi"},
    {"district_id": "33", "name": "Agailjhara"}, {"district_id": "33", "name": "Mehendiganj"},
    {"district_id": "33", "name": "Muladi"}, {"district_id": "33", "name": "Hizla"},
    # Bhola (34)
    {"district_id": "34", "name": "Bhola Sadar"}, {"district_id": "34", "name": "Borhanuddin"},
    {"district_id": "34", "name": "Charfasson"}, {"district_id": "34", "name": "Daulatkhan"},
    {"district_id": "34", "name": "Lalmohan"}, {"district_id": "34", "name": "Manpura"},
    {"district_id": "34", "name": "Tazumuddin"},
    # Barguna (35)
    {"district_id": "35", "name": "Amtali"}, {"district_id": "35", "name": "Barguna Sadar"},
    {"district_id": "35", "name": "Betagi"}, {"district_id": "35", "name": "Bamna"},
    {"district_id": "35", "name": "Pathorghata"}, {"district_id": "35", "name": "Taltali"},
    # Sylhet (36)
    {"district_id": "36", "name": "Balaganj"}, {"district_id": "36", "name": "Beanibazar"},
    {"district_id": "36", "name": "Bishwanath"}, {"district_id": "36", "name": "Companiganj"},
    {"district_id": "36", "name": "Fenchuganj"}, {"district_id": "36", "name": "Golapganj"},
    {"district_id": "36", "name": "Gowainghat"}, {"district_id": "36", "name": "Jaintiapur"},
    {"district_id": "36", "name": "Kanaighat"}, {"district_id": "36", "name": "Sylhet Sadar"},
    {"district_id": "36", "name": "Zakiganj"}, {"district_id": "36", "name": "Dakshin Surma"},
    {"district_id": "36", "name": "Osmaninagar"},
    # Moulvibazar (37)
    {"district_id": "37", "name": "Barlekha"}, {"district_id": "37", "name": "Kamalganj"},
    {"district_id": "37", "name": "Kulaura"}, {"district_id": "37", "name": "Moulvibazar Sadar"},
    {"district_id": "37", "name": "Rajnagar"}, {"district_id": "37", "name": "Sreemangal"},
    {"district_id": "37", "name": "Juri"},
    # Habiganj (38)
    {"district_id": "38", "name": "Nabiganj"}, {"district_id": "38", "name": "Bahubal"},
    {"district_id": "38", "name": "Ajmiriganj"}, {"district_id": "38", "name": "Baniachong"},
    {"district_id": "38", "name": "Lakhai"}, {"district_id": "38", "name": "Chunarughat"},
    {"district_id": "38", "name": "Habiganj Sadar"}, {"district_id": "38", "name": "Madhabpur"},
    # Sunamganj (39)
    {"district_id": "39", "name": "Sunamganj Sadar"}, {"district_id": "39", "name": "South Sunamganj"},
    {"district_id": "39", "name": "Bishwamvarpur"}, {"district_id": "39", "name": "Chhatak"},
    {"district_id": "39", "name": "Jagannathpur"}, {"district_id": "39", "name": "Dowarabazar"},
    {"district_id": "39", "name": "Tahirpur"}, {"district_id": "39", "name": "Dharmapasha"},
    {"district_id": "39", "name": "Jamalganj"}, {"district_id": "39", "name": "Shalla"},
    {"district_id": "39", "name": "Derai"},
    # Narsingdi (40)
    {"district_id": "40", "name": "Belabo"}, {"district_id": "40", "name": "Monohardi"},
    {"district_id": "40", "name": "Narsingdi Sadar"}, {"district_id": "40", "name": "Palash"},
    {"district_id": "40", "name": "Raipura"}, {"district_id": "40", "name": "Shibpur"},
    # Gazipur (41)
    {"district_id": "41", "name": "Kaliganj"}, {"district_id": "41", "name": "Kaliakair"},
    {"district_id": "41", "name": "Kapasia"}, {"district_id": "41", "name": "Gazipur Sadar"},
    {"district_id": "41", "name": "Sreepur"},
    # Shariatpur (42)
    {"district_id": "42", "name": "Shariatpur Sadar"}, {"district_id": "42", "name": "Naria"},
    {"district_id": "42", "name": "Zajira"}, {"district_id": "42", "name": "Gosairhat"},
    {"district_id": "42", "name": "Bhedarganj"}, {"district_id": "42", "name": "Damudya"},
    # Narayanganj (43)
    {"district_id": "43", "name": "Araihazar"}, {"district_id": "43", "name": "Bandar"},
    {"district_id": "43", "name": "Narayanganj Sadar"}, {"district_id": "43", "name": "Rupganj"},
    {"district_id": "43", "name": "Sonargaon"},
    # Tangail (44)
    {"district_id": "44", "name": "Basail"}, {"district_id": "44", "name": "Bhuapur"},
    {"district_id": "44", "name": "Delduar"}, {"district_id": "44", "name": "Ghatail"},
    {"district_id": "44", "name": "Gopalpur"}, {"district_id": "44", "name": "Madhupur"},
    {"district_id": "44", "name": "Mirzapur"}, {"district_id": "44", "name": "Nagarpur"},
    {"district_id": "44", "name": "Sakhipur"}, {"district_id": "44", "name": "Tangail Sadar"},
    {"district_id": "44", "name": "Kalihati"}, {"district_id": "44", "name": "Dhanbari"},
    # Kishoreganj (45)
    {"district_id": "45", "name": "Itna"}, {"district_id": "45", "name": "Katiadi"},
    {"district_id": "45", "name": "Bhairab"}, {"district_id": "45", "name": "Tarail"},
    {"district_id": "45", "name": "Hossainpur"}, {"district_id": "45", "name": "Pakundia"},
    {"district_id": "45", "name": "Kuliarchar"}, {"district_id": "45", "name": "Kishoreganj Sadar"},
    {"district_id": "45", "name": "Karimganj"}, {"district_id": "45", "name": "Bajitpur"},
    {"district_id": "45", "name": "Austagram"}, {"district_id": "45", "name": "Mithamain"},
    {"district_id": "45", "name": "Nikli"},
    # Manikganj (46)
    {"district_id": "46", "name": "Harirampur"}, {"district_id": "46", "name": "Saturia"},
    {"district_id": "46", "name": "Manikganj Sadar"}, {"district_id": "46", "name": "Ghior"},
    {"district_id": "46", "name": "Shibalaya"}, {"district_id": "46", "name": "Daulatpur"},
    {"district_id": "46", "name": "Singair"},
    # Dhaka (47)
    {"district_id": "47", "name": "Savar"}, {"district_id": "47", "name": "Dhamrai"},
    {"district_id": "47", "name": "Keraniganj"}, {"district_id": "47", "name": "Nawabganj"},
    {"district_id": "47", "name": "Dohar"},
    # Munshiganj (48)
    {"district_id": "48", "name": "Munshiganj Sadar"}, {"district_id": "48", "name": "Sreenagar"},
    {"district_id": "48", "name": "Sirajdikhan"}, {"district_id": "48", "name": "Louhajang"},
    {"district_id": "48", "name": "Gazaria"}, {"district_id": "48", "name": "Tongibari"},
    # Rajbari (49)
    {"district_id": "49", "name": "Rajbari Sadar"}, {"district_id": "49", "name": "Goalanda"},
    {"district_id": "49", "name": "Pangsha"}, {"district_id": "49", "name": "Baliakandi"},
    {"district_id": "49", "name": "Kalukhali"},
    # Madaripur (50)
    {"district_id": "50", "name": "Madaripur Sadar"}, {"district_id": "50", "name": "Kalkini"},
    {"district_id": "50", "name": "Rajoir"}, {"district_id": "50", "name": "Shibchar"},
    # Gopalganj (51)
    {"district_id": "51", "name": "Gopalganj Sadar"}, {"district_id": "51", "name": "Kashiani"},
    {"district_id": "51", "name": "Tungipara"}, {"district_id": "51", "name": "Kotalipara"},
    {"district_id": "51", "name": "Muksudpur"},
    # Faridpur (52)
    {"district_id": "52", "name": "Faridpur Sadar"}, {"district_id": "52", "name": "Alfadanga"},
    {"district_id": "52", "name": "Boalmari"}, {"district_id": "52", "name": "Sadarpur"},
    {"district_id": "52", "name": "Nagarkanda"}, {"district_id": "52", "name": "Bhanga"},
    {"district_id": "52", "name": "Charbhadrasan"}, {"district_id": "52", "name": "Madhukhali"},
    {"district_id": "52", "name": "Saltha"},
    # Panchagarh (53)
    {"district_id": "53", "name": "Panchagarh Sadar"}, {"district_id": "53", "name": "Debiganj"},
    {"district_id": "53", "name": "Boda"}, {"district_id": "53", "name": "Atwari"},
    {"district_id": "53", "name": "Tetulia"},
    # Dinajpur (54)
    {"district_id": "54", "name": "Nawabganj"}, {"district_id": "54", "name": "Birganj"},
    {"district_id": "54", "name": "Ghoraghat"}, {"district_id": "54", "name": "Birampur"},
    {"district_id": "54", "name": "Parbatipur"}, {"district_id": "54", "name": "Bochaganj"},
    {"district_id": "54", "name": "Kaharole"}, {"district_id": "54", "name": "Fulbari"},
    {"district_id": "54", "name": "Dinajpur Sadar"}, {"district_id": "54", "name": "Hakimpur"},
    {"district_id": "54", "name": "Khansama"}, {"district_id": "54", "name": "Birol"},
    {"district_id": "54", "name": "Chirirbandar"},
    # Lalmonirhat (55)
    {"district_id": "55", "name": "Lalmonirhat Sadar"}, {"district_id": "55", "name": "Kaliganj"},
    {"district_id": "55", "name": "Hatibandha"}, {"district_id": "55", "name": "Patgram"},
    {"district_id": "55", "name": "Aditmari"},
    # Nilphamari (56)
    {"district_id": "56", "name": "Syedpur"}, {"district_id": "56", "name": "Domar"},
    {"district_id": "56", "name": "Dimla"}, {"district_id": "56", "name": "Jaldhaka"},
    {"district_id": "56", "name": "Kishoreganj"}, {"district_id": "56", "name": "Nilphamari Sadar"},
    # Gaibandha (57)
    {"district_id": "57", "name": "Sadullapur"}, {"district_id": "57", "name": "Gaibandha Sadar"},
    {"district_id": "57", "name": "Palashbari"}, {"district_id": "57", "name": "Saghata"},
    {"district_id": "57", "name": "Gobindaganj"}, {"district_id": "57", "name": "Sundarganj"},
    {"district_id": "57", "name": "Phulchhari"},
    # Thakurgaon (58)
    {"district_id": "58", "name": "Thakurgaon Sadar"}, {"district_id": "58", "name": "Pirganj"},
    {"district_id": "58", "name": "Ranisankail"}, {"district_id": "58", "name": "Haripur"},
    {"district_id": "58", "name": "Baliadangi"},
    # Rangpur (59)
    {"district_id": "59", "name": "Rangpur Sadar"}, {"district_id": "59", "name": "Gangachara"},
    {"district_id": "59", "name": "Taragonj"}, {"district_id": "59", "name": "Badarganj"},
    {"district_id": "59", "name": "Mithapukur"}, {"district_id": "59", "name": "Pirganj"},
    {"district_id": "59", "name": "Kaunia"}, {"district_id": "59", "name": "Pirgachha"},
    # Kurigram (60)
    {"district_id": "60", "name": "Kurigram Sadar"}, {"district_id": "60", "name": "Nageshwari"},
    {"district_id": "60", "name": "Bhurungamari"}, {"district_id": "60", "name": "Phulbari"},
    {"district_id": "60", "name": "Rajarhat"}, {"district_id": "60", "name": "Ulipur"},
    {"district_id": "60", "name": "Chilmari"}, {"district_id": "60", "name": "Rowmari"},
    {"district_id": "60", "name": "Char Rajibpur"},
    # Sherpur (61)
    {"district_id": "61", "name": "Sherpur Sadar"}, {"district_id": "61", "name": "Nalitabari"},
    {"district_id": "61", "name": "Sreebordi"}, {"district_id": "61", "name": "Nokla"},
    {"district_id": "61", "name": "Jhenaigati"},
    # Mymensingh (62)
    {"district_id": "62", "name": "Fulbaria"}, {"district_id": "62", "name": "Trishal"},
    {"district_id": "62", "name": "Bhaluka"}, {"district_id": "62", "name": "Muktagachha"},
    {"district_id": "62", "name": "Mymensingh Sadar"}, {"district_id": "62", "name": "Dhobaura"},
    {"district_id": "62", "name": "Phulpur"}, {"district_id": "62", "name": "Haluaghat"},
    {"district_id": "62", "name": "Gaffargaon"}, {"district_id": "62", "name": "Gauripur"},
    {"district_id": "62", "name": "Nandail"}, {"district_id": "62", "name": "Ishwarganj"},
    {"district_id": "62", "name": "Tarakanda"},
    # Jamalpur (63)
    {"district_id": "63", "name": "Jamalpur Sadar"}, {"district_id": "63", "name": "Melandah"},
    {"district_id": "63", "name": "Islampur"}, {"district_id": "63", "name": "Dewanganj"},
    {"district_id": "63", "name": "Sarishabari"}, {"district_id": "63", "name": "Madarganj"},
    {"district_id": "63", "name": "Bakshiganj"},
    # Netrokona (64)
    {"district_id": "64", "name": "Barhatta"}, {"district_id": "64", "name": "Durgapur"},
    {"district_id": "64", "name": "Kendua"}, {"district_id": "64", "name": "Atpara"},
    {"district_id": "64", "name": "Madan"}, {"district_id": "64", "name": "Khaliajuri"},
    {"district_id": "64", "name": "Kalmakanda"}, {"district_id": "64", "name": "Mohanganj"},
    {"district_id": "64", "name": "Purbadhala"}, {"district_id": "64", "name": "Netrokona Sadar"},
]


# ─── Build lookup indexes ─────────────────────────────────────────────────────

def _normalize(s: str) -> str:
    """Lowercase, strip, remove hyphens/extra spaces."""
    return re.sub(r'[\s\-]+', ' ', s.lower().strip())

# District name → district record (includes aliases)
_district_lookup: dict[str, dict] = {}
_district_names: list[str] = []

for d in DISTRICTS:
    norm = _normalize(d["name"])
    _district_lookup[norm] = d
    _district_names.append(norm)
    for alias in d.get("aliases", []):
        norm_alias = _normalize(alias)
        _district_lookup[norm_alias] = d
        _district_names.append(norm_alias)

# Upazila name → list of upazila records (may span multiple districts)
_upazila_lookup: dict[str, list[dict]] = {}
_upazila_names: list[str] = []

for u in UPAZILAS:
    norm = _normalize(u["name"])
    if norm not in _upazila_lookup:
        _upazila_lookup[norm] = []
        _upazila_names.append(norm)
    _upazila_lookup[norm].append(u)


# ─── Public API ────────────────────────────────────────────────────────────────

def fuzzy_match_location(filename: str) -> dict:
    """
    Parse a filename like 'Cumilla_Brammanpara - DC Food Comilla (1).xlsx'
    and fuzzy-match to Division/District/Upazila.

    Returns: {"division": str, "district": str, "upazila": str}
    All fields may be "Unknown" if no match found.
    """
    result = {"division": "Unknown", "district": "Unknown", "upazila": "Unknown"}

    # Strip extension
    name_no_ext = os.path.splitext(filename)[0]

    # Split on underscore to get [district_part, upazila_part, ...]
    parts = name_no_ext.split("_")
    if len(parts) < 2:
        # Try to match the whole name as district at least
        parts = [name_no_ext, ""]

    # Clean each part: remove extra suffixes like " - DC Food Comilla (1)"
    district_raw = re.split(r'\s*[-–]\s*', parts[0])[0].strip()
    upazila_raw = re.split(r'\s*[-–]\s*', parts[1])[0].strip() if len(parts) > 1 else ""

    # ── Match District ──
    district_norm = _normalize(district_raw)
    matched_district = None

    # Exact match first
    if district_norm in _district_lookup:
        matched_district = _district_lookup[district_norm]
    else:
        # Fuzzy match
        close = get_close_matches(district_norm, _district_names, n=1, cutoff=0.6)
        if close:
            matched_district = _district_lookup[close[0]]

    if matched_district:
        result["district"] = matched_district["name"]
        div_id = matched_district["division_id"]
        result["division"] = DIVISIONS.get(div_id, "Unknown")

    # ── Match Upazila ──
    if upazila_raw:
        upazila_norm = _normalize(upazila_raw)

        # Try exact match
        if upazila_norm in _upazila_lookup:
            candidates = _upazila_lookup[upazila_norm]
            # If we matched a district, prefer upazila from that district
            if matched_district:
                for c in candidates:
                    if c["district_id"] == matched_district["id"]:
                        result["upazila"] = c["name"]
                        break
                else:
                    result["upazila"] = candidates[0]["name"]
            else:
                result["upazila"] = candidates[0]["name"]
        else:
            # Fuzzy match
            close = get_close_matches(upazila_norm, _upazila_names, n=3, cutoff=0.6)
            if close:
                # Prefer match within the same district
                best = None
                for match_name in close:
                    for c in _upazila_lookup[match_name]:
                        if matched_district and c["district_id"] == matched_district["id"]:
                            best = c
                            break
                    if best:
                        break
                if not best:
                    best = _upazila_lookup[close[0]][0]
                result["upazila"] = best["name"]

    return result


def get_division_for_district(district_name: str) -> str:
    """Get division name for a given district name."""
    norm = _normalize(district_name)
    if norm in _district_lookup:
        return DIVISIONS.get(_district_lookup[norm]["division_id"], "Unknown")
    return "Unknown"

def get_dynamic_upazilas(db: Session = None):
    """Get upazilas from DB if possible, else fallback."""
    from .models import Upazila
    if db:
        db_upazilas = db.query(Upazila).filter(Upazila.is_active == True).all()
        if db_upazilas:
             # structure matching the return of get_geo_info
             upazilas = {}
             for u in db_upazilas:
                 dist_name = u.district_name
                 if dist_name not in upazilas:
                     upazilas[dist_name] = []
                 upazilas[dist_name].append(u.name)
             return upazilas
             
    # Fallback to hardcoded
    upazilas = {}
    district_map = {d["id"]: d["name"] for d in DISTRICTS}
    for u in UPAZILAS:
        dist_name = district_map.get(u["district_id"], "Unknown")
        if dist_name not in upazilas:
            upazilas[dist_name] = []
        upazilas[dist_name].append(u["name"])
    return upazilas
