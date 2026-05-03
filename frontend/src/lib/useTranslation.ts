import { useState, useEffect } from 'react';

type Lang = 'en' | 'bn';

const translations: Record<Lang, Record<string, string>> = {
  en: {
    "title": "Data Validator",
    "subtitle": "Upload your excel files to automatically normalize digits, clean dates, and validate NID numbers.",
    "view_stats": "View Statistics Dashboard",
    "drop_here": "Drop the Excel file here...",
    "drag_drop": "Drag & drop your Excel file here",
    "click_browse": "or click to browse from your computer",
    "supported_formats": "Supported formats: .xlsx, .xls",
    
    "total_unique": "Total Unique Records",
    "validated_unique": "Validated (Unique)",
    "invalid_records": "Invalid Records",
    "data_integrity": "Data Integrity",
    
    "national_quota": "National Quota",
    "distribution_target": "Distribution Target",
    "total_valid": "Total Valid",
    "successfully_verified": "Successfully Verified",
    "remaining": "Remaining",
    "pending_quota": "Pending to meet Quota",
    "completion_pct": "Completion %",
    "nationwide_status": "Current nationwide status",
    
    "stats_title": "Statistics Dashboard",
    "stats_subtitle": "Validation results by Division, District & Upazila",
    "search": "Search district or upazila...",
    "invalid_only": "Invalid Only",
    "showing_invalid": "Showing Invalid",
    "search_records": "Search Records",
    "refresh": "Refresh",
    
    "upload_processing": "Processing Upload",
    "new_records": "New Records",
    "download_invalid": "Download Invalid Format",
    "download_valid": "Download Valid Format",
    "export_csv_all": "Standard CSV (Full)",
    "export_csv_new": "Standard CSV (New Data)",
    "export_csv_started": "Standard CSV export started! Check the Task Tray.",
    
    "card_no": "Card No",
    "beneficiary_name_bn": "Name (Bangla)",
    "beneficiary_name_en": "Name (English)",
    "father_name": "Father's Name",
    "dob_nid": "Date of Birth",
    "occupation": "Occupation",
    "village_name": "Village",
    "ward_no": "Ward No",
    "union_name": "Union",
    "nid_number": "NID Number",
    "mobile_no": "Mobile No",
    "spouse_name": "Spouse Name",
    "spouse_nid": "Spouse NID",
    "spouse_dob": "Spouse DOB",
    "dealer_name": "Dealer Name",
    "dealer_nid": "Dealer NID",
    "dealer_mobile": "Dealer Mobile",
    "gender": "Gender",
    "religion": "Religion",
    "beneficiary_info": "Beneficiary Information",
    "spouse_info": "Spouse Information",
    "dealer_info": "Dealer Information"
  },
  bn: {
    "title": "ডেটা ভ্যালিডেটর",
    "subtitle": "নম্বর স্বাভাবিক করতে, তারিখ ঠিক করতে এবং NID যাচাই করতে আপনার এক্সেল ফাইলগুলি আপলোড করুন।",
    "view_stats": "পরিসংখ্যান ড্যাশবোর্ড দেখুন",
    "drop_here": "এক্সেল ফাইলটি এখানে ছেড়ে দিন...",
    "drag_drop": "আপনার এক্সেল ফাইলটি এখানে টেনে আনুন",
    "click_browse": "অথবা আপনার কম্পিউটার থেকে ব্রাউজ করতে ক্লিক করুন",
    "supported_formats": "সমর্থিত ফর্ম্যাট: .xlsx, .xls",
    
    "total_unique": "সর্বমোট স্বতন্ত্র রেকর্ড",
    "validated_unique": "সঠিক রেকর্ড",
    "invalid_records": "ভুল রেকর্ড",
    "data_integrity": "ডেটা সঠিকতার হার",

    "national_quota": "জাতীয় কোটা",
    "distribution_target": "বিতরণ লক্ষ্যমাত্রা",
    "total_valid": "মোট সঠিক",
    "successfully_verified": "সফলভাবে যাচাইকৃত",
    "remaining": "অবশিষ্ট",
    "pending_quota": "ভরাট বাকি",
    "completion_pct": "সম্পূর্ণতার হার (%)",
    "nationwide_status": "বর্তমান দেশব্যাপী অবস্থা",

    "stats_title": "পরিসংখ্যান ড্যাশবোর্ড",
    "stats_subtitle": "বিভাগ, জেলা এবং উপজেলা অনুযায়ী যাচাইকরণের ফলাফল",
    "search": "জেলা বা উপজেলা খুঁজুন...",
    "invalid_only": "শুধুমাত্র ভুল",
    "showing_invalid": "ভুল রেকর্ড দেখাচ্ছে",
    "search_records": "রেকর্ড খুঁজুন",
    "refresh": "রিফ্রেশ করুন",

    "upload_processing": "আপলোড প্রক্রিয়া চলছে",
    "new_records": "নতুন রেকর্ড",
    "download_invalid": "ভুলগুলো ডাউনলোড করুন",
    "download_valid": "সঠিকগুলো ডাউনলোড করুন",
    "export_csv_all": "স্ট্যান্ডার্ড সিএসভি (সব)",
    "export_csv_new": "স্ট্যান্ডার্ড সিএসভি (শুধু নতুন)",
    "export_csv_started": "Standard সিএসভি এক্সপোর্ট শুরু হয়েছে! টাস্ক ট্রে চেক করুন।",

    "card_no": "কার্ড নং",
    "beneficiary_name_bn": "উপকার ভোগীর নাম (বাংলা) (NID সাথে মিল থাকতে হবে)",
    "beneficiary_name_en": "উপকার ভোগীর নাম (ইংরেজি) (NID সাথে মিল থাকতে হবে)",
    "father_name": "পিতার নাম",
    "dob_nid": "জন্ম তারিখ (NID সাথে মিল থাকতে হবে)",
    "occupation": "পেশা",
    "village_name": "গ্রামের নাম",
    "ward_no": "ওয়ার্ড নং",
    "union_name": "ইউনিয়নের নাম",
    "nid_number": "জাতীয় পরিচয় পত্র নম্বর",
    "mobile_no": "মোবাইল নং (নিজ নামে)",
    "spouse_name": "স্বামী/স্ত্রীর নাম (NID সাথে মিল থাকতে হবে)",
    "spouse_nid": "স্বামী/স্ত্রী জাতীয় পরিচয় নম্বর",
    "spouse_dob": "স্বামী/স্ত্রী জন্ম তারিখ (NID সাথে মিল থাকতে হবে)",
    "dealer_name": "রেজিস্টার্ড ডিলারের নাম (NID সাথে মিল থাকতে হবে)",
    "dealer_nid": "রেজিস্টার্ড ডিলারের এনআইডি নম্বর",
    "dealer_mobile": "রেজিস্টার্ড ডিলারের মোবাইল নং",
    "gender": "লিঙ্গ",
    "religion": "ধর্ম",
    "beneficiary_info": "উপকার ভোগীর তথ্য",
    "spouse_info": "স্বামী/স্ত্রীর তথ্য",
    "dealer_info": "ডিলারের তথ্য"
  }
};

let globalLang: Lang = 'en';
const listeners = new Set<(lang: Lang) => void>();

export const useTranslation = () => {
  const [lang, setLang] = useState<Lang>(globalLang);

  useEffect(() => {
    // Client-side init
    if (typeof window !== 'undefined') {
      const stored = localStorage.getItem('ffp_lang') as Lang;
      if (stored && (stored === 'en' || stored === 'bn')) {
        globalLang = stored;
        setLang(stored);
      }
    }
    listeners.add(setLang);
    return () => { listeners.delete(setLang); };
  }, []);

  const toggleLang = () => {
    const next: Lang = lang === 'en' ? 'bn' : 'en';
    globalLang = next;
    if (typeof window !== 'undefined') localStorage.setItem('ffp_lang', next);
    listeners.forEach(l => l(next));
  };

  const t = (key: string): string => {
    return translations[lang][key] || key;
  };

  return { lang, toggleLang, t };
};
