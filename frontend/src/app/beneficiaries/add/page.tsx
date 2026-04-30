"use client";
/**
 * Manual Add Beneficiary Page
 * /beneficiaries/add
 * 
 * Features:
 * - 19 standard fields for full FFP compatibility
 * - Permission-aware Geo Selection (Auto-populates and locks based on user access)
 * - Real-time validation for NID and DOB
 * - Bengali digits to English normalization on-the-fly
 * - Premium dark-mode UI with glassmorphism effects
 * 
 * Author: Fayez Ahmed, Assistant Programmer, DG Food
 */
import React, { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { 
  ArrowLeft, Save, User as UserIcon, Calendar, Hash, Phone, 
  MapPin, Briefcase, Heart, Book, ShieldAlert, Sparkles, PlusCircle, Search, UserCheck
} from "lucide-react";
import { fetchWithAuth, getUser } from "@/lib/auth";
import { Toaster, toast } from "react-hot-toast";
import type { User } from "@/types/ffp";

interface GeoInfo {
  divisions: string[];
  districts: Record<string, string[]>;
  upazilas: Record<string, string[]>;
}

export default function AddBeneficiaryPage() {
  const router = useRouter();
  const [user, setUser] = useState<User | null>(null);
  const [geoInfo, setGeoInfo] = useState<GeoInfo>({ divisions: [], districts: {}, upazilas: {} });
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);

  // Form State
  const [formData, setFormData] = useState({
    name: "", name_en: "", nid: "", dob: "",
    father_husband_name: "", mobile: "", card_no: "",
    division: "", district: "", upazila: "", union_name: "", ward: "", address: "",
    gender: "", occupation: "", religion: "",
    spouse_name: "", spouse_nid: "", spouse_dob: "",
    // Dealer fields
    dealer_id: "" as string | number,
    dealer_name: "",
    dealer_mobile: "",
    dealer_nid: ""
  });

  const [dealers, setDealers] = useState<any[]>([]);
  const [isNewDealer, setIsNewDealer] = useState(false);
  const [fetchingDealers, setFetchingDealers] = useState(false);

  // Init & Geo Loading
  useEffect(() => {
    const u = getUser();
    setUser(u);
    
    // Set initial geo values from user access
    setFormData(prev => ({
      ...prev,
      division: u?.division_access || "",
      district: u?.district_access || "",
      upazila: u?.upazila_access || ""
    }));

    const loadGeo = async () => {
      try {
        const res = await fetchWithAuth("/api/geo/info");
        if (res.ok) setGeoInfo(await res.json());
      } catch (e) {}
      finally { setLoading(false); }
    };
    loadGeo();
  }, []);

  // Fetch dealers when upazila changes
  useEffect(() => {
    if (formData.upazila && formData.district) {
      const fetchDealers = async () => {
        setFetchingDealers(true);
        try {
          const res = await fetchWithAuth(`/api/records/dealers?upazila=${formData.upazila}`);
          if (res.ok) {
            const data = await res.json();
            setDealers(data.dealers || []);
          }
        } catch (e) {
          console.error("Failed to fetch dealers", e);
        } finally {
          setFetchingDealers(false);
        }
      };
      fetchDealers();
    } else {
      setDealers([]);
    }
  }, [formData.upazila, formData.district]);

  // Normalization: Bengali -> English digits
  const normalizeDigits = (val: string) => {
    const map: any = { '০':'0', '১':'1', '২':'2', '৩':'3', '৪':'4', '৫':'5', '৬':'6', '৭':'7', '৮':'8', '৯':'9' };
    return val.replace(/[০-৯]/g, s => map[s]);
  };

  const handleInputChange = (field: string, value: string) => {
    let normalized = value;
    if (["nid", "dob", "mobile", "card_no", "spouse_nid", "spouse_dob"].includes(field)) {
      normalized = normalizeDigits(value);
    }
    setFormData(prev => ({ ...prev, [field]: normalized }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    // Basic validation
    if (!formData.name || !formData.name_en || !formData.nid || !formData.dob) {
      toast.error("Please fill all mandatory fields (Name, NID, DOB)");
      return;
    }
    if (!formData.division || !formData.district || !formData.upazila) {
      toast.error("Location (Division/District/Upazila) is required");
      return;
    }

    // Dealer validation
    if (!isNewDealer && !formData.dealer_id) {
      toast.error("Please select a dealer or register a new one");
      return;
    }
    if (isNewDealer && (!formData.dealer_name || !formData.dealer_nid)) {
      toast.error("Dealer Name and NID are mandatory for new registration");
      return;
    }

    setSubmitting(true);
    try {
      const res = await fetchWithAuth("/api/records/beneficiary", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formData),
      });

      const result = await res.json();
      if (res.ok) {
        toast.success("Beneficiary added successfully!");
        setTimeout(() => router.push("/beneficiaries"), 1500);
      } else {
        toast.error(result.detail || "Failed to add beneficiary");
      }
    } catch (e) {
      toast.error("A connection error occurred");
    } finally {
      setSubmitting(false);
    }
  };

  const divLocked = !!user?.division_access;
  const distLocked = !!user?.district_access;
  const upzLocked = !!user?.upazila_access;

  const availableDistricts = formData.division ? (geoInfo.districts[formData.division] || []) : [];
  const availableUpazilas = formData.district ? (geoInfo.upazilas[formData.district] || []) : [];

  if (loading) return (
    <div className="min-h-screen bg-[#0d0f14] flex items-center justify-center">
      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div>
    </div>
  );

  const inputClass = "w-full bg-slate-900/50 border border-slate-700/50 rounded-xl px-4 py-2.5 text-slate-200 text-sm focus:border-indigo-500/50 focus:ring-4 focus:ring-indigo-500/5 transition-all outline-none placeholder:text-slate-600";
  const labelClass = "block text-xs font-medium text-slate-400 mb-1.5 ml-1";
  const sectionHeaderClass = "flex items-center gap-2 text-sm font-bold text-slate-200 mb-4 border-b border-slate-800 pb-2";

  return (
    <div className="min-h-screen bg-[#0d0f14] text-slate-200 pb-20 selection:bg-indigo-500/30">
      <Toaster position="top-right" />
      
      {/* Background Decor */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none opacity-20">
        <div className="absolute top-[-10%] right-[-10%] w-[40%] h-[40%] bg-indigo-600/20 rounded-full blur-[120px]"></div>
        <div className="absolute bottom-[-10%] left-[-10%] w-[40%] h-[40%] bg-emerald-600/10 rounded-full blur-[120px]"></div>
      </div>

      <div className="max-w-5xl mx-auto px-4 pt-8 relative z-10">
        {/* Navigation */}
        <button 
          onClick={() => router.back()}
          className="flex items-center gap-2 text-slate-400 hover:text-slate-200 transition-colors mb-6 group"
        >
          <ArrowLeft className="w-4 h-4 group-hover:-translate-x-1 transition-transform" />
          <span className="text-sm">Back to Beneficiaries</span>
        </button>

        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div className="flex items-center gap-4">
            <div className="p-3 bg-indigo-500/10 rounded-2xl border border-indigo-500/20">
              <PlusCircle className="w-7 h-7 text-indigo-400" />
            </div>
            <div>
              <h1 className="text-2xl font-bold text-white tracking-tight">Add New Beneficiary</h1>
              <p className="text-slate-400 text-sm">Manual entry for single record verification</p>
            </div>
          </div>
        </div>

        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Location Details */}
          <div className="bg-slate-900/40 border border-slate-800/80 rounded-3xl p-6 backdrop-blur-xl">
            <h3 className={sectionHeaderClass}>
              <MapPin className="w-4 h-4 text-indigo-400" /> Location Details
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div>
                <label className={labelClass}>Division / বিভাগ *</label>
                <select 
                  value={formData.division} 
                  onChange={e => { handleInputChange("division", e.target.value); handleInputChange("district", ""); handleInputChange("upazila", ""); }}
                  disabled={divLocked}
                  className={`${inputClass} ${divLocked ? "bg-slate-800/50 cursor-not-allowed" : ""}`}
                >
                  <option value="">Select Division</option>
                  {geoInfo.divisions.map(d => <option key={d} value={d}>{d}</option>)}
                </select>
              </div>
              <div>
                <label className={labelClass}>District / জেলা *</label>
                <select 
                  value={formData.district} 
                  onChange={e => { handleInputChange("district", e.target.value); handleInputChange("upazila", ""); }}
                  disabled={distLocked || !formData.division}
                  className={`${inputClass} ${distLocked || !formData.division ? "bg-slate-800/50 cursor-not-allowed" : ""}`}
                >
                  <option value="">Select District</option>
                  {availableDistricts.map(d => <option key={d} value={d}>{d}</option>)}
                </select>
              </div>
              <div>
                <label className={labelClass}>Upazila / উপজেলা *</label>
                <select 
                  value={formData.upazila} 
                  onChange={e => handleInputChange("upazila", e.target.value)}
                  disabled={upzLocked || !formData.district}
                  className={`${inputClass} ${upzLocked || !formData.district ? "bg-slate-800/50 cursor-not-allowed" : ""}`}
                >
                  <option value="">Select Upazila</option>
                  {availableUpazilas.map(u => <option key={u} value={u}>{u}</option>)}
                </select>
              </div>
              <div>
                <label className={labelClass}>Union/Paurashava / ইউনিয়ন/পৌরসভা</label>
                <input type="text" value={formData.union_name} onChange={e => handleInputChange("union_name", e.target.value)} className={inputClass} placeholder="e.g. কালুখালী" />
              </div>
              <div>
                <label className={labelClass}>Ward /ওয়ার্ড</label>
                <input type="text" value={formData.ward} onChange={e => handleInputChange("ward", e.target.value)} className={inputClass} placeholder="e.g. ০৪" />
              </div>
              <div>
                <label className={labelClass}>Village/Area / গ্রাম/পাড়া/মহল্লা</label>
                <input type="text" value={formData.address} onChange={e => handleInputChange("address", e.target.value)} className={inputClass} placeholder="Village name" />
              </div>
            </div>
          </div>

          {/* Identity Info */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-slate-900/40 border border-slate-800/80 rounded-3xl p-6 backdrop-blur-xl">
              <h3 className={sectionHeaderClass}>
                <UserIcon className="w-4 h-4 text-emerald-400" /> Identity Details
              </h3>
              <div className="space-y-4">
                <div>
                  <label className={labelClass}>Beneficiary Name (Bangla) *</label>
                  <input type="text" required value={formData.name} onChange={e => handleInputChange("name", e.target.value)} className={inputClass} placeholder="যেমন: মোঃ আবুল হোসেন" />
                </div>
                <div>
                  <label className={labelClass}>Beneficiary Name (English) *</label>
                  <input type="text" required value={formData.name_en} onChange={e => handleInputChange("name_en", e.target.value)} className={inputClass} placeholder="e.g. MD. ABUL HOSSEN" />
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className={labelClass}>NID Number *</label>
                    <input type="text" required maxLength={17} value={formData.nid} onChange={e => handleInputChange("nid", e.target.value)} className={inputClass} placeholder="10 or 17 digits" />
                  </div>
                  <div>
                    <label className={labelClass}>Date of Birth *</label>
                    <input type="date" required value={formData.dob} onChange={e => handleInputChange("dob", e.target.value)} className={inputClass} />
                  </div>
                </div>
              </div>
            </div>

            <div className="bg-slate-900/40 border border-slate-800/80 rounded-3xl p-6 backdrop-blur-xl">
              <h3 className={sectionHeaderClass}>
                <Sparkles className="w-4 h-4 text-amber-400" /> Additional Details
              </h3>
              <div className="space-y-4">
                <div>
                  <label className={labelClass}>Father / Husband Name</label>
                  <input type="text" value={formData.father_husband_name} onChange={e => handleInputChange("father_husband_name", e.target.value)} className={inputClass} placeholder="Name" />
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className={labelClass}>Mobile Number</label>
                    <input type="text" maxLength={11} value={formData.mobile} onChange={e => handleInputChange("mobile", e.target.value)} className={inputClass} placeholder="017..." />
                  </div>
                  <div>
                    <label className={labelClass}>Card Number</label>
                    <input type="text" value={formData.card_no} onChange={e => handleInputChange("card_no", e.target.value)} className={inputClass} placeholder="ID Card No" />
                  </div>
                </div>
                <div className="grid grid-cols-3 gap-4">
                  <div>
                    <label className={labelClass}>Gender</label>
                    <select value={formData.gender} onChange={e => handleInputChange("gender", e.target.value)} className={inputClass}>
                      <option value="">Select</option>
                      <option value="Male">Male</option>
                      <option value="Female">Female</option>
                      <option value="Other">Other</option>
                    </select>
                  </div>
                  <div>
                    <label className={labelClass}>Occupation</label>
                    <input type="text" value={formData.occupation} onChange={e => handleInputChange("occupation", e.target.value)} className={inputClass} placeholder="Farmer..." />
                  </div>
                  <div>
                    <label className={labelClass}>Religion</label>
                    <select value={formData.religion} onChange={e => handleInputChange("religion", e.target.value)} className={inputClass}>
                      <option value="">Select</option>
                      <option value="Islam">Islam</option>
                      <option value="Hinduism">Hinduism</option>
                      <option value="Christianity">Christianity</option>
                      <option value="Buddhism">Buddhism</option>
                    </select>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Spouse Details */}
          <div className="bg-slate-900/40 border border-slate-800/80 rounded-3xl p-6 backdrop-blur-xl">
            <h3 className={sectionHeaderClass}>
              <Heart className="w-4 h-4 text-rose-400" /> Spouse Info (Optional)
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div>
                <label className={labelClass}>Spouse Name</label>
                <input type="text" value={formData.spouse_name} onChange={e => handleInputChange("spouse_name", e.target.value)} className={inputClass} placeholder="Name" />
              </div>
              <div>
                <label className={labelClass}>Spouse NID</label>
                <input type="text" value={formData.spouse_nid} onChange={e => handleInputChange("spouse_nid", e.target.value)} className={inputClass} placeholder="NID Number" />
              </div>
              <div>
                <label className={labelClass}>Spouse DOB</label>
                <input type="date" value={formData.spouse_dob} onChange={e => handleInputChange("spouse_dob", e.target.value)} className={inputClass} />
              </div>
            </div>
          </div>

          {/* Dealer Information */}
          <div className="bg-slate-900/40 border border-slate-800/80 rounded-3xl p-6 backdrop-blur-xl">
            <h3 className={sectionHeaderClass}>
              <UserCheck className="w-4 h-4 text-blue-400" /> Dealer Information *
            </h3>
            
            <div className="space-y-6">
              <div className="flex items-center gap-4 bg-slate-800/30 p-4 rounded-xl border border-slate-700/30">
                <button
                  type="button"
                  onClick={() => { setIsNewDealer(false); handleInputChange("dealer_id", ""); }}
                  className={`flex-1 py-2 text-sm font-medium rounded-lg transition-all ${!isNewDealer ? "bg-indigo-600 text-white shadow-lg" : "text-slate-400 hover:bg-slate-700/50"}`}
                >
                  Select Existing Dealer
                </button>
                <button
                  type="button"
                  onClick={() => { setIsNewDealer(true); handleInputChange("dealer_id", ""); }}
                  className={`flex-1 py-2 text-sm font-medium rounded-lg transition-all ${isNewDealer ? "bg-indigo-600 text-white shadow-lg" : "text-slate-400 hover:bg-slate-700/50"}`}
                >
                  Register New Dealer
                </button>
              </div>

              {!isNewDealer ? (
                <div>
                  <label className={labelClass}>Select Dealer / ডিলার নির্বাচন করুন *</label>
                  <div className="relative">
                    <select
                      value={formData.dealer_id}
                      onChange={e => handleInputChange("dealer_id", e.target.value)}
                      disabled={fetchingDealers || !formData.upazila}
                      className={`${inputClass} appearance-none pr-10 ${fetchingDealers ? "opacity-50" : ""}`}
                    >
                      <option value="">{fetchingDealers ? "Loading dealers..." : "Choose a dealer"}</option>
                      {dealers.map(d => (
                        <option key={d.id} value={d.id}>{d.name} ({d.nid}) - {d.mobile || 'No Mobile'}</option>
                      ))}
                    </select>
                    <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none">
                      <Search className="w-4 h-4 text-slate-500" />
                    </div>
                  </div>
                  {!formData.upazila && <p className="text-xs text-amber-500 mt-2 ml-1">Please select an Upazila first to see dealers</p>}
                  {formData.upazila && dealers.length === 0 && !fetchingDealers && (
                    <p className="text-xs text-amber-500 mt-2 ml-1">No dealers found in {formData.upazila}. Please register a new one.</p>
                  )}
                </div>
              ) : (
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6 animate-in fade-in slide-in-from-top-2 duration-300">
                  <div>
                    <label className={labelClass}>Dealer Name *</label>
                    <input 
                      type="text" 
                      value={formData.dealer_name} 
                      onChange={e => handleInputChange("dealer_name", e.target.value)} 
                      className={inputClass} 
                      placeholder="ডিলারের নাম" 
                    />
                  </div>
                  <div>
                    <label className={labelClass}>Dealer NID *</label>
                    <input 
                      type="text" 
                      value={formData.dealer_nid} 
                      onChange={e => handleInputChange("dealer_nid", e.target.value)} 
                      className={inputClass} 
                      placeholder="১০ বা ১৭ ডিজিট" 
                    />
                  </div>
                  <div>
                    <label className={labelClass}>Dealer Mobile</label>
                    <input 
                      type="text" 
                      value={formData.dealer_mobile} 
                      onChange={e => handleInputChange("dealer_mobile", e.target.value)} 
                      className={inputClass} 
                      placeholder="017..." 
                    />
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Actions */}
          <div className="flex items-center justify-end gap-4 pt-6">
            <button
              type="button"
              onClick={() => router.back()}
              className="px-6 py-3 text-sm font-medium text-slate-400 hover:text-slate-200 transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={submitting}
              className={`flex items-center gap-2 px-8 py-3 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm font-bold rounded-2xl shadow-lg shadow-indigo-600/20 transition-all transform active:scale-95`}
            >
              {submitting ? (
                <>
                  <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
                  <span>Saving...</span>
                </>
              ) : (
                <>
                  <Save className="w-4 h-4" />
                  <span>Save Beneficiary</span>
                </>
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
