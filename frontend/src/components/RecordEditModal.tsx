"use client";
import React, { useState } from "react";
import { X, Save, Loader2, Info } from "lucide-react";
import { fetchWithAuth } from "@/lib/auth";
import type { BeneficiaryRecord } from "@/types/ffp";
import { useTranslation } from "@/lib/useTranslation";

interface RecordEditModalProps {
  record: BeneficiaryRecord;
  onClose: () => void;
  onSaved: () => void;
}

interface FieldConfig {
  key: string;
  labelKey: string;
  readOnly?: boolean;
  type?: "text" | "date" | "select";
  options?: string[];
  fullWidth?: boolean;
}

const ROOT_KEYS = [
  "dob", "name", "card_no", "mobile", 
  "father_husband_name", "name_bn", "name_en", "ward", "union_name",
  // ── Standard canonical columns (dedicated DB columns) ──
  "occupation", "gender", "religion", "address",
  "spouse_name", "spouse_nid", "spouse_dob",
];

const GENDER_OPTIONS = ['পুরুষ', 'মহিলা', 'অন্যান্য'];
const RELIGION_OPTIONS = ['ইসলাম', 'হিন্দু', 'বৌদ্ধ', 'খ্রিস্টান', 'অন্যান্য'];

const SECTION_1_FIELDS: FieldConfig[] = [
  { key: "nid",                 labelKey: "nid_number",          readOnly: true, fullWidth: true },
  { key: "card_no",             labelKey: "card_no"            },
  { key: "name_bn",             labelKey: "beneficiary_name_bn" },
  { key: "name_en",             labelKey: "beneficiary_name_en" },
  { key: "father_husband_name", labelKey: "father_name"        },
  { key: "dob",                 labelKey: "dob_nid",             type: "date" },
  { key: "occupation",          labelKey: "occupation"         },
  { key: "address",             labelKey: "village_name"       },
  { key: "ward",                labelKey: "ward_no"            },
  { key: "union_name",          labelKey: "union_name"         },
  { key: "mobile",              labelKey: "mobile_no"          },
  { key: "gender",              labelKey: "gender",              type: "select", options: GENDER_OPTIONS },
  { key: "religion",            labelKey: "religion",            type: "select", options: RELIGION_OPTIONS },
];

const SECTION_2_FIELDS: FieldConfig[] = [
  { key: "spouse_name", labelKey: "spouse_name" },
  { key: "spouse_nid",  labelKey: "spouse_nid"  },
  { key: "spouse_dob",  labelKey: "spouse_dob",  type: "date"   },
];

const SECTION_3_FIELDS: FieldConfig[] = [
  { key: "dealer_name",   labelKey: "dealer_name",   readOnly: true },
  { key: "dealer_nid",    labelKey: "dealer_nid",    readOnly: true },
  { key: "dealer_mobile", labelKey: "dealer_mobile", readOnly: true },
];

const ALL_FIELDS = [...SECTION_1_FIELDS, ...SECTION_2_FIELDS, ...SECTION_3_FIELDS];

export default function RecordEditModal({ record, onClose, onSaved }: RecordEditModalProps) {
  const { t } = useTranslation();
  const [form, setForm] = useState<Record<string, string>>(() => {
    const init: Record<string, string> = {};
    ALL_FIELDS.forEach(f => {
      init[f.key] = (record as any)[f.key] ?? "";
    });
    return init;
  });
  const [saving, setSaving] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError]   = useState<string | null>(null);

  React.useEffect(() => {
    const fetchFull = async () => {
      try {
        const isInvalid = !!(record as any).error_message;
        const url = isInvalid ? `/api/records/invalid/${record.id}` : `/api/records/beneficiaries/${record.id}`;
        const res = await fetchWithAuth(url);
        if (res.ok) {
          const fullData = await res.json();
          setForm(prev => {
            const next = { ...prev };
            ALL_FIELDS.forEach(f => {
              // Tier 1: root column
              if (fullData[f.key] !== undefined && fullData[f.key] !== null) {
                next[f.key] = String(fullData[f.key]);
              }
              // Tier 2: extended_fields
              else if (fullData.extended_fields && fullData.extended_fields[f.key] !== undefined && fullData.extended_fields[f.key] !== null) {
                next[f.key] = String(fullData.extended_fields[f.key]);
              }
              // Tier 3: dealer sub-object
              else if (fullData.dealer && fullData.dealer[f.key.replace('dealer_', '')] !== undefined) {
                next[f.key] = fullData.dealer[f.key.replace('dealer_', '')];
              }
            });
            return next;
          });
        }
      } catch (e) {} finally {
        setLoading(false);
      }
    };
    fetchFull();
  }, [record.id, record]);

  const handleChange = (key: string, value: string) => {
    setForm(prev => ({ ...prev, [key]: value }));
  };

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      const payload: any = {};
      const extra_fields: Record<string, string> = {};
      
      ALL_FIELDS.filter(f => !f.readOnly).forEach(f => {
        if (ROOT_KEYS.includes(f.key)) {
          payload[f.key] = form[f.key];
        } else {
          extra_fields[f.key] = form[f.key];
        }
      });
      
      if (Object.keys(extra_fields).length > 0) {
        payload.extra_fields = extra_fields;
      }

      const isInvalid = !!(record as any).error_message;
      const url = isInvalid ? `/api/records/invalid/${record.id}` : `/api/records/beneficiaries/${record.id}`;

      const res = await fetchWithAuth(url, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || `Error ${res.status}`);
      }

      onSaved();
    } catch (e: any) {
      setError(e.message || "Failed to save");
    } finally {
      setSaving(false);
    }
  };

  const inputClass = (readOnly?: boolean) =>
    `w-full px-3 py-2 rounded-lg text-sm border transition-all outline-none ${
      readOnly
        ? "bg-slate-800/60 border-slate-700 text-slate-500 cursor-not-allowed"
        : "bg-slate-900/70 border-slate-600 text-slate-200 focus:border-indigo-500 hover:border-slate-500"
    }`;

  const renderField = (f: FieldConfig) => (
    <div key={f.key} className={f.fullWidth ? "col-span-2" : ""}>
      <label className="block text-[11px] text-slate-500 mb-1 uppercase tracking-wider">{t(f.labelKey)}</label>
      {f.type === "select" ? (
        <select
          value={form[f.key]}
          onChange={e => handleChange(f.key, e.target.value)}
          disabled={!!f.readOnly}
          className={inputClass(f.readOnly)}
        >
          <option value="">Select...</option>
          {f.options?.map(opt => (
            <option key={opt} value={opt}>{opt}</option>
          ))}
        </select>
      ) : (
        <input
          type={f.type || "text"}
          value={form[f.key]}
          onChange={e => handleChange(f.key, e.target.value)}
          readOnly={!!f.readOnly}
          className={inputClass(f.readOnly)}
          id={`edit-field-${f.key}`}
        />
      )}
    </div>
  );

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm" onClick={onClose}>
      <div
        className="relative w-full max-w-2xl max-h-[90vh] overflow-y-auto bg-[#12141a] border border-slate-700/60 rounded-2xl shadow-2xl"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="sticky top-0 z-10 flex items-center justify-between px-6 py-4 bg-[#12141a] border-b border-slate-700/40">
          <div>
            <h2 className="text-lg font-bold text-slate-100">Edit Record</h2>
            <div className="flex items-center gap-2 mt-0.5">
              <Info className="w-3.5 h-3.5 text-amber-400" />
              <p className="text-[11px] text-amber-400/80">Saving will reset verification status to Unverified</p>
            </div>
          </div>
          <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-slate-700/50 text-slate-400 hover:text-slate-200 transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-6 space-y-10">
          {loading && (
            <div className="flex flex-col items-center justify-center py-12 gap-3">
              <Loader2 className="w-8 h-8 animate-spin text-indigo-500" />
              <p className="text-sm text-slate-500">Loading full record detail...</p>
            </div>
          )}
          
          {!loading && (
            <>
              {/* Section 1: Beneficiary Information */}
              <section>
                <h3 className="text-xs font-semibold text-indigo-400 uppercase tracking-widest mb-4 flex items-center gap-2">
                  <span className="w-1.5 h-1.5 rounded-full bg-indigo-500"></span>
                  {t('beneficiary_info')}
                </h3>
                <div className="grid grid-cols-2 gap-x-4 gap-y-4">
                  {SECTION_1_FIELDS.map(renderField)}
                </div>
              </section>

              {/* Section 2: Spouse Information */}
              <section>
                <h3 className="text-xs font-semibold text-indigo-400 uppercase tracking-widest mb-4 flex items-center gap-2">
                  <span className="w-1.5 h-1.5 rounded-full bg-indigo-500"></span>
                  {t('spouse_info')}
                </h3>
                <div className="grid grid-cols-2 gap-x-4 gap-y-4">
                  {SECTION_2_FIELDS.map(renderField)}
                </div>
              </section>

              {/* Section 3: Dealer Information */}
              <section>
                <h3 className="text-xs font-semibold text-indigo-400 uppercase tracking-widest mb-4 flex items-center gap-2">
                  <span className="w-1.5 h-1.5 rounded-full bg-indigo-500"></span>
                  {t('dealer_info')}
                </h3>
                <div className="grid grid-cols-2 gap-x-4 gap-y-4">
                  {SECTION_3_FIELDS.map(renderField)}
                </div>
              </section>

              {error && (
                <div className="flex items-center gap-2 px-4 py-3 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-sm">
                  <X className="w-4 h-4 shrink-0" />
                  {error}
                </div>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        <div className="sticky bottom-0 flex justify-end gap-3 px-6 py-4 bg-[#12141a] border-t border-slate-700/40">
          <button 
            onClick={onClose} 
            disabled={saving}
            className="px-4 py-2 text-sm text-slate-400 hover:text-slate-200 transition-colors disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={saving || loading}
            className="flex items-center gap-2 px-6 py-2 text-sm bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white rounded-lg transition-colors font-semibold"
            id="record-edit-save-btn"
          >
            {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
            Save Changes
          </button>
        </div>
      </div>
    </div>
  );
}
