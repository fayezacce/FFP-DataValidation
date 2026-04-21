"use client";
import React from "react";
import { X, CheckCircle2, Clock } from "lucide-react";
import type { BeneficiaryRecord } from "@/types/ffp";
import { useTranslation } from "@/lib/useTranslation";

interface RecordDetailModalProps {
  record: BeneficiaryRecord | null;
  onClose: () => void;
  onEdit: (record: BeneficiaryRecord) => void;
}

const FieldRow = ({ label, value }: { label: string; value?: string | null }) => {
  if (!value?.trim()) return null;
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[11px] text-slate-500 uppercase tracking-wider">{label}</span>
      <span className="text-sm text-slate-200 font-medium break-words">{value}</span>
    </div>
  );
};

export default function RecordDetailModal({ record, onClose, onEdit }: RecordDetailModalProps) {
  const { t } = useTranslation();
  if (!record) return null;

  const ext = record.extended_fields || {};
  const raw = record.raw_data || {};

  // All canonical column keys + standard Bengali header variants
  // Result: additional_fields shows only genuinely non-standard extra columns
  const shownKeys = new Set([
    "Status","Cleaned_NID","Cleaned_DOB","Excel_Row","Message","Extracted_Name",
    "nid","dob","name","card_no","mobile","division","district","upazila",
    "father_husband_name","name_bn","name_en","ward","union_name",
    "occupation","gender","religion","address",
    "spouse_name","spouse_nid","spouse_dob",
    "dealer_name","dealer_nid","dealer_mobile","dealer_id",
    "verification_status","verified_by","verified_at",
    "কার্ড নং","Card_No",
    "উপকার ভোগীর নাম (বাংলা) (NID সাথে মিল থাকতে হবে)","নাম (বাংলা)","বাংলা নাম",
    "উপকার ভোগীর নাম (ইংরেজি) (NID সাথে মিল থাকতে হবে)","নাম (ইংরেজি)","Name_EN",
    "পিতার নাম","পিতা / স্বামীর নাম","Father_Name",
    "জন্ম তারিখ (NID সাথে মিল থাকতে হবে)","জন্ম তারিখ",
    "পেশা","Occupation",
    "গ্রামের নাম","গ্রাম","ঠিকানা","Village",
    "ওয়ার্ড নং","ওয়াড নং","Ward_No",
    "ইউনিয়নের নাম","ইউনিয়ন","Union",
    "জাতীয় পরিচয় পত্র নম্বর","NID",
    "মোবাইল নং (নিজ নামে)","Mobile",
    "লিঙ্গ","Gender",
    "ধর্ম","Religion",
    "স্বামী/স্ত্রীর নাম (NID সাথে মিল থাকতে হবে)","স্বামী/স্ত্রীর নাম",
    "স্বামী/স্ত্রী জাতীয় পরিচয় নম্বর",
    "স্বামী/স্ত্রী জন্ম তারিখ (NID সাথে মিল থাকতে হবে)","স্বামী/স্ত্রী জন্ম তারিখ",
    "রেজিস্ট্রার্ড ডিলারের নাম (NID সাথে মিল থাকতে হবে)",
    "রেজিস্টার্ড ডিলারের নাম (NID সাথে মিল থাকতে হবে)","ডিলারের নাম","Dealer_Name",
    "রেজিস্ট্রার্ড ডিলারের এনআইডি নম্বর",
    "রেজিস্টার্ড ডিলারের এনআইডি নম্বর","Dealer_NID","ডিলার এনআইডি",
    "রেজিস্ট্রার্ড ডিলারের মোবাইল নং",
    "রেজিস্টার্ড ডিলারের মোবাইল নং","Dealer_Mobile",
  ]);
  const extraEntries = Object.entries(raw).filter(([k]) => !shownKeys.has(k) && raw[k] != null && String(raw[k]).trim());

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm" onClick={onClose}>
      <div
        className="relative w-full max-w-2xl max-h-[90vh] overflow-y-auto bg-[#12141a] border border-slate-700/60 rounded-2xl shadow-2xl"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="sticky top-0 z-10 flex items-center justify-between px-6 py-4 bg-[#12141a] border-b border-slate-700/40">
          <div>
            <h2 className="text-lg font-bold text-slate-100">Beneficiary Details</h2>
            <p className="text-xs text-slate-500 mt-0.5">NID: {record.nid}</p>
          </div>
          <div className="flex items-center gap-3">
            <span className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${
              record.verification_status === "verified"
                ? "bg-emerald-500/15 text-emerald-400 border border-emerald-500/30"
                : "bg-amber-500/15 text-amber-400 border border-amber-500/30"
            }`}>
              {record.verification_status === "verified"
                ? <><CheckCircle2 className="w-3 h-3" /> Verified</>
                : <><Clock className="w-3 h-3" /> Unverified</>}
            </span>
            <button
              onClick={() => onEdit(record)}
              className="px-3 py-1.5 text-sm bg-indigo-600/20 border border-indigo-500/40 text-indigo-400 hover:bg-indigo-600/30 rounded-lg transition-colors"
            >
              Edit
            </button>
            <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-slate-700/50 text-slate-400 hover:text-slate-200 transition-colors">
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>

        <div className="p-6 space-y-8">
          {/* Section 1: Beneficiary Information */}
          <section>
            <h3 className="text-xs font-semibold text-indigo-400 uppercase tracking-widest mb-4 flex items-center gap-2">
              <span className="w-1.5 h-1.5 rounded-full bg-indigo-500"></span>
              {t('beneficiary_info')}
            </h3>
            <div className="grid grid-cols-2 gap-x-6 gap-y-4">
              <FieldRow label={t('card_no')}              value={record.card_no} />
              <FieldRow label={t('beneficiary_name_bn')}   value={ext.name_bn || record.name_bn} />
              <FieldRow label={t('beneficiary_name_en')}   value={record.name || ext.name_en} />
              <FieldRow label={t('father_name')}           value={record.father_husband_name || ext.father_husband_name} />
              <FieldRow label={t('dob_nid')}               value={record.dob} />
              <FieldRow label={t('occupation')}            value={ext.occupation} />
              <FieldRow label={t('village_name')}          value={ext.address} />
              <FieldRow label={t('ward_no')}               value={record.ward || ext.ward} />
              <FieldRow label={t('union_name')}            value={record.union_name || ext.union_name} />
              <FieldRow label={t('nid_number')}            value={record.nid} />
              <FieldRow label={t('mobile_no')}             value={record.mobile} />
              <FieldRow label={t('gender')}                value={ext.gender} />
              <FieldRow label={t('religion')}              value={ext.religion} />
            </div>
          </section>

          {/* Section 2: Spouse Information */}
          <section>
            <h3 className="text-xs font-semibold text-indigo-400 uppercase tracking-widest mb-4 flex items-center gap-2">
              <span className="w-1.5 h-1.5 rounded-full bg-indigo-500"></span>
              {t('spouse_info')}
            </h3>
            <div className="grid grid-cols-2 gap-x-6 gap-y-4">
              <FieldRow label={t('spouse_name')} value={ext.spouse_name} />
              <FieldRow label={t('spouse_nid')}  value={ext.spouse_nid} />
              <FieldRow label={t('spouse_dob')}  value={ext.spouse_dob} />
            </div>
          </section>

          {/* Section 3: Dealer Information */}
          <section>
            <h3 className="text-xs font-semibold text-indigo-400 uppercase tracking-widest mb-4 flex items-center gap-2">
              <span className="w-1.5 h-1.5 rounded-full bg-indigo-500"></span>
              {t('dealer_info')}
            </h3>
            <div className="grid grid-cols-2 gap-x-6 gap-y-4">
              <FieldRow label={t('dealer_name')}   value={record.dealer_name || ext.dealer_name} />
              <FieldRow label={t('dealer_nid')}    value={record.dealer_nid  || ext.dealer_nid} />
              <FieldRow label={t('dealer_mobile')} value={ext.dealer_mobile} />
            </div>
          </section>

          {/* Extra raw fields */}
          {extraEntries.length > 0 && (
            <section className="pt-4 border-t border-slate-800">
              <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-widest mb-4">
                {t('additional_fields')}
              </h3>
              <div className="grid grid-cols-2 gap-x-6 gap-y-4">
                {extraEntries.map(([k, v]) => (
                  <FieldRow key={k} label={k} value={String(v)} />
                ))}
              </div>
            </section>
          )}

          {/* Verification info */}
          {record.verification_status === "verified" && record.verified_by && (
            <p className="text-xs text-slate-500 border-t border-slate-800 pt-4 text-center italic">
              Verified by <span className="text-slate-300 font-medium">{record.verified_by}</span>
              {record.verified_at && <> on {new Date(record.verified_at).toLocaleString()}</>}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
