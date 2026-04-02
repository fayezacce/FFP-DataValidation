/**
 * FFP Data Validation Platform - Edit Statistics Modal
 */

import React from "react";
import { X, Edit2, Save, RefreshCw } from "lucide-react";
import { StatsEntry } from "@/types/ffp";

interface EditStatsModalProps {
  entry: StatsEntry | null;
  form: {
    district: string;
    upazila: string;
    total: number;
    valid: number;
    invalid: number;
  };
  saving: boolean;
  onClose: () => void;
  onFormChange: (field: string, value: any) => void;
  onSave: () => void;
}

const EditStatsModal: React.FC<EditStatsModalProps> = ({
  entry,
  form,
  saving,
  onClose,
  onFormChange,
  onSave
}) => {
  if (!entry) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-300">
      <div className="bg-[#121214] border border-[#1e1e20] w-full max-w-lg rounded-3xl overflow-hidden shadow-[0_0_50px_rgba(0,0,0,0.5)] flex flex-col scale-in-center">
        <div className="p-6 border-b border-[#1e1e20] flex justify-between items-center bg-[#161618]">
          <div className="flex items-center gap-4">
            <div className="w-12 h-12 rounded-2xl bg-amber-500/10 flex items-center justify-center">
              <Edit2 className="w-6 h-6 text-amber-500" />
            </div>
            <div>
              <h2 className="text-xl font-black text-white tracking-tight italic uppercase">Manual Adjustment</h2>
              <p className="text-gray-500 text-xs font-bold">{entry.upazila}, {entry.district}</p>
            </div>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-white/5 rounded-full transition-colors text-gray-500 hover:text-white">
            <X className="w-6 h-6" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-6 bg-[#0c0c0d] space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-1.5">
              <label className="text-[10px] font-black text-gray-500 uppercase tracking-widest pl-1">District Name</label>
              <input 
                type="text" 
                value={form.district} 
                onChange={(e) => onFormChange("district", e.target.value)}
                className="w-full bg-[#1a1a1c] border border-[#2a2a2e] rounded-xl px-4 py-3 text-white text-sm focus:border-amber-500/50 outline-none transition-all"
              />
            </div>
            <div className="space-y-1.5">
              <label className="text-[10px] font-black text-gray-500 uppercase tracking-widest pl-1">Upazila Name</label>
              <input 
                type="text" 
                value={form.upazila} 
                onChange={(e) => onFormChange("upazila", e.target.value)}
                className="w-full bg-[#1a1a1c] border border-[#2a2a2e] rounded-xl px-4 py-3 text-white text-sm focus:border-amber-500/50 outline-none transition-all"
              />
            </div>
          </div>

          <div className="grid grid-cols-3 gap-4 pb-4">
            <div className="space-y-1.5">
              <label className="text-[10px] font-black text-gray-500 uppercase tracking-widest pl-1">Grand Total</label>
              <input 
                type="number" 
                value={form.total} 
                onChange={(e) => onFormChange("total", parseInt(e.target.value) || 0)}
                className="w-full bg-[#1a1a1c] border border-[#2a2a2e] rounded-xl px-4 py-3 text-white text-sm focus:border-emerald-500/50 outline-none transition-all"
              />
            </div>
            <div className="space-y-1.5">
              <label className="text-[10px] font-black text-gray-500 uppercase tracking-widest pl-1">Total Valid</label>
              <input 
                type="number" 
                value={form.valid} 
                onChange={(e) => onFormChange("valid", parseInt(e.target.value) || 0)}
                className="w-full bg-[#1a1a1c] border border-[#2a2a2e] rounded-xl px-4 py-3 text-white text-sm focus:border-emerald-500/50 outline-none transition-all font-bold text-emerald-400"
              />
            </div>
            <div className="space-y-1.5">
              <label className="text-[10px] font-black text-gray-500 uppercase tracking-widest pl-1">Total Invalid</label>
              <input 
                type="number" 
                value={form.invalid} 
                onChange={(e) => onFormChange("invalid", parseInt(e.target.value) || 0)}
                className="w-full bg-[#1a1a1c] border border-[#2a2a2e] rounded-xl px-4 py-3 text-white text-sm focus:border-red-500/50 outline-none transition-all font-bold text-red-500"
              />
            </div>
          </div>

          <p className="text-[10px] text-gray-600 bg-black/30 p-3 rounded-lg border border-white/5 italic">
            Note: Manually updating these values affects the summary dashboard only. Master record IDs should be corrected via Geo Cleanup or specialized migration scripts.
          </p>

          <div className="pt-2">
            <button 
              onClick={onSave}
              disabled={saving}
              className="w-full py-4 bg-emerald-600 hover:bg-emerald-500 disabled:bg-emerald-600/50 text-white font-black uppercase tracking-widest rounded-2xl transition-all shadow-lg shadow-emerald-600/20 flex items-center justify-center gap-2"
            >
              {saving ? <RefreshCw className="w-5 h-5 animate-spin" /> : <Save className="w-5 h-5" />}
              {saving ? "SAVING..." : "COMMIT CHANGES"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default EditStatsModal;
