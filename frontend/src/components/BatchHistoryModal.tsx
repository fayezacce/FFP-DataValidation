/**
 * FFP Data Validation Platform - Batch History Modal
 */

import React from "react";
import { X, Clock, FileSpreadsheet, CheckCircle2, FileWarning, Hash, RefreshCw, Trash2, Download, FileText } from "lucide-react";
import { StatsEntry, Batch } from "@/types/ffp";

interface BatchHistoryModalProps {
  entry: StatsEntry | null;
  loading: boolean;
  batches: Batch[];
  deletingBatchId: number | null;
  onClose: () => void;
  onDeleteBatch: (id: number) => void;
}

const BatchHistoryModal: React.FC<BatchHistoryModalProps> = ({
  entry,
  loading,
  batches,
  deletingBatchId,
  onClose,
  onDeleteBatch
}) => {
  if (!entry) return null;

  const formatDate = (iso: string) => {
    return new Date(iso).toLocaleString('en-GB', { 
      day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' 
    });
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-300">
      <div className="bg-[#121214] border border-[#1e1e20] w-full max-w-4xl max-h-[85vh] rounded-3xl overflow-hidden shadow-[0_0_50px_rgba(0,0,0,0.5)] flex flex-col scale-in-center">
        <div className="p-6 border-b border-[#1e1e20] flex justify-between items-center bg-[#161618]">
          <div className="flex items-center gap-4">
            <div className="w-12 h-12 rounded-2xl bg-blue-500/10 flex items-center justify-center">
              <Clock className="w-6 h-6 text-blue-500" />
            </div>
            <div>
              <h2 className="text-xl font-black text-white tracking-tight italic uppercase">Upload History</h2>
              <p className="text-gray-500 text-xs font-bold">{entry.upazila}, {entry.district}</p>
            </div>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-white/5 rounded-full transition-colors text-gray-500 hover:text-white">
            <X className="w-6 h-6" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-6 custom-scrollbar bg-[#0c0c0d]">
          {loading ? (
            <div className="flex flex-col items-center justify-center py-20 space-y-4">
              <RefreshCw className="w-10 h-10 text-blue-500 animate-spin" />
              <p className="text-gray-500 font-black text-xs uppercase tracking-widest">Loading Batch History...</p>
            </div>
          ) : batches.length === 0 ? (
            <div className="text-center py-20 bg-white/[0.02] rounded-2xl border border-white/5">
              <FileSpreadsheet className="w-12 h-12 text-gray-700 mx-auto mb-4" />
              <p className="text-gray-500 font-bold">No history available for this upazila.</p>
            </div>
          ) : (
            <div className="space-y-4">
              {batches.map((batch) => (
                <div 
                  key={batch.id} 
                  className={`p-5 rounded-2xl border transition-all ${batch.status === 'deleted' ? 'bg-red-500/5 border-red-500/20 opacity-50' : 'bg-[#161618] border-[#1e1e20] hover:border-blue-500/30 shadow-lg'}`}
                >
                  <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-4">
                    <div className="flex items-center gap-3">
                      <div className={`p-2 rounded-lg ${batch.status === 'deleted' ? 'bg-red-500/10' : 'bg-blue-500/10'}`}>
                        <FileSpreadsheet className={`w-4 h-4 ${batch.status === 'deleted' ? 'text-red-500' : 'text-blue-500'}`} />
                      </div>
                      <div>
                        <div className="font-bold text-white text-sm truncate max-w-[300px]" title={batch.filename}>{batch.filename}</div>
                        <div className="text-[10px] text-gray-500 font-bold uppercase tracking-wider">{formatDate(batch.created_at)} • by {batch.username}</div>
                      </div>
                    </div>
                    {batch.status !== 'deleted' && (
                      <div className="flex items-center gap-2">
                        <span className="text-[10px] font-black uppercase px-2 py-1 rounded bg-emerald-500/10 text-emerald-500 border border-emerald-500/20">Active</span>
                        <button 
                          onClick={() => onDeleteBatch(batch.id)}
                          disabled={deletingBatchId === batch.id}
                          className="p-2 rounded-lg bg-red-500/10 text-red-500 hover:bg-red-500 hover:text-white transition-all disabled:opacity-50"
                          title="Delete Batch Data"
                        >
                          {deletingBatchId === batch.id ? <RefreshCw className="w-3.5 h-3.5 animate-spin" /> : <Trash2 className="w-3.5 h-3.5" />}
                        </button>
                      </div>
                    )}
                    {batch.status === 'deleted' && <span className="text-[10px] font-black uppercase px-2 py-1 rounded bg-red-500/10 text-red-500 border border-red-500/20">Deleted</span>}
                  </div>

                  <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
                    <div className="bg-black/20 p-3 rounded-xl border border-white/5">
                      <div className="text-[9px] font-black text-gray-500 uppercase tracking-tighter mb-1">Processed</div>
                      <div className="text-lg font-black text-white tracking-tight">{batch.total_rows.toLocaleString()}</div>
                    </div>
                    <div className="bg-emerald-500/5 p-3 rounded-xl border border-emerald-500/10">
                      <div className="text-[9px] font-black text-emerald-500/60 uppercase tracking-tighter mb-1">Valid</div>
                      <div className="text-lg font-black text-emerald-400 tracking-tight">{batch.valid_count.toLocaleString()}</div>
                    </div>
                    <div className="bg-red-500/5 p-3 rounded-xl border border-red-500/10">
                      <div className="text-[9px] font-black text-red-500/60 uppercase tracking-tighter mb-1">Invalid</div>
                      <div className="text-lg font-black text-red-400 tracking-tight">{batch.invalid_count.toLocaleString()}</div>
                    </div>
                    <div className="bg-blue-500/5 p-3 rounded-xl border border-blue-500/10">
                      <div className="text-[9px] font-black text-blue-500/60 uppercase tracking-tighter mb-1">New</div>
                      <div className="text-lg font-black text-blue-400 tracking-tight">{batch.new_records.toLocaleString()}</div>
                    </div>
                    <div className="bg-amber-500/5 p-3 rounded-xl border border-amber-500/10">
                      <div className="text-[9px] font-black text-amber-500/60 uppercase tracking-tighter mb-1">Updated</div>
                      <div className="text-lg font-black text-amber-400 tracking-tight">{batch.updated_records.toLocaleString()}</div>
                    </div>
                  </div>

                  {batch.status !== 'deleted' && (
                    <div className="mt-4 pt-4 border-t border-white/5 flex flex-wrap gap-2">
                       {batch.valid_url && (
                        <a href={batch.valid_url} className="px-3 py-1.5 rounded-lg bg-emerald-500/10 text-emerald-500 text-[10px] font-bold hover:bg-emerald-500 hover:text-white transition-all flex items-center gap-1.5">
                          <Download className="w-3 h-3" /> Valid.xlsx
                        </a>
                      )}
                      {batch.invalid_url && (
                        <a href={batch.invalid_url} className="px-3 py-1.5 rounded-lg bg-red-500/10 text-red-500 text-[10px] font-bold hover:bg-red-500 hover:text-white transition-all flex items-center gap-1.5">
                          <Download className="w-3 h-3" /> Invalid.xlsx
                        </a>
                      )}
                      {batch.pdf_url && (
                        <a href={batch.pdf_url} className="px-3 py-1.5 rounded-lg bg-blue-500/10 text-blue-500 text-[10px] font-bold hover:bg-blue-500 hover:text-white transition-all flex items-center gap-1.5">
                          <FileText className="w-3 h-3" /> Valid_Report.pdf
                        </a>
                      )}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default BatchHistoryModal;
