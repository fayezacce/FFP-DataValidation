/**
 * FFP Data Validation Platform - Hierarchical Statistics Table
 */

import React, { useState } from "react";
import { 
  ChevronRight, ChevronDown, MapPin, Hash, CheckCircle2, FileWarning, 
  Clock, FileSpreadsheet, FileText, Printer, Search as SearchIcon, 
  Edit2, Trash2, RefreshCw, BarChart3, Download
} from "lucide-react";
import { StatsEntry } from "@/types/ffp";

interface StatsTableProps {
  hierarchy: any[];
  isAdmin: boolean;
  onOpenHistory: (entry: StatsEntry) => void;
  onOpenEdit: (entry: StatsEntry) => void;
  onDeleteUpazila: (entry: StatsEntry) => void;
  onRecheck: (entry: StatsEntry) => void;
  recheckLoading: string | null;
  recheckResult: { msg: string; flagged: number } | null;
  buildLiveExportUrl: (entry: StatsEntry, fmt: string) => string;
  buildLiveExportInvalidUrl: (entry: StatsEntry, fmt: string) => string;
  downloadFileWithAuth: (url: string, filename: string) => void;
}

const StatsTable: React.FC<StatsTableProps> = ({
  hierarchy,
  isAdmin,
  onOpenHistory,
  onOpenEdit,
  onDeleteUpazila,
  onRecheck,
  recheckLoading,
  recheckResult,
  buildLiveExportUrl,
  buildLiveExportInvalidUrl,
  downloadFileWithAuth
}) => {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  const toggleExpand = (id: string) => {
    setExpanded(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const formatDate = (iso: string) => {
    return new Date(iso).toLocaleString('en-GB', { 
      day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' 
    });
  };

  const getHealthColor = (valid: number, total: number) => {
    if (total === 0) return "";
    const pct = (valid / total) * 100;
    if (pct >= 100) return "text-emerald-400";
    if (pct >= 90) return "text-yellow-400";
    return "text-red-400";
  };

  return (
    <div className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden shadow-2xl">
      <div className="overflow-x-auto custom-scrollbar">
        <table className="w-full text-left border-collapse min-w-[1000px]">
          <thead>
            <tr className="bg-[#1a1a1c] text-gray-500 text-[10px] uppercase font-black tracking-widest border-b border-[#2a2a2e]">
              <th className="px-6 py-4 w-12"></th>
              <th className="px-6 py-4 min-w-[200px]">Location (Division / District / Upazila)</th>
              <th className="px-6 py-4 text-center">Total (Unique)</th>
              <th className="px-6 py-4 text-center">Validated</th>
              <th className="px-6 py-4 text-center">Invalid</th>
              <th className="px-6 py-4 text-center">Quota Status</th>
              <th className="px-6 py-4 text-center">Action</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[#1e1e20]">
            {hierarchy.map((divNode: any) => (
              <React.Fragment key={divNode.name}>
                {/* Division Row */}
                <tr 
                  className="bg-[#0f0f11] hover:bg-[#161618] cursor-pointer transition-colors group"
                  onClick={() => toggleExpand(divNode.name)}
                >
                  <td className="px-6 py-4 text-center">
                    {expanded[divNode.name] ? <ChevronDown className="w-4 h-4 text-emerald-500" /> : <ChevronRight className="w-4 h-4 text-gray-600" />}
                  </td>
                  <td className="px-6 py-4">
                    <div className="flex items-center gap-3">
                      <div className="w-8 h-8 rounded-lg bg-emerald-500/10 flex items-center justify-center">
                        <MapPin className="w-4 h-4 text-emerald-500" />
                      </div>
                      <span className="font-black text-lg text-emerald-500 uppercase tracking-tight">{divNode.name}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 text-center font-bold text-gray-300">{divNode.total.toLocaleString()}</td>
                  <td className="px-6 py-4 text-center font-bold text-emerald-400">{divNode.valid.toLocaleString()}</td>
                  <td className="px-6 py-4 text-center font-bold text-red-500">{divNode.invalid.toLocaleString()}</td>
                  <td className="px-6 py-4 text-center">
                    <div className="inline-flex items-center px-2 py-1 rounded-md bg-white/5 border border-white/10">
                      <span className="text-[10px] text-gray-500 uppercase font-black mr-2">TTL Quota:</span>
                      <span className="text-sm font-black text-white">{divNode.quota.toLocaleString()}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4"></td>
                </tr>

                {expanded[divNode.name] && Object.values(divNode.districts).map((distNode: any) => (
                  <React.Fragment key={distNode.name}>
                    {/* District Row */}
                    <tr 
                      className="bg-[#141416] hover:bg-[#1c1c1e] cursor-pointer transition-colors group"
                      onClick={() => toggleExpand(divNode.name + distNode.name)}
                    >
                      <td className="px-6 py-4 text-center pl-10">
                        {expanded[divNode.name + distNode.name] ? <ChevronDown className="w-3 h-3 text-blue-500" /> : <ChevronRight className="w-3 h-3 text-gray-700" />}
                      </td>
                      <td className="px-6 py-4 pl-12">
                        <div className="flex items-center gap-3">
                          <div className="w-6 h-6 rounded-md bg-blue-500/10 flex items-center justify-center">
                            <MapPin className="w-3 h-3 text-blue-500" />
                          </div>
                          <span className="font-bold text-blue-400 uppercase tracking-wide">{distNode.name}</span>
                        </div>
                      </td>
                      <td className="px-6 py-4 text-center text-sm font-semibold text-gray-400">{distNode.total.toLocaleString()}</td>
                      <td className="px-6 py-4 text-center text-sm font-semibold text-emerald-500/80">{distNode.valid.toLocaleString()}</td>
                      <td className="px-6 py-4 text-center text-sm font-semibold text-red-500/80">{distNode.invalid.toLocaleString()}</td>
                      <td className="px-6 py-4 text-center">
                        <span className="text-xs font-bold text-gray-500">{distNode.quota.toLocaleString()}</span>
                      </td>
                      <td className="px-6 py-4"></td>
                    </tr>

                    {expanded[divNode.name + distNode.name] && distNode.upazilas.map((upz: StatsEntry) => (
                      <tr key={upz.id} className="bg-[#121214] hover:bg-emerald-500/[0.02] transition-colors group/row">
                        <td className="px-6 py-4"></td>
                        <td className="px-6 py-4 pl-20 min-w-[300px]">
                          <div className="flex flex-col">
                            <div className="flex items-center gap-2 mb-1">
                              <span className="font-bold text-white group-hover/row:text-emerald-400 transition-colors">{upz.upazila}</span>
                              {upz.version > 1 && (
                                <span className="bg-amber-500/10 text-amber-500 text-[8px] font-black uppercase px-1.5 py-0.5 rounded border border-amber-500/20">V{upz.version}</span>
                              )}
                            </div>
                            <div className="flex items-center text-[10px] text-gray-600 gap-1.5 flex-wrap">
                              <span className="flex items-center gap-1"><Clock className="w-2.5 h-2.5" /> {formatDate(upz.updated_at)}</span>
                              <span className="text-gray-800">|</span>
                              <span className="text-emerald-500/60 font-medium truncate max-w-[150px]" title={upz.filename}>{upz.filename}</span>
                            </div>
                          </div>
                        </td>
                        <td className="px-6 py-4 text-center">
                          <div className="flex flex-col items-center">
                            <span className="text-sm font-bold text-gray-300">{upz.total.toLocaleString()}</span>
                            <span className="text-[9px] font-black text-gray-600 uppercase tracking-tighter">Total Unique</span>
                          </div>
                        </td>
                        <td className="px-6 py-4 text-center">
                          <div className="flex flex-col items-center">
                            <span className={`text-sm font-bold ${getHealthColor(upz.valid, upz.total)}`}>{upz.valid.toLocaleString()}</span>
                            <span className="text-[9px] font-black text-gray-600 uppercase tracking-tighter">Success</span>
                          </div>
                        </td>
                        <td className="px-6 py-4 text-center">
                          <div className="flex flex-col items-center">
                            <span className="text-sm font-bold text-red-500/80">{upz.invalid.toLocaleString()}</span>
                            <span className="text-[9px] font-black text-gray-600 uppercase tracking-tighter">Failed</span>
                          </div>
                        </td>
                        <td className="px-6 py-4 text-center">
                          <div className="w-full max-w-[120px] mx-auto space-y-1.5">
                            <div className="flex justify-between text-[9px] font-black uppercase tracking-tighter">
                              <span className="text-gray-500">Utilization</span>
                              <span className="text-gray-400">{upz.quota > 0 ? Math.round((upz.valid / upz.quota) * 100) : 0}%</span>
                            </div>
                            <div className="h-1.5 w-full bg-black/40 rounded-full overflow-hidden border border-white/5">
                              <div 
                                className={`h-full transition-all duration-1000 ${upz.valid > upz.quota ? 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.5)]' : 'bg-emerald-500'}`}
                                style={{ width: `${Math.min(100, (upz.valid / (upz.quota || 1)) * 100)}%` }}
                              ></div>
                            </div>
                            <div className="text-[9px] font-medium text-gray-600 text-center uppercase">Quota: {upz.quota.toLocaleString()}</div>
                          </div>
                        </td>
                        <td className="px-6 py-4">
                          <div className="flex items-center justify-center gap-1.5 opacity-40 group-hover/row:opacity-100 transition-all duration-300">
                            {/* Live Export Dropdown Placeholder - Simplified for modular component */}
                            <button 
                              onClick={() => downloadFileWithAuth(buildLiveExportUrl(upz, "xlsx"), `${upz.district}_${upz.upazila}_Live.xlsx`)}
                              className="p-2 rounded-lg bg-emerald-500/10 text-emerald-500 hover:bg-emerald-500 hover:text-white transition-all"
                              title="Live Export Valid"
                            >
                              <Download className="w-3.5 h-3.5" />
                            </button>
                            
                            <button 
                              onClick={() => onRecheck(upz)}
                              disabled={recheckLoading === `${upz.division}|${upz.district}|${upz.upazila}`}
                              className={`p-2 rounded-lg ${recheckLoading === `${upz.division}|${upz.district}|${upz.upazila}` ? 'bg-amber-500/20 animate-spin' : 'bg-amber-500/10'} text-amber-500 hover:bg-amber-500 hover:text-white transition-all`}
                              title="Fraud Detection"
                            >
                              <BarChart3 className="w-3.5 h-3.5" />
                            </button>

                            <button 
                              onClick={() => onOpenHistory(upz)}
                              className="p-2 rounded-lg bg-blue-500/10 text-blue-500 hover:bg-blue-500 hover:text-white transition-all"
                              title="Upload History"
                            >
                              <Clock className="w-3.5 h-3.5" />
                            </button>

                            {isAdmin && (
                              <>
                                <button 
                                  onClick={() => onOpenEdit(upz)}
                                  className="p-2 rounded-lg bg-gray-500/10 text-gray-400 hover:bg-gray-500 hover:text-white transition-all"
                                  title="Edit Records"
                                >
                                  <Edit2 className="w-3.5 h-3.5" />
                                </button>
                                <button 
                                  onClick={() => onDeleteUpazila(upz)}
                                  className="p-2 rounded-lg bg-red-500/10 text-red-500 hover:bg-red-500 hover:text-white transition-all"
                                  title="Wipe Data"
                                >
                                  <Trash2 className="w-3.5 h-3.5" />
                                </button>
                              </>
                            )}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </React.Fragment>
                ))}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default StatsTable;
