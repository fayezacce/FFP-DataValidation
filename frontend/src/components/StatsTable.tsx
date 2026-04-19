/**
 * FFP Data Validation Platform - Hierarchical Statistics Table with Multi-Select
 */

import React, { useState } from "react";
import { 
  ChevronRight, ChevronDown, MapPin, CheckCircle2, FileWarning, 
  Clock, FileSpreadsheet, FileText, Download, Square, CheckSquare, MinusSquare,
  BarChart3, Edit2, Trash2, Loader2
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
  downloadFileWithAuth: (url: string, filename: string, onStart?: () => void, onFinish?: () => void) => void;
  selectedDivisions: Set<string>;
  selectedDistricts: Set<string>;
  onToggleDivision: (divName: string) => void;
  onToggleDistrict: (divName: string, distName: string) => void;
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
  downloadFileWithAuth,
  selectedDivisions,
  selectedDistricts,
  onToggleDivision,
  onToggleDistrict,
}) => {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [downloadingId, setDownloadingId] = useState<string | null>(null);

  const toggleExpand = (id: string) => {
    setExpanded(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const expandAll = () => {
    const next: Record<string, boolean> = {};
    hierarchy.forEach(div => {
      next[div.name] = true;
      Object.keys(div.districts).forEach(dist => {
        next[div.name + dist] = true;
      });
    });
    setExpanded(next);
  };

  const collapseAll = () => {
    setExpanded({});
  };

  const formatDate = (iso: string) => {
    return new Date(iso).toLocaleString('en-GB', { 
      day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' 
    });
  };

  // Remaining Calculation: Target (Quota) - Valid Records
  const getRemaining = (target: number, valid: number) => {
    const rem = target - valid;
    if (rem < 0) return `(${Math.abs(rem)})`;
    return rem.toLocaleString();
  };

  const isDivFullySelected = (divNode: any) => selectedDivisions.has(divNode.name);
  const isDivPartiallySelected = (divNode: any) => {
    if (selectedDivisions.has(divNode.name)) return false;
    return Object.values(divNode.districts).some((d: any) => selectedDistricts.has(`${divNode.name}|${d.name}`));
  };
  const isDistSelected = (divName: string, distName: string) => 
    selectedDivisions.has(divName) || selectedDistricts.has(`${divName}|${distName}`);

  // Header Checkbox Logic
  const allDivsSelected = hierarchy.length > 0 && hierarchy.every(div => selectedDivisions.has(div.name));
  const someDivsSelected = hierarchy.some(div => selectedDivisions.has(div.name) || Object.values(div.districts).some((d: any) => selectedDistricts.has(`${div.name}|${d.name}`)));

  const handleToggleAll = () => {
    if (allDivsSelected) {
      hierarchy.forEach(div => {
        if (selectedDivisions.has(div.name)) onToggleDivision(div.name);
      });
    } else {
      hierarchy.forEach(div => {
        if (!selectedDivisions.has(div.name)) onToggleDivision(div.name);
      });
    }
  };

  return (
    <div className="space-y-4">
      {/* Expand/Collapse Controls */}
      <div className="flex justify-end gap-3 px-1">
        <button onClick={expandAll} className="text-[10px] uppercase font-bold text-slate-400 hover:text-white transition-colors underline underline-offset-4">Expand All</button>
        <button onClick={collapseAll} className="text-[10px] uppercase font-bold text-slate-400 hover:text-white transition-colors underline underline-offset-4">Collapse All</button>
      </div>

      <div className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden shadow-2xl">
        <div className="overflow-x-auto custom-scrollbar">
          <table className="w-full text-left border-collapse min-w-[1100px]">
            <thead>
              <tr className="bg-[#1a1a1c] text-gray-500 text-[10px] uppercase font-black tracking-widest border-b border-[#2a2a2e]">
                <th className="px-3 py-4 w-10 text-center">
                  <button onClick={handleToggleAll} className="p-0.5 rounded hover:bg-white/10 transition-colors">
                    {allDivsSelected ? <CheckSquare className="w-4 h-4 text-emerald-500" /> : someDivsSelected ? <MinusSquare className="w-4 h-4 text-amber-500" /> : <Square className="w-4 h-4 text-gray-600" />}
                  </button>
                </th>
                <th className="px-2 py-4 w-10"></th>
                <th className="px-6 py-4 min-w-[180px]">Division / District / Upazila</th>
                <th className="px-4 py-4 text-center">Target</th>
                <th className="px-4 py-4 text-center">Total</th>
                <th className="px-4 py-4 text-center text-emerald-400/80 font-black">Valid</th>
                <th className="px-4 py-4 text-center text-amber-400 font-black">Rem.</th>
                <th className="px-4 py-4 text-center text-red-400/80 font-black">Invalid</th>
                <th className="px-6 py-4 text-center">Actions & Downloads</th>
                <th className="px-6 py-4 text-right">Updated</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[#1e1e20]">
              {hierarchy.map((divNode: any) => (
                <React.Fragment key={divNode.name}>
                  {/* Division Row */}
                  <tr className="bg-[#0f0f11] hover:bg-[#161618] transition-colors group">
                    <td className="px-3 py-4 text-center">
                      <button onClick={() => onToggleDivision(divNode.name)} className="p-0.5 rounded hover:bg-white/10 transition-colors">
                        {isDivFullySelected(divNode) ? <CheckSquare className="w-4 h-4 text-emerald-500" /> : isDivPartiallySelected(divNode) ? <MinusSquare className="w-4 h-4 text-amber-500" /> : <Square className="w-4 h-4 text-gray-600" />}
                      </button>
                    </td>
                    <td className="px-2 py-4 text-center cursor-pointer" onClick={() => toggleExpand(divNode.name)}>
                      {expanded[divNode.name] ? <ChevronDown className="w-4 h-4 text-emerald-500" /> : <ChevronRight className="w-4 h-4 text-gray-600" />}
                    </td>
                    <td className="px-6 py-4 cursor-pointer" onClick={() => toggleExpand(divNode.name)}>
                      <div className="flex items-center gap-3">
                        <MapPin className="w-4 h-4 text-emerald-500/50" />
                        <span className="font-black text-lg text-emerald-500 uppercase tracking-tight">{divNode.name}</span>
                      </div>
                    </td>
                    <td className="px-4 py-4 text-center font-bold text-gray-500">{divNode.quota.toLocaleString()}</td>
                    <td className="px-4 py-4 text-center font-bold text-gray-300">{divNode.total.toLocaleString()}</td>
                    <td className="px-4 py-4 text-center font-bold text-emerald-400">{divNode.valid.toLocaleString()}</td>
                    <td className="px-4 py-4 text-center font-bold text-amber-500">{getRemaining(divNode.quota, divNode.valid)}</td>
                    <td className="px-4 py-4 text-center font-bold text-red-500">{divNode.invalid.toLocaleString()}</td>
                    <td className="px-6 py-4"></td>
                    <td className="px-6 py-4"></td>
                  </tr>

                  {expanded[divNode.name] && Object.values(divNode.districts).map((distNode: any) => (
                    <React.Fragment key={distNode.name}>
                      {/* District Row */}
                      <tr className="bg-[#141416] hover:bg-[#1c1c1e] transition-colors group">
                        <td className="px-3 py-4 text-center pl-8">
                          <button onClick={() => onToggleDistrict(divNode.name, distNode.name)} className="p-0.5 rounded hover:bg-white/10 transition-colors">
                            {isDistSelected(divNode.name, distNode.name) ? <CheckSquare className="w-3.5 h-3.5 text-blue-500" /> : <Square className="w-3.5 h-3.5 text-gray-700" />}
                          </button>
                        </td>
                        <td className="px-2 py-4 text-center pl-8 cursor-pointer" onClick={() => toggleExpand(divNode.name + distNode.name)}>
                          {expanded[divNode.name + distNode.name] ? <ChevronDown className="w-3 h-3 text-blue-500" /> : <ChevronRight className="w-3 h-3 text-gray-700" />}
                        </td>
                        <td className="px-6 py-4 pl-12 cursor-pointer" onClick={() => toggleExpand(divNode.name + distNode.name)}>
                          <span className="font-bold text-blue-400 uppercase tracking-wide">{distNode.name}</span>
                        </td>
                        <td className="px-4 py-4 text-center text-sm font-semibold text-gray-600">{distNode.quota.toLocaleString()}</td>
                        <td className="px-4 py-4 text-center text-sm font-semibold text-gray-400">{distNode.total.toLocaleString()}</td>
                        <td className="px-4 py-4 text-center text-sm font-semibold text-emerald-500/80">{distNode.valid.toLocaleString()}</td>
                        <td className="px-4 py-4 text-center text-sm font-semibold text-amber-500/80">{getRemaining(distNode.quota, distNode.valid)}</td>
                        <td className="px-4 py-4 text-center text-sm font-semibold text-red-500/80">{distNode.invalid.toLocaleString()}</td>
                        <td className="px-6 py-4"></td>
                        <td className="px-6 py-4"></td>
                      </tr>

                      {expanded[divNode.name + distNode.name] && distNode.upazilas.map((upz: StatsEntry) => (
                        <tr key={upz.id} className="bg-[#121214] hover:bg-emerald-500/[0.02] transition-colors group/row border-l-2 border-transparent hover:border-emerald-500/30">
                          <td className="px-3 py-4"></td>
                          <td className="px-2 py-4 text-center">
                            <MapPin className="w-3 h-3 text-slate-700 mx-auto" />
                          </td>
                          <td className="px-6 py-4 pl-16">
                            <span className="font-bold text-white group-hover/row:text-emerald-400 transition-colors uppercase tracking-tight">{upz.upazila}</span>
                          </td>
                          <td className="px-4 py-4 text-center text-sm font-medium text-slate-500">{upz.quota.toLocaleString()}</td>
                          <td className="px-4 py-4 text-center text-sm font-medium text-slate-400">{upz.total.toLocaleString()}</td>
                          <td className="px-4 py-4 text-center text-sm font-bold text-emerald-500/70">{upz.valid.toLocaleString()}</td>
                          <td className="px-4 py-4 text-center text-sm font-bold text-amber-500/70">{getRemaining(upz.quota, upz.valid)}</td>
                          <td className="px-4 py-4 text-center text-sm font-bold text-red-500/70">{upz.invalid.toLocaleString()}</td>
                          <td className="px-6 py-4">
                            <div className="flex items-center justify-center gap-2">
                              {/* Action Tools: Downloads with loading states */}
                              <div className="flex bg-slate-900/50 rounded-lg p-1 border border-slate-800">
                                <button 
                                  disabled={!!downloadingId}
                                  onClick={() => {
                                    const id = `${upz.id}-checked`;
                                    const qs = new URLSearchParams({
                                      division: upz.division,
                                      district: upz.district,
                                      upazila: upz.upazila,
                                      upazila_id: upz.id?.toString() || "",
                                    });
                                    downloadFileWithAuth(
                                      `/api/export/live-checked?${qs.toString()}`, 
                                      `${upz.district}_${upz.upazila}_Checked.xlsx`,
                                      () => setDownloadingId(id),
                                      () => setDownloadingId(null)
                                    );
                                  }}
                                  className={`p-1.5 rounded transition-all flex items-center gap-1 ${
                                    downloadingId === `${upz.id}-checked` ? 'bg-cyan-500/20 text-cyan-400' : 'hover:bg-cyan-500/20 text-cyan-500'
                                  } ${!!downloadingId ? 'opacity-50 cursor-not-allowed' : ''}`} 
                                  title="Checked xlsx (All records, Highlighted)"
                                >
                                  {downloadingId === `${upz.id}-checked` ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <FileSpreadsheet className="w-3.5 h-3.5" />}
                                  <span className="text-[10px] font-bold">Checked</span>
                                </button>

                                <div className="w-[1px] bg-slate-800 mx-1"></div>

                                <button 
                                  disabled={!!downloadingId}
                                  onClick={() => {
                                    const id = `${upz.id}-valid`;
                                    downloadFileWithAuth(
                                      buildLiveExportUrl(upz, "xlsx"), 
                                      `${upz.district}_${upz.upazila}_Valid.xlsx`,
                                      () => setDownloadingId(id),
                                      () => setDownloadingId(null)
                                    );
                                  }}
                                  className={`p-1.5 rounded transition-all flex items-center gap-1 ${
                                    downloadingId === `${upz.id}-valid` ? 'bg-emerald-500/20 text-emerald-400' : 'hover:bg-emerald-500/20 text-emerald-500'
                                  } ${!!downloadingId ? 'opacity-50 cursor-not-allowed' : ''}`}
                                  title="Valid Records Only"
                                >
                                  {downloadingId === `${upz.id}-valid` ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <CheckCircle2 className="w-3.5 h-3.5" />}
                                  <span className="text-[10px] font-bold">Valid</span>
                                </button>

                                <div className="w-[1px] bg-slate-800 mx-1"></div>

                                <button 
                                  disabled={!!downloadingId || !upz.invalid}
                                  onClick={() => {
                                    const id = `${upz.id}-invalid`;
                                    downloadFileWithAuth(
                                      buildLiveExportInvalidUrl(upz, "pdf"), 
                                      `${upz.district}_${upz.upazila}_Invalid.pdf`,
                                      () => setDownloadingId(id),
                                      () => setDownloadingId(null)
                                    );
                                  }}
                                  className={`p-1.5 rounded transition-all flex items-center gap-1 ${
                                    downloadingId === `${upz.id}-invalid` ? 'bg-red-500/20 text-red-400' :
                                    !upz.invalid ? 'text-slate-700' :
                                    'hover:bg-red-500/20 text-red-500'
                                  } ${(!!downloadingId || !upz.invalid) ? 'opacity-40 cursor-not-allowed' : ''}`} 
                                  title={!upz.invalid ? "No invalid records" : "Invalid Summary PDF"}
                                >
                                  {downloadingId === `${upz.id}-invalid` ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <FileText className="w-3.5 h-3.5" />}
                                  <span className="text-[10px] font-bold">Invalid</span>
                                </button>
                              </div>

                              <div className="w-[1px] h-6 bg-slate-800/50 mx-1"></div>

                              {/* Action Tools */}
                              <button onClick={() => onRecheck(upz)} disabled={recheckLoading === `${upz.division}|${upz.district}|${upz.upazila}`}
                                className={`p-1.5 rounded-lg ${recheckLoading === `${upz.division}|${upz.district}|${upz.upazila}` ? 'bg-amber-500/20 animate-spin' : 'bg-amber-500/10'} text-amber-500 hover:bg-amber-500 hover:text-white transition-all`}
                                title="Reanalyze (Fraud Detection)">
                                <BarChart3 className="w-3.5 h-3.5" />
                              </button>
                              
                              <button onClick={() => onOpenHistory(upz)} className="p-1.5 rounded-lg bg-blue-500/10 text-blue-500 hover:bg-blue-500 hover:text-white transition-all" title="Upload History">
                                <Clock className="w-3.5 h-3.5" />
                              </button>

                              {isAdmin && (
                                <>
                                  <button onClick={() => onOpenEdit(upz)} className="p-1.5 rounded-lg bg-gray-700/20 text-gray-500 hover:bg-gray-700 hover:text-white transition-all" title="Edit">
                                    <Edit2 className="w-3.5 h-3.5" />
                                  </button>
                                  <button onClick={() => onDeleteUpazila(upz)} className="p-1.5 rounded-lg bg-red-900/10 text-red-800 hover:bg-red-600 hover:text-white transition-all" title="Wipe">
                                    <Trash2 className="w-3.5 h-3.5" />
                                  </button>
                                </>
                              )}
                            </div>
                          </td>
                          <td className="px-6 py-4 text-right text-[10px] text-slate-500 font-mono">
                            {formatDate(upz.updated_at)}
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
    </div>
  );
};

export default StatsTable;
