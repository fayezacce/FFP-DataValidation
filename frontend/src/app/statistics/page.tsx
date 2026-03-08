"use client";

import React, { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { BarChart3, ArrowLeft, RefreshCw, MapPin, Clock, Hash, FileSpreadsheet, CheckCircle2, FileWarning, FileText, Printer, Edit2, X, Save, Search as SearchIcon, Download } from "lucide-react";
import { fetchWithAuth, getBackendUrl, downloadFileWithAuth, isAuthenticated, isAdmin } from "@/lib/auth";

interface StatsEntry {
  division: string;
  district: string;
  upazila: string;
  total: number;
  valid: number;
  invalid: number;
  filename: string;
  version: number;
  created_at: string;
  updated_at: string;
  pdf_url?: string;
  excel_url?: string;
  excel_valid_url?: string;
  excel_invalid_url?: string;
}

interface StatsResponse {
  entries: StatsEntry[];
  grand_total: { total: number; valid: number; invalid: number };
}

export default function StatisticsPage() {
  const [data, setData] = useState<StatsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const router = useRouter();

  useEffect(() => {
    if (!isAuthenticated()) {
      router.push("/login?callback=/statistics");
    }
  }, [router]);

  // Edit State
  const [editingEntry, setEditingEntry] = useState<StatsEntry | null>(null);
  const [editForm, setEditForm] = useState({ old_district: "", old_upazila: "", district: "", upazila: "", total: 0, valid: 0, invalid: 0 });
  const [savingEdit, setSavingEdit] = useState(false);


  const openEditModal = (entry: StatsEntry) => {
    setEditingEntry(entry);
    setEditForm({ 
      old_district: entry.district,
      old_upazila: entry.upazila,
      district: entry.district, 
      upazila: entry.upazila, 
      total: entry.total, 
      valid: entry.valid, 
      invalid: entry.invalid 
    });
  };

  const handleSaveEdit = async () => {
    if (!editingEntry) return;
    setSavingEdit(true);
    try {
      const res = await fetchWithAuth(`${getBackendUrl()}/statistics/update`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          old_district: editForm.old_district,
          old_upazila: editForm.old_upazila,
          new_district: editForm.district,
          new_upazila: editForm.upazila,
          total: editForm.total,
          valid: editForm.valid,
          invalid: editForm.invalid,
        }),
      });
      if (!res.ok) {
        const errData = await res.json();
        throw new Error(errData.detail || "Failed to update statistics");
      }
      
      setEditingEntry(null);
      fetchStats();
    } catch (err: any) {
      alert("Error saving: " + err.message);
    } finally {
      setSavingEdit(false);
    }
  };

  const fetchStats = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetchWithAuth(`${getBackendUrl()}/statistics`);
      if (!res.ok) throw new Error("Failed to load statistics");
      const json = await res.json();
      setData(json);
    } catch (err: any) {
      setError(err.message || "Failed to load statistics");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchStats();
  }, []);

  // Group entries by division
  const groupedByDivision: Record<string, StatsEntry[]> = {};
  if (data?.entries) {
    for (const entry of data.entries) {
      const div = entry.division || "Unknown";
      if (!groupedByDivision[div]) groupedByDivision[div] = [];
      groupedByDivision[div].push(entry);
    }
  }

  const divisionKeys = Object.keys(groupedByDivision).sort();

  // Per-division totals
  const divisionTotals = (entries: StatsEntry[]) => ({
    total: entries.reduce((s, e) => s + e.total, 0),
    valid: entries.reduce((s, e) => s + e.valid, 0),
    invalid: entries.reduce((s, e) => s + e.invalid, 0),
  });

  const formatDate = (iso: string) => {
    if (!iso) return "—";
    try {
      const d = new Date(iso);
      return d.toLocaleString("en-GB", { day: "2-digit", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" });
    } catch {
      return iso;
    }
  };

  const getHealthColor = (valid: number, total: number) => {
    if (total === 0) return "";
    const pct = (valid / total) * 100;
    if (pct >= 100) return "text-emerald-400";
    if (pct >= 90) return "text-yellow-400";
    return "text-red-400";
  };

  return (
    <main className="min-h-screen p-4 md:p-8 lg:p-12 text-slate-200 flex flex-col">
      <div className="max-w-7xl mx-auto space-y-8 flex-1 w-full">

        {/* Header */}
        <header className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
          <div className="flex items-center gap-4">
            <Link
              href="/"
              className="p-2 rounded-lg bg-slate-800/50 border border-slate-700 hover:bg-slate-700/50 transition-colors print:hidden"
            >
              <ArrowLeft className="w-5 h-5 text-slate-400" />
            </Link>
            <div>
              <h1 className="text-3xl md:text-4xl font-extrabold tracking-tight bg-gradient-to-r from-cyan-400 to-blue-400 bg-clip-text text-transparent">
                Statistics Dashboard
              </h1>
              <p className="text-sm text-slate-400 mt-1">Validation results by Division, District & Upazila</p>
            </div>
          </div>
          <div className="flex items-center gap-3 print:hidden">
            <Link
              href="/search"
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600/20 border border-indigo-500/30 text-indigo-400 hover:bg-indigo-600/30 transition-all font-medium"
            >
              <SearchIcon className="w-4 h-4" />
              <span>Search Records</span>
            </Link>
            <button
              onClick={() => window.print()}
              disabled={loading}
              className="bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-300 px-4 py-2 rounded-lg font-medium flex items-center gap-2 transition-all text-sm"
            >
              <Printer className="w-4 h-4" />
              Print
            </button>
            <button
              onClick={fetchStats}
              disabled={loading}
              className="bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-300 px-4 py-2 rounded-lg font-medium flex items-center gap-2 transition-all text-sm"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? "animate-spin text-blue-400" : ""}`} />
              Refresh
            </button>
          </div>
        </header>

        {/* Error */}
        {error && (
          <div className="p-4 rounded-lg bg-red-500/10 border border-red-500/50 text-red-400">
            {error}
          </div>
        )}

        {/* Loading */}
        {loading && !data && (
          <div className="glass-panel rounded-2xl p-12 flex flex-col items-center gap-4">
            <div className="w-8 h-8 border-2 border-slate-600 border-t-blue-400 rounded-full animate-spin" />
            <p className="text-slate-400">Loading statistics...</p>
          </div>
        )}

        {/* Empty state */}
        {!loading && data && data.entries.length === 0 && (
          <div className="glass-panel rounded-2xl p-12 flex flex-col items-center gap-4 text-center print:hidden">
            <BarChart3 className="w-16 h-16 text-slate-600" />
            <h2 className="text-xl font-semibold text-slate-300">No Statistics Yet</h2>
            <p className="text-slate-500 max-w-md">Validate Excel files to start building your statistics dashboard. Results are accumulated and grouped by Division, District, and Upazila.</p>
            <Link href="/" className="mt-4 bg-indigo-600 hover:bg-indigo-500 text-white px-6 py-2.5 rounded-lg font-medium transition-all">
              Upload & Validate
            </Link>
          </div>
        )}

        {/* Grand Total Cards */}
        {data && data.entries.length > 0 && (
          <>
            <div className="grid grid-cols-3 gap-5">
              <div className="glass-panel p-5 rounded-2xl border-t-4 border-t-blue-500">
                <p className="text-xs font-medium text-slate-500 uppercase tracking-wider">Total Records</p>
                <p className="text-3xl font-bold text-blue-400 mt-1">{data.grand_total.total.toLocaleString()}</p>
              </div>
              <div className="glass-panel p-5 rounded-2xl border-t-4 border-t-emerald-500">
                <p className="text-xs font-medium text-slate-500 uppercase tracking-wider">Valid Records</p>
                <p className="text-3xl font-bold text-emerald-400 mt-1">{data.grand_total.valid.toLocaleString()}</p>
              </div>
              <div className="glass-panel p-5 rounded-2xl border-t-4 border-t-red-500">
                <p className="text-xs font-medium text-slate-500 uppercase tracking-wider">Invalid Records</p>
                <p className="text-3xl font-bold text-red-400 mt-1">{data.grand_total.invalid.toLocaleString()}</p>
              </div>
            </div>

            {/* Statistics Table grouped by Division */}
            <div className="glass-panel rounded-2xl overflow-hidden">
              <div className="p-5 border-b border-slate-700/50 flex items-center gap-3">
                <BarChart3 className="w-5 h-5 text-cyan-400" />
                <h2 className="text-xl font-semibold">Detailed Statistics</h2>
                <span className="text-sm text-slate-500 ml-auto">{data.entries.length} entries across {divisionKeys.length} divisions</span>
              </div>

              <div className="overflow-x-auto">
                <table className="w-full text-sm text-left">
                  <thead className="bg-slate-800/80 text-slate-300 uppercase text-xs sticky top-0 z-10">
                    <tr>
                      <th className="px-5 py-3 font-medium">Division</th>
                      <th className="px-5 py-3 font-medium">District</th>
                      <th className="px-5 py-3 font-medium">Upazila</th>
                      <th className="px-5 py-3 font-medium text-right">Total</th>
                      <th className="px-5 py-3 font-medium text-right">Valid</th>
                      <th className="px-5 py-3 font-medium text-right">Invalid</th>
                      <th className="px-5 py-3 font-medium text-center">
                        <span className="flex items-center justify-center gap-1"><Hash className="w-3 h-3" />Ver</span>
                      </th>
                      <th className="px-5 py-3 font-medium text-center">Downloads</th>
                      <th className="px-5 py-3 font-medium cursor-default" title="Last Updated">
                        <span className="flex items-center gap-1"><Clock className="w-3 h-3" />Updated</span>
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-700/30">
                    {divisionKeys.map((divName) => {
                      const entries = groupedByDivision[divName];
                      const dt = divisionTotals(entries);
                      return (
                        <React.Fragment key={divName}>
                          {entries.map((entry, idx) => (
                            <tr key={`${divName}-${idx}`} className="hover:bg-slate-800/30 transition-colors">
                              {idx === 0 && (
                                <td
                                  rowSpan={entries.length}
                                  className="px-5 py-3 font-semibold text-cyan-300 align-top border-r border-slate-700/20 bg-slate-800/20"
                                >
                                  <div className="flex items-center gap-2">
                                    <MapPin className="w-4 h-4 text-cyan-500 shrink-0" />
                                    {divName}
                                  </div>
                                </td>
                              )}
                              <td className="px-5 py-3 font-medium text-slate-200">{entry.district}</td>
                              <td className="px-5 py-3 text-slate-300">{entry.upazila}</td>
                              <td className="px-5 py-3 text-right font-mono text-blue-400">{entry.total.toLocaleString()}</td>
                              <td className="px-5 py-3 text-right font-mono text-emerald-400">{entry.valid.toLocaleString()}</td>
                              <td className={`px-5 py-3 text-right font-mono ${entry.invalid > 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                                {entry.invalid.toLocaleString()}
                              </td>
                              <td className="px-5 py-3 text-center">
                                <span className={`inline-flex items-center py-0.5 px-2 rounded-full text-xs font-medium ${
                                  entry.version > 1 ? 'bg-amber-500/10 text-amber-400 border border-amber-500/20' : 'bg-slate-700/50 text-slate-400'
                                }`}>
                                  v{entry.version}
                                </span>
                              </td>
                              <td className="px-5 py-3 text-center">                                  <div className="flex items-center justify-center gap-2">                                   {entry.excel_url && (
                                    <button 
                                      onClick={() => downloadFileWithAuth(entry.excel_url!, entry.excel_url!.split('/').pop() || "stats_all.xlsx")}
                                      title="Download All Rows (Processed)" 
                                      className="p-1.5 rounded-md bg-blue-500/10 text-blue-400 hover:bg-blue-500/20 active:bg-blue-500/30 transition-colors border border-blue-500/20"
                                    >
                                      <FileSpreadsheet className="w-4 h-4" />
                                    </button>
                                  )}
                                  {entry.excel_valid_url && entry.valid > 0 && (
                                    <button 
                                      onClick={() => downloadFileWithAuth(entry.excel_valid_url!, entry.excel_valid_url!.split('/').pop() || "stats_valid.xlsx")}
                                      title="Download Valid Rows" 
                                      className="p-1.5 rounded-md bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500/20 active:bg-emerald-500/30 transition-colors border border-emerald-500/20"
                                    >
                                      <CheckCircle2 className="w-4 h-4" />
                                    </button>
                                  )}
                                  {entry.excel_invalid_url && entry.invalid > 0 && (
                                    <button 
                                      onClick={() => downloadFileWithAuth(entry.excel_invalid_url!, entry.excel_invalid_url!.split('/').pop() || "stats_invalid.xlsx")}
                                      title="Download Invalid Rows" 
                                      className="p-1.5 rounded-md bg-red-500/10 text-red-400 hover:bg-red-500/20 active:bg-red-500/30 transition-colors border border-red-500/20"
                                    >
                                      <FileWarning className="w-4 h-4" />
                                    </button>
                                  )}
                                  {entry.pdf_url && (
                                    <button 
                                      onClick={() => downloadFileWithAuth(entry.pdf_url!, entry.pdf_url!.split('/').pop() || "stats_report.pdf")}
                                      title="Download PDF Summary" 
                                      className="p-1.5 rounded-md bg-amber-500/10 text-amber-500 hover:bg-amber-500/20 active:bg-amber-500/30 transition-colors border border-amber-500/20"
                                    >
                                      <FileText className="w-4 h-4" />
                                    </button>
                                  )}

                                  <div className="w-px h-5 bg-slate-700/50 mx-1 print:hidden" />
                                  {isAdmin() && (
                                    <button onClick={() => openEditModal(entry)} title="Manual Edit" className="p-1.5 rounded-md bg-slate-500/10 text-slate-400 hover:bg-slate-500/20 hover:text-slate-200 transition-colors border border-slate-500/20 print:hidden">
                                      <Edit2 className="w-4 h-4" />
                                    </button>
                                  )}
                                </div>
                              </td>
                              <td className="px-5 py-3 text-slate-500 text-xs whitespace-nowrap">
                                {formatDate(entry.updated_at)}
                              </td>
                            </tr>
                          ))}
                          {/* Division subtotal */}
                          <tr className="bg-slate-800/50 border-b-2 border-slate-600/30">
                            <td className="px-5 py-2 text-xs text-slate-400 font-semibold uppercase tracking-wider" colSpan={3}>
                              {divName} — Subtotal
                            </td>
                            <td className="px-5 py-2 text-right font-mono text-blue-300 font-semibold text-xs">{dt.total.toLocaleString()}</td>
                            <td className="px-5 py-2 text-right font-mono text-emerald-300 font-semibold text-xs">{dt.valid.toLocaleString()}</td>
                            <td className="px-5 py-2 text-right font-mono text-red-300 font-semibold text-xs">{dt.invalid.toLocaleString()}</td>
                            <td colSpan={3}></td>
                          </tr>
                        </React.Fragment>
                      );
                    })}
                    {/* Grand Total */}
                    <tr className="bg-slate-700/50 font-bold">
                      <td colSpan={3} className="px-5 py-4 text-slate-200 uppercase text-sm tracking-wider">Grand Total</td>
                      <td className="px-5 py-4 text-right font-mono text-blue-300 text-lg">{data.grand_total.total.toLocaleString()}</td>
                      <td className="px-5 py-4 text-right font-mono text-emerald-300 text-lg">{data.grand_total.valid.toLocaleString()}</td>
                      <td className="px-5 py-4 text-right font-mono text-red-300 text-lg">{data.grand_total.invalid.toLocaleString()}</td>
                      <td colSpan={3}></td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </>
        )}
      </div>

      {/* Footer */}
      <footer className="mt-12 py-6 border-t border-slate-800/50 print:hidden">
        <div className="max-w-7xl mx-auto text-center">
          <p className="text-sm text-slate-500">
            © {new Date().getFullYear()} Computer Network Unit | Directorate General of Food
          </p>
        </div>
      </footer>

      {/* Edit Modal */}
      {editingEntry && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm print:hidden">
          <div className="bg-slate-900 border border-slate-700 p-6 rounded-2xl w-full max-w-sm shadow-xl relative animate-in fade-in zoom-in-95 duration-200">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-xl font-bold text-slate-100">Edit Statistics</h3>
              <button disabled={savingEdit} onClick={() => setEditingEntry(null)} className="text-slate-400 hover:text-slate-200 p-1">
                <X className="w-5 h-5" />
              </button>
            </div>
            <p className="text-sm text-slate-400 mb-6 font-medium">
              {editingEntry.district} — {editingEntry.upazila}
            </p>
            
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-slate-400 mb-1">District</label>
                  <input type="text" value={editForm.district} onChange={e => setEditForm({...editForm, district: e.target.value})} className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500 transition-colors" />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-400 mb-1">Upazila</label>
                  <input type="text" value={editForm.upazila} onChange={e => setEditForm({...editForm, upazila: e.target.value})} className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500 transition-colors" />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-slate-400 mb-1">Total Records</label>
                <input type="number" value={editForm.total} onChange={e => setEditForm({...editForm, total: parseInt(e.target.value) || 0})} className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-blue-300 font-mono focus:outline-none focus:border-blue-500 transition-colors" />
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-slate-400 mb-1">Valid</label>
                  <input type="number" value={editForm.valid} onChange={e => setEditForm({...editForm, valid: parseInt(e.target.value) || 0})} className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-emerald-400 font-mono focus:outline-none focus:border-emerald-500 transition-colors" />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-400 mb-1">Invalid</label>
                  <input type="number" value={editForm.invalid} onChange={e => setEditForm({...editForm, invalid: parseInt(e.target.value) || 0})} className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-red-400 font-mono focus:outline-none focus:border-red-500 transition-colors" />
                </div>
              </div>
            </div>
            
            <div className="mt-8 flex justify-end gap-3">
              <button disabled={savingEdit} onClick={() => setEditingEntry(null)} className="px-4 py-2 rounded-lg text-slate-300 hover:bg-slate-800 transition-colors font-medium">
                Cancel
              </button>
              <button disabled={savingEdit} onClick={handleSaveEdit} className="px-5 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-medium flex items-center gap-2 transition-all">
                {savingEdit ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
                Save
              </button>
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
