"use client";



import React, { useState, useEffect } from "react";

import { useRouter } from "next/navigation";

import Link from "next/link";

import { BarChart3, ArrowLeft, RefreshCw, MapPin, Clock, Hash, FileSpreadsheet, CheckCircle2, FileWarning, FileText, Printer, Edit2, X, Save, Search as SearchIcon, Download, ChevronRight, ChevronDown, Trash2 } from "lucide-react";

import { fetchWithAuth, getBackendUrl, downloadFileWithAuth, isAuthenticated, isAdmin } from "@/lib/auth";



interface StatsEntry {

  division: string;

  district: string;

  upazila: string;

  total: number;

  valid: number;

  invalid: number;
  quota: number;
  filename: string;

  version: number;

  created_at: string;

  updated_at: string;

  pdf_url?: string;

  pdf_invalid_url?: string;

  excel_url?: string;

  excel_valid_url?: string;

  excel_invalid_url?: string;

}



interface StatsResponse {

  entries: StatsEntry[];

  grand_total: { total: number; valid: number; invalid: number };
  master_counts: {
    divisions: Record<string, number>;
    districts: Record<string, number>;
  };

}



interface Batch {

  id: number;

  filename: string;

  uploader_id: number;

  username: string;

  total_rows: number;

  valid_count: number;

  invalid_count: number;

  new_records: number;

  updated_records: number;

  created_at: string;

  status: string;

}



export default function StatisticsPage() {

  const [data, setData] = useState<StatsResponse | null>(null);

  const [loading, setLoading] = useState(true);

  const [error, setError] = useState<string | null>(null);
  const [statsEtag, setStatsEtag] = useState<string | null>(null);

  const [user, setUser] = useState<any>(null);
  const [isMounted, setIsMounted] = useState(false);

  useEffect(() => {
    setIsMounted(true);
  }, []);

  const [savingEdit, setSavingEdit] = useState(false);



  // Filter & Sort State

  const [searchTerm, setSearchTerm] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [showInvalidOnly, setShowInvalidOnly] = useState(false);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedSearch(searchTerm), 300);
    return () => clearTimeout(timer);
  }, [searchTerm]);

  const [sortBy, setSortBy] = useState<"name" | "total" | "valid" | "updated">("name");

  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("asc");



  // History State

  const [historyEntry, setHistoryEntry] = useState<StatsEntry | null>(null);

  const [historyLoading, setHistoryLoading] = useState(false);

  const [historyData, setHistoryData] = useState<Batch[]>([]);

  const [deletingBatchId, setDeletingBatchId] = useState<number | null>(null);



  // Edit State

  const [editingEntry, setEditingEntry] = useState<StatsEntry | null>(null);

  const [editForm, setEditForm] = useState({

    old_district: "",

    old_upazila: "",

    district: "",

    upazila: "",

    total: 0,

    valid: 0,

    invalid: 0

  });



  // Tree Expansion State

  const [expanded, setExpanded] = useState<Record<string, boolean>>({});



  const toggleExpand = (id: string) => {

    setExpanded(prev => ({ ...prev, [id]: !prev[id] }));

  };





  const handleDownloadAllValid = () => {
    downloadFileWithAuth("/api/downloads/valid-zip", "all_valid_records.zip");
  };



  const handleDownloadAllInvalid = () => {
    downloadFileWithAuth("/api/downloads/invalid-zip", "all_invalid_records.zip");
  };

  const buildLiveExportInvalidUrl = (entry: StatsEntry, fmt: string) => {
    const p = new URLSearchParams({
      division: entry.division,
      district: entry.district,
      upazila: entry.upazila,
      fmt,
    });
    return "/api/upazila/live-export-invalid?" + p.toString();
  };

  const buildLiveExportUrl = (entry: StatsEntry, fmt: string) => {
    const p = new URLSearchParams({
      division: entry.division,
      district: entry.district,
      upazila: entry.upazila,
      fmt,
    });
    return "/api/upazila/live-export?" + p.toString();
  };

  const [recheckLoading, setRecheckLoading] = React.useState<string | null>(null);
  const [recheckResult, setRecheckResult] = React.useState<{ msg: string; flagged: number } | null>(null);

  const handleRecheck = async (entry: StatsEntry) => {
    const key = entry.division + "|" + entry.district + "|" + entry.upazila;
    setRecheckLoading(key);
    setRecheckResult(null);
    try {
      const p = new URLSearchParams({
        division: entry.division,
        district: entry.district,
        upazila: entry.upazila,
        fmt: "json",
      });
      const resp = await fetchWithAuth("/api/upazila/recheck?" + p.toString(), { method: "POST" });
      const data = await resp.json();
      if (data.flagged_count === 0) {
        setRecheckResult({ msg: "No suspicious NIDs in " + entry.upazila + " (" + data.total_checked + " checked)", flagged: 0 });
      } else {
        const dlp = new URLSearchParams({ division: entry.division, district: entry.district, upazila: entry.upazila, fmt: "xlsx" });
        downloadFileWithAuth("/api/upazila/recheck?" + dlp.toString(), entry.district + "_" + entry.upazila + "_fraud_report.xlsx");
        setRecheckResult({ msg: data.flagged_count + " suspicious NID(s) in " + entry.upazila + " - report downloaded", flagged: data.flagged_count });
      }
    } catch {
      setRecheckResult({ msg: "Re-check failed", flagged: -1 });
    } finally {
      setRecheckLoading(null);
    }
  };



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

    setUser(JSON.parse(localStorage.getItem("ffp_user") || "null"));

    fetchStats();

  }, []);



  const openHistory = async (entry: StatsEntry) => {

    setHistoryEntry(entry);

    setHistoryLoading(true);

    try {

      const res = await fetchWithAuth(`${getBackendUrl()}/statistics/history?district=${encodeURIComponent(entry.district)}&upazila=${encodeURIComponent(entry.upazila)}`);

      if (!res.ok) throw new Error("Failed to load history");

      const json = await res.json();

      setHistoryData(json);

    } catch (err: any) {

      alert("Error loading history: " + err.message);

    } finally {

      setHistoryLoading(false);

    }

  };



  const handleDeleteBatch = async (batchId: number) => {

    if (!confirm("Are you sure? This will delete all records associated with this upload and revert counts.")) return;



    setDeletingBatchId(batchId);

    try {

      const res = await fetchWithAuth(`${getBackendUrl()}/batches/${batchId}`, {

        method: "DELETE"

      });

      if (!res.ok) throw new Error("Failed to delete batch");



      // Refresh current history and main stats

      if (historyEntry) {

        setHistoryData((prev: Batch[]) => prev.map((b: Batch) => b.id === batchId ? { ...b, status: 'deleted' } : b));

      }

      fetchStats();

    } catch (err: any) {

      alert("Error: " + err.message);

    } finally {

      setDeletingBatchId(null);

    }

  };



  const handleDeleteUpazila = async (entry: StatsEntry) => {

    if (!confirm(`CRITICAL: This will delete ALL data (Valid & Invalid records) and files for ${entry.upazila}, ${entry.district}. This action cannot be undone. Proceed?`)) return;



    try {

      const res = await fetchWithAuth(`${getBackendUrl()}/statistics/${encodeURIComponent(entry.division)}/${encodeURIComponent(entry.district)}/${encodeURIComponent(entry.upazila)}`, {

        method: "DELETE"

      });

      if (!res.ok) throw new Error("Failed to delete upazila data");



      alert("Upazila data deleted successfully.");

      fetchStats();

    } catch (err: any) {

      alert("Error: " + err.message);

    }

  };



  // Filter and sort entries

  const filteredEntries = (data?.entries || []).filter(e => {
    const matchesSearch =
      e.district.toLowerCase().includes(searchTerm.toLowerCase()) ||
      e.upazila.toLowerCase().includes(searchTerm.toLowerCase()) ||
      e.division.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesInvalid = !showInvalidOnly || e.invalid > 0;
    return matchesSearch && matchesInvalid;
  }).sort((a, b) => {

    let result = 0;

    if (sortBy === "name") result = a.district.localeCompare(b.district) || a.upazila.localeCompare(b.upazila);

    else if (sortBy === "total") result = a.total - b.total;

    else if (sortBy === "valid") result = a.valid - b.valid;

    else if (sortBy === "updated") result = new Date(a.updated_at).getTime() - new Date(b.updated_at).getTime();



    return sortOrder === "asc" ? result : -result;

  });



  // Hierarchical Grouping: Division > District > Upazila

  const hierarchy = React.useMemo(() => {

    const list: any[] = [];

    const divMap: Record<string, any> = {};



    filteredEntries.forEach(entry => {

      const div = entry.division || "Unknown";

      const dist = entry.district || "Unknown";



      if (!divMap[div]) {
        divMap[div] = { name: div, total: 0, valid: 0, invalid: 0, quota: 0, districts: {} };
        list.push(divMap[div]);
      }



      const divNode = divMap[div];

      if (!divNode.districts[dist]) {
        divNode.districts[dist] = { name: dist, total: 0, valid: 0, invalid: 0, quota: 0, upazilas: [] };
      }



      const distNode = divNode.districts[dist];
      distNode.upazilas.push(entry);



      // Roll up totals

      distNode.total += entry.total;

      distNode.valid += entry.valid;

      distNode.invalid += entry.invalid;
      distNode.quota += (entry.quota || 0);

      divNode.total += entry.total;
      divNode.valid += entry.valid;
      divNode.invalid += entry.invalid;
      divNode.quota += (entry.quota || 0);

    });



    return list.sort((a, b) => a.name.localeCompare(b.name));

  }, [filteredEntries]);



  // Per-division totals

  const divisionTotals = (entries: StatsEntry[]) => ({

    total: entries.reduce((s: number, e: StatsEntry) => s + e.total, 0),

    valid: entries.reduce((s: number, e: StatsEntry) => s + e.valid, 0),
    invalid: entries.reduce((s: number, e: StatsEntry) => s + e.invalid, 0),
    quota: entries.reduce((s: number, e: StatsEntry) => s + (e.quota || 0), 0),
  });



  const formatDate = (iso: string) => {

    return new Date(iso).toLocaleString('en-GB', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' });

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

      <div className="max-w-7xl mx-auto space-y-8 flex-1 w-full print:hidden">



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

          <div className="flex flex-wrap items-center gap-3 print:hidden">

            <div className="relative group">

              <SearchIcon className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500 group-focus-within:text-indigo-400 transition-colors" />

              <input

                type="text"

                placeholder="Search district or upazila..."

                value={searchTerm}

                onChange={e => setSearchTerm(e.target.value)}

                className="pl-9 pr-4 py-2 bg-slate-800/50 border border-slate-700 rounded-lg text-sm text-slate-200 focus:outline-none focus:border-indigo-500/50 focus:ring-1 focus:ring-indigo-500/20 w-64 transition-all"

              />

            </div>

            <button
              onClick={() => setShowInvalidOnly(prev => !prev)}
              className={`px-4 py-2 rounded-lg font-medium flex items-center gap-2 transition-all text-sm border ${
                showInvalidOnly
                  ? 'bg-red-600/30 border-red-500/50 text-red-300'
                  : 'bg-slate-800/50 border-slate-700/50 text-slate-400 hover:bg-slate-700/50'
              }`}
            >
              <FileWarning className="w-4 h-4" />
              {showInvalidOnly ? 'Showing Invalid Only' : 'Show Invalid Only'}
            </button>


            <div className="h-8 w-px bg-slate-700/50 mx-1" />



            <Link

              href="/search"

              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600/20 border border-indigo-500/30 text-indigo-400 hover:bg-indigo-600/30 transition-all font-medium text-sm"

            >

              <SearchIcon className="w-4 h-4" />

              <span>Search Records</span>

            </Link>

            <button

              onClick={handleDownloadAllValid}

              disabled={loading || !data || data.entries.every(e => !e.excel_valid_url)}

              className="bg-emerald-600/20 hover:bg-emerald-600/30 border border-emerald-500/30 text-emerald-400 px-4 py-2 rounded-lg font-medium flex items-center gap-2 transition-all text-sm disabled:opacity-50 disabled:cursor-not-allowed"

            >

              <Download className="w-4 h-4" />

              Download All Valid

            </button>

            <button

              onClick={handleDownloadAllInvalid}

              disabled={loading || !data || data.entries.every(e => !e.excel_invalid_url || e.invalid === 0)}

              className="bg-red-600/20 hover:bg-red-600/30 border border-red-500/30 text-red-400 px-4 py-2 rounded-lg font-medium flex items-center gap-2 transition-all text-sm disabled:opacity-50 disabled:cursor-not-allowed"

            >

              <FileWarning className="w-4 h-4" />

              Download All Invalid

            </button>

            <button

              onClick={() => window.print()}

              disabled={loading}

              className="bg-slate-800/50 hover:bg-slate-700/50 border border-slate-700/50 text-slate-300 px-4 py-2 rounded-lg font-medium flex items-center gap-2 transition-all text-sm"

            >

              <Printer className="w-4 h-4" />

              Print

            </button>

            <button

              onClick={fetchStats}

              disabled={loading}

              className="bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2 rounded-lg font-medium flex items-center gap-2 transition-all shadow-lg shadow-indigo-500/20 text-sm"

            >

              <RefreshCw className={`w-4 h-4 ${loading ? "animate-spin" : ""}`} />

              Refresh

            </button>

          </div>

        </header>



        <div className="flex items-center justify-between text-xs text-slate-500 bg-slate-900/50 p-3 rounded-xl border border-slate-800/50 animate-in fade-in slide-in-from-top-2 duration-300 print:hidden">

          <div className="flex gap-4">

            <span className="flex items-center gap-1.5"><Clock className="w-3.5 h-3.5" /> Last sync: <b>{isMounted ? new Date().toLocaleTimeString() : "--:--"}</b></span>

          </div>

          <div className="flex gap-4">

            <span className="text-slate-600">Actions:</span>

            <button

              onClick={() => {

                const all: Record<string, boolean> = {};

                hierarchy.forEach(div => {

                  all[`div-${div.name}`] = true;

                  Object.keys(div.districts).forEach(dist => all[`dist-${div.name}-${dist}`] = true);

                });

                setExpanded(all);

              }}

              className="hover:text-cyan-400 decoration-cyan-400/30 underline underline-offset-4 transition-colors"

            >

              Expand All

            </button>

            <button

              onClick={() => setExpanded({})}

              className="hover:text-indigo-400 decoration-indigo-400/30 underline underline-offset-4 transition-colors"

            >

              Collapse All

            </button>

            <div className="w-px h-3 bg-slate-700/50 self-center" />

            <span className="text-slate-600">Sort:</span>

            <button onClick={() => { setSortBy("name"); setSortOrder((prev: "asc" | "desc") => prev === "asc" ? "desc" : "asc"); }} className={`hover:text-slate-300 transition-colors ${sortBy === "name" ? "text-indigo-400 font-bold underline" : ""}`}>Name</button>

            <button onClick={() => { setSortBy("total"); setSortOrder((prev: "asc" | "desc") => prev === "asc" ? "desc" : "asc"); }} className={`hover:text-slate-300 transition-colors ${sortBy === "total" ? "text-indigo-400 font-bold underline" : ""}`}>Total</button>

          </div>

        </div>



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

            <div className="grid grid-cols-1 sm:grid-cols-4 gap-5">
              <div className="glass-panel p-5 rounded-2xl border-t-4 border-t-blue-500">
                <p className="text-xs font-medium text-slate-500 uppercase tracking-wider">Total Records</p>
                <p className="text-3xl font-bold text-blue-400 mt-1">{data.grand_total.total.toLocaleString()}</p>
              </div>

              <div className="glass-panel p-5 rounded-2xl border-t-4 border-t-emerald-500">
                <p className="text-xs font-medium text-slate-500 uppercase tracking-wider">Valid Records (Target)</p>
                <div className="flex items-baseline gap-2 mt-1">
                  <p className="text-3xl font-bold text-emerald-400">{data.grand_total.valid.toLocaleString()}</p>
                  <p className="text-sm text-slate-500">/ {(data.entries.reduce((s, e) => s + (e.quota || 0), 0)).toLocaleString()}</p>
                </div>
              </div>

              <div className="glass-panel p-5 rounded-2xl border-t-4 border-t-amber-500">
                <p className="text-xs font-medium text-slate-500 uppercase tracking-wider">Remaining to Target</p>
                <p className="text-3xl font-bold text-amber-400 mt-1">
                  {Math.max(0, (data.entries.reduce((s, e) => s + (e.quota || 0), 0)) - data.grand_total.valid).toLocaleString()}
                </p>
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

                <div className="flex items-center gap-3 ml-auto">

                  <span className="text-xs px-2 py-1 bg-blue-500/10 text-blue-400 rounded-full border border-blue-500/20">

                    {Object.keys(data.master_counts.districts).length} Total Districts

                  </span>

                  <span className="text-sm text-slate-500">{filteredEntries.length} entries across {hierarchy.length} divisions</span>

                </div>

              </div>



              <div className="overflow-x-auto">

                <table className="w-full text-sm text-left">

                  <thead className="bg-slate-800/80 text-slate-300 uppercase text-xs sticky top-0 z-10">

                    <tr>

                      <th className="px-3 md:px-5 py-3 font-medium">Division</th>
                      <th className="px-5 py-3 font-medium">District</th>
                      <th className="px-5 py-3 font-medium">Upazila</th>
                      <th className="px-5 py-3 font-medium text-right">Target</th>
                      <th className="px-5 py-3 font-medium text-right">Total</th>
                      <th className="px-5 py-3 font-medium text-right text-emerald-400">Valid</th>
                      <th className="px-5 py-3 font-medium text-right text-amber-400">Rem.</th>
                      <th className="px-5 py-3 font-medium text-right">Invalid</th>
                      <th className="px-5 py-3 font-medium text-center">
                        <span className="flex items-center justify-center gap-1"><Hash className="w-3 h-3" />Ver</span>
                      </th>
                      <th className="px-5 py-3 font-medium text-center">Downloads</th>
                      <th className="px-3 md:px-5 py-3 font-medium cursor-default" title="Last Updated">
                        <span className="flex items-center gap-1"><Clock className="w-3 h-3" />Updated</span>
                      </th>

                    </tr>

                  </thead>

                  <tbody className="divide-y divide-slate-700/30">

                    {hierarchy.map((div: any) => {

                      const divId = `div-${div.name}`;

                      const isDivExpanded = expanded[divId];



                      return (

                        <React.Fragment key={divId}>

                          {/* Division Row */}

                          <tr className="bg-slate-800/60 transition-colors group">

                            <td className="px-5 py-3 border-r border-slate-700/20" colSpan={3}>

                              <div className="flex items-center gap-3">

                                <button

                                  onClick={() => toggleExpand(divId)}

                                  className="p-1 rounded bg-slate-700/50 text-slate-400 hover:text-cyan-400 transition-all active:scale-90"

                                >

                                  {isDivExpanded ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}

                                </button>

                                <div className="flex items-center gap-2">

                                  <MapPin className="w-4 h-4 text-cyan-500 shrink-0" />

                                  <span className="font-bold text-cyan-300 uppercase tracking-tighter text-base">{div.name}</span>

                                  <span className="text-[10px] bg-cyan-500/10 text-cyan-500 px-1.5 py-0.5 rounded border border-cyan-500/20 ml-2">DIVISION</span>

                                  <span className="text-[10px] text-slate-400 ml-2 italic">

                                    ({Object.keys(div.districts).length} of {data.master_counts.divisions[div.name] || 0} districts uploaded)

                                  </span>

                                </div>
                              </div>
                            </td>

                            <td className="px-5 py-3 text-right font-mono text-slate-400 font-bold text-base">{div.quota.toLocaleString()}</td>
                            <td className="px-5 py-3 text-right font-mono text-blue-300 font-bold text-base">{div.total.toLocaleString()}</td>
                            <td className="px-5 py-3 text-right font-mono text-emerald-300 font-bold text-base">{div.valid.toLocaleString()}</td>
                            <td className="px-5 py-3 text-right font-mono text-amber-300 font-bold text-base">{Math.max(0, div.quota - div.valid).toLocaleString()}</td>
                            <td className="px-5 py-3 text-right font-mono text-red-300 font-bold text-base">{div.invalid.toLocaleString()}</td>
                            <td colSpan={3} className="px-5 py-3"></td>

                          </tr>



                          {isDivExpanded && Object.keys(div.districts).sort().map(distName => {

                            const dist = div.districts[distName];

                            const distId = `dist-${div.name}-${distName}`;

                            const isDistExpanded = expanded[distId];



                            return (

                              <React.Fragment key={distId}>

                                {/* District Subtotal Row */}

                                <tr className="bg-slate-800/30 transition-colors">

                                  <td className="px-5 py-2.5 pl-12" colSpan={3}>

                                    <div className="flex items-center gap-3">

                                      <button

                                        onClick={() => toggleExpand(distId)}

                                        className="p-1 rounded bg-slate-800/50 text-slate-500 hover:text-indigo-400 transition-all active:scale-90"

                                      >

                                        {isDistExpanded ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}

                                      </button>

                                      <span className="font-semibold text-slate-200">{distName}</span>

                                      <span className="text-[9px] bg-slate-700/50 text-slate-400 px-1.5 py-0.5 rounded border border-slate-700 ml-1">DISTRICT</span>

                                      <span className="text-[10px] text-slate-400 ml-2 italic">

                                        ({dist.upazilas.length} of {data.master_counts.districts[distName] || 0} upazilas uploaded)

                                      </span>

                                    </div>

                                  </td>

                                  <td className="px-5 py-2 text-right font-mono text-slate-500 font-semibold text-sm">{dist.quota.toLocaleString()}</td>
                                  <td className="px-5 py-2 text-right font-mono text-blue-400/80 font-semibold text-sm">{dist.total.toLocaleString()}</td>
                                  <td className="px-5 py-2 text-right font-mono text-emerald-400/80 font-semibold text-sm">{dist.valid.toLocaleString()}</td>
                                  <td className="px-5 py-2 text-right font-mono text-amber-400/80 font-semibold text-sm">{Math.max(0, dist.quota - dist.valid).toLocaleString()}</td>
                                  <td className="px-5 py-2 text-right font-mono text-red-400/80 font-semibold text-sm">{dist.invalid.toLocaleString()}</td>
                                  <td colSpan={3}></td>

                                </tr>



                                {/* Upazila Rows */}

                                {isDistExpanded && dist.upazilas.map((entry: any, uIdx: number) => (

                                  <tr key={`${div.name}-${distName}-${uIdx}`} className="hover:bg-slate-700/20 transition-colors text-xs border-l border-indigo-500/20">

                                    <td className="px-5 py-2 pl-24 text-slate-400 italic" colSpan={2}>

                                      {/* This spacing aligns Upazila column */}

                                    </td>

                                    <td className="px-5 py-2 text-slate-300 font-medium">{entry.upazila}</td>

                                    <td className="px-5 py-2 text-right font-mono text-slate-500">{entry.quota || 0}</td>
                                    <td className="px-5 py-2 text-right font-mono text-blue-400/70">{entry.total.toLocaleString()}</td>
                                    <td className="px-5 py-2 text-right font-mono text-emerald-400/70">
                                      <div className="flex flex-col items-end">
                                        <span>{entry.valid.toLocaleString()}</span>
                                        {entry.quota > 0 && (
                                          <div className="w-16 h-1 bg-slate-800 rounded-full mt-1 overflow-hidden">
                                            <div
                                              className="h-full bg-emerald-500"
                                              style={{ width: `${Math.min(100, (entry.valid / entry.quota) * 100)}%` }}
                                            />
                                          </div>
                                        )}
                                      </div>
                                    </td>
                                    <td className="px-5 py-2 text-right font-mono text-amber-400/70">{Math.max(0, (entry.quota || 0) - entry.valid).toLocaleString()}</td>
                                    <td className={`px-5 py-2 text-right font-mono ${entry.invalid > 0 ? 'text-red-400/70' : 'text-emerald-400/70'}`}>
                                      {entry.invalid.toLocaleString()}
                                    </td>

                                    <td className="px-5 py-2 text-center">

                                      <span className={`inline-flex items-center py-0.5 px-1.5 rounded-full text-[10px] font-medium ${entry.version > 1 ? 'bg-amber-500/10 text-amber-400 border border-amber-500/20' : 'bg-slate-700/50 text-slate-400'

                                        }`}>

                                        v{entry.version}

                                      </span>

                                    </td>

                                    <td className="px-5 py-2 text-center">

                                      <div className="flex items-center justify-center gap-1.5">

                                        <button onClick={() => openHistory(entry)} title="View History" className="p-1 rounded-md bg-slate-500/10 text-slate-500 hover:bg-slate-500/20 hover:text-slate-300 transition-colors border border-slate-500/20">

                                          <Clock className="w-3.5 h-3.5" />

                                        </button>



                                        <div className="w-px h-3 bg-slate-700/50 mx-0.5" />



                                        {entry.excel_url && (

                                          <button

                                            onClick={() => downloadFileWithAuth(entry.excel_url!, entry.excel_url!.split('/').pop() || "stats_all.xlsx")}

                                            title="Download All Rows"

                                            className="p-1 rounded-md bg-blue-500/10 text-blue-400/80 hover:bg-blue-500/20 transition-colors border border-blue-500/20"

                                          >

                                            <FileSpreadsheet className="w-3.5 h-3.5" />

                                          </button>

                                        )}

                                        {entry.valid > 0 && (
                                          <>
                                            <button
                                              onClick={() => downloadFileWithAuth(buildLiveExportUrl(entry, "xlsx"), entry.district + "_" + entry.upazila + "_live_valid.xlsx")}
                                              title="Download Live Valid XLS (all versions merged)"
                                              className="p-1 rounded-md bg-emerald-500/10 text-emerald-400/80 hover:bg-emerald-500/20 transition-colors border border-emerald-500/20"
                                            >
                                              <CheckCircle2 className="w-3.5 h-3.5" />
                                            </button>
                                            <button
                                              onClick={() => downloadFileWithAuth(buildLiveExportUrl(entry, "pdf"), entry.district + "_" + entry.upazila + "_live_valid.pdf")}
                                              title="Download Live Valid PDF (all versions merged)"
                                              className="p-1 rounded-md bg-teal-500/10 text-teal-400/80 hover:bg-teal-500/20 transition-colors border border-teal-500/20"
                                            >
                                              <FileText className="w-3.5 h-3.5" />
                                            </button>
                                          </>
                                        )}

                                        {entry.invalid > 0 && (
                                          <>
                                            <button
                                              onClick={() => downloadFileWithAuth(buildLiveExportInvalidUrl(entry, "xlsx"), entry.district + "_" + entry.upazila + "_live_invalid.xlsx")}
                                              title="Download Live Invalid XLS (all versions merged)"
                                              className="p-1 rounded-md bg-orange-500/10 text-orange-400/80 hover:bg-orange-500/20 transition-colors border border-orange-500/20"
                                            >
                                              <FileWarning className="w-3.5 h-3.5" />
                                            </button>
                                            <button
                                              onClick={() => downloadFileWithAuth(buildLiveExportInvalidUrl(entry, "pdf"), entry.district + "_" + entry.upazila + "_live_invalid.pdf")}
                                              title="Download Live Invalid PDF (all versions merged)"
                                              className="p-1 rounded-md bg-red-500/10 text-red-400/80 hover:bg-red-500/20 transition-colors border border-red-500/20"
                                            >
                                              <FileText className="w-3.5 h-3.5" />
                                            </button>
                                            <button
                                              onClick={() => downloadFileWithAuth(`/api/upazila/trailing-zeros-pdf?division=${encodeURIComponent(entry.division)}&district=${encodeURIComponent(entry.district)}&upazila=${encodeURIComponent(entry.upazila)}`, `${entry.district}_${entry.upazila}_trailing_zeros.pdf`)}
                                              title="Download records with 2+ trailing zeros (PDF)"
                                              className="p-1 rounded-md bg-yellow-500/10 text-yellow-400/80 hover:bg-yellow-500/20 transition-colors border border-yellow-500/20"
                                            >
                                              <Hash className="w-3.5 h-3.5" />
                                            </button>
                                          </>
                                        )}

                                        {entry.excel_invalid_url && entry.invalid > 0 && (

                                          <button

                                            onClick={() => downloadFileWithAuth(entry.excel_invalid_url!, entry.excel_invalid_url!.split('/').pop() || "stats_invalid.xlsx")}

                                            title="Download Invalid Rows (Excel)"

                                            className="p-1 rounded-md bg-red-500/10 text-red-400/80 hover:bg-red-500/20 transition-colors border border-red-500/20"

                                          >

                                            <FileWarning className="w-3.5 h-3.5" />

                                          </button>

                                        )}

                                        {entry.pdf_invalid_url && entry.invalid > 0 && (

                                          <button

                                            onClick={() => downloadFileWithAuth(entry.pdf_invalid_url!, entry.pdf_invalid_url!.split('/').pop() || "invalid_report.pdf")}

                                            title="Download Invalid Records PDF Report"

                                            className="p-1 rounded-md bg-orange-500/10 text-orange-400/80 hover:bg-orange-500/20 transition-colors border border-orange-500/20"

                                          >

                                            <FileText className="w-3.5 h-3.5" />

                                          </button>

                                        )}



                                        <div className="w-px h-3 bg-slate-700/50 mx-0.5" />
                                        {user?.role === 'admin' && (
                                          <div className="flex gap-1.5">
                                            <button
                                              onClick={() => handleRecheck(entry)}
                                              disabled={recheckLoading === entry.division + "|" + entry.district + "|" + entry.upazila}
                                              title="Re-check stored NIDs for fraud patterns"
                                              className="p-1 rounded-md bg-purple-500/10 text-purple-400/70 hover:bg-purple-500/20 hover:text-purple-300 transition-colors border border-purple-500/20 disabled:opacity-40"
                                            >
                                              <RefreshCw className="w-3.5 h-3.5" />
                                            </button>
                                            <button onClick={() => openEditModal(entry)} title="Manual Edit" className="p-1 rounded-md bg-amber-500/10 text-amber-500/70 hover:bg-amber-500/20 hover:text-amber-300 transition-colors border border-amber-500/20">
                                              <Edit2 className="w-3.5 h-3.5" />
                                            </button>
                                            <button onClick={() => handleDeleteUpazila(entry)} title="Delete All Records" className="p-1 rounded-md bg-red-500/10 text-red-500/70 hover:bg-red-500/20 hover:text-red-300 transition-colors border border-red-500/20">
                                              <Trash2 className="w-3.5 h-3.5" />
                                            </button>
                                          </div>
                                        )}

                                      </div>

                                    </td>

                                    <td className="px-5 py-2 text-slate-600 text-[10px] whitespace-nowrap">

                                      {formatDate(entry.updated_at)}

                                    </td>

                                  </tr>

                                ))}

                              </React.Fragment>

                            );

                          })}
                        </React.Fragment>
                      );
                    })}

                    {/* Grand Total */}

                    <tr className="bg-slate-900 border-t-2 border-indigo-500/50 font-bold">

                      <td colSpan={3} className="px-5 py-5 text-indigo-400 uppercase text-sm tracking-wider flex items-center gap-2">

                        <CheckCircle2 className="w-5 h-5" />

                        Grand Total Summary

                      </td>

                      <td className="px-5 py-5 text-right font-mono text-slate-400 text-xl">{(data.entries.reduce((s, e) => s + (e.quota || 0), 0)).toLocaleString()}</td>
                      <td className="px-5 py-5 text-right font-mono text-blue-300 text-xl">{data.grand_total.total.toLocaleString()}</td>
                      <td className="px-5 py-5 text-right font-mono text-emerald-300 text-xl">{data.grand_total.valid.toLocaleString()}</td>
                      <td className="px-5 py-5 text-right font-mono text-amber-300 text-xl">{Math.max(0, (data.entries.reduce((s, e) => s + (e.quota || 0), 0)) - data.grand_total.valid).toLocaleString()}</td>
                      <td className="px-5 py-5 text-right font-mono text-red-300 text-xl">{data.grand_total.invalid.toLocaleString()}</td>
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

                  <input type="text" value={editForm.district} onChange={e => setEditForm({ ...editForm, district: e.target.value })} className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500 transition-colors" />

                </div>

                <div>

                  <label className="block text-sm font-medium text-slate-400 mb-1">Upazila</label>

                  <input type="text" value={editForm.upazila} onChange={e => setEditForm({ ...editForm, upazila: e.target.value })} className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500 transition-colors" />

                </div>

              </div>

              <div>

                <label className="block text-sm font-medium text-slate-400 mb-1">Total Records</label>

                <input type="number" value={editForm.total} onChange={e => setEditForm({ ...editForm, total: parseInt(e.target.value) || 0 })} className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-blue-300 font-mono focus:outline-none focus:border-blue-500 transition-colors" />

              </div>

              <div className="grid grid-cols-2 gap-4">

                <div>

                  <label className="block text-sm font-medium text-slate-400 mb-1">Valid</label>

                  <input type="number" value={editForm.valid} onChange={e => setEditForm({ ...editForm, valid: parseInt(e.target.value) || 0 })} className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-emerald-400 font-mono focus:outline-none focus:border-emerald-500 transition-colors" />

                </div>

                <div>

                  <label className="block text-sm font-medium text-slate-400 mb-1">Invalid</label>

                  <input type="number" value={editForm.invalid} onChange={e => setEditForm({ ...editForm, invalid: parseInt(e.target.value) || 0 })} className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-red-400 font-mono focus:outline-none focus:border-red-500 transition-colors" />

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



      {/* History Modal */}

      {historyEntry && (

        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-md animate-in fade-in duration-300">

          <div className="bg-slate-900 border border-slate-700/50 rounded-2xl w-full max-w-4xl max-h-[90vh] flex flex-col shadow-2xl overflow-hidden animate-in zoom-in-95 slide-in-from-bottom-4 duration-300">

            <div className="p-6 border-b border-slate-800 flex justify-between items-center bg-slate-900/50 backdrop-blur">

              <div>

                <h3 className="text-xl font-bold flex items-center gap-2">

                  <Clock className="w-5 h-5 text-indigo-400" />

                  Upload History

                </h3>

                <p className="text-sm text-slate-400 mt-0.5">{historyEntry.district} — {historyEntry.upazila}</p>

              </div>

              <button

                onClick={() => setHistoryEntry(null)}

                className="p-2 rounded-lg hover:bg-slate-800 text-slate-500 hover:text-slate-200 transition-all"

              >

                <X className="w-5 h-5" />

              </button>

            </div>



            <div className="flex-1 overflow-auto p-6">

              {historyLoading ? (

                <div className="flex flex-col items-center justify-center py-20 gap-4">

                  <RefreshCw className="w-8 h-8 text-indigo-500 animate-spin" />

                  <p className="text-slate-500">Retrieving revision history...</p>

                </div>

              ) : historyData.length === 0 ? (

                <div className="text-center py-20 text-slate-500 italic">No detailed upload history found.</div>

              ) : (

                <div className="space-y-4">

                  {historyData.map((batch: Batch) => (

                    <div key={batch.id} className={`p-4 rounded-xl border ${batch.status === 'deleted' ? 'bg-red-500/5 border-red-500/20' : 'bg-slate-800/30 border-slate-700/50'} flex items-center justify-between group transition-all hover:border-slate-600`}>

                      <div className="flex gap-4 items-start">

                        <div className={`p-2 rounded-lg shrink-0 ${batch.status === 'deleted' ? 'bg-red-500/10' : 'bg-indigo-500/10'}`}>

                          <FileSpreadsheet className={`w-5 h-5 ${batch.status === 'deleted' ? 'text-red-400' : 'text-indigo-400'}`} />

                        </div>

                        <div>

                          <p className={`font-medium ${batch.status === 'deleted' ? 'text-red-400 line-through opacity-50' : 'text-slate-200'}`}>{batch.filename}</p>

                          <div className="flex flex-wrap gap-x-4 gap-y-1 mt-1 text-xs text-slate-500">

                            <span className="flex items-center gap-1"><Clock className="w-3 h-3" /> {formatDate(batch.created_at)}</span>

                            <span className="flex items-center gap-1 text-slate-400 font-medium">Uploader: {batch.username}</span>

                            <span className="px-1.5 py-0.5 rounded bg-slate-700/50 text-slate-300">ID: #{batch.id}</span>

                          </div>



                          <div className="mt-3 flex gap-3">

                            <div className="px-2 py-1 rounded bg-blue-500/10 border border-blue-500/20">

                              <p className="text-[10px] text-blue-500 uppercase font-bold tracking-tighter">Total</p>

                              <p className="text-sm font-mono text-blue-400 font-bold">{batch.total_rows}</p>

                            </div>

                            <div className="px-2 py-1 rounded bg-emerald-500/10 border border-emerald-500/20">

                              <p className="text-[10px] text-emerald-500 uppercase font-bold tracking-tighter">New</p>

                              <p className="text-sm font-mono text-emerald-400 font-bold">+{batch.new_records}</p>

                            </div>

                            <div className="px-2 py-1 rounded bg-amber-500/10 border border-amber-500/20">

                              <p className="text-[10px] text-amber-500 uppercase font-bold tracking-tighter">Updated</p>

                              <p className="text-sm font-mono text-amber-400 font-bold">{batch.updated_records}</p>

                            </div>

                            <div className="px-2 py-1 rounded bg-red-500/10 border border-red-500/20">

                              <p className="text-[10px] text-red-500 uppercase font-bold tracking-tighter">Invalid</p>

                              <p className="text-sm font-mono text-red-400 font-bold">{batch.invalid_count}</p>

                            </div>

                          </div>

                        </div>

                      </div>



                      <div className="flex gap-2">

                        {user?.role === 'admin' && batch.status !== 'deleted' && (

                          <button

                            onClick={() => handleDeleteBatch(batch.id)}

                            disabled={deletingBatchId === batch.id}

                            className="p-2.5 rounded-lg bg-red-500/10 text-red-400 hover:bg-red-500/20 border border-red-500/20 transition-all active:scale-95"

                          >

                            {deletingBatchId === batch.id ? (

                              <RefreshCw className="w-4 h-4 animate-spin" />

                            ) : (

                              <X className="w-4 h-4" />

                            )}

                          </button>

                        )}

                        {batch.status === 'deleted' && (

                          <span className="text-[10px] uppercase font-bold text-red-500/50 border border-red-500/20 px-2 py-1 rounded rotate-12">Deleted</span>

                        )}

                      </div>

                    </div>

                  ))}

                </div>

              )}

            </div>



            <div className="p-6 bg-slate-800/30 border-t border-slate-800 flex justify-between items-center">

              <p className="text-xs text-slate-500 italic max-w-md">Deleting a batch will remove its records from the system and recalculate Upazila totals. If NIDs were updated in later batches, they won't be reverted.</p>

              <button

                onClick={() => setHistoryEntry(null)}

                className="bg-slate-700 hover:bg-slate-600 text-slate-200 px-6 py-2 rounded-lg font-medium transition-all"

              >

                Close

              </button>

            </div>

          </div>

        </div>

      )}


      {/* ─────────────────────────────────────────────────────────────────
          PRINT ONLY EXECUTIVE SUMMARY VIEW
          ───────────────────────────────────────────────────────────────── */}
      {data && (
        <div className="hidden print:block print-report">
          <h1>Food Friendly Program — Data Validation Summary</h1>
          <p>Generated on: {isMounted ? new Date().toLocaleString() : ""}</p>

          <table>
            <thead>
              <tr>
                <th className="text-left w-1/2">Geographic Location</th>
                <th className="text-right">Total Checked</th>
                <th className="text-right">Valid NIDs</th>
                <th className="text-right">Invalid Rows</th>
              </tr>
            </thead>
            <tbody>
              {hierarchy.map((div: any) => (
                <React.Fragment key={`print-div-${div.name}`}>
                  <tr className="division-row">
                    <td>{div.name} Division</td>
                    <td className="text-right">{div.total.toLocaleString()}</td>
                    <td className="text-right">{div.valid.toLocaleString()}</td>
                    <td className="text-right">{div.invalid.toLocaleString()}</td>
                  </tr>
                  {Object.values(div.districts).map((dist: any) => (
                    <React.Fragment key={`print-dist-${div.name}-${dist.name}`}>
                      <tr className="district-row">
                        <td>↳ {dist.name} District</td>
                        <td className="text-right">{dist.total.toLocaleString()}</td>
                        <td className="text-right">{dist.valid.toLocaleString()}</td>
                        <td className="text-right">{dist.invalid.toLocaleString()}</td>
                      </tr>
                      {dist.upazilas.map((upz: any) => (
                        <tr key={`print-upz-${div.name}-${dist.name}-${upz.upazila}`} className="upazila-row">
                          <td>↳ {upz.upazila}</td>
                          <td className="text-right">{upz.total.toLocaleString()}</td>
                          <td className="text-right">{upz.valid.toLocaleString()}</td>
                          <td className="text-right">{upz.invalid.toLocaleString()}</td>
                        </tr>
                      ))}
                    </React.Fragment>
                  ))}
                </React.Fragment>
              ))}
              {/* Grand Total Footer */}
              <tr className="grand-total">
                <td>NATIONAL GRAND TOTAL</td>
                <td className="text-right text-lg">{data.grand_total.total.toLocaleString()}</td>
                <td className="text-right text-green-700 text-lg">{data.grand_total.valid.toLocaleString()}</td>
                <td className="text-right text-red-700 text-lg">{data.grand_total.invalid.toLocaleString()}</td>
              </tr>
            </tbody>
          </table>
        </div>
      )}

    </main>

  );

}

