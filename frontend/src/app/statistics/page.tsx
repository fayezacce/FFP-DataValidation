"use client";

import React, { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { 
  ArrowLeft, RefreshCw, BarChart3, FileWarning, Search as SearchIcon, 
  Download, Printer, FileText, CheckCircle2, Clock 
} from "lucide-react";
import { fetchWithAuth, getBackendUrl, downloadFileWithAuth } from "@/lib/auth";

import { StatsEntry, StatsResponse, Batch } from "@/types/ffp";
import DashboardCards from "@/components/DashboardCards";
import StatsTable from "@/components/StatsTable";
import BatchHistoryModal from "@/components/BatchHistoryModal";
import EditStatsModal from "@/components/EditStatsModal";

export default function StatisticsPage() {
  const [data, setData] = useState<StatsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  
  const [user, setUser] = useState<any>(null);
  const [isMounted, setIsMounted] = useState(false);

  useEffect(() => {
    setIsMounted(true);
    setUser(JSON.parse(localStorage.getItem("ffp_user") || "null"));
    fetchStats();
  }, []);

  // Filter & Sort State
  const [searchTerm, setSearchTerm] = useState("");
  const [showInvalidOnly, setShowInvalidOnly] = useState(false);

  // History State
  const [historyEntry, setHistoryEntry] = useState<StatsEntry | null>(null);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyData, setHistoryData] = useState<Batch[]>([]);
  const [deletingBatchId, setDeletingBatchId] = useState<number | null>(null);

  // Edit State
  const [editingEntry, setEditingEntry] = useState<StatsEntry | null>(null);
  const [savingEdit, setSavingEdit] = useState(false);
  const [editForm, setEditForm] = useState({
    district: "",
    upazila: "",
    total: 0,
    valid: 0,
    invalid: 0
  });

  // Recheck state
  const [recheckLoading, setRecheckLoading] = React.useState<string | null>(null);
  const [recheckResult, setRecheckResult] = React.useState<{ msg: string; flagged: number } | null>(null);

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

  const handleDownloadAllValid = async () => {
    try {
      const res = await fetchWithAuth("/api/export/zip-valid", { method: "POST" });
      if (res.ok) alert("Valid records zip task started! Check the Task Tray in the bottom right corner.");
      else alert("Failed to start valid records zip task");
    } catch(e) { console.error(e); }
  };

  const handleDownloadAllInvalid = async () => {
    try {
      const res = await fetchWithAuth("/api/export/zip-invalid", { method: "POST" });
      if (res.ok) alert("Invalid records zip task started! Check the Task Tray in the bottom right corner.");
      else alert("Failed to start invalid records zip task");
    } catch(e) { console.error(e); }
  };

  const buildLiveExportInvalidUrl = (entry: StatsEntry, fmt: string) => {
    const p = new URLSearchParams({
      division: entry.division,
      district: entry.district,
      upazila: entry.upazila,
      upazila_id: entry.id?.toString() || "",
      fmt,
    });
    return "/api/export/live-invalid?" + p.toString();
  };

  const buildLiveExportUrl = (entry: StatsEntry, fmt: string) => {
    const p = new URLSearchParams({
      division: entry.division,
      district: entry.district,
      upazila: entry.upazila,
      upazila_id: entry.id?.toString() || "",
      fmt,
    });
    return "/api/export/live?" + p.toString();
  };

  const handleRecheck = async (entry: StatsEntry) => {
    const key = `${entry.division}|${entry.district}|${entry.upazila}`;
    setRecheckLoading(key);
    setRecheckResult(null);
    try {
      const p = new URLSearchParams({
        division: entry.division,
        district: entry.district,
        upazila: entry.upazila,
        fmt: "json",
      });
      const resp = await fetchWithAuth("/api/export/recheck?" + p.toString(), { method: "POST" });
      const data = await resp.json();
      if (data.flagged_count === 0) {
        alert(`No suspicious NIDs in ${entry.upazila} (${data.total_checked} checked)`);
      } else {
        const dlp = new URLSearchParams({ division: entry.division, district: entry.district, upazila: entry.upazila, fmt: "xlsx" });
        downloadFileWithAuth("/api/export/recheck?" + dlp.toString(), `${entry.district}_${entry.upazila}_fraud_report.xlsx`);
      }
    } catch {
      alert("Re-check failed");
    } finally {
      setRecheckLoading(null);
    }
  };

  const openHistory = async (entry: StatsEntry) => {
    setHistoryEntry(entry);
    setHistoryLoading(true);
    try {
      const res = await fetchWithAuth(`${getBackendUrl()}/batches/history?district=${encodeURIComponent(entry.district)}&upazila=${encodeURIComponent(entry.upazila)}`);
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
      const res = await fetchWithAuth(`${getBackendUrl()}/batches/${batchId}`, { method: "DELETE" });
      if (!res.ok) throw new Error("Failed to delete batch");
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

  const openEditModal = (entry: StatsEntry) => {
    setEditingEntry(entry);
    setEditForm({
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
          old_district: editingEntry.district,
          old_upazila: editingEntry.upazila,
          new_district: editForm.district,
          new_upazila: editForm.upazila,
          total: editForm.total,
          valid: editForm.valid,
          invalid: editForm.invalid,
        }),
      });
      if (!res.ok) throw new Error("Failed to update statistics");
      setEditingEntry(null);
      fetchStats();
    } catch (err: any) {
      alert("Error saving: " + err.message);
    } finally {
      setSavingEdit(false);
    }
  };

  const handleDeleteUpazila = async (entry: StatsEntry) => {
    if (!confirm(`CRITICAL: This will delete ALL data (Valid & Invalid records) and files for ${entry.upazila}, ${entry.district}. proceed?`)) return;
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

  const handleFormChange = (field: string, value: any) => {
    setEditForm(prev => ({ ...prev, [field]: value }));
  };

  // Filter Data & Prepare Hierarchy for StatsTable
  const filteredEntries = (data?.entries || []).filter((e: StatsEntry) => {
    const matchesSearch =
      e.district.toLowerCase().includes(searchTerm.toLowerCase()) ||
      e.upazila.toLowerCase().includes(searchTerm.toLowerCase()) ||
      e.division.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesInvalid = !showInvalidOnly || e.invalid > 0;
    return matchesSearch && matchesInvalid;
  });

  const hierarchy = React.useMemo(() => {
    const list: any[] = [];
    const divMap: Record<string, any> = {};

    filteredEntries.forEach((entry: StatsEntry) => {
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

  return (
    <main className="min-h-screen p-4 md:p-8 lg:p-12 text-slate-200 flex flex-col">
      <div className="max-w-7xl mx-auto space-y-8 flex-1 w-full print:hidden">

        {/* Header Controls */}
        <header className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
          <div className="flex items-center gap-4">
            <Link href="/" className="p-2 rounded-lg bg-slate-800/50 border border-slate-700 hover:bg-slate-700/50 transition-colors">
              <ArrowLeft className="w-5 h-5 text-slate-400" />
            </Link>
            <div>
              <h1 className="text-3xl md:text-4xl font-extrabold tracking-tight bg-gradient-to-r from-cyan-400 to-blue-400 bg-clip-text text-transparent">
                Statistics Dashboard
              </h1>
              <p className="text-sm text-slate-400 mt-1">Validation results by Division, District & Upazila</p>
            </div>
          </div>
          
          <div className="flex flex-wrap items-center gap-3">
            <div className="relative group">
              <SearchIcon className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500 group-focus-within:text-indigo-400 transition-colors" />
              <input
                type="text"
                placeholder="Search district or upazila..."
                value={searchTerm}
                onChange={e => setSearchTerm(e.target.value)}
                className="pl-9 pr-4 py-2 bg-slate-800/50 border border-slate-700 rounded-lg text-sm text-slate-200 focus:outline-none focus:border-indigo-500/50 w-64 transition-all"
              />
            </div>
            
            <button
              onClick={() => setShowInvalidOnly(prev => !prev)}
              className={`px-4 py-2 rounded-lg font-medium flex items-center gap-2 transition-all text-sm border ${showInvalidOnly ? 'bg-red-600/30 border-red-500/50 text-red-300' : 'bg-slate-800/50 border-slate-700/50 text-slate-400'}`}
            >
              <FileWarning className="w-4 h-4" /> {showInvalidOnly ? 'Showing Invalid' : 'Invalid Only'}
            </button>
            <Link href="/search" className="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600/20 text-indigo-400 text-sm border border-indigo-500/30">
              <SearchIcon className="w-4 h-4" /> Search Records
            </Link>
            <button onClick={fetchStats} className="bg-indigo-600 text-white px-4 py-2 rounded-lg text-sm flex gap-2">
              <RefreshCw className={`w-4 h-4 ${loading ? "animate-spin" : ""}`} /> Refresh
            </button>
          </div>
        </header>

        {error && <div className="p-4 rounded-lg bg-red-500/10 border border-red-500/50 text-red-400">{error}</div>}

        {/* Grand Total Cards */}
        {data && <DashboardCards grandTotal={data.grand_total} loading={loading} />}

        {/* Empty State */}
        {!loading && data && data.entries.length === 0 && (
          <div className="glass-panel rounded-2xl p-12 flex flex-col items-center text-center">
            <BarChart3 className="w-16 h-16 text-slate-600" />
            <h2 className="text-xl font-semibold text-slate-300 mt-4">No Statistics Yet</h2>
            <Link href="/" className="mt-4 bg-indigo-600 text-white px-6 py-2.5 rounded-lg transition-all">Upload & Validate</Link>
          </div>
        )}

        {/* Statistics Hierarchy Table */}
        {data && data.entries.length > 0 && (
          <StatsTable 
            hierarchy={hierarchy}
            isAdmin={user?.role === 'admin'}
            onOpenHistory={openHistory}
            onOpenEdit={openEditModal}
            onDeleteUpazila={handleDeleteUpazila}
            onRecheck={handleRecheck}
            recheckLoading={recheckLoading}
            recheckResult={recheckResult}
            buildLiveExportUrl={buildLiveExportUrl}
            buildLiveExportInvalidUrl={buildLiveExportInvalidUrl}
            downloadFileWithAuth={downloadFileWithAuth}
          />
        )}
      </div>

      <footer className="mt-12 py-6 border-t border-slate-800/50 text-center text-sm text-slate-500">
        © {new Date().getFullYear()} Computer Network Unit | Directorate General of Food
      </footer>

      {/* Modals */}
      <BatchHistoryModal 
        entry={historyEntry}
        loading={historyLoading}
        batches={historyData}
        deletingBatchId={deletingBatchId}
        onClose={() => setHistoryEntry(null)}
        onDeleteBatch={handleDeleteBatch}
      />

      <EditStatsModal 
        entry={editingEntry}
        form={editForm}
        saving={savingEdit}
        onClose={() => setEditingEntry(null)}
        onFormChange={handleFormChange}
        onSave={handleSaveEdit}
      />
    </main>
  );
}
