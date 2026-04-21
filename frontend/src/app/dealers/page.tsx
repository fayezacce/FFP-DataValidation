"use client";
/**
 * Dealer Management Page
 * /dealers
 *
 * Features:
 * - Paginated dealer table with beneficiary counts
 * - Cross-upazila warning badge (same NID in multiple upazilas)
 * - Geo filter with permission locking
 * - Edit dealer name/mobile (propagates to all linked valid_records.data)
 * - Click dealer row → show linked beneficiaries
 * - Hotlink-friendly: reads ?division, ?district, ?upazila from URL
 */
import React, { useState, useEffect, useCallback, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import {
  Store, Search, RefreshCw, ChevronLeft, ChevronRight,
  Pencil, AlertTriangle, Users, X, Save, Loader2,
  ShieldAlert, CheckCircle2
} from "lucide-react";
import { fetchWithAuth, getUser } from "@/lib/auth";
import GeoFilterBar from "@/components/GeoFilterBar";
import type { DealerRecord, BeneficiaryRecord, User } from "@/types/ffp";

const PAGE_SIZE = 50;

// ── Sub-components ────────────────────────────────────────────────────────────

const CrossBadge = () => (
  <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-medium bg-orange-500/15 text-orange-400 border border-orange-500/30">
    <AlertTriangle className="w-2.5 h-2.5" /> Cross-Upazila
  </span>
);

interface EditDealerModalProps {
  dealer: DealerRecord;
  onClose: () => void;
  onSaved: (updatedName: string, updatedMobile: string) => void;
}

function EditDealerModal({ dealer, onClose, onSaved }: EditDealerModalProps) {
  const [name,   setName]   = useState(dealer.name);
  const [mobile, setMobile] = useState(dealer.mobile || "");
  const [saving, setSaving] = useState(false);
  const [error,  setError]  = useState<string | null>(null);

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      const res = await fetchWithAuth(`/api/records/dealers/${dealer.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, mobile }),
      });
      if (!res.ok) {
        const e = await res.json().catch(() => ({}));
        throw new Error(e.detail || `Error ${res.status}`);
      }
      onSaved(name, mobile);
    } catch (e: any) {
      setError(e.message);
    } finally { setSaving(false); }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm" onClick={onClose}>
      <div className="relative w-full max-w-sm bg-[#12141a] border border-slate-700/60 rounded-2xl shadow-2xl p-6" onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-base font-bold text-slate-100">Edit Dealer</h2>
          <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-slate-700/50 text-slate-400"><X className="w-4 h-4" /></button>
        </div>

        <div className="space-y-3 mb-5">
          <div>
            <label className="block text-xs text-slate-500 mb-1">NID (immutable)</label>
            <input value={dealer.nid} readOnly className="w-full px-3 py-2 text-sm bg-slate-800/60 border border-slate-700 text-slate-500 rounded-lg cursor-not-allowed" />
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1">Dealer Name</label>
            <input value={name} onChange={e => setName(e.target.value)} className="w-full px-3 py-2 text-sm bg-slate-900/70 border border-slate-600 text-slate-200 rounded-lg focus:border-indigo-500 outline-none" id="dealer-edit-name" />
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1">Mobile</label>
            <input value={mobile} onChange={e => setMobile(e.target.value)} className="w-full px-3 py-2 text-sm bg-slate-900/70 border border-slate-600 text-slate-200 rounded-lg focus:border-indigo-500 outline-none" id="dealer-edit-mobile" />
          </div>
        </div>

        {error && <p className="text-sm text-red-400 mb-3">{error}</p>}

        <p className="text-xs text-slate-500 mb-4">Changes will propagate to all linked beneficiary export data.</p>

        <div className="flex justify-end gap-2">
          <button onClick={onClose} className="px-4 py-2 text-sm text-slate-400 hover:text-slate-200">Cancel</button>
          <button onClick={handleSave} disabled={saving} className="flex items-center gap-2 px-5 py-2 text-sm bg-indigo-600 hover:bg-indigo-500 disabled:opacity-60 text-white rounded-lg font-medium" id="dealer-edit-save">
            {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />} Save
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Dealer Beneficiary Panel (side-panel) ─────────────────────────────────────

function DealerBeneficiaryPanel({ dealer, onClose }: { dealer: DealerRecord; onClose: () => void }) {
  const [data, setData]   = useState<{ records: any[]; total: number; page: number } | null>(null);
  const [page, setPage]   = useState(1);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        const res = await fetchWithAuth(`/api/records/dealers/${dealer.id}?page=${page}&page_size=30`);
        if (res.ok) {
          const d = await res.json();
          setData({ records: d.beneficiaries.records, total: d.beneficiaries.total, page });
        }
      } catch {} finally { setLoading(false); }
    };
    load();
  }, [dealer.id, page]);

  const totalPages = data ? Math.ceil(data.total / 30) : 1;

  return (
    <div className="fixed inset-y-0 right-0 z-40 w-full max-w-md bg-[#12141a] border-l border-slate-700/40 shadow-2xl flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-4 border-b border-slate-700/40">
        <div>
          <h3 className="text-sm font-bold text-slate-100">{dealer.name}</h3>
          <p className="text-xs text-slate-500 mt-0.5">NID: {dealer.nid} • {dealer.upazila}</p>
        </div>
        <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-slate-700/50 text-slate-400"><X className="w-4 h-4" /></button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 gap-3 px-5 py-3 border-b border-slate-700/40">
        <div className="bg-slate-800/40 rounded-xl p-3">
          <p className="text-xs text-slate-500">Beneficiaries</p>
          <p className="text-xl font-bold text-slate-100">{dealer.beneficiary_count.toLocaleString("en-IN")}</p>
        </div>
        <div className="bg-slate-800/40 rounded-xl p-3">
          <p className="text-xs text-slate-500">Status</p>
          {dealer.cross_upazila_warning
            ? <CrossBadge />
            : <span className="text-xs text-emerald-400 flex items-center gap-1 mt-1"><CheckCircle2 className="w-3 h-3" /> Single Upazila</span>}
        </div>
      </div>

      {/* Beneficiary list */}
      <div className="flex-1 overflow-y-auto px-5 py-3 space-y-2">
        {loading && <div className="text-center py-8 text-slate-500 text-sm">Loading...</div>}
        {!loading && data?.records.map(r => (
          <div key={r.id} className="flex items-center justify-between px-3 py-2.5 bg-slate-800/30 rounded-xl hover:bg-slate-800/50 transition-colors">
            <div>
              <p className="text-sm text-slate-200 font-medium">{r.name}</p>
              <p className="text-xs text-slate-500">{r.nid} • {r.dob}</p>
            </div>
            <span className={`text-[10px] px-2 py-0.5 rounded-full ${
              r.verification_status === "verified"
                ? "bg-emerald-500/15 text-emerald-400"
                : "bg-amber-500/15 text-amber-400"
            }`}>
              {r.verification_status === "verified" ? "Verified" : "Pending"}
            </span>
          </div>
        ))}
      </div>

      {/* Pagination */}
      {data && data.total > 30 && (
        <div className="flex items-center justify-between px-5 py-3 border-t border-slate-700/40">
          <span className="text-xs text-slate-500">Page {page} of {totalPages}</span>
          <div className="flex gap-2">
            <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="p-1 rounded text-slate-500 hover:text-slate-300 disabled:opacity-30"><ChevronLeft className="w-4 h-4" /></button>
            <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages} className="p-1 rounded text-slate-500 hover:text-slate-300 disabled:opacity-30"><ChevronRight className="w-4 h-4" /></button>
          </div>
        </div>
      )}
    </div>
  );
}

// ── MAIN PAGE CONTENT (needs Suspense wrapper for useSearchParams) ─────────

function DealersContent() {
  const searchParams = useSearchParams();
  const [user, setUser]             = useState<User | null>(null);
  const [dealers, setDealers]       = useState<DealerRecord[]>([]);
  const [total, setTotal]           = useState(0);
  const [page, setPage]             = useState(1);
  const [loading, setLoading]       = useState(false);
  const [search, setSearch]         = useState("");
  const [division, setDivision]     = useState(searchParams.get("division") || "");
  const [district, setDistrict]     = useState(searchParams.get("district") || "");
  const [upazila, setUpazila]       = useState(searchParams.get("upazila")  || "");
  const [editDealer, setEditDealer] = useState<DealerRecord | null>(null);
  const [panelDealer, setPanelDealer] = useState<DealerRecord | null>(null);
  const [toast, setToast]           = useState<string | null>(null);

  useEffect(() => { setUser(getUser()); }, []);

  const notify = (msg: string) => { setToast(msg); setTimeout(() => setToast(null), 3000); };

  const fetchDealers = useCallback(async () => {
    if (!division && !district && !upazila) return;
    setLoading(true);
    try {
      const params = new URLSearchParams({ page: String(page), page_size: String(PAGE_SIZE) });
      if (division) params.set("division", division);
      if (district) params.set("district", district);
      if (upazila)  params.set("upazila", upazila);
      if (search)   params.set("search", search);

      const res = await fetchWithAuth(`/api/records/dealers?${params}`);
      if (res.ok) {
        const data = await res.json();
        setDealers(data.dealers || []);
        setTotal(data.total || 0);
      }
    } catch {} finally { setLoading(false); }
  }, [page, division, district, upazila, search]);

  useEffect(() => { fetchDealers(); }, [fetchDealers]);

  const handleGeoChange = (div: string, dist: string, upz: string) => {
    setDivision(div); setDistrict(dist); setUpazila(upz); setPage(1);
  };

  const handleDealerSaved = (id: number, name: string, mobile: string) => {
    setDealers(prev => prev.map(d => d.id === id ? { ...d, name, mobile } : d));
    setEditDealer(null);
    notify("✓ Dealer updated");
  };

  const totalPages   = Math.ceil(total / PAGE_SIZE);
  const hasGeoFilter = !!(division || district || upazila);
  const canManage    = user?.role === "admin" || user?.role === "uploader";

  return (
    <div className="min-h-screen bg-[#0d0f14] text-slate-200" style={{ fontFamily: "'Inter', sans-serif" }}>
      {/* Toast */}
      {toast && (
        <div className="fixed top-5 right-5 z-[100] px-5 py-3 bg-emerald-600 text-white text-sm rounded-xl shadow-lg">
          {toast}
        </div>
      )}

      {editDealer && (
        <EditDealerModal
          dealer={editDealer}
          onClose={() => setEditDealer(null)}
          onSaved={(name, mobile) => handleDealerSaved(editDealer.id, name, mobile)}
        />
      )}

      {panelDealer && (
        <DealerBeneficiaryPanel dealer={panelDealer} onClose={() => setPanelDealer(null)} />
      )}

      <div className="max-w-[1400px] mx-auto px-4 sm:px-6 py-8">
        {/* Header */}
        <div className="flex items-center gap-3 mb-6">
          <div className="p-2.5 bg-emerald-500/15 rounded-xl border border-emerald-500/30">
            <Store className="w-6 h-6 text-emerald-400" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-slate-100">Dealer Management</h1>
            <p className="text-sm text-slate-500 mt-0.5">Manage dealers and their linked beneficiaries</p>
          </div>
        </div>

        {/* Filters */}
        <div className="bg-slate-900/60 border border-slate-700/40 rounded-2xl p-4 mb-4 space-y-3">
          <GeoFilterBar user={user} onChange={handleGeoChange} initialValues={{ division, district, upazila }} />
          <div className="flex items-center gap-3 border-t border-slate-700/30 pt-3">
            <div className="relative flex-1 max-w-sm">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500" />
              <input
                type="text"
                placeholder="Search dealer name or NID..."
                value={search}
                onChange={e => { setSearch(e.target.value); setPage(1); }}
                className="w-full pl-9 pr-3 py-1.5 text-xs bg-slate-800/60 border border-slate-700 text-slate-300 placeholder-slate-600 rounded-lg focus:border-indigo-500 outline-none"
              />
            </div>
            <button onClick={fetchDealers} className="p-1.5 text-slate-500 hover:text-slate-300 transition-colors"><RefreshCw className="w-4 h-4" /></button>
          </div>
        </div>

        {!hasGeoFilter && (
          <div className="flex flex-col items-center justify-center py-24 text-center">
            <Store className="w-12 h-12 text-slate-700 mb-4" />
            <p className="text-slate-500 text-sm">Select a location above to load dealers.</p>
          </div>
        )}

        {hasGeoFilter && (
          <div className="bg-slate-900/40 border border-slate-700/40 rounded-2xl overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm" id="dealer-table">
                <thead>
                  <tr className="border-b border-slate-700/60 bg-slate-800/40">
                    <th className="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">#</th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Dealer Name</th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">NID</th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Mobile</th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Upazila</th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Beneficiaries</th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Flags</th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-700/20">
                  {loading && (
                    <tr><td colSpan={8} className="text-center py-14 text-slate-500">
                      <div className="flex items-center justify-center gap-2"><RefreshCw className="w-4 h-4 animate-spin" /> Loading...</div>
                    </td></tr>
                  )}

                  {!loading && dealers.map((dealer, i) => (
                    <tr
                      key={dealer.id}
                      className="hover:bg-slate-800/40 transition-colors cursor-pointer"
                      onClick={() => setPanelDealer(dealer)}
                    >
                      <td className="px-4 py-3 text-slate-600 text-xs">{(page - 1) * PAGE_SIZE + i + 1}</td>
                      <td className="px-4 py-3 text-slate-200 font-medium">{dealer.name}</td>
                      <td className="px-4 py-3 text-slate-400 font-mono text-xs">{dealer.nid}</td>
                      <td className="px-4 py-3 text-slate-400 text-xs">{dealer.mobile || "—"}</td>
                      <td className="px-4 py-3 text-slate-400 text-xs">{dealer.upazila}</td>
                      <td className="px-4 py-3">
                        <button
                          className="flex items-center gap-1.5 text-indigo-400 hover:text-indigo-300 transition-colors text-sm"
                          onClick={e => { e.stopPropagation(); setPanelDealer(dealer); }}
                        >
                          <Users className="w-3.5 h-3.5" />
                          {dealer.beneficiary_count.toLocaleString("en-IN")}
                        </button>
                      </td>
                      <td className="px-4 py-3">
                        {dealer.cross_upazila_warning
                          ? <CrossBadge />
                          : <span className="text-slate-600 text-xs">—</span>}
                      </td>
                      <td className="px-4 py-3" onClick={e => e.stopPropagation()}>
                        {canManage && (
                          <button
                            onClick={() => setEditDealer(dealer)}
                            title="Edit Dealer"
                            className="p-1.5 rounded-lg text-slate-500 hover:text-amber-400 hover:bg-amber-500/10 transition-colors"
                          >
                            <Pencil className="w-4 h-4" />
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}

                  {!loading && dealers.length === 0 && hasGeoFilter && (
                    <tr><td colSpan={8} className="text-center py-16 text-slate-500 text-sm">No dealers found.</td></tr>
                  )}
                </tbody>
              </table>
            </div>

            <div className="flex items-center justify-between px-5 py-3 border-t border-slate-700/40">
              <p className="text-xs text-slate-500">
                Showing {dealers.length} of <span className="text-slate-300 font-medium">{total.toLocaleString("en-IN")}</span> dealers
              </p>
              <div className="flex items-center gap-2">
                <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="p-1.5 rounded-lg text-slate-500 hover:text-slate-300 disabled:opacity-30 transition-colors">
                  <ChevronLeft className="w-4 h-4" />
                </button>
                <span className="text-xs text-slate-400 px-2">Page <span className="text-slate-200 font-medium">{page}</span> of {totalPages || 1}</span>
                <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages} className="p-1.5 rounded-lg text-slate-500 hover:text-slate-300 disabled:opacity-30 transition-colors">
                  <ChevronRight className="w-4 h-4" />
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── PAGE SHELL — required by Next.js 14 for useSearchParams ─────────────────

export default function DealersPage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-[#0d0f14] flex items-center justify-center">
        <div className="flex items-center gap-3 text-slate-400">
          <RefreshCw className="w-5 h-5 animate-spin" />
          <span className="text-sm">Loading...</span>
        </div>
      </div>
    }>
      <DealersContent />
    </Suspense>
  );
}
