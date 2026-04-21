"use client";
/**
 * Beneficiary Management Page
 * /beneficiaries
 *
 * Features:
 * - Server-side paginated table (fast — uses dedicated columns, no JSON)
 * - Geo filter (Division / District / Upazila) — permission-locked for non-admin users
 * - Status filter (valid / invalid / all)
 * - Verification filter (verified / unverified)
 * - Column selector (persisted in localStorage)
 * - View detail modal, Edit modal, Delete with confirmation
 * - Bulk verify toolbar
 * - Hotlink-friendly: reads ?division, ?district, ?upazila, ?filter from URL params
 */
import React, { useState, useEffect, useCallback, useRef, Suspense } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import {
  Users, Search, SlidersHorizontal, Eye, Pencil, Trash2,
  CheckCircle2, Clock, ChevronDown, ChevronLeft, ChevronRight,
  Download, RefreshCw, ShieldCheck, AlertTriangle, Plus,
} from "lucide-react";
import { fetchWithAuth, getUser } from "@/lib/auth";
import GeoFilterBar from "@/components/GeoFilterBar";
import RecordDetailModal from "@/components/RecordDetailModal";
import RecordEditModal from "@/components/RecordEditModal";
import type { BeneficiaryRecord, InvalidBeneficiaryRecord, User } from "@/types/ffp";

// ── Column definitions ────────────────────────────────────────────────────────

const ALL_COLUMNS = [
  { key: "nid",                 label: "NID",          defaultOn: true  },
  { key: "name",                label: "Name",         defaultOn: true  },
  { key: "name_bn",             label: "Name (বাংলা)", defaultOn: false },
  { key: "dob",                 label: "DOB",          defaultOn: true  },
  { key: "father_husband_name", label: "Father/Hus.",  defaultOn: true  },
  { key: "card_no",             label: "Card No",      defaultOn: false },
  { key: "mobile",              label: "Mobile",       defaultOn: false },
  { key: "ward",                label: "Ward",         defaultOn: false },
  { key: "union_name",          label: "Union",        defaultOn: false },
  { key: "division",            label: "Division",     defaultOn: false },
  { key: "district",            label: "District",     defaultOn: false },
  { key: "upazila",             label: "Upazila",      defaultOn: false },
  { key: "dealer_name",         label: "Dealer",       defaultOn: false },
  { key: "verification_status", label: "Status",       defaultOn: true  },
];

const LS_COL_KEY = "ffp_beneficiary_columns";
const PAGE_SIZE  = 50;

function getDefaultColumns(): Set<string> {
  try {
    const saved = localStorage.getItem(LS_COL_KEY);
    if (saved) return new Set(JSON.parse(saved));
  } catch {}
  return new Set(ALL_COLUMNS.filter(c => c.defaultOn).map(c => c.key));
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const StatusBadge = ({ status }: { status: "verified" | "unverified" }) =>
  <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-medium ${
    status === "verified"
      ? "bg-emerald-500/15 text-emerald-400 border border-emerald-500/30"
      : "bg-amber-500/15 text-amber-400 border border-amber-500/30"
  }`}>
    {status === "verified" ? <CheckCircle2 className="w-3 h-3" /> : <Clock className="w-3 h-3" />}
    {status === "verified" ? "Verified" : "Pending"}
  </span>;

const CellValue = ({ col, record }: { col: string; record: BeneficiaryRecord }) => {
  if (col === "verification_status") return <StatusBadge status={record.verification_status} />;
  const v = (record as any)[col];
  return <span className="truncate">{v || <span className="text-slate-600">—</span>}</span>;
};

// ─────────────────────────────────────────────────────────────────────────────
// MAIN PAGE
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// PAGE SHELL — wraps content in Suspense (required by Next.js 14 for useSearchParams)
// ─────────────────────────────────────────────────────────────────────────────

export default function BeneficiariesPage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-[#0d0f14] flex items-center justify-center">
        <div className="flex items-center gap-3 text-slate-400">
          <RefreshCw className="w-5 h-5 animate-spin" />
          <span className="text-sm">Loading...</span>
        </div>
      </div>
    }>
      <BeneficiariesContent />
    </Suspense>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// INNER CONTENT — uses useSearchParams so must be inside Suspense above
// ─────────────────────────────────────────────────────────────────────────────

function BeneficiariesContent() {
  const searchParams  = useSearchParams();
  const router        = useRouter();
  const [user, setUser]           = useState<User | null>(null);
  const [records, setRecords]     = useState<BeneficiaryRecord[]>([]);
  const [invalidRecs, setInvalidRecs]   = useState<InvalidBeneficiaryRecord[]>([]);
  const [total, setTotal]         = useState(0);
  const [page,  setPage]          = useState(1);
  const [loading, setLoading]     = useState(false);
  const [search,  setSearch]      = useState("");
  const [filter,  setFilter]      = useState<"valid" | "invalid" | "all">(
    (searchParams.get("filter") as any) || "valid"
  );
  const [verifyFilter, setVerifyFilter] = useState<"" | "verified" | "unverified">("");
  const [division, setDivision]   = useState(searchParams.get("division") || "");
  const [district, setDistrict]   = useState(searchParams.get("district") || "");
  const [upazila,  setUpazila]    = useState(searchParams.get("upazila")  || "");
  const [visibleCols, setVisibleCols] = useState<Set<string>>(new Set());
  const [showColPicker, setShowColPicker] = useState(false);
  const [selected, setSelected]   = useState<Set<number>>(new Set());
  const [viewRecord, setViewRecord] = useState<BeneficiaryRecord | null>(null);
  const [editRecord, setEditRecord] = useState<BeneficiaryRecord | null>(null);
  const [deleteId,   setDeleteId]   = useState<number | null>(null);
  const [toast,   setToast]       = useState<string | null>(null);
  const searchRef = useRef<ReturnType<typeof setTimeout>>();

  // Init
  useEffect(() => {
    const u = getUser();
    setUser(u);
    setVisibleCols(getDefaultColumns());
  }, []);

  // Notify toast
  const notify = (msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(null), 3500);
  };

  // ── Fetch ──
  const fetchRecords = useCallback(async () => {
    if (!division && !district && !upazila) return; // require at least one geo
    setLoading(true);
    try {
      const params = new URLSearchParams({
        page: String(page),
        page_size: String(PAGE_SIZE),
      });
      if (division)      params.set("division", division);
      if (district)      params.set("district", district);
      if (upazila)       params.set("upazila", upazila);
      if (search)        params.set("search", search);
      if (verifyFilter)  params.set("verification_status", verifyFilter);

      if (filter === "valid" || filter === "all") {
        const res = await fetchWithAuth(`/api/records/beneficiaries?${params}`);
        if (res.ok) {
          const data = await res.json();
          setRecords(data.records || []);
          setTotal(data.total || 0);
        }
      }

      if (filter === "invalid") {
        const res = await fetchWithAuth(`/api/records/invalid?${params}`);
        if (res.ok) {
          const data = await res.json();
          setInvalidRecs(data.records || []);
          setTotal(data.total || 0);
        }
      }
    } catch {}
    finally { setLoading(false); }
  }, [page, filter, division, district, upazila, search, verifyFilter]);

  useEffect(() => { fetchRecords(); }, [fetchRecords]);

  // Debounce search
  const handleSearchChange = (val: string) => {
    setSearch(val);
    clearTimeout(searchRef.current);
    searchRef.current = setTimeout(() => setPage(1), 400);
  };

  // Column picker persistence
  const toggleCol = (key: string) => {
    setVisibleCols(prev => {
      const next = new Set(prev);
      next.has(key) ? next.delete(key) : next.add(key);
      localStorage.setItem(LS_COL_KEY, JSON.stringify([...next]));
      return next;
    });
  };

  // Geo filter change
  const handleGeoChange = (div: string, dist: string, upz: string) => {
    setDivision(div);  setDistrict(dist);  setUpazila(upz);
    setPage(1);
  };

  // Row selection
  const toggleSelect = (id: number) => {
    setSelected(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };
  const isAllSelected = records.length > 0 && records.every(r => selected.has(r.id));
  const toggleAll = () => {
    setSelected(prev => {
      const next = new Set(prev);
      if (isAllSelected) records.forEach(r => next.delete(r.id));
      else               records.forEach(r => next.add(r.id));
      return next;
    });
  };

  // Bulk verify
  const handleBulkVerify = async () => {
    if (!selected.size) return;
    try {
      const res = await fetchWithAuth("/api/records/beneficiaries/bulk-verify", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ record_ids: [...selected] }),
      });
      if (res.ok) {
        const data = await res.json();
        notify(`✓ ${data.count} records verified`);
        setSelected(new Set());
        fetchRecords();
      }
    } catch {}
  };

  // Delete
  const handleDelete = async (id: number, isInvalid = false) => {
    const url = isInvalid ? `/api/records/invalid/${id}` : `/api/records/beneficiaries/${id}`;
    try {
      const res = await fetchWithAuth(url, { method: "DELETE" });
      if (res.ok) {
        notify("Record deleted");
        setDeleteId(null);
        fetchRecords();
      }
    } catch {}
  };

  // View detail
  const handleViewDetail = async (record: BeneficiaryRecord) => {
    try {
      const res = await fetchWithAuth(`/api/records/beneficiaries/${record.id}`);
      if (res.ok) setViewRecord(await res.json());
    } catch {}
  };

  const activeColumns = ALL_COLUMNS.filter(c => visibleCols.has(c.key));
  const totalPages    = Math.ceil(total / PAGE_SIZE);
  const canManage     = user?.role === "admin" || user?.role === "uploader";
  const hasGeoFilter  = !!(division || district || upazila);

  return (
    <div className="min-h-screen bg-[#0d0f14] text-slate-200" style={{ fontFamily: "'Inter', sans-serif" }}>
      {/* Toast */}
      {toast && (
        <div className="fixed top-5 right-5 z-[100] px-5 py-3 bg-emerald-600 text-white text-sm rounded-xl shadow-lg animate-fade-in">
          {toast}
        </div>
      )}

      {/* Delete confirm */}
      {deleteId != null && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
          <div className="bg-[#12141a] border border-red-500/30 rounded-2xl p-6 w-full max-w-sm shadow-2xl">
            <div className="flex items-center gap-3 mb-3">
              <AlertTriangle className="w-6 h-6 text-red-400 flex-shrink-0" />
              <h3 className="text-base font-bold text-slate-100">Confirm Delete</h3>
            </div>
            <p className="text-sm text-slate-400 mb-5">This action is permanent. Stats will be updated automatically.</p>
            <div className="flex gap-3 justify-end">
              <button onClick={() => setDeleteId(null)} className="px-4 py-2 text-sm text-slate-400 hover:text-slate-200 transition-colors">Cancel</button>
              <button onClick={() => handleDelete(deleteId, filter === "invalid")} className="px-4 py-2 text-sm bg-red-600 hover:bg-red-500 text-white rounded-lg transition-colors font-medium">Delete</button>
            </div>
          </div>
        </div>
      )}

      {/* Modals */}
      {viewRecord && !editRecord && (
        <RecordDetailModal
          record={viewRecord}
          onClose={() => setViewRecord(null)}
          onEdit={r => { setEditRecord(r); setViewRecord(null); }}
        />
      )}
      {editRecord && (
        <RecordEditModal
          record={editRecord}
          onClose={() => setEditRecord(null)}
          onSaved={() => { setEditRecord(null); notify("Record saved"); fetchRecords(); }}
        />
      )}

      <div className="max-w-[1600px] mx-auto px-4 sm:px-6 py-8">
        {/* Header */}
        <div className="flex flex-wrap items-start justify-between gap-4 mb-6">
          <div className="flex items-center gap-3">
            <div className="p-2.5 bg-indigo-500/15 rounded-xl border border-indigo-500/30">
              <Users className="w-6 h-6 text-indigo-400" />
            </div>
            <div>
              <h1 className="text-2xl font-bold text-slate-100">Beneficiary Management</h1>
              <p className="text-sm text-slate-500 mt-0.5">View, edit, verify and manage beneficiary records</p>
            </div>
          </div>
          {canManage && (
            <button
              onClick={() => router.push("/beneficiaries/add")}
              className="flex items-center gap-2 px-4 py-2 text-sm bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl transition-colors font-medium"
              id="add-beneficiary-btn"
            >
              <Plus className="w-4 h-4" /> Add Record
            </button>
          )}
        </div>

        {/* Filter bar */}
        <div className="bg-slate-900/60 border border-slate-700/40 rounded-2xl p-4 mb-4 space-y-3">
          <GeoFilterBar
            user={user}
            onChange={handleGeoChange}
            initialValues={{ division, district, upazila }}
          />

          <div className="flex flex-wrap items-center gap-3 pt-1 border-t border-slate-700/30">
            {/* Record type tabs */}
            <div className="flex items-center gap-1 bg-slate-800/60 rounded-lg p-1">
              {(["valid", "invalid", "all"] as const).map(f => (
                <button
                  key={f}
                  onClick={() => { setFilter(f); setPage(1); }}
                  className={`px-3 py-1.5 text-xs font-medium rounded-md transition-all capitalize ${
                    filter === f
                      ? "bg-indigo-600 text-white shadow"
                      : "text-slate-400 hover:text-slate-200"
                  }`}
                >
                  {f}
                </button>
              ))}
            </div>

            {/* Verification filter */}
            {filter !== "invalid" && (
              <select
                value={verifyFilter}
                onChange={e => { setVerifyFilter(e.target.value as any); setPage(1); }}
                className="px-3 py-1.5 text-xs bg-slate-800/60 border border-slate-700 text-slate-300 rounded-lg focus:border-indigo-500 outline-none"
              >
                <option value="">All Statuses</option>
                <option value="unverified">Unverified</option>
                <option value="verified">Verified</option>
              </select>
            )}

            {/* Search */}
            <div className="relative flex-1 min-w-[200px]">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500" />
              <input
                type="text"
                placeholder="Search name, NID, mobile..."
                value={search}
                onChange={e => handleSearchChange(e.target.value)}
                className="w-full pl-9 pr-3 py-1.5 text-xs bg-slate-800/60 border border-slate-700 text-slate-300 placeholder-slate-600 rounded-lg focus:border-indigo-500 outline-none"
              />
            </div>

            {/* Column picker */}
            <div className="relative">
              <button
                onClick={() => setShowColPicker(v => !v)}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-slate-800/60 border border-slate-700 text-slate-400 hover:text-slate-200 rounded-lg transition-colors"
              >
                <SlidersHorizontal className="w-3.5 h-3.5" /> Columns
              </button>
              {showColPicker && (
                <div className="absolute right-0 mt-1 z-30 bg-[#12141a] border border-slate-700/60 rounded-xl shadow-xl p-3 w-56">
                  {ALL_COLUMNS.map(c => (
                    <label key={c.key} className="flex items-center gap-2.5 py-1.5 cursor-pointer hover:text-slate-100 text-slate-400 text-sm">
                      <input
                        type="checkbox"
                        checked={visibleCols.has(c.key)}
                        onChange={() => toggleCol(c.key)}
                        className="w-3.5 h-3.5 accent-indigo-500"
                      />
                      {c.label}
                    </label>
                  ))}
                </div>
              )}
            </div>

            <button onClick={fetchRecords} className="p-1.5 text-slate-500 hover:text-slate-300 transition-colors" title="Refresh">
              <RefreshCw className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Prompt if no geo selected */}
        {!hasGeoFilter && (
          <div className="flex flex-col items-center justify-center py-24 text-center">
            <Users className="w-12 h-12 text-slate-700 mb-4" />
            <p className="text-slate-500 text-sm">Select a Division, District, or Upazila above to load records.</p>
          </div>
        )}

        {/* Bulk actions bar */}
        {selected.size > 0 && filter === "valid" && (
          <div className="flex items-center justify-between px-4 py-2 mb-3 bg-indigo-600/10 border border-indigo-500/30 rounded-xl text-sm">
            <span className="text-indigo-300 font-medium">{selected.size} record{selected.size > 1 ? "s" : ""} selected</span>
            <div className="flex gap-2">
              <button
                onClick={handleBulkVerify}
                className="flex items-center gap-1.5 px-3 py-1.5 bg-emerald-600/20 border border-emerald-500/30 text-emerald-400 hover:bg-emerald-600/30 rounded-lg transition-colors text-xs"
              >
                <ShieldCheck className="w-3.5 h-3.5" /> Verify Selected
              </button>
              <button onClick={() => setSelected(new Set())} className="text-xs text-slate-400 hover:text-slate-200 px-2 transition-colors">
                Clear
              </button>
            </div>
          </div>
        )}

        {/* Table */}
        {hasGeoFilter && (
          <div className="bg-slate-900/40 border border-slate-700/40 rounded-2xl overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm" id="beneficiary-table">
                <thead>
                  <tr className="border-b border-slate-700/60 bg-slate-800/40">
                    {canManage && filter === "valid" && (
                      <th className="w-10 px-4 py-3">
                        <input type="checkbox" checked={isAllSelected} onChange={toggleAll} className="accent-indigo-500" />
                      </th>
                    )}
                    <th className="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">#</th>
                    {activeColumns.map(c => (
                      <th key={c.key} className="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider whitespace-nowrap">
                        {c.label}
                      </th>
                    ))}
                    <th className="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-700/20">
                  {loading && (
                    <tr><td colSpan={activeColumns.length + 3} className="text-center py-16 text-slate-500">
                      <div className="flex items-center justify-center gap-2">
                        <RefreshCw className="w-4 h-4 animate-spin" /> Loading...
                      </div>
                    </td></tr>
                  )}

                  {!loading && filter !== "invalid" && records.map((record, i) => (
                    <tr
                      key={record.id}
                      className={`transition-colors ${selected.has(record.id) ? "bg-indigo-600/8" : "hover:bg-slate-800/40"}`}
                    >
                      {canManage && (
                        <td className="px-4 py-2.5">
                          <input type="checkbox" checked={selected.has(record.id)} onChange={() => toggleSelect(record.id)} className="accent-indigo-500" />
                        </td>
                      )}
                      <td className="px-4 py-2.5 text-slate-600 text-xs">{(page - 1) * PAGE_SIZE + i + 1}</td>
                      {activeColumns.map(c => (
                        <td key={c.key} className="px-4 py-2.5 text-slate-300 max-w-[200px]">
                          <CellValue col={c.key} record={record} />
                        </td>
                      ))}
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-1.5">
                          <button onClick={() => handleViewDetail(record)} title="View" className="p-1.5 rounded-lg text-slate-500 hover:text-indigo-400 hover:bg-indigo-500/10 transition-colors">
                            <Eye className="w-4 h-4" />
                          </button>
                          {canManage && (
                            <>
                              <button onClick={() => setEditRecord(record)} title="Edit" className="p-1.5 rounded-lg text-slate-500 hover:text-amber-400 hover:bg-amber-500/10 transition-colors">
                                <Pencil className="w-4 h-4" />
                              </button>
                              <button onClick={() => setDeleteId(record.id)} title="Delete" className="p-1.5 rounded-lg text-slate-500 hover:text-red-400 hover:bg-red-500/10 transition-colors">
                                <Trash2 className="w-4 h-4" />
                              </button>
                            </>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}

                  {!loading && filter === "invalid" && invalidRecs.map((record, i) => (
                    <tr key={record.id} className="hover:bg-slate-800/40 transition-colors">
                      {canManage && <td className="px-4 py-2.5" />}
                      <td className="px-4 py-2.5 text-slate-600 text-xs">{(page - 1) * PAGE_SIZE + i + 1}</td>
                      {activeColumns.map(c => (
                        <td key={c.key} className="px-4 py-2.5 text-slate-300 max-w-[200px]">
                          <CellValue col={c.key} record={record} />
                        </td>
                      ))}
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-1.5">
                          <button onClick={() => handleViewDetail(record as any)} title="View" className="p-1.5 rounded-lg text-slate-500 hover:text-indigo-400 hover:bg-indigo-500/10 transition-colors">
                            <Eye className="w-4 h-4" />
                          </button>
                          {canManage && (
                            <>
                              <button onClick={() => setEditRecord(record as any)} title="Edit" className="p-1.5 rounded-lg text-slate-500 hover:text-amber-400 hover:bg-amber-500/10 transition-colors">
                                <Pencil className="w-4 h-4" />
                              </button>
                              <button onClick={() => setDeleteId(record.id)} title="Delete" className="p-1.5 rounded-lg text-slate-500 hover:text-red-400 hover:bg-red-500/10 transition-colors">
                                <Trash2 className="w-4 h-4" />
                              </button>
                            </>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}

                  {!loading && filter !== "invalid" && records.length === 0 && hasGeoFilter && (
                    <tr><td colSpan={activeColumns.length + 3} className="text-center py-16 text-slate-500 text-sm">
                      No records found for this selection.
                    </td></tr>
                  )}
                  {!loading && filter === "invalid" && invalidRecs.length === 0 && hasGeoFilter && (
                    <tr><td colSpan={activeColumns.length + 3} className="text-center py-16 text-slate-500 text-sm">
                      No invalid records found.
                    </td></tr>
                  )}
                </tbody>
              </table>
            </div>

            {/* Pagination footer */}
            <div className="flex items-center justify-between px-5 py-3 border-t border-slate-700/40">
              <p className="text-xs text-slate-500">
                Showing {records.length || invalidRecs.length} of <span className="text-slate-300 font-medium">{total.toLocaleString("en-IN")}</span> records
              </p>
              <div className="flex items-center gap-2">
                <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="p-1.5 rounded-lg text-slate-500 hover:text-slate-300 disabled:opacity-30 transition-colors">
                  <ChevronLeft className="w-4 h-4" />
                </button>
                <span className="text-xs text-slate-400 px-2">
                  Page <span className="text-slate-200 font-medium">{page}</span> of {totalPages || 1}
                </span>
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
