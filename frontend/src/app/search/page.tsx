"use client";

import React, { useState } from "react";
import Link from "next/link";
import { ArrowLeft, Search as SearchIcon, Database, MapPin, Clock, FileText, User, Hash, Trash2, AlertTriangle, X, ChevronDown } from "lucide-react";

interface ValidRecord {
  id: number;
  nid: string;
  dob: string;
  name: string;
  division: string;
  district: string;
  upazila: string;
  source_file: string;
  upload_batch?: number;
  data?: Record<string, any>;
  created_at: string;
  updated_at?: string;
}

export default function SearchPage() {
  const [query, setQuery] = useState("");
  const [searchType, setSearchType] = useState<"nid" | "dob" | "name">("nid");
  const [results, setResults] = useState<ValidRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [selectedRecord, setSelectedRecord] = useState<ValidRecord | null>(null);
  const [deleteConfirm, setDeleteConfirm] = useState<ValidRecord | null>(null);
  const [deleting, setDeleting] = useState(false);


  const handleSearch = async (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    if (!query.trim()) return;

    setLoading(true);
    setSearched(true);
    try {
      const res = await fetch(`/api/search?query=${encodeURIComponent(query.trim())}&type=${searchType}`);
      if (!res.ok) throw new Error("Search failed");
      const data = await res.json();
      setResults(data);
    } catch (err) {
      console.error(err);
      alert("Error performing search. Make sure the backend is running.");
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async (record: ValidRecord) => {
    setDeleting(true);
    try {
      const res = await fetch(`/api/record/${record.id}`, { method: "DELETE" });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || "Delete failed");
      }
      // Remove from results
      setResults(prev => prev.filter(r => r.id !== record.id));
      setDeleteConfirm(null);
      if (selectedRecord?.id === record.id) setSelectedRecord(null);
    } catch (err: any) {
      alert("Error deleting record: " + err.message);
    } finally {
      setDeleting(false);
    }
  };

  const formatDate = (iso?: string) => {
    if (!iso) return "—";
    try {
      return new Date(iso).toLocaleString("en-GB", {
        day: "2-digit", month: "short", year: "numeric",
        hour: "2-digit", minute: "2-digit"
      });
    } catch { return iso; }
  };

  const placeholders: Record<string, string> = {
    nid: "Enter NID (e.g. 1990123456789)",
    dob: "Enter DOB (e.g. 15-05-1990)",
    name: "Enter name to search..."
  };

  return (
    <main className="min-h-screen p-4 md:p-8 lg:p-12 text-slate-200 flex flex-col items-center">
      <div className="max-w-6xl w-full space-y-8">
        
        {/* Header */}
        <header className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link
              href="/statistics"
              className="p-2 rounded-lg bg-slate-800/50 border border-slate-700 hover:bg-slate-700/50 transition-colors"
            >
              <ArrowLeft className="w-5 h-5" />
            </Link>
            <div>
              <h1 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-indigo-400">
                Record Search
              </h1>
              <p className="text-slate-400 text-sm">Search by NID, Date of Birth, or Name across all valid records</p>
            </div>
          </div>
          <Database className="w-8 h-8 text-indigo-500/50" />
        </header>

        {/* Search Bar */}
        <div className="glass-panel p-6 rounded-2xl border border-slate-700/50 bg-slate-800/30 backdrop-blur-xl">
          <form onSubmit={handleSearch} className="flex gap-3 flex-wrap">
            {/* Search Type Selector */}
            <div className="relative">
              <select
                value={searchType}
                onChange={(e) => setSearchType(e.target.value as "nid" | "dob" | "name")}
                className="appearance-none bg-slate-900/80 border border-slate-700 rounded-xl px-4 py-3 pr-10 text-slate-200 focus:outline-none focus:ring-2 focus:ring-indigo-500/50 cursor-pointer font-medium text-sm"
              >
                <option value="nid">🔢 NID</option>
                <option value="dob">📅 DOB</option>
                <option value="name">👤 Name</option>
              </select>
              <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500 pointer-events-none" />
            </div>
            
            <div className="relative flex-1 min-w-[200px]">
              <div className="absolute inset-y-0 left-3 flex items-center pointer-events-none">
                <SearchIcon className="h-5 w-5 text-slate-500" />
              </div>
              <input
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder={placeholders[searchType]}
                className="block w-full pl-10 pr-4 py-3 bg-slate-900/50 border border-slate-700 rounded-xl text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500/50 focus:border-indigo-500 transition-all"
              />
            </div>
            <button
              type="submit"
              disabled={loading}
              className="px-8 py-3 bg-gradient-to-r from-indigo-600 to-blue-600 hover:from-indigo-500 hover:to-blue-500 text-white font-semibold rounded-xl transition-all shadow-lg shadow-indigo-500/20 disabled:opacity-50 flex items-center gap-2"
            >
              {loading ? "Searching..." : "Search"}
            </button>
          </form>
        </div>

        {/* Results */}
        <div className="space-y-4">
          {loading ? (
            <div className="flex flex-col items-center justify-center py-20 space-y-4">
              <div className="w-12 h-12 border-4 border-indigo-500/20 border-t-indigo-500 rounded-full animate-spin"></div>
              <p className="text-slate-400 font-medium">Searching database...</p>
            </div>
          ) : searched && results.length === 0 ? (
            <div className="glass-panel p-12 rounded-2xl border border-slate-700/50 bg-slate-900/40 text-center space-y-4">
              <div className="w-16 h-16 bg-slate-800 rounded-full flex items-center justify-center mx-auto">
                <User className="w-8 h-8 text-slate-500" />
              </div>
              <h2 className="text-xl font-semibold text-slate-300">No Records Found</h2>
              <p className="text-slate-500 max-w-md mx-auto">
                No valid records matching &quot;{query}&quot; ({searchType.toUpperCase()}). Ensure the data has been uploaded and validated.
              </p>
            </div>
          ) : results.length > 0 ? (
            <>
              <div className="flex items-center justify-between px-2">
                <span className="text-sm font-medium text-slate-400">Found {results.length} result{results.length > 1 ? 's' : ''}</span>
                <span className="text-xs text-slate-500">Search type: {searchType.toUpperCase()}</span>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {results.map((record) => (
                  <div key={record.id} className="glass-panel p-5 rounded-2xl border border-slate-700/50 bg-slate-800/40 hover:bg-slate-800/60 transition-all group">
                    <div className="flex justify-between items-start mb-4">
                      <div className="p-2.5 rounded-xl bg-indigo-500/10 text-indigo-400 group-hover:bg-indigo-500/20 transition-colors">
                        <User className="w-5 h-5" />
                      </div>
                      <div className="flex items-center gap-2">
                        {record.upload_batch && (
                          <span className="text-[10px] uppercase tracking-wider font-bold text-slate-500 px-2 py-1 bg-slate-900/50 rounded-md border border-slate-800">
                            Batch v{record.upload_batch}
                          </span>
                        )}
                        <button
                          onClick={() => setDeleteConfirm(record)}
                          title="Delete Record"
                          className="p-1.5 rounded-md bg-red-500/10 text-red-400/60 hover:text-red-400 hover:bg-red-500/20 transition-colors opacity-0 group-hover:opacity-100"
                        >
                          <Trash2 className="w-3.5 h-3.5" />
                        </button>
                      </div>
                    </div>
                    
                    <h3 className="text-lg font-bold text-slate-100 mb-1">{record.name}</h3>
                    <div className="space-y-2.5 text-sm">
                      <div className="flex items-center gap-2 text-slate-400">
                        <Hash className="w-4 h-4 text-slate-500" />
                        <span>NID: <span className="text-slate-200 font-mono">{record.nid}</span></span>
                      </div>
                      <div className="flex items-center gap-2 text-slate-400">
                        <Clock className="w-4 h-4 text-slate-500" />
                        <span>DOB: <span className="text-slate-200">{record.dob}</span></span>
                      </div>
                      <div className="pt-2 border-t border-slate-700/50 mt-2 space-y-1">
                        <div className="flex items-center gap-2 text-xs text-slate-500">
                          <MapPin className="w-3 h-3" />
                          <span>{record.division} • {record.district} • {record.upazila}</span>
                        </div>
                        <div className="flex items-center gap-2 text-xs text-slate-500">
                          <FileText className="w-3 h-3" />
                          <span className="truncate" title={record.source_file}>{record.source_file}</span>
                        </div>
                        {record.updated_at && (
                          <div className="flex items-center gap-2 text-xs text-slate-600">
                            <Clock className="w-3 h-3" />
                            <span>Updated: {formatDate(record.updated_at)}</span>
                          </div>
                        )}
                      </div>
                    </div>

                    <button 
                      onClick={() => setSelectedRecord(record)}
                      className="w-full mt-4 py-2 px-4 bg-slate-700/50 hover:bg-slate-700 text-slate-300 rounded-lg text-xs font-semibold transition-all border border-slate-600/50 flex items-center justify-center gap-2"
                    >
                      <FileText className="w-3.5 h-3.5" />
                      View Full Details
                    </button>
                  </div>
                ))}
              </div>

              {/* Detail Modal */}
              {selectedRecord && (
                <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm animate-in fade-in duration-300">
                  <div className="glass-panel w-full max-w-2xl max-h-[80vh] overflow-hidden rounded-2xl border border-slate-700 flex flex-col shadow-2xl">
                    <div className="p-6 border-b border-slate-800 flex justify-between items-center bg-slate-900/50">
                      <div className="flex items-center gap-3">
                        <div className="p-2 bg-indigo-500/20 rounded-lg text-indigo-400">
                          <User className="w-5 h-5" />
                        </div>
                        <div>
                          <h2 className="text-xl font-bold">{selectedRecord.name}</h2>
                          <p className="text-xs text-slate-500 mt-0.5">NID: {selectedRecord.nid}</p>
                        </div>
                      </div>
                      <button 
                        onClick={() => setSelectedRecord(null)}
                        className="p-2 hover:bg-slate-800 rounded-lg transition-colors"
                      >
                        <X className="w-5 h-5" />
                      </button>
                    </div>
                    
                    <div className="p-6 overflow-y-auto custom-scrollbar space-y-6">
                      <div className="grid grid-cols-2 gap-4">
                        <div className="bg-slate-900/50 p-4 rounded-xl border border-slate-800/50">
                          <p className="text-[10px] uppercase tracking-wider text-slate-500 font-bold mb-1">National ID</p>
                          <p className="text-lg font-mono text-indigo-400">{selectedRecord.nid}</p>
                        </div>
                        <div className="bg-slate-900/50 p-4 rounded-xl border border-slate-800/50">
                          <p className="text-[10px] uppercase tracking-wider text-slate-500 font-bold mb-1">Date of Birth</p>
                          <p className="text-lg text-slate-200">{selectedRecord.dob}</p>
                        </div>
                      </div>

                      {selectedRecord.data && (
                        <div>
                          <h4 className="text-sm font-bold text-slate-400 uppercase tracking-widest mb-4 flex items-center gap-2">
                             <Database className="w-4 h-4" />
                             Full Beneficiary Information
                          </h4>
                          <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-4">
                            {Object.entries(selectedRecord.data).map(([key, value]) => {
                              if (['Cleaned_NID', 'Cleaned_DOB', 'Status', 'Message', 'Excel_Row'].includes(key)) return null;
                              return (
                                <div key={key} className="border-b border-slate-800 pb-2">
                                  <p className="text-[10px] uppercase text-slate-500 font-bold">{key.replace(/_/g, ' ')}</p>
                                  <p className="text-sm text-slate-300 font-medium">{String(value ?? '—')}</p>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      )}

                      <div className="pt-4 border-t border-slate-800 space-y-2">
                         <div className="flex items-center gap-2 text-xs text-slate-500">
                           <MapPin className="w-3.5 h-3.5" />
                           <span>Location: {selectedRecord.division} › {selectedRecord.district} › {selectedRecord.upazila}</span>
                         </div>
                         <div className="flex items-center gap-2 text-xs text-slate-500">
                           <FileText className="w-3.5 h-3.5" />
                           <span>Source: {selectedRecord.source_file}</span>
                         </div>
                         <div className="flex items-center gap-2 text-xs text-slate-500">
                           <Clock className="w-3.5 h-3.5" />
                           <span>Added: {formatDate(selectedRecord.created_at)} | Updated: {formatDate(selectedRecord.updated_at)}</span>
                         </div>
                      </div>
                    </div>
                    
                    <div className="p-4 bg-slate-900/80 border-t border-slate-800 flex justify-between">
                      <button 
                        onClick={() => { setDeleteConfirm(selectedRecord); }}
                        className="px-4 py-2 bg-red-500/10 hover:bg-red-500/20 text-red-400 rounded-lg font-medium transition-all flex items-center gap-2 text-sm border border-red-500/20"
                      >
                        <Trash2 className="w-4 h-4" />
                        Delete Record
                      </button>
                      <button 
                        onClick={() => setSelectedRecord(null)}
                        className="px-6 py-2 bg-slate-800 hover:bg-slate-700 text-white rounded-lg font-medium transition-all"
                      >
                        Close
                      </button>
                    </div>
                  </div>
                </div>
              )}

              {/* Delete Confirmation Modal */}
              {deleteConfirm && (
                <div className="fixed inset-0 z-[60] flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm">
                  <div className="bg-slate-900 border border-slate-700 p-6 rounded-2xl w-full max-w-sm shadow-2xl">
                    <div className="flex items-center gap-3 mb-4">
                      <div className="p-2 bg-red-500/20 rounded-lg">
                        <AlertTriangle className="w-5 h-5 text-red-400" />
                      </div>
                      <h3 className="text-lg font-bold text-slate-100">Delete Record?</h3>
                    </div>
                    <p className="text-sm text-slate-400 mb-2">
                      This will permanently delete the following record:
                    </p>
                    <div className="bg-slate-800/50 p-3 rounded-lg mb-6 text-sm space-y-1">
                      <p className="text-slate-300 font-semibold">{deleteConfirm.name}</p>
                      <p className="text-slate-400 font-mono text-xs">NID: {deleteConfirm.nid}</p>
                      <p className="text-slate-500 text-xs">{deleteConfirm.district} • {deleteConfirm.upazila}</p>
                    </div>
                    <div className="flex justify-end gap-3">
                      <button 
                        disabled={deleting} 
                        onClick={() => setDeleteConfirm(null)} 
                        className="px-4 py-2 rounded-lg text-slate-300 hover:bg-slate-800 transition-colors font-medium"
                      >
                        Cancel
                      </button>
                      <button 
                        disabled={deleting} 
                        onClick={() => handleDelete(deleteConfirm)}
                        className="px-5 py-2 rounded-lg bg-red-600 hover:bg-red-500 text-white font-medium flex items-center gap-2 transition-all disabled:opacity-50"
                      >
                        {deleting ? (
                          <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                        ) : (
                          <Trash2 className="w-4 h-4" />
                        )}
                        Delete
                      </button>
                    </div>
                  </div>
                </div>
              )}
            </>
          ) : !searched && (
            <div className="flex flex-col items-center justify-center py-20 text-slate-600">
              <SearchIcon className="w-16 h-16 mb-4 opacity-20" />
              <p>Enter an NID, Date of Birth, or Name to search the validated records.</p>
            </div>
          )}
        </div>

        {/* Footer */}
        <footer className="pt-8 border-t border-slate-800 text-center space-y-1">
          <p className="text-slate-500 text-xs font-medium">
            &copy; {new Date().getFullYear()} Computer Network Unit | Directorate General of Food
          </p>
        </footer>
      </div>
    </main>
  );
}
