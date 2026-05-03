import React, { useState, useEffect } from "react";
import { toast } from "react-hot-toast";
import { PlusCircle, Search, Edit2, Trash2 } from "lucide-react";
import { fetchWithAuth } from "@/lib/auth";

interface HeaderAlias {
  id: number;
  original_header: string;
  canonical_key: string;
}

const CANONICAL_KEYS = [
  { key: "nid_number", title: "NID Number" },
  { key: "dob", title: "Date of Birth" },
  { key: "name_bn", title: "Beneficiary Name (Bangla)" },
  { key: "name_en", title: "Beneficiary Name (English)" },
  { key: "father_husband_name", title: "Father/Husband Name" },
  { key: "mother_name", title: "Mother Name" },
  { key: "spouse_name", title: "Spouse Name" },
  { key: "spouse_nid", title: "Spouse NID" },
  { key: "spouse_dob", title: "Spouse DOB" },
  { key: "mobile", title: "Mobile Number" },
  { key: "gender", title: "Gender" },
  { key: "religion", title: "Religion" },
  { key: "occupation", title: "Occupation" },
  { key: "address", title: "Address" },
  { key: "ward", title: "Ward No" },
  { key: "union_name", title: "Union Name" },
  { key: "dealer_name", title: "Dealer Name" },
  { key: "dealer_nid", title: "Dealer NID" },
  { key: "dealer_mobile", title: "Dealer Mobile" },
  { key: "card_no", title: "Card No" },
  { key: "serial_no", title: "Serial No" },
  { key: "master_serial", title: "Master Serial" },
  { key: "remarks", title: "Remarks" },
];

export default function HeaderAliasTab() {
  const [aliases, setAliases] = useState<HeaderAlias[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingAlias, setEditingAlias] = useState<HeaderAlias | null>(null);
  
  const [formData, setFormData] = useState({
    original_header: "",
    canonical_key: "nid_number"
  });

  useEffect(() => {
    fetchAliases();
  }, []);

  const fetchAliases = async () => {
    setLoading(true);
    try {
      const res = await fetchWithAuth("/api/admin/header-aliases/");
      if (!res.ok) throw new Error("Failed to fetch aliases");
      const data = await res.json();
      setAliases(data);
    } catch (err: any) {
      toast.error(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.original_header.trim()) {
      toast.error("Original header cannot be empty");
      return;
    }

    try {
      const url = editingAlias 
        ? `/api/admin/header-aliases/${editingAlias.id}/` 
        : "/api/admin/header-aliases/";
      
      const method = editingAlias ? "PUT" : "POST";
      
      const res = await fetchWithAuth(url, {
        method,
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(formData),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || "Operation failed");
      }

      toast.success(editingAlias ? "Alias updated!" : "Alias added!");
      setIsModalOpen(false);
      fetchAliases();
    } catch (err: any) {
      toast.error(err.message);
    }
  };

  const handleDelete = async (id: number) => {
    if (!window.confirm("Delete this column mapping?")) return;
    try {
      const res = await fetchWithAuth(`/api/admin/header-aliases/${id}/`, {
        method: "DELETE",
      });
      if (!res.ok) throw new Error("Failed to delete");
      toast.success("Deleted successfully");
      fetchAliases();
    } catch (err: any) {
      toast.error(err.message);
    }
  };

  // Group aliases by canonical key
  const groupedAliases = CANONICAL_KEYS.map(ck => ({
    ...ck,
    items: aliases.filter(a => a.canonical_key === ck.key && a.original_header.toLowerCase().includes(search.toLowerCase()))
  })).filter(g => g.items.length > 0 || search === "");

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl shadow-lg">
        <div>
           <h2 className="text-2xl font-black tracking-tight text-white italic uppercase">Header Variations</h2>
           <p className="text-xs text-gray-500 font-medium">Map different Excel column spelling variations to the system&apos;s standard canonical fields.</p>
        </div>
        <button
          onClick={() => {
            setEditingAlias(null);
            setFormData({ original_header: "", canonical_key: "nid_number" });
            setIsModalOpen(true);
          }}
          className="px-6 py-2.5 rounded-2xl bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-black uppercase tracking-widest transition-all shadow-lg shadow-emerald-600/20 flex items-center gap-2"
        >
          <PlusCircle className="w-4 h-4" />
          Add Spelling
        </button>
      </div>

      <div className="relative group">
        <input
          type="text"
          placeholder="Search original header text..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full pl-12 pr-4 py-3 bg-[#121214] border border-[#1e1e20] rounded-2xl focus:border-emerald-500 outline-none transition-all text-sm text-white"
        />
        <Search className="w-5 h-5 text-gray-600 absolute left-4 top-1/2 -translate-y-1/2 group-focus-within:text-emerald-500 transition-colors" />
      </div>

      {loading ? (
        <div className="flex justify-center py-20">
          <div className="w-8 h-8 border-4 border-emerald-500/20 border-t-emerald-500 rounded-full animate-spin"></div>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {groupedAliases.map((group) => (
            <div key={group.key} className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden flex flex-col hover:border-emerald-500/30 transition-colors shadow-xl">
              <div className="bg-white/5 border-b border-[#1e1e20] px-5 py-3 flex justify-between items-center">
                <h3 className="font-black text-gray-200 text-xs uppercase tracking-wider">{group.title}</h3>
                <span className="text-[10px] font-mono text-emerald-400 bg-emerald-500/10 border border-emerald-500/20 px-2 py-0.5 rounded uppercase font-bold">
                  {group.key}
                </span>
              </div>
              <div className="p-5 flex-1 flex flex-col gap-2 relative">
                {group.items.length === 0 ? (
                  <p className="text-[10px] text-gray-600 text-center italic py-4">No variations found.</p>
                ) : (
                  group.items.map((alias) => (
                    <div key={alias.id} className="flex justify-between items-center group bg-black/40 hover:bg-white/5 px-4 py-3 rounded-xl border border-white/5 hover:border-white/10 transition-all">
                      <span className="text-sm font-medium text-gray-300" style={{ fontFamily: 'Nikosh, sans-serif' }}>
                        {alias.original_header}
                      </span>
                      <div className="opacity-0 group-hover:opacity-100 transition-opacity flex gap-2">
                        <button 
                          onClick={() => {
                            setEditingAlias(alias);
                            setFormData({ original_header: alias.original_header, canonical_key: alias.canonical_key });
                            setIsModalOpen(true);
                          }}
                          className="p-1.5 text-gray-500 hover:text-emerald-400 bg-white/5 rounded-lg border border-white/10 hover:border-emerald-500/30 transition-all"
                          title="Edit"
                        >
                          <Edit2 className="w-3.5 h-3.5" />
                        </button>
                        <button 
                          onClick={() => handleDelete(alias.id)}
                          className="p-1.5 text-gray-500 hover:text-red-400 bg-white/5 rounded-lg border border-white/10 hover:border-red-500/30 transition-all"
                          title="Delete"
                        >
                          <Trash2 className="w-3.5 h-3.5" />
                        </button>
                      </div>
                    </div>
                  ))
                )}
                
                <button
                  onClick={() => {
                    setEditingAlias(null);
                    setFormData({ original_header: "", canonical_key: group.key });
                    setIsModalOpen(true);
                  }}
                  className="mt-3 w-full py-2 border border-dashed border-[#2a2a2e] rounded-xl text-[10px] font-black uppercase tracking-widest text-gray-500 hover:bg-white/5 hover:text-emerald-500 hover:border-emerald-500/50 transition-all"
                >
                  + Add Variation
                </button>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Modal */}
      {isModalOpen && (
        <div className="fixed inset-0 bg-black/80 backdrop-blur-md z-[100] flex items-center justify-center p-4">
          <div className="bg-[#121214] border border-[#1e1e20] rounded-[2rem] shadow-2xl w-full max-w-md overflow-hidden relative">
            <div className="bg-white/5 px-8 py-5 border-b border-[#1e1e20]">
              <h3 className="text-xl font-black text-white italic uppercase tracking-tight">
                {editingAlias ? "Edit Mapping" : "New Mapping"}
              </h3>
            </div>
            
            <form onSubmit={handleSubmit} className="p-8 space-y-6">
              <div>
                <label className="text-[10px] font-black text-gray-500 uppercase tracking-widest mb-2 block italic">System Field (Target)</label>
                <select
                  value={formData.canonical_key}
                  onChange={(e) => setFormData({ ...formData, canonical_key: e.target.value })}
                  className="w-full bg-black border border-white/10 rounded-2xl px-5 py-3 text-sm focus:border-emerald-500 outline-none transition-all text-white appearance-none cursor-pointer"
                  required
                >
                  {CANONICAL_KEYS.map((ck) => (
                    <option key={ck.key} value={ck.key}>
                      {ck.title}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label className="text-[10px] font-black text-gray-500 uppercase tracking-widest mb-2 block italic">Excel Column Header (Exact Text)</label>
                <input
                  type="text"
                  value={formData.original_header}
                  onChange={(e) => setFormData({ ...formData, original_header: e.target.value })}
                  className="w-full bg-black border border-white/10 rounded-2xl px-5 py-3 text-sm focus:border-emerald-500 outline-none transition-all text-white"
                  placeholder="e.g., জাতীয় পরিচয়পত্র নম্বর"
                  required
                  style={{ fontFamily: 'Nikosh, sans-serif' }}
                />
              </div>

              <div className="pt-4 flex gap-4">
                <button
                  type="button"
                  onClick={() => setIsModalOpen(false)}
                  className="flex-1 px-4 py-3 rounded-2xl bg-white/5 hover:bg-white/10 text-white text-xs font-black uppercase tracking-widest transition-all"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="flex-1 px-4 py-3 rounded-2xl bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-black uppercase tracking-widest transition-all shadow-lg shadow-emerald-600/20"
                >
                  Save Mapping
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
