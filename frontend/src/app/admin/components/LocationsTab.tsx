"use client";

import React, { useState, useEffect } from "react";
import { fetchWithAuth, getBackendUrl } from "@/lib/auth";

interface GeoNode {
  id: number;
  name: string;
  type: "division" | "district" | "upazila";
  aliases: { id: number; alias_name: string }[];
  districts?: GeoNode[];
  upazilas?: GeoNode[];
  quota?: number;
}

export default function LocationsTab() {
  const [tree, setTree] = useState<GeoNode[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState("");
  const [expandedNodes, setExpandedNodes] = useState<Record<string, boolean>>({});

  // Unified Modal State
  const [managerNode, setManagerNode] = useState<GeoNode | null>(null);
  const [newName, setNewName] = useState("");
  const [newAlias, setNewAlias] = useState("");
  const [childName, setChildName] = useState("");
  const [quota, setQuota] = useState<number>(0);
  const [actionLoading, setActionLoading] = useState(false);

  const fetchTree = async () => {
    try {
      const res = await fetchWithAuth(getBackendUrl() + "/admin/geo/tree");
      if (res.ok) setTree(await res.json());
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTree();
  }, []);

  const toggleNode = (key: string) => {
    setExpandedNodes(prev => ({ ...prev, [key]: !prev[key] }));
  };

  const matchesSearch = (node: GeoNode): boolean => {
    if (node.name.toLowerCase().includes(searchTerm.toLowerCase())) return true;
    if (node.aliases.some(a => a.alias_name.toLowerCase().includes(searchTerm.toLowerCase()))) return true;
    if (node.districts) return node.districts.some(matchesSearch);
    if (node.upazilas) return node.upazilas.some(matchesSearch);
    return false;
  };

  // Open Manager
  const openManager = (node: GeoNode) => {
    setManagerNode(node);
    setNewName(node.name);
    setNewAlias("");
    setChildName("");
    setQuota(node.quota || 0);
  };

  const closeManager = () => setManagerNode(null);

  // Rename Handler (ID-based)
  const handleRename = async () => {
    if (!managerNode) return;
    setActionLoading(true);
    try {
      const res = await fetchWithAuth(getBackendUrl() + "/admin/location/rename", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          id: managerNode.id,
          level: managerNode.type,
          new_name: newName
        }),
      });
      if (res.ok) {
         // Update local node state to prevent jumpy UI before refetch
         setManagerNode({ ...managerNode, name: newName });
         await fetchTree();
      } else {
        const d = await res.json();
        alert(d.detail || "Rename failed");
      }
    } finally {
      setActionLoading(false);
    }
  };

  // Alias Handlers
  const handleAddAlias = async () => {
    if (!managerNode || !newAlias) return;
    setActionLoading(true);
    try {
      const res = await fetchWithAuth(getBackendUrl() + "/admin/geo/aliases", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          alias_name: newAlias,
          target_type: managerNode.type,
          target_id: managerNode.id
        })
      });
      if (res.ok) {
        setNewAlias("");
        await fetchTree();
        // Update managerNode aliases for instant UI feedback
        const updatedTree = await (await fetchWithAuth(getBackendUrl() + "/admin/geo/tree")).json();
        setTree(updatedTree);
        // Find updated node in tree
        const findNode = (list: GeoNode[]): GeoNode | null => {
           for (const n of list) {
             if (n.id === managerNode.id && n.type === managerNode.type) return n;
             if (n.districts) { const found = findNode(n.districts); if (found) return found; }
             if (n.upazilas) { const found = findNode(n.upazilas); if (found) return found; }
           }
           return null;
        };
        const fresh = findNode(updatedTree);
        if (fresh) setManagerNode(fresh);
      } else {
        const d = await res.json();
        alert(d.detail || "Failed to add alias");
      }
    } finally {
      setActionLoading(false);
    }
  };

  const handleDeleteAlias = async (id: number) => {
    setActionLoading(true);
    const res = await fetchWithAuth(getBackendUrl() + `/admin/geo/aliases/${id}`, { method: "DELETE" });
    if (res.ok) {
        if (managerNode) {
            setManagerNode({ ...managerNode, aliases: managerNode.aliases.filter(a => a.id !== id) });
        }
        await fetchTree();
    }
    setActionLoading(false);
  };

  // Add Child Handler
  const handleAddChild = async () => {
    if (!managerNode || !childName) return;
    setActionLoading(true);
    try {
      const res = await fetchWithAuth(getBackendUrl() + "/admin/geo/upazilas", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          division_name: (managerNode as any).division_name || managerNode.name, 
          district_name: managerNode.name,
          name: childName,
          quota: quota || 0
        }),
      });
      if (res.ok) {
        setChildName("");
        setQuota(0);
        await fetchTree();
        const updatedTree = await (await fetchWithAuth(getBackendUrl() + "/admin/geo/tree")).json();
        setTree(updatedTree);
        // Find node to refresh list in modal
        const findNode = (list: GeoNode[]): GeoNode | null => {
           for (const n of list) {
             if (n.id === managerNode.id && n.type === managerNode.type) return n;
             if (n.districts) { const found = findNode(n.districts); if (found) return found; }
             if (n.upazilas) { const found = findNode(n.upazilas); if (found) return found; }
           }
           return null;
        };
        const fresh = findNode(updatedTree);
        if (fresh) setManagerNode(fresh);
      }
    } finally {
      setActionLoading(false);
    }
  };

  const handleUpdateQuota = async () => {
    if (!managerNode || managerNode.type !== 'upazila') return;
    setActionLoading(true);
    try {
      const res = await fetchWithAuth(getBackendUrl() + `/admin/upazilas/${managerNode.id}/quota`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ quota })
      });
      if (res.ok) {
        setManagerNode({ ...managerNode, quota });
        await fetchTree();
        alert("Quota updated successfully");
      } else {
        const d = await res.json();
        alert(d.detail || "Quota update failed");
      }
    } finally {
      setActionLoading(false);
    }
  };

  const runCleanup = async () => {
    if (!confirm("Run global geo-cleanup to resolve aliases?")) return;
    setLoading(true);
    await fetchWithAuth(getBackendUrl() + "/admin/maintenance/run-cleanup", { method: "POST" });
    alert("Cleanup started in background.");
    setLoading(false);
  };

  const renderNode = (node: GeoNode, level: number) => {
    const isExpanded = expandedNodes[`${node.type}_${node.id}`] || searchTerm !== "";
    const isVisible = searchTerm === "" || matchesSearch(node);
    if (!isVisible) return null;

    const colors = {
      division: "text-emerald-400 bg-emerald-500/10 border-emerald-500/20",
      district: "text-indigo-400 bg-indigo-500/10 border-indigo-500/20",
      upazila: "text-cyan-400 bg-cyan-500/10 border-cyan-500/20"
    };

    return (
      <div key={`${node.type}_${node.id}`} className="flex flex-col">
        <div className={`flex items-center group py-2 px-3 rounded-xl hover:bg-white/5 cursor-pointer transition-all ${level > 0 ? 'ml-4 border-l border-white/10 pl-6 my-0.5' : 'mt-4'}`} onClick={() => openManager(node)}>
          <button 
            onClick={(e) => { e.stopPropagation(); toggleNode(`${node.type}_${node.id}`); }}
            className={`mr-3 p-1 rounded hover:bg-white/10 transition-transform ${isExpanded ? 'rotate-90' : ''} ${node.type === 'upazila' ? 'invisible' : ''}`}
          >
            <svg className="w-4 h-4 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" /></svg>
          </button>
              <div className="flex-1 flex items-center space-x-3">
                 <span className={`px-2.5 py-1 rounded text-[10px] font-black uppercase tracking-widest border ${colors[node.type]}`}>{node.type}</span>
                 <span className="text-sm font-bold text-gray-100">{node.name}</span>
                 {node.type === 'upazila' && (
                    <span className="text-[10px] font-black text-emerald-400 bg-emerald-500/10 px-2 py-0.5 rounded-full border border-emerald-500/20">
                      QUOTA: {node.quota?.toLocaleString('en-IN')}
                    </span>
                 )}
                 <div className="flex flex-wrap gap-1">
                    {node.aliases.map(a => (
                      <span key={a.id} className="px-2 py-0.5 rounded-full bg-white/5 border border-white/10 text-[10px] text-gray-400">{a.alias_name}</span>
                    ))}
                 </div>
              </div>
          <div className="opacity-0 group-hover:opacity-100 transition-opacity">
             <span className="text-[10px] font-bold text-gray-600 bg-white/5 px-2 py-1 rounded uppercase tracking-tighter">Edit</span>
          </div>
        </div>
        {isExpanded && (
          <div className="flex flex-col">
            {node.districts?.map(d => renderNode(d, level + 1))}
            {node.upazilas?.map(u => renderNode(u, level + 1))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="flex flex-col space-y-6">
      <div className="flex items-center justify-between pb-6 border-b border-white/5">
        <div>
           <h2 className="text-2xl font-black tracking-tight text-white italic uppercase">GEO HUB</h2>
           <p className="text-xs text-gray-500 font-medium">Industry-standard hierarchical management of Bangladesh locations.</p>
        </div>
        <div className="flex items-center space-x-3">
           <div className="relative group">
              <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-600 group-focus-within:text-indigo-500 transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" /></svg>
              <input type="text" placeholder="Quick Search..." value={searchTerm} onChange={e => setSearchTerm(e.target.value)} className="bg-black border border-white/10 rounded-2xl px-10 py-2.5 text-sm focus:border-indigo-500 outline-none w-64 transition-all" />
           </div>
           <button onClick={runCleanup} className="px-6 py-2.5 rounded-2xl bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-black uppercase tracking-widest transition-all shadow-lg shadow-indigo-600/20">Apply Sync</button>
        </div>
      </div>

      <div className="bg-[#121214] border border-white/5 rounded-3xl p-8 shadow-2xl min-h-[600px] overflow-hidden">
         {loading ? <div className="animate-pulse flex items-center justify-center h-64 text-gray-600 font-bold italic">REACHING DATABASE...</div> : tree.map(div => renderNode(div, 0))}
      </div>

      {/* COMPREHENSIVE LOCATION MANAGER MODAL */}
      {managerNode && (
        <div className="fixed inset-0 bg-black/90 backdrop-blur-xl z-[100] flex items-center justify-center p-4" onClick={closeManager}>
          <div className="bg-[#121214] border border-white/10 rounded-[2.5rem] p-10 w-full max-w-2xl shadow-[0_0_100px_rgba(0,0,0,1)] relative overflow-hidden" onClick={e => e.stopPropagation()}>
            <div className={`absolute top-0 right-0 w-64 h-64 blur-[120px] opacity-20 pointer-events-none -mr-32 -mt-32 transition-colors ${managerNode.type === 'division' ? 'bg-emerald-500' : managerNode.type === 'district' ? 'bg-indigo-500' : 'bg-cyan-500'}`} />
            
            <div className="relative z-10">
              <div className="flex items-center justify-between mb-10">
                <div className="flex items-center space-x-4">
                   <span className={`px-4 py-1.5 rounded-full text-[12px] font-black uppercase tracking-widest border ${managerNode.type === 'division' ? 'text-emerald-400 border-emerald-500/20' : managerNode.type === 'district' ? 'text-indigo-400 border-indigo-500/20' : 'text-cyan-400 border-cyan-500/20'}`}>{managerNode.type}</span>
                   <h3 className="text-3xl font-black text-white tracking-tighter">{managerNode.name}</h3>
                </div>
                <button onClick={closeManager} className="p-3 text-gray-500 hover:text-white rounded-full bg-white/5 hover:bg-white/10 transition-all">
                  <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
                </button>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-12">
                <div className="space-y-10">
                  {/* RENAME SECTION */}
                  <section>
                    <label className="text-[10px] font-black text-gray-500 uppercase tracking-widest mb-4 block">Primary Location Name</label>
                    <div className="flex space-x-2">
                      <input type="text" value={newName} onChange={e => setNewName(e.target.value)} className="flex-1 bg-black border border-white/10 rounded-2xl px-5 py-3 text-sm focus:border-indigo-500 outline-none transition-all" />
                      <button onClick={handleRename} disabled={actionLoading || newName === managerNode.name} className="px-5 py-3 rounded-2xl bg-white/5 hover:bg-white/10 text-white font-bold text-xs uppercase disabled:opacity-30">Save</button>
                    </div>
                  </section>

                  {/* ALIAS SECTION */}
                  <section>
                    <label className="text-[10px] font-black text-gray-500 uppercase tracking-widest mb-4 block">Semantic Mapping Aliases</label>
                    <div className="space-y-2 mb-4">
                      {managerNode.aliases.length === 0 && <p className="text-xs text-gray-600 italic">No aliases registered.</p>}
                      {managerNode.aliases.map(a => (
                        <div key={a.id} className="flex items-center justify-between bg-black/40 border border-white/5 px-4 py-2.5 rounded-xl group/alias">
                          <span className="text-sm text-gray-400">{a.alias_name}</span>
                          <button onClick={() => handleDeleteAlias(a.id)} className="text-gray-600 hover:text-red-400 transition-colors">
                             <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                          </button>
                        </div>
                      ))}
                    </div>
                    <div className="flex space-x-2">
                       <input type="text" placeholder="Add Alias..." value={newAlias} onChange={e => setNewAlias(e.target.value)} className="flex-1 bg-black border border-white/10 rounded-2xl px-5 py-3 text-sm focus:border-emerald-500 outline-none transition-all" />
                       <button onClick={handleAddAlias} disabled={actionLoading || !newAlias} className="p-3 rounded-2xl bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500 hover:text-white transition-all">
                         <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" /></svg>
                       </button>
                    </div>
                  </section>

                  {/* QUOTA SECTION (UPAZILA ONLY) */}
                  {managerNode.type === 'upazila' && (
                    <section>
                      <label className="text-[10px] font-black text-gray-500 uppercase tracking-widest mb-4 block">Distribution Quota</label>
                      <div className="flex space-x-2">
                        <input 
                          type="number" 
                          value={quota} 
                          onChange={e => setQuota(parseInt(e.target.value) || 0)} 
                          className="flex-1 bg-black border border-white/10 rounded-2xl px-5 py-3 text-sm focus:border-indigo-500 outline-none transition-all" 
                        />
                        <button 
                          onClick={handleUpdateQuota} 
                          disabled={actionLoading || quota === managerNode.quota} 
                          className="px-6 py-3 rounded-2xl bg-indigo-600 hover:bg-indigo-500 text-white font-black text-xs uppercase tracking-widest transition-all disabled:opacity-30 shadow-lg shadow-indigo-600/20"
                        >
                          Update Quota
                        </button>
                      </div>
                      <p className="text-[10px] text-gray-600 mt-4 italic">Quota is used to calculate "Remaining" and "Completion %" in statistics.</p>
                    </section>
                  )}
                </div>

                <div className="space-y-10">
                  {/* ADD CHILD SECTION */}
                  {managerNode.type !== 'upazila' && (
                    <section>
                      <label className="text-[10px] font-black text-gray-500 uppercase tracking-widest mb-4 block">Add Sub-{managerNode.type === 'division' ? 'District' : 'Upazila'}</label>
                      <div className="flex space-x-2">
                        <input type="text" placeholder={`New ${managerNode.type === 'division' ? 'District' : 'Upazila'} Name...`} value={childName} onChange={e => setChildName(e.target.value)} className="flex-1 bg-black border border-white/10 rounded-2xl px-5 py-3 text-sm focus:border-cyan-500 outline-none transition-all" />
                        <button onClick={handleAddChild} disabled={actionLoading || !childName} className="p-3 rounded-2xl bg-cyan-500/10 text-cyan-400 hover:bg-cyan-500 hover:text-white transition-all">
                          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" /></svg>
                        </button>
                      </div>
                      <p className="text-[10px] text-gray-600 mt-4 leading-relaxed italic">This immediately creates a new master location in the hierarchy.</p>
                    </section>
                  )}
                  
                  {/* SYSTEM INFO */}
                  <section className="bg-white/5 p-6 rounded-3xl border border-white/10">
                    <label className="text-[10px] font-black text-gray-500 uppercase tracking-widest mb-4 block italic">System Metadata</label>
                    <div className="space-y-4">
                       <div className="flex justify-between">
                         <span className="text-[10px] font-bold text-gray-500">DATABASE ID</span>
                         <span className="text-[10px] font-mono text-indigo-400">#{managerNode.id}</span>
                       </div>
                       <div className="flex justify-between">
                         <span className="text-[10px] font-bold text-gray-500">PARENT</span>
                         <span className="text-[10px] font-bold text-gray-300">{(managerNode as any).district_name || (managerNode as any).division_name || "NONE"}</span>
                       </div>
                    </div>
                  </section>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
