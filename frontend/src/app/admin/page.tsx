"use client";

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { fetchWithAuth, getBackendUrl, isAdmin, getUser } from "@/lib/auth";

export default function AdminPage() {
  const [activeTab, setActiveTab] = useState("users");
  
  // Users state
  const [users, setUsers] = useState<any[]>([]);
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newRole, setNewRole] = useState("viewer");

  // Config State
  const [configs, setConfigs] = useState<any[]>([]);
  const [rateLimit, setRateLimit] = useState("");

  // Instances State
  const [instances, setInstances] = useState<any[]>([]);
  const [instName, setInstName] = useState("");
  const [instUrl, setInstUrl] = useState("");
  const [instKey, setInstKey] = useState("");

  // Upazilas State
  const [upazilas, setUpazilas] = useState<any[]>([]);
  const [upzDiv, setUpzDiv] = useState("");
  const [upzDist, setUpzDist] = useState("");
  const [upzName, setUpzName] = useState("");

  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const router = useRouter();

  useEffect(() => {
    const user = getUser();
    if (!user || user.role !== "admin") {
      router.push("/");
      return;
    }
    loadData();
  }, [router]);

  const loadData = async () => {
    try {
      setLoading(true);
      const [uRes, cRes, iRes, upzRes] = await Promise.all([
        fetchWithAuth(`${getBackendUrl()}/auth/users`),
        fetchWithAuth(`${getBackendUrl()}/admin/config`),
        fetchWithAuth(`${getBackendUrl()}/admin/instances`),
        fetchWithAuth(`${getBackendUrl()}/admin/upazilas`),
      ]);

      if (uRes.ok) setUsers(await uRes.json());
      if (cRes.ok) {
        const cData = await cRes.json();
        setConfigs(cData);
        const rl = cData.find((c: any) => c.key === "rate_limit_value");
        if (rl) setRateLimit(rl.value);
      }
      if (iRes.ok) setInstances(await iRes.json());
      if (upzRes.ok) setUpazilas(await upzRes.json());
      
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const showMsg = (msg: string, isErr = false) => {
    if (isErr) setError(msg);
    else setSuccess(msg);
    setTimeout(() => { setError(""); setSuccess(""); }, 3000);
  }

  // --- Users ---
  const handleCreateUser = async (e: React.FormEvent) => {
    e.preventDefault();
    setActionLoading(true);
    try {
      const response = await fetchWithAuth(`${getBackendUrl()}/auth/users`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: newUsername, password: newPassword, role: newRole }),
      });
      if (!response.ok) throw new Error((await response.json()).detail || "Failed to create user");
      setNewUsername(""); setNewPassword(""); showMsg("User created");
      loadData();
    } catch (err: any) { showMsg(err.message, true); } finally { setActionLoading(false); }
  };

  const handleDeleteUser = async (id: number) => {
    if (!confirm("Delete user?")) return;
    try {
      const response = await fetchWithAuth(`${getBackendUrl()}/auth/users/${id}`, { method: "DELETE" });
      if (!response.ok) throw new Error("Failed to delete user");
      showMsg("User deleted"); loadData();
    } catch (err: any) { showMsg(err.message, true); }
  };

  // --- Config ---
  const handleUpdateConfig = async (e: React.FormEvent) => {
    e.preventDefault();
    setActionLoading(true);
    try {
      const response = await fetchWithAuth(`${getBackendUrl()}/admin/config/rate_limit_value`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: rateLimit }),
      });
      if (!response.ok) throw new Error("Failed to update config");
      showMsg("Config updated"); loadData();
    } catch (err: any) { showMsg(err.message, true); } finally { setActionLoading(false); }
  };

  // --- Instances ---
  const handleCreateInstance = async (e: React.FormEvent) => {
    e.preventDefault();
    setActionLoading(true);
    try {
      const response = await fetchWithAuth(`${getBackendUrl()}/admin/instances`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: instName, url: instUrl, api_key: instKey }),
      });
      if (!response.ok) throw new Error("Failed to create instance");
      setInstName(""); setInstUrl(""); setInstKey(""); showMsg("Instance added"); loadData();
    } catch (err: any) { showMsg(err.message, true); } finally { setActionLoading(false); }
  };

  const handleDeleteInstance = async (id: number) => {
    if (!confirm("Delete instance?")) return;
    try {
      await fetchWithAuth(`${getBackendUrl()}/admin/instances/${id}`, { method: "DELETE" });
      showMsg("Instance deleted"); loadData();
    } catch (err: any) { showMsg(err.message, true); }
  };

  const handleSyncInstance = async (id: number) => {
    setActionLoading(true);
    try {
      const response = await fetchWithAuth(`${getBackendUrl()}/admin/instances/${id}/trigger-sync`, { method: "POST" });
      if (!response.ok) throw new Error((await response.json()).detail || "Sync failed");
      const data = await response.json();
      showMsg(`Sync complete. Imported ${data.synced_count} records.`); loadData();
    } catch (err: any) { showMsg(err.message, true); } finally { setActionLoading(false); }
  };

  // --- Upazilas ---
  const handleCreateUpz = async (e: React.FormEvent) => {
    e.preventDefault();
    setActionLoading(true);
    try {
      const response = await fetchWithAuth(`${getBackendUrl()}/admin/upazilas`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ division_name: upzDiv, district_name: upzDist, name: upzName }),
      });
      if (!response.ok) throw new Error((await response.json()).detail || "Failed to create upazila");
      setUpzDiv(""); setUpzDist(""); setUpzName(""); showMsg("Upazila added"); loadData();
    } catch (err: any) { showMsg(err.message, true); } finally { setActionLoading(false); }
  };

  const handleDeleteUpz = async (id: number) => {
    if (!confirm("Delete upazila?")) return;
    try {
      await fetchWithAuth(`${getBackendUrl()}/admin/upazilas/${id}`, { method: "DELETE" });
      showMsg("Upazila deleted"); loadData();
    } catch (err: any) { showMsg(err.message, true); }
  };

  if (loading) return (
    <div className="min-h-screen bg-[#0a0a0b] flex items-center justify-center">
      <div className="w-8 h-8 border-4 border-emerald-500/20 border-t-emerald-500 rounded-full animate-spin"></div>
    </div>
  );

  return (
    <div className="min-h-screen bg-[#0a0a0b] text-white p-8">
      <div className="max-w-6xl mx-auto">
        <header className="mb-8">
          <h1 className="text-4xl font-black tracking-tight mb-2">System Administration</h1>
          <p className="text-gray-500">Manage Users, Configurations, and Remote Sync</p>
        </header>

        {(error || success) && (
          <div className={`p-4 mb-6 rounded-xl ${error ? 'bg-red-500/10 text-red-500 border border-red-500/20' : 'bg-emerald-500/10 text-emerald-500 border border-emerald-500/20'}`}>
            {error || success}
          </div>
        )}

        {/* Tabs */}
        <div className="flex space-x-2 border-b border-[#1e1e20] mb-8">
          {['users', 'config', 'instances', 'upazilas'].map(tab => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-4 py-3 font-semibold text-sm capitalize transition-colors border-b-2 ${activeTab === tab ? 'border-emerald-500 text-white' : 'border-transparent text-gray-500 hover:text-gray-300'}`}
            >
              {tab}
            </button>
          ))}
        </div>

        {/* --- USERS TAB --- */}
        {activeTab === 'users' && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
            <div className="lg:col-span-1">
              <div className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl">
                <h2 className="text-xl font-bold mb-6">Create New User</h2>
                <form onSubmit={handleCreateUser} className="space-y-4">
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Username</label>
                    <input type="text" value={newUsername} onChange={(e) => setNewUsername(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" required />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Password</label>
                    <input type="password" value={newPassword} onChange={(e) => setNewPassword(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" required />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Role</label>
                    <select value={newRole} onChange={(e) => setNewRole(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors">
                      <option value="viewer">Viewer</option>
                      <option value="uploader">Uploader</option>
                      <option value="admin">Admin</option>
                    </select>
                  </div>
                  <button type="submit" disabled={actionLoading} className="w-full py-3 rounded-xl bg-emerald-600 hover:bg-emerald-500 font-bold disabled:opacity-50">Add User</button>
                </form>
              </div>
            </div>
            <div className="lg:col-span-2">
              <div className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden">
                <table className="w-full text-left">
                  <thead>
                    <tr className="bg-[#1a1a1c] border-b border-[#1e1e20]">
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">User / API Key</th>
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Role</th>
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-[#1e1e20]">
                    {users.map((u) => (
                      <tr key={u.id} className="hover:bg-[#161618] group">
                        <td className="px-6 py-4">
                          <div className="flex flex-col">
                            <span className="font-medium">{u.username}</span>
                            {u.api_key && <span className="text-xs text-emerald-500 font-mono mt-1">Key: {u.api_key}</span>}
                          </div>
                        </td>
                        <td className="px-6 py-4">
                          <span className={`px-2 py-1 rounded-md text-[10px] font-bold uppercase ${u.role === 'admin' ? 'bg-purple-500/10 text-purple-400' : 'bg-gray-500/10 text-gray-400'}`}>{u.role}</span>
                        </td>
                        <td className="px-6 py-4 text-right">
                          <button onClick={() => handleDeleteUser(u.id)} className="text-red-500 opacity-0 group-hover:opacity-100 transition-opacity">Delete</button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* --- CONFIG TAB --- */}
        {activeTab === 'config' && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
            <div className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl">
              <h2 className="text-xl font-bold mb-6">API Configuration</h2>
              <form onSubmit={handleUpdateConfig} className="space-y-4">
                <div>
                  <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">NID Verify Request Rate Limit</label>
                  <p className="text-xs text-gray-500 mb-2">Format: `requests/period` (e.g. `60/minute`, `1000/day`)</p>
                  <input type="text" value={rateLimit} onChange={(e) => setRateLimit(e.target.value)} placeholder="60/minute" className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" required />
                </div>
                <button type="submit" disabled={actionLoading} className="w-full py-3 rounded-xl bg-purple-600 hover:bg-purple-500 font-bold disabled:opacity-50">Save Configuration</button>
              </form>
            </div>
          </div>
        )}

        {/* --- INSTANCES TAB --- */}
        {activeTab === 'instances' && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
            <div className="lg:col-span-1">
              <div className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl">
                <h2 className="text-xl font-bold mb-6">Add Remote Sync Target</h2>
                <form onSubmit={handleCreateInstance} className="space-y-4">
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Instance Name</label>
                    <input type="text" value={instName} onChange={(e) => setInstName(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" required />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Base URL</label>
                    <input type="url" value={instUrl} onChange={(e) => setInstUrl(e.target.value)} placeholder="http://192.168.1.5:8000" className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" required />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">API Key</label>
                    <input type="text" value={instKey} onChange={(e) => setInstKey(e.target.value)} placeholder="Remote server API Key" className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" required />
                  </div>
                  <button type="submit" disabled={actionLoading} className="w-full py-3 rounded-xl bg-blue-600 hover:bg-blue-500 font-bold disabled:opacity-50">Add Instance</button>
                </form>
              </div>
            </div>
            <div className="lg:col-span-2">
              <div className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden">
                <table className="w-full text-left">
                  <thead>
                    <tr className="bg-[#1a1a1c] border-b border-[#1e1e20]">
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Instance</th>
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Last Synced</th>
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-[#1e1e20]">
                    {instances.map((i) => (
                      <tr key={i.id} className="hover:bg-[#161618] group">
                        <td className="px-6 py-4">
                          <div className="flex flex-col">
                            <span className="font-bold">{i.name}</span>
                            <span className="text-xs text-gray-500 mt-1">{i.url}</span>
                          </div>
                        </td>
                        <td className="px-6 py-4 text-sm text-gray-400">
                          {i.last_synced_at ? new Date(i.last_synced_at).toLocaleString() : 'Never'}
                        </td>
                        <td className="px-6 py-4 text-right space-x-4">
                          <button onClick={() => handleSyncInstance(i.id)} disabled={actionLoading} className="text-emerald-500 font-bold hover:text-emerald-400 disabled:opacity-50">Sync Now</button>
                          <button onClick={() => handleDeleteInstance(i.id)} className="text-red-500 opacity-0 group-hover:opacity-100 uppercase text-xs">Delete</button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* --- UPAZILAS TAB --- */}
        {activeTab === 'upazilas' && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
            <div className="lg:col-span-1">
              <div className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl">
                <h2 className="text-xl font-bold mb-6">Add Master Upazila</h2>
                <form onSubmit={handleCreateUpz} className="space-y-4">
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Division</label>
                    <input type="text" value={upzDiv} onChange={(e) => setUpzDiv(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" required />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">District</label>
                    <input type="text" value={upzDist} onChange={(e) => setUpzDist(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" required />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Upazila Name</label>
                    <input type="text" value={upzName} onChange={(e) => setUpzName(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" required />
                  </div>
                  <button type="submit" disabled={actionLoading} className="w-full py-3 rounded-xl bg-emerald-600 hover:bg-emerald-500 font-bold disabled:opacity-50">Add Upazila</button>
                </form>
              </div>
            </div>
            <div className="lg:col-span-2">
              <div className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden max-h-[600px] overflow-y-auto">
                <table className="w-full text-left">
                  <thead>
                    <tr className="bg-[#1a1a1c] border-b border-[#1e1e20] sticky top-0 z-10">
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Division</th>
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">District</th>
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Upazila</th>
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-[#1e1e20]">
                    {upazilas.map((u) => (
                      <tr key={u.id} className="hover:bg-[#161618] group">
                        <td className="px-6 py-4 text-gray-400">{u.division_name}</td>
                        <td className="px-6 py-4 text-gray-300">{u.district_name}</td>
                        <td className="px-6 py-4 font-bold">{u.name}</td>
                        <td className="px-6 py-4 text-right">
                          <button onClick={() => handleDeleteUpz(u.id)} className="text-red-500 opacity-0 group-hover:opacity-100 transition-opacity">Delete</button>
                        </td>
                      </tr>
                    ))}
                    {upazilas.length === 0 && (
                      <tr>
                        <td colSpan={4} className="p-8 text-center text-gray-500">No database upazilas added. System will use default hardcoded list.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

      </div>
    </div>
  );
}
