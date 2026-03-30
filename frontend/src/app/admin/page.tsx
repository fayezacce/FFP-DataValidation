"use client";

import React, { useState, useEffect, type FormEvent, type ChangeEvent } from "react";
import { useRouter } from "next/navigation";
import { fetchWithAuth, getBackendUrl, isAdmin, getUser } from "@/lib/auth";
import ChangePasswordModal from "@/components/ChangePasswordModal";
import Link from "next/link";

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
  const [trailingZeroLimit, setTrailingZeroLimit] = useState("");

  // Whitelist State
  const [tzWhitelist, setTzWhitelist] = useState<any[]>([]);
  const [newTzNid, setNewTzNid] = useState("");

  // Instances State
  const [instances, setInstances] = useState<any[]>([]);
  const [instName, setInstName] = useState("");
  const [instUrl, setInstUrl] = useState("");
  const [instApiKey, setInstApiKey] = useState("");

  // Upazilas State
  const [upazilas, setUpazilas] = useState<any[]>([]);
  const [upzDiv, setUpzDiv] = useState("");
  const [upzDist, setUpzDist] = useState("");
  const [upzName, setUpzName] = useState("");

  // Location Rename State
  const [renameLevel, setRenameLevel] = useState("upazila");
  const [renameOldName, setRenameOldName] = useState("");
  const [renameNewName, setRenameNewName] = useState("");
  const [renameParentName, setRenameParentName] = useState("");

  // Upazila Inline Edit State
  const [editingUpzId, setEditingUpzId] = useState<number | null>(null);
  const [editingUpzName, setEditingUpzName] = useState("");
  const [editingUpzQuota, setEditingUpzQuota] = useState<number | string>(0);

  // Logs State
  const [auditLogs, setAuditLogs] = useState<any[]>([]);
  const [apiLogs, setApiLogs] = useState<any[]>([]);

  // Maintenance State
  const [maintenanceLoading, setMaintenanceLoading] = useState(false);
  const [orphanPreview, setOrphanPreview] = useState<any | null>(null);
  const [cleanupResult, setCleanupResult] = useState<any | null>(null);
  const [deleteResult, setDeleteResult] = useState<any | null>(null);

  const formatAuditSummary = (log: any) => {
    const details = log.details || {};
    const old = details.old || {};
    const latest = details.new || {};

    if (log.action === "LOGIN_FAIL") return `Failed login attempt for user: ${latest.username || 'unknown'}`;
    if (log.action === "LOGIN_SUCCESS") return `Successful login`;

    if (log.target_table === "upazilas") {
      if (log.action === "CREATE") return `Created upazila: ${latest.name}`;
      if (log.action === "DELETE") return `Deleted upazila: ${old.name}`;
      if (log.action === "RENAME") return `Renamed upazila from ${old.name} to ${latest.name}`;
    }

    if (log.target_table === "users") {
      if (log.action === "CREATE") return `Created user: ${latest.username} (${latest.role})`;
      if (log.action === "UPDATE") {
        const changes = [];
        if (latest.role && latest.role !== old.role) changes.push(`role to ${latest.role}`);
        if (latest.password) changes.push(`password`);
        return `Updated user ${latest.username || ''}: ${changes.join(', ') || 'settings'}`;
      }
      if (log.action === "DELETE") return `Deleted user: ${old.username}`;
    }

    if (log.target_table === "remote_instances") {
      if (log.action === "CREATE") return `Added sync target: ${latest.name}`;
      if (log.action === "DELETE") return `Removed sync target: ${old.name}`;
    }

    if (log.target_table === "database") {
      if (log.action === "IMPORT") return `Restored database from ${latest.filename}`;
      if (log.action === "EXPORT") return `Exported database backup`;
    }

    if (log.target_table === "summary_stats") {
      if (log.action === "CREATE") return `Validated file: ${latest.filename} (${latest.total_rows} rows)`;
      if (log.action === "UPDATE") return `Manually updated stats for upazila`;
      if (log.action === "DELETE") return `Deleted file/batch and its records`;
    }

    if (log.target_table === "valid_records") {
      if (log.action === "DELETE") return `Deleted record: NID ${old.nid}`;
    }

    return `${log.action} on ${log.target_table}`;
  };

  const getAuditDiff = (log: any) => {
    const details = log.details || {};
    const old = details.old || {};
    const latest = details.new || {};

    if (log.action !== "UPDATE") return null;

    const diff: any = {};
    const allKeys = Array.from(new Set([...Object.keys(old), ...Object.keys(latest)]));

    allKeys.forEach(key => {
      if (key === 'updated_at' || key === 'created_at') return;
      if (JSON.stringify(old[key]) !== JSON.stringify(latest[key])) {
        diff[key] = { from: old[key], to: latest[key] };
      }
    });

    return Object.keys(diff).length > 0 ? diff : null;
  };

  // DB Management State
  const [isExporting, setIsExporting] = useState(false);
  const [isImporting, setIsImporting] = useState(false);
  const [selectedSqlFile, setSelectedSqlFile] = useState<File | null>(null);

  // --- User Management ---
  const [editingUser, setEditingUser] = useState<any | null>(null);

  const [isPassModalOpen, setIsPassModalOpen] = useState(false);
  const [editApiRateLimit, setEditApiRateLimit] = useState<number>(60);
  const [editApiTotalLimit, setEditApiTotalLimit] = useState<string>("");
  const [editApiIpWhitelist, setEditApiIpWhitelist] = useState<string>("");

  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState(false);
  const [testingInstanceId, setTestingInstanceId] = useState<number | null>(null);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [selectedAuditLog, setSelectedAuditLog] = useState<any | null>(null);
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
      const [uRes, cRes, iRes, upzRes, tzRes] = await Promise.all([
        fetchWithAuth(`${getBackendUrl()}/auth/users`),
        fetchWithAuth(`${getBackendUrl()}/admin/config`),
        fetchWithAuth(`${getBackendUrl()}/admin/instances`),
        fetchWithAuth(`${getBackendUrl()}/admin/upazilas`),
        fetchWithAuth(`${getBackendUrl()}/admin/trailing-zero-whitelist`),
      ]);

      if (uRes.ok) setUsers(await uRes.json());
      if (cRes.ok) {
        const cData = await cRes.json();
        setConfigs(cData);
        const rl = cData.find((c: any) => c.key === "rate_limit_value");
        if (rl) setRateLimit(rl.value);
        const tzLimitConf = cData.find((c: any) => c.key === "trailing_zero_limit");
        if (tzLimitConf) setTrailingZeroLimit(tzLimitConf.value);
      }
      if (iRes.ok) setInstances(await iRes.json());
      if (upzRes.ok) setUpazilas(await upzRes.json());
      if (tzRes.ok) setTzWhitelist(await tzRes.json());

      const [auditRes, apiRes] = await Promise.all([
        fetchWithAuth(`${getBackendUrl()}/admin/audit-logs?limit=50`),
        fetchWithAuth(`${getBackendUrl()}/admin/api-usage?limit=50`),
      ]);

      if (auditRes.ok) setAuditLogs(await auditRes.json());
      if (apiRes.ok) setApiLogs(await apiRes.json());

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
      const response = await fetchWithAuth(`${getBackendUrl()} /auth/users`, {
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
      const response = await fetchWithAuth(`${getBackendUrl()} /auth/users / ${id} `, { method: "DELETE" });
      if (!response.ok) throw new Error("Failed to delete user");
      showMsg("User deleted"); loadData();
    } catch (err: any) { showMsg(err.message, true); }
  };

  // --- Config ---
  const handleUpdateConfig = async (e: React.FormEvent) => {
    e.preventDefault();
    setActionLoading(true);
    try {
      const response1 = await fetchWithAuth(`${getBackendUrl()}/admin/config/rate_limit_value`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: rateLimit }),
      });
      const response2 = await fetchWithAuth(`${getBackendUrl()}/admin/config/trailing_zero_limit`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: trailingZeroLimit }),
      });
      if (!response1.ok || !response2.ok) throw new Error("Failed to update config");
      showMsg("Config updated"); loadData();
    } catch (err: any) { showMsg(err.message, true); } finally { setActionLoading(false); }
  };

  // --- Whitelist ---
  const handleAddTzWhitelist = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newTzNid) return;
    setActionLoading(true);
    try {
      const response = await fetchWithAuth(`${getBackendUrl()}/admin/trailing-zero-whitelist`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ nid: newTzNid.trim() }),
      });
      if (!response.ok) throw new Error((await response.json()).detail || "Failed to add NID");
      setNewTzNid(""); showMsg("NID added to whitelist"); loadData();
    } catch (err: any) { showMsg(err.message, true); } finally { setActionLoading(false); }
  };

  const handleRemoveTzWhitelist = async (nid: string) => {
    if (!confirm("Remove NID from whitelist?")) return;
    try {
      await fetchWithAuth(`${getBackendUrl()}/admin/trailing-zero-whitelist/${nid}`, { method: "DELETE" });
      showMsg("NID removed from whitelist"); loadData();
    } catch (err: any) { showMsg(err.message, true); }
  };

  // --- Instances ---
  const handleCreateInstance = async (e: React.FormEvent) => {
    e.preventDefault();
    setActionLoading(true);
    try {
      const response = await fetchWithAuth(`${getBackendUrl()} /admin/instances`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: instName, url: instUrl, api_key: instApiKey }),
      });
      if (!response.ok) throw new Error("Failed to create instance");
      setInstName(""); setInstUrl(""); setInstApiKey(""); showMsg("Instance added"); loadData();
    } catch (err: any) { showMsg(err.message, true); } finally { setActionLoading(false); }
  };

  const handleDeleteInstance = async (id: number) => {
    if (!confirm("Delete instance?")) return;
    try {
      await fetchWithAuth(`${getBackendUrl()} /admin/instances / ${id} `, { method: "DELETE" });
      showMsg("Instance deleted"); loadData();
    } catch (err: any) { showMsg(err.message, true); }
  };

  const handleSyncInstance = async (id: number) => {
    setActionLoading(true);
    try {
      const response = await fetchWithAuth(`${getBackendUrl()} /admin/instances / ${id}/trigger-sync`, { method: "POST" });
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

  // --- DB Management ---
  const handleExportDB = async () => {
    setIsExporting(true);
    try {
      const response = await fetchWithAuth(`${getBackendUrl()}/admin/db/export`);
      if (!response.ok) throw new Error("Export failed");

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `ffp_db_export_${new Date().toISOString().split('T')[0]}.sql`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      showMsg("Database export started");
    } catch (err: any) {
      showMsg(err.message, true);
    } finally {
      setIsExporting(false);
    }
  };

  const handleImportDB = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedSqlFile) return;
    if (!confirm("WARNING: This will overwrite CURRENT data. Proceed?")) return;

    setIsImporting(true);
    try {
      const formData = new FormData();
      formData.append("file", selectedSqlFile);

      const response = await fetchWithAuth(`${getBackendUrl()}/admin/db/import`, {
        method: "POST",
        body: formData,
      });

      if (!response.ok) throw new Error((await response.json()).detail || "Import failed");

      showMsg("Database imported successfully! Page will reload.");
      setTimeout(() => window.location.reload(), 2000);
    } catch (err: any) {
      showMsg(err.message, true);
    } finally {
      setIsImporting(false);
    }
  };

  const handleRenameLocation = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!renameOldName || !renameNewName) return;
    try {
      setActionLoading(true);
      const res = await fetchWithAuth(`${getBackendUrl()}/admin/location/rename`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          level: renameLevel,
          old_name: renameOldName,
          new_name: renameNewName,
          parent_name: renameParentName || null
        })
      });
      if (res.ok) {
        showMsg("Location renamed successfully");
        setRenameOldName("");
        setRenameNewName("");
        setRenameParentName("");
        loadData();
      } else {
        const d = await res.json();
        showMsg(d.detail || "Rename failed", true);
      }
    } catch (err: any) { showMsg(err.message, true); }
    finally { setActionLoading(false); }
  };

  const handleSaveUpazilaInline = async (u: any) => {
    const nameChanged = editingUpzName && editingUpzName !== u.name;
    const quotaChanged = editingUpzQuota !== (u.quota || 0);

    if (!nameChanged && !quotaChanged) {
      setEditingUpzId(null);
      return;
    }

    try {
      setActionLoading(true);

      // Handle Rename if changed
      if (nameChanged) {
        const res = await fetchWithAuth(`${getBackendUrl()}/admin/location/rename`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            level: "upazila",
            old_name: u.name,
            new_name: editingUpzName,
            parent_name: u.district_name
          })
        });
        if (!res.ok) {
          const d = await res.json();
          showMsg(d.detail || "Rename failed", true);
          return;
        }
      }

      // Handle Quota if changed
      if (quotaChanged) {
        const quotaRes = await fetchWithAuth(`${getBackendUrl()}/admin/upazilas/${u.id}/quota`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ quota: Number(editingUpzQuota) })
        });
        if (!quotaRes.ok) {
          const d = await quotaRes.json();
          showMsg(d.detail || "Quota update failed", true);
          return;
        }
      }

      showMsg("Upazila updated successfully");
      setEditingUpzId(null);
      loadData();
    } catch (err: any) { showMsg(err.message, true); }
    finally { setActionLoading(false); }
  };

  const handleUpdateUser = async (userId: number) => {
    try {
      setActionLoading(true);
      const updateData: any = {};
      if (newPassword) updateData.password = newPassword;
      if (newRole) updateData.role = newRole;

      // API limits
      updateData.api_rate_limit = editApiRateLimit;
      updateData.api_total_limit = editApiTotalLimit === "" ? null : parseInt(editApiTotalLimit);
      updateData.api_ip_whitelist = editApiIpWhitelist;

      const res = await fetchWithAuth(`${getBackendUrl()}/auth/users/${userId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(updateData)
      });
      if (res.ok) {
        showMsg("User updated successfully");
        setEditingUser(null);
        setNewPassword("");
        loadData();
      } else {
        const d = await res.json();
        showMsg(d.detail || "Update failed", true);
      }
    } catch (err: any) { showMsg(err.message, true); }
    finally { setActionLoading(false); }
  };

  const handleGenerateApiKey = async (userId: number) => {
    if (!confirm("Generate new API key? This will overwrite existing key.")) return;
    try {
      setActionLoading(true);
      const res = await fetchWithAuth(`${getBackendUrl()}/auth/users/${userId}/generate-api-key`, {
        method: "POST"
      });
      if (res.ok) {
        const data = await res.json();
        // The API returns the raw key ONCE during generation on the user object's api_key property
        if (data.api_key) {
          prompt("WARNING: This key will only be shown ONCE. Copy it now:", data.api_key);
        } else {
          showMsg("API Key generated");
        }
        loadData();
      } else {
        const d = await res.json();
        showMsg(d.detail || "Generation failed", true);
      }
    } catch (err: any) { showMsg(err.message, true); }
    finally { setActionLoading(false); }
  };

  const handleTestInstance = async (id: number) => {
    try {
      setTestingInstanceId(id);
      const res = await fetchWithAuth(`${getBackendUrl()}/admin/instances/${id}/test`, {
        method: "POST"
      });
      const data = await res.json();
      if (data.status === "online") {
        showMsg("Connection successful!");
      } else {
        showMsg(`Connection failed: ${data.message}`, true);
      }
    } catch (err: any) {
      showMsg(`Error: ${err.message}`, true);
    } finally {
      setTestingInstanceId(null);
    }
  };

  const renderJson = (data: any) => {
    if (!data) return "{}";
    return JSON.stringify(data, null, 2);
  };

  if (loading) return (
    <div className="min-h-screen bg-[#0a0a0b] flex items-center justify-center">
      <div className="w-8 h-8 border-4 border-emerald-500/20 border-t-emerald-500 rounded-full animate-spin"></div>
    </div>
  );

  return (
    <div className="min-h-screen bg-[#0a0a0b] text-white p-4 md:p-8">
      <div className="max-w-6xl mx-auto">
        <header className="mb-8">
          <div className="flex flex-col md:flex-row md:justify-between md:items-start gap-4 mb-2">
            <div>
              <h1 className="text-3xl md:text-4xl font-black tracking-tight mb-2">System Administration</h1>
              <p className="text-gray-500 text-sm">Manage Users, Configurations, and Sync</p>
            </div>
            <div className="flex flex-wrap gap-3">
              <button
                onClick={() => setIsPassModalOpen(true)}
                className="px-4 py-2 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-xs sm:text-sm font-semibold hover:bg-[#222224] transition-colors"
              >
                Change Password
              </button>
              <Link href="/" className="px-4 py-2 rounded-xl bg-emerald-600 hover:bg-emerald-500 text-xs sm:text-sm font-bold transition-colors">
                Back to Dashboard
              </Link>
            </div>
          </div>
        </header>

        {(error || success) && (
          <div className={`p-4 mb-6 rounded-xl ${error ? 'bg-red-500/10 text-red-500 border border-red-500/20' : 'bg-emerald-500/10 text-emerald-500 border border-emerald-500/20'}`}>
            {error || success}
          </div>
        )}

        {/* Tabs */}
        <div className="flex space-x-2 border-b border-[#1e1e20] mb-8 overflow-x-auto custom-scrollbar whitespace-nowrap pb-1">
          {['users', 'config', 'instances', 'upazilas', 'database', 'maintenance', 'audit', 'api'].map(tab => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-4 py-3 font-semibold text-xs sm:text-sm capitalize transition-colors border-b-2 ${
                activeTab === tab ? 'border-emerald-500 text-white' : 'border-transparent text-gray-500 hover:text-gray-300'
              } ${tab === 'maintenance' ? 'text-amber-400!' : ''}`}
            >
              {tab === 'api' ? 'API Usage' : tab === 'audit' ? 'Audit Logs' : tab}
            </button>
          ))}
        </div>

        {/* --- MAINTENANCE TAB --- */}
        {activeTab === 'maintenance' && (
          <div className="space-y-6">
            {/* Warning Banner */}
            <div className="p-4 rounded-xl bg-amber-500/10 border border-amber-500/30 text-amber-300 text-sm flex items-start gap-3">
              <span className="text-xl mt-0.5">⚠️</span>
              <div>
                <p className="font-bold mb-1">Maintenance — Use with Care</p>
                <p className="text-amber-400/80">
                  <strong>Scan</strong> and <strong>Cleanup</strong> never delete data.
                  <strong className="text-red-400"> Delete Unresolved</strong> permanently removes records whose upazila cannot be matched to the master geo table — run Scan first to see exactly what will be removed.
                </p>
              </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Scan Panel */}
              <div className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl">
                <h2 className="text-xl font-bold mb-2">1. Scan for Orphaned Records</h2>
                <p className="text-xs text-gray-500 mb-4">Dry-run only. Shows records whose upazila names don't match the master geo table, and counts rows with missing geo IDs. Zero changes made.</p>
                <button
                  onClick={async () => {
                    setMaintenanceLoading(true);
                    setOrphanPreview(null);
                    try {
                      const res = await fetchWithAuth(`${getBackendUrl()}/admin/maintenance/preview-orphans`);
                      if (!res.ok) throw new Error((await res.json()).detail || 'Scan failed');
                      setOrphanPreview(await res.json());
                    } catch (err: any) { showMsg(err.message, true); }
                    finally { setMaintenanceLoading(false); }
                  }}
                  disabled={maintenanceLoading}
                  className="w-full py-3 rounded-xl bg-blue-600 hover:bg-blue-500 font-bold disabled:opacity-50 transition-colors"
                >
                  {maintenanceLoading ? 'Scanning...' : '🔍 Scan for Orphans'}
                </button>

                {orphanPreview && (
                  <div className="mt-4 space-y-3 text-xs">
                    {/* Null ID Counts */}
                    <div className="p-3 rounded-lg bg-[#1a1a1c] border border-[#2a2a2e]">
                      <p className="font-semibold text-gray-300 mb-2">Rows with NULL geo IDs (need backfill):</p>
                      {Object.entries(orphanPreview.null_geo_id_counts || {}).map(([tbl, cnt]: any) => (
                        <div key={tbl} className="flex justify-between py-0.5">
                          <span className="text-gray-500">{tbl}</span>
                          <span className={cnt > 0 ? 'text-amber-400 font-bold' : 'text-emerald-400'}>{cnt.toLocaleString()} rows</span>
                        </div>
                      ))}
                    </div>
                    {/* Orphan Groups */}
                    {Object.entries(orphanPreview.orphans_by_table || {}).map(([tbl, data]: any) => (
                      data.orphan_groups > 0 && (
                        <div key={tbl} className="p-3 rounded-lg bg-red-500/5 border border-red-500/20">
                          <p className="font-semibold text-red-400 mb-1">{tbl}: {data.orphan_groups} orphan group(s)</p>
                          {data.orphans.slice(0, 5).map((o: any, i: number) => (
                            <div key={i} className="text-gray-400 py-0.5">
                              <span className="text-gray-500">{o.district} / {o.upazila}</span> — <span className="text-red-400">{o.row_count} rows</span>
                            </div>
                          ))}
                          {data.orphans.length > 5 && <p className="text-gray-500 italic">...and {data.orphans.length - 5} more</p>}
                        </div>
                      )
                    ))}
                    {Object.values(orphanPreview.orphans_by_table || {}).every((d: any) => d.orphan_groups === 0) &&
                      Object.values(orphanPreview.null_geo_id_counts || {}).every((v: any) => v === 0) && (
                      <div className="p-3 rounded-lg bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-center font-semibold">
                        ✅ All records are clean — no orphans or missing IDs!
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* Cleanup Panel */}
              <div className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl">
                <h2 className="text-xl font-bold mb-2">2. Run Geo Cleanup</h2>
                <p className="text-xs text-gray-500 mb-1">Performs two safe operations on all data tables:</p>
                <ul className="text-xs text-gray-500 list-disc list-inside mb-4 space-y-0.5">
                  <li>Trim leading/trailing whitespace from division, district, and upazila fields</li>
                  <li>Backfill <code className="bg-black/30 px-1 rounded">division_id</code>, <code className="bg-black/30 px-1 rounded">district_id</code>, <code className="bg-black/30 px-1 rounded">upazila_id</code> for all rows</li>
                </ul>
                <p className="text-[10px] text-red-400/70 mb-4 italic">⛔ Does NOT delete any records. Recommended: run Scan first.</p>
                <button
                  onClick={async () => {
                    if (!confirm('Run geo cleanup?\n\nThis will:\n• Trim whitespace from geo name fields\n• Backfill missing geo IDs\n\nNo records will be deleted. Continue?')) return;
                    setMaintenanceLoading(true);
                    setCleanupResult(null);
                    try {
                      const res = await fetchWithAuth(`${getBackendUrl()}/admin/maintenance/run-cleanup`, { method: 'POST' });
                      const data = await res.json();
                      if (!res.ok) throw new Error(data.detail || 'Cleanup failed');
                      setCleanupResult(data);
                      showMsg(data.success ? 'Cleanup complete!' : 'Cleanup finished with some warnings.');
                    } catch (err: any) { showMsg(err.message, true); }
                    finally { setMaintenanceLoading(false); }
                  }}
                  disabled={maintenanceLoading}
                  className="w-full py-3 rounded-xl bg-amber-600 hover:bg-amber-500 font-bold disabled:opacity-50 transition-colors"
                >
                  {maintenanceLoading ? 'Running...' : '🧹 Run Cleanup (Safe)'}
                </button>

                {cleanupResult && (
                  <div className="mt-4 space-y-3 text-xs">
                    {/* Summary */}
                    <div className={`p-3 rounded-lg border ${cleanupResult.success ? 'bg-emerald-500/10 border-emerald-500/20' : 'bg-amber-500/10 border-amber-500/20'}`}>
                      <p className={`font-bold mb-1 ${cleanupResult.success ? 'text-emerald-400' : 'text-amber-400'}`}>
                        {cleanupResult.success ? '✅ Completed successfully' : '⚠️ Completed with warnings'}
                      </p>
                      <div className="flex gap-4">
                        <span className="text-gray-400">Rows trimmed: <strong className="text-white">{cleanupResult.rows_trimmed}</strong></span>
                        <span className="text-gray-400">IDs backfilled: <strong className="text-white">{cleanupResult.ids_backfilled}</strong></span>
                        {cleanupResult.unresolved_count > 0 && (
                          <span className="text-amber-400">Unresolved: <strong>{cleanupResult.unresolved_count}</strong></span>
                        )}
                      </div>
                    </div>
                    {/* Step Log */}
                    <div className="p-3 rounded-lg bg-[#1a1a1c] border border-[#2a2a2e] max-h-40 overflow-y-auto custom-scrollbar">
                      {cleanupResult.report?.steps?.map((s: string, i: number) => (
                        <p key={i} className="text-gray-400 py-0.5">{s}</p>
                      ))}
                      {cleanupResult.report?.errors?.map((e: string, i: number) => (
                        <p key={`e${i}`} className="text-red-400 py-0.5">❌ {e}</p>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Delete Unresolved Panel — shown only when scan found orphans */}
            {orphanPreview && Object.values(orphanPreview.orphans_by_table || {}).some((d: any) => d.orphan_groups > 0) && (
              <div className="bg-[#121214] border-2 border-red-500/40 p-6 rounded-2xl">
                <div className="flex items-center gap-2 mb-2">
                  <span className="text-xl">🗑️</span>
                  <h2 className="text-xl font-bold text-red-400">3. Delete Unresolved Records</h2>
                  <span className="ml-auto text-[10px] uppercase font-bold text-red-400 border border-red-500/30 px-2 py-0.5 rounded">Irreversible</span>
                </div>
                <p className="text-xs text-gray-400 mb-3">
                  Permanently removes all records whose upazila cannot be matched to the master geo table.
                  After deletion, <strong className="text-white">SummaryStats are recalculated from live data</strong> — so the Stats page
                  grand total, cards, Excel, and PDF downloads will all be consistent.
                </p>

                {/* Deletion preview from scan */}
                <div className="mb-4 p-3 rounded-lg bg-red-500/5 border border-red-500/20 text-xs space-y-1">
                  <p className="font-semibold text-red-400 mb-1">Will be deleted:</p>
                  {Object.entries(orphanPreview.orphans_by_table || {}).map(([tbl, data]: any) =>
                    data.orphan_groups > 0 && (
                      <div key={tbl} className="flex justify-between text-gray-400">
                        <span>{tbl}</span>
                        <span className="text-red-400 font-bold">
                          {data.orphans.reduce((s: number, o: any) => s + o.row_count, 0).toLocaleString()} rows
                          <span className="text-gray-500 font-normal ml-1">({data.orphan_groups} group{data.orphan_groups > 1 ? 's' : ''})</span>
                        </span>
                      </div>
                    )
                  )}
                </div>

                <button
                  onClick={async () => {
                    const totalRows = Object.values(orphanPreview.orphans_by_table || {}).reduce(
                      (sum: number, d: any) => sum + d.orphans.reduce((s: number, o: any) => s + o.row_count, 0), 0
                    );
                    if (!confirm(
                      `⚠️ PERMANENT DELETION\n\nThis will delete ${totalRows.toLocaleString()} unresolved records from the database and recalculate all summary stats.\n\nThis CANNOT be undone. Make sure you have a database backup.\n\nProceed?`
                    )) return;
                    setMaintenanceLoading(true);
                    setDeleteResult(null);
                    try {
                      const res = await fetchWithAuth(`${getBackendUrl()}/admin/maintenance/delete-unresolved`, { method: 'DELETE' });
                      const data = await res.json();
                      if (!res.ok) throw new Error(data.detail || 'Delete failed');
                      setDeleteResult(data);
                      setOrphanPreview(null);
                      showMsg(data.success ? `Deleted ${data.total_records_deleted?.toLocaleString()} records. Stats recalculated.` : 'Deletion finished with errors.');
                    } catch (err: any) { showMsg(err.message, true); }
                    finally { setMaintenanceLoading(false); }
                  }}
                  disabled={maintenanceLoading}
                  className="w-full py-3 rounded-xl bg-red-700 hover:bg-red-600 font-bold disabled:opacity-50 transition-colors text-white"
                >
                  {maintenanceLoading ? 'Deleting...' : '🗑️ Delete Unresolved & Recalculate Stats'}
                </button>
              </div>
            )}

            {/* Delete Result */}
            {deleteResult && (
              <div className={`p-4 rounded-xl border text-xs ${deleteResult.success ? 'bg-emerald-500/10 border-emerald-500/20' : 'bg-amber-500/10 border-amber-500/20'}`}>
                <p className={`font-bold text-sm mb-3 ${deleteResult.success ? 'text-emerald-400' : 'text-amber-400'}`}>
                  {deleteResult.success ? '✅ Deletion complete — all totals recalculated' : '⚠️ Deletion finished with warnings'}
                </p>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                  <div className="p-2 rounded bg-[#1a1a1c] border border-[#2a2a2e]">
                    <p className="text-gray-500 uppercase text-[9px] font-bold">Records Deleted</p>
                    <p className="text-red-400 font-mono font-bold text-lg">{deleteResult.total_records_deleted?.toLocaleString()}</p>
                  </div>
                  <div className="p-2 rounded bg-[#1a1a1c] border border-[#2a2a2e]">
                    <p className="text-gray-500 uppercase text-[9px] font-bold">Stats Rows Deleted</p>
                    <p className="text-red-400 font-mono font-bold text-lg">{deleteResult.summary_stats_rows_deleted?.toLocaleString()}</p>
                  </div>
                  <div className="p-2 rounded bg-[#1a1a1c] border border-[#2a2a2e]">
                    <p className="text-gray-500 uppercase text-[9px] font-bold">Batches Archived</p>
                    <p className="text-amber-400 font-mono font-bold text-lg">{deleteResult.upload_batches_marked_deleted?.toLocaleString()}</p>
                  </div>
                  <div className="p-2 rounded bg-[#1a1a1c] border border-[#2a2a2e]">
                    <p className="text-gray-500 uppercase text-[9px] font-bold">Stats Recalculated</p>
                    <p className="text-emerald-400 font-mono font-bold text-lg">{deleteResult.stats_recalculated?.toLocaleString()}</p>
                  </div>
                </div>
                {deleteResult.report?.errors?.length > 0 && (
                  <div className="mt-3 p-2 rounded bg-red-500/10 border border-red-500/20">
                    {deleteResult.report.errors.map((e: string, i: number) => (
                      <p key={i} className="text-red-400">❌ {e}</p>
                    ))}
                  </div>
                )}
              </div>
            )}

          </div>
        )}


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
                <div className="overflow-x-auto">
                  <table className="w-full text-left">
                    <thead>
                      <tr className="bg-[#1a1a1c] border-b border-[#1e1e20]">
                        <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">User / API Key</th>
                        <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Role / Limits</th>
                        <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Usage</th>
                        <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase text-right">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-[#1e1e20]">
                      {users.map((u) => (
                        <tr key={u.id} className="hover:bg-[#161618] group">
                          <td className="px-6 py-4">
                            <div className="flex flex-col">
                              <span className="font-medium">{u.username}</span>
                              {u.api_key ? (
                                <div className="flex items-center space-x-2 mt-1">
                                  <span className="text-[10px] text-emerald-500 font-mono bg-emerald-500/5 px-1.5 py-0.5 rounded border border-emerald-500/10">
                                    {u.api_key.substring(0, 8)}...{u.api_key.substring(u.api_key.length - 4)}
                                  </span>
                                  <button
                                    onClick={() => { navigator.clipboard.writeText(u.api_key); showMsg("Copied to clipboard"); }}
                                    className="text-[10px] text-gray-500 hover:text-white"
                                    title="Copy Key"
                                  >
                                    Copy
                                  </button>
                                </div>
                              ) : (
                                <span className="text-[10px] text-gray-500 italic mt-1">No API Key</span>
                              )}
                            </div>
                          </td>
                          <td className="px-6 py-4">
                            <div className="flex flex-col space-y-1">
                              {editingUser?.id === u.id ? (
                                <>
                                  <select
                                    value={newRole}
                                    onChange={(e) => setNewRole(e.target.value)}
                                    className="bg-[#1a1a1c] border border-[#2a2a2e] text-xs rounded px-2 py-1 outline-none focus:border-emerald-500 mb-1"
                                  >
                                    <option value="viewer">Viewer</option>
                                    <option value="uploader">Uploader</option>
                                    <option value="admin">Admin</option>
                                  </select>
                                  <div className="flex flex-col space-y-1">
                                    <div className="flex items-center space-x-1">
                                      <span className="text-[9px] text-gray-500 uppercase w-8">Rate:</span>
                                      <input
                                        type="text"
                                        value={editApiRateLimit}
                                        onChange={(e) => setEditApiRateLimit(parseInt(e.target.value) || 0)}
                                        className="bg-[#1a1a1c] border border-[#2a2a2e] text-[10px] rounded px-1.5 py-0.5 outline-none focus:border-emerald-500 flex-1"
                                        placeholder="60/minute"
                                      />
                                    </div>
                                    <div className="flex items-center space-x-1">
                                      <span className="text-[9px] text-gray-500 uppercase w-8">Total:</span>
                                      <input
                                        type="text"
                                        value={editApiTotalLimit}
                                        onChange={(e) => setEditApiTotalLimit(e.target.value)}
                                        className="bg-[#1a1a1c] border border-[#2a2a2e] text-[10px] rounded px-1.5 py-0.5 outline-none focus:border-emerald-500 flex-1"
                                        placeholder="No limit"
                                      />
                                    </div>
                                    <div className="flex items-center space-x-1">
                                      <span className="text-[9px] text-gray-500 uppercase w-8">IPs:</span>
                                      <input
                                        type="text"
                                        value={editApiIpWhitelist}
                                        onChange={(e) => setEditApiIpWhitelist(e.target.value)}
                                        className="bg-[#1a1a1c] border border-[#2a2a2e] text-[10px] rounded px-1.5 py-0.5 outline-none focus:border-emerald-500 flex-1"
                                        placeholder="IP Whitelist (comma-sep)"
                                      />
                                    </div>
                                  </div>
                                </>
                              ) : (
                                <>
                                  <span className={`w-fit px-2 py-0.5 rounded text-[10px] font-bold uppercase ${u.role === 'admin' ? 'bg-purple-500/10 text-purple-400' : u.role === 'uploader' ? 'bg-blue-500/10 text-blue-400' : 'bg-gray-500/10 text-gray-400'}`}>
                                    {u.role}
                                  </span>
                                  {u.api_rate_limit && (
                                    <span className="text-[10px] text-gray-500">Rate: {u.api_rate_limit}</span>
                                  )}
                                  {u.api_total_limit !== null && (
                                    <span className="text-[10px] text-gray-500">Total: {u.api_total_limit} max</span>
                                  )}
                                  {u.api_ip_whitelist && (
                                    <span className="text-[10px] text-cyan-500/80" title={u.api_ip_whitelist}>IP Restricted</span>
                                  )}
                                </>
                              )}
                            </div>
                          </td>
                          <td className="px-6 py-4">
                            <div className="flex flex-col">
                              <span className="text-xs font-medium text-white">{u.api_usage_count || 0} calls</span>
                              {u.api_key_last_used && (
                                <span className="text-[10px] text-gray-500 mt-0.5">Last: {new Date(u.api_key_last_used).toLocaleDateString()}</span>
                              )}
                            </div>
                          </td>
                          <td className="px-6 py-4 text-right">
                            {editingUser?.id === u.id ? (
                              <div className="flex flex-col items-end space-y-2">
                                <input
                                  type="password"
                                  placeholder="Reset Password"
                                  value={newPassword}
                                  onChange={(e) => setNewPassword(e.target.value)}
                                  className="bg-[#1a1a1c] border border-[#2a2a2e] text-xs rounded px-2 py-1 outline-none focus:border-emerald-500 w-full max-w-[120px]"
                                />
                                <div className="flex items-center space-x-2">
                                  <button onClick={() => handleUpdateUser(u.id)} className="text-emerald-500 hover:text-emerald-400 text-xs font-bold px-2 py-1 rounded bg-emerald-500/10">Save</button>
                                  <button onClick={() => { setEditingUser(null); setNewPassword(""); setNewRole(""); }} className="text-gray-500 hover:text-gray-400 text-xs font-bold">Cancel</button>
                                </div>
                              </div>
                            ) : (
                              <div className="flex items-center justify-end space-x-3">
                                <button
                                  onClick={() => handleGenerateApiKey(u.id)}
                                  className="text-emerald-500 hover:text-emerald-400 transition-colors text-[10px] font-bold border border-emerald-500/20 px-2 py-1 rounded bg-emerald-500/5"
                                >
                                  {u.api_key ? 'Regen Key' : 'Gen Key'}
                                </button>
                                <button onClick={() => {
                                  setEditingUser(u);
                                  setNewRole(u.role);
                                  setEditApiRateLimit(u.api_rate_limit || "");
                                  setEditApiTotalLimit(u.api_total_limit !== null ? u.api_total_limit.toString() : "");
                                  setEditApiIpWhitelist(u.api_ip_whitelist || "");
                                }} className="text-gray-400 hover:text-white transition-colors text-xs font-bold">Edit</button>
                                <button onClick={() => handleDeleteUser(u.id)} className="text-gray-400 hover:text-red-500 transition-colors text-xs font-bold">Del</button>
                              </div>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* --- CONFIG TAB --- */}
        {activeTab === 'config' && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
            <div className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl">
              <h2 className="text-xl font-bold mb-6">System Configuration</h2>
              <form onSubmit={handleUpdateConfig} className="space-y-4">
                <div>
                  <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">NID Verify Request Rate Limit</label>
                  <p className="text-xs text-gray-500 mb-2">Format: `requests/period` (e.g. `60/minute`, `1000/day`)</p>
                  <input type="text" value={rateLimit} onChange={(e) => setRateLimit(e.target.value)} placeholder="60/minute" className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" required />
                </div>
                <div>
                  <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Trailing Zero Auto-Reject Limit</label>
                  <p className="text-xs text-gray-500 mb-2">Set how many trailing zeros trigger an automatic invalid flag for 17-digit NIDs (e.g. 6). Set to 0 to disable.</p>
                  <input type="number" min="0" value={trailingZeroLimit} onChange={(e) => setTrailingZeroLimit(e.target.value)} placeholder="6" className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" />
                </div>
                <button type="submit" disabled={actionLoading} className="w-full py-3 rounded-xl bg-purple-600 hover:bg-purple-500 font-bold disabled:opacity-50">Save Configuration</button>
              </form>
            </div>

            <div className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl">
              <h2 className="text-xl font-bold mb-6">Trailing Zero Whitelist</h2>
              <p className="text-xs text-gray-500 mb-4">Add 17-digit NIDs that end in many zeros but are actually valid to prevent them from being flagged.</p>
              <form onSubmit={handleAddTzWhitelist} className="space-y-4 mb-6">
                <div className="flex gap-2">
                  <input type="text" value={newTzNid} onChange={(e) => setNewTzNid(e.target.value)} placeholder="Enter 17-digit NID" className="flex-1 px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" required />
                  <button type="submit" disabled={actionLoading} className="px-6 py-3 rounded-xl bg-emerald-600 hover:bg-emerald-500 font-bold disabled:opacity-50">Add</button>
                </div>
              </form>
              <div className="max-h-[300px] overflow-y-auto custom-scrollbar pr-2">
                {tzWhitelist.length === 0 ? (
                  <p className="text-sm text-gray-500 italic text-center py-4">No whitelisted NIDs.</p>
                ) : (
                  <div className="space-y-2">
                    {tzWhitelist.map(item => (
                      <div key={item.nid} className="flex items-center justify-between bg-[#1a1a1c] p-3 rounded-lg border border-[#2a2a2e]">
                        <div>
                          <p className="font-mono text-sm text-white">{item.nid}</p>
                          <p className="text-[10px] text-gray-500">Added by {item.added_by || 'System'}</p>
                        </div>
                        <button onClick={() => handleRemoveTzWhitelist(item.nid)} className="text-red-500 hover:text-red-400 text-xs font-bold p-2">Remove</button>
                      </div>
                    ))}
                  </div>
                )}
              </div>
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
                    <input type="text" value={instName} onChange={(e) => setInstName(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" placeholder="Central Server" required />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Base URL</label>
                    <input type="url" value={instUrl} onChange={(e) => setInstUrl(e.target.value)} placeholder="http://1.2.3.4:8000" className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" required />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">API Key</label>
                    <input type="password" value={instApiKey} onChange={(e) => setInstApiKey(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 transition-colors" placeholder="The remote API key" required />
                  </div>
                  <button type="submit" disabled={actionLoading} className="w-full py-3 rounded-xl bg-emerald-600 hover:bg-emerald-500 font-bold disabled:opacity-50">Add Instance</button>
                </form>
              </div>
            </div>
            <div className="lg:col-span-2">
              <div className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-left">
                    <thead>
                      <tr className="bg-[#1a1a1c] border-b border-[#1e1e20]">
                        <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Instance</th>
                        <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Status / Sync</th>
                        <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase text-right">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-[#1e1e20]">
                      {instances.map((inst) => (
                        <tr key={inst.id} className="hover:bg-[#161618] group">
                          <td className="px-6 py-4">
                            <div className="flex flex-col">
                              <span className="font-bold">{inst.name}</span>
                              <span className="text-xs text-gray-500">{inst.url}</span>
                            </div>
                          </td>
                          <td className="px-6 py-4 text-sm text-gray-400">
                            <div className="flex flex-col space-y-1">
                              <span className={`w-fit px-2 py-0.5 rounded text-[10px] font-bold uppercase ${inst.is_active ? 'bg-emerald-500/10 text-emerald-500' : 'bg-gray-500/10 text-gray-400'}`}>
                                {inst.is_active ? 'Active' : 'Inactive'}
                              </span>
                              <span className="text-[10px] text-gray-500">
                                Last Sync: {inst.last_synced_at ? new Date(inst.last_synced_at).toLocaleString() : 'Never'}
                              </span>
                            </div>
                          </td>
                          <td className="px-6 py-4 text-right">
                            <div className="flex items-center justify-end space-x-3">
                              <button
                                onClick={() => handleTestInstance(inst.id)}
                                disabled={testingInstanceId === inst.id || actionLoading}
                                className="text-xs font-bold text-emerald-500 hover:text-emerald-400 disabled:opacity-50"
                              >
                                {testingInstanceId === inst.id ? 'Testing...' : 'Test'}
                              </button>
                              <button
                                onClick={() => handleSyncInstance(inst.id)}
                                disabled={actionLoading}
                                className="text-xs font-bold text-blue-500 hover:text-blue-400 disabled:opacity-50"
                              >
                                Sync
                              </button>
                              <button onClick={() => handleDeleteInstance(inst.id)} className="text-gray-500 hover:text-red-500 transition-colors text-xs font-bold font-mono">DEL</button>
                            </div>
                          </td>
                        </tr>
                      ))}
                      {instances.length === 0 && (
                        <tr>
                          <td colSpan={3} className="px-6 py-12 text-center text-gray-500 italic">No remote instances configured.</td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
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

              <div className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl mt-8">
                <h2 className="text-xl font-bold mb-2 text-cyan-400">Rename Location</h2>
                <p className="text-xs text-gray-500 mb-6 italic">Changes will reflect everywhere and update existing records.</p>
                <form onSubmit={handleRenameLocation} className="space-y-4">
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Level</label>
                    <select value={renameLevel} onChange={(e) => setRenameLevel(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-cyan-500 transition-colors">
                      <option value="division">Division</option>
                      <option value="district">District</option>
                    </select>
                  </div>
                  {renameLevel === 'district' && (
                    <div>
                      <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Parent Name (Division)</label>
                      <input type="text" value={renameParentName} onChange={(e) => setRenameParentName(e.target.value)} placeholder="Division name" className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-cyan-500 transition-colors" />
                    </div>
                  )}
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Old Name</label>
                    <input type="text" value={renameOldName} onChange={(e) => setRenameOldName(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-cyan-500 transition-colors" required />
                  </div>
                  <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">New Name</label>
                    <input type="text" value={renameNewName} onChange={(e) => setRenameNewName(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-cyan-500 transition-colors" required />
                  </div>
                  <button type="submit" disabled={actionLoading} className="w-full py-3 rounded-xl bg-cyan-600 hover:bg-cyan-500 font-bold disabled:opacity-50">Rename Location</button>
                </form>
              </div>
            </div>
            <div className="lg:col-span-2">
              <div className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden max-h-[600px] overflow-y-auto">
                <div className="overflow-x-auto">
                  <table className="w-full text-left">
                    <thead>
                      <tr className="bg-[#1a1a1c] border-b border-[#1e1e20] sticky top-0 z-10">
                        <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Division</th>
                        <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">District</th>
                        <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Upazila</th>
                        <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Quota</th>
                        <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase text-right">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-[#1e1e20]">
                      {upazilas.map((u) => (
                        <tr key={u.id} className="hover:bg-[#161618] group">
                          <td className="px-6 py-4 text-gray-400">{u.division_name}</td>
                          <td className="px-6 py-4 text-gray-300">{u.district_name}</td>
                          <td className="px-6 py-4">
                            {editingUpzId === u.id ? (
                              <input
                                type="text"
                                autoFocus
                                value={editingUpzName}
                                onChange={(e) => setEditingUpzName(e.target.value)}
                                onKeyDown={(e) => {
                                  if (e.key === 'Enter') handleSaveUpazilaInline(u);
                                  if (e.key === 'Escape') setEditingUpzId(null);
                                }}
                                className="bg-[#1a1a1c] border border-emerald-500 text-white rounded px-2 py-1 outline-none w-full"
                              />
                            ) : (
                              <span className="font-bold">{u.name}</span>
                            )}
                          </td>
                          <td className="px-6 py-4">
                            {editingUpzId === u.id ? (
                              <input
                                type="number"
                                value={editingUpzQuota}
                                onChange={(e) => setEditingUpzQuota(e.target.value)}
                                className="bg-[#1a1a1c] border border-cyan-500 text-white rounded px-2 py-1 outline-none w-24"
                              />
                            ) : (
                              <span className="text-gray-300">{u.quota || 0}</span>
                            )}
                          </td>
                          <td className="px-6 py-4 text-right">
                            <div className="flex justify-end items-center space-x-3">
                              {editingUpzId === u.id ? (
                                <>
                                  <button onClick={() => handleSaveUpazilaInline(u)} className="text-emerald-500 text-sm font-bold">Save</button>
                                  <button onClick={() => setEditingUpzId(null)} className="text-gray-500 text-sm">Cancel</button>
                                </>
                              ) : (
                                <>
                                  <button
                                    onClick={() => {
                                      setEditingUpzId(u.id);
                                      setEditingUpzName(u.name);
                                      setEditingUpzQuota(u.quota || 0);
                                    }}
                                    className="text-emerald-500 opacity-0 group-hover:opacity-100 transition-opacity text-sm"
                                  >
                                    Edit
                                  </button>
                                  <button onClick={() => handleDeleteUpz(u.id)} className="text-red-500 opacity-0 group-hover:opacity-100 transition-opacity text-sm">Delete</button>
                                </>
                              )}
                            </div>
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
          </div>
        )}

        {/* --- API USAGE TAB --- */}
        {activeTab === 'api' && (
          <div className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-left">
                <thead>
                  <tr className="bg-[#1a1a1c] border-b border-[#1e1e20]">
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Time</th>
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">User</th>
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Endpoint</th>
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Status</th>
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Duration</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[#1e1e20]">
                  {apiLogs.map((log: any) => (
                    <tr key={log.id} className="hover:bg-[#161618] text-sm">
                      <td className="px-6 py-4 text-gray-500 whitespace-nowrap">
                        {new Date(log.created_at).toLocaleString()}
                      </td>
                      <td className="px-6 py-4 font-medium">{log.username || 'System'}</td>
                      <td className="px-6 py-4 text-gray-400 font-mono text-xs">{log.path}</td>
                      <td className="px-6 py-4">
                        <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${log.status_code >= 400 ? 'bg-red-500/10 text-red-500' : 'bg-emerald-500/10 text-emerald-500'}`}>
                          {log.status_code}
                        </span>
                      </td>
                      <td className="px-6 py-4 text-gray-500">{log.latency_ms ? Math.round(log.latency_ms) : 0}ms</td>
                    </tr>
                  ))}
                  {apiLogs.length === 0 && (
                    <tr><td colSpan={5} className="p-8 text-center text-gray-500">No API usage recorded.</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}
        {activeTab === 'audit' && (
          <div className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-left">
                <thead>
                  <tr className="bg-[#1a1a1c] border-b border-[#1e1e20]">
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase w-32">Time</th>
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase w-32">User</th>
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase w-24">Action</th>
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase w-32">Table</th>
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Details</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[#1e1e20]">
                  {auditLogs.map((log) => (
                    <tr
                      key={log.id}
                      className="hover:bg-[#161618] text-sm cursor-pointer transition-colors"
                      onClick={() => setSelectedAuditLog(log)}
                    >
                      <td className="px-6 py-4 text-gray-500 whitespace-nowrap">
                        {new Date(log.created_at).toLocaleString()}
                      </td>
                      <td className="px-6 py-4 font-medium">{log.username || 'System'}</td>
                      <td className="px-6 py-4">
                        <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase ${log.action === 'DELETE' || log.action === 'LOGIN_FAIL' ? 'bg-red-500/10 text-red-500' :
                          log.action === 'CREATE' || log.action === 'LOGIN_SUCCESS' ? 'bg-emerald-500/10 text-emerald-500' :
                            'bg-blue-500/10 text-blue-500'
                          }`}>
                          {log.action}
                        </span>
                      </td>
                      <td className="px-6 py-4 text-gray-400 capitalize">{log.target_table?.replace('_', ' ')}</td>
                      <td className="px-6 py-4 max-w-md truncate text-xs text-gray-400">
                        {formatAuditSummary(log)}
                      </td>
                    </tr>
                  ))}
                  {auditLogs.length === 0 && (
                    <tr><td colSpan={5} className="p-8 text-center text-gray-500">No audit logs found.</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* --- DATABASE TAB --- */}
        {activeTab === 'database' && (
          <div className="space-y-8">
            <div className="bg-amber-500/10 border border-amber-500/20 p-6 rounded-2xl">
              <div className="flex items-center space-x-4">
                <div className="p-3 bg-amber-500/20 rounded-xl">
                  <svg className="w-6 h-6 text-amber-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                  </svg>
                </div>
                <div>
                  <h3 className="text-lg font-bold text-amber-500">Caution: Database Operations</h3>
                  <p className="text-sm text-gray-400">Importing a database will overwrite all existing records. Automated backups are currently active (Hourly).</p>
                </div>
              </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
              <div className="bg-[#121214] border border-[#1e1e20] p-8 rounded-2xl">
                <h2 className="text-2xl font-black mb-4">Export Backup</h2>
                <p className="text-gray-500 mb-8 leading-relaxed">
                  Download a complete SQL dump of the system state, including users, beneficiary records, and upload history.
                </p>
                <button
                  onClick={handleExportDB}
                  disabled={isExporting}
                  className="w-full py-4 rounded-xl bg-emerald-600 hover:bg-emerald-500 font-black text-white transition-all transform active:scale-95 disabled:opacity-50 flex items-center justify-center space-x-2"
                >
                  {isExporting ? (
                    <div className="w-5 h-5 border-2 border-white/20 border-t-white rounded-full animate-spin"></div>
                  ) : (
                    <>
                      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                      </svg>
                      <span>Download Full SQL Backup</span>
                    </>
                  )}
                </button>
              </div>

              <div className="bg-[#121214] border border-[#1e1e20] p-8 rounded-2xl">
                <h2 className="text-2xl font-black mb-4">Restore Database</h2>
                <p className="text-gray-500 mb-8 leading-relaxed">
                  Upload a previously exported `.sql` file to restore the system to a previous state.
                </p>
                <form onSubmit={handleImportDB} className="space-y-4">
                  <div className="relative group">
                    <input
                      type="file"
                      accept=".sql"
                      onChange={(e) => setSelectedSqlFile(e.target.files?.[0] || null)}
                      className="absolute inset-0 w-full h-full opacity-0 cursor-pointer z-10"
                    />
                    <div className="w-full px-4 py-4 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-center group-hover:border-emerald-500/50 transition-colors">
                      <span className="text-sm text-gray-400">
                        {selectedSqlFile ? selectedSqlFile.name : "Click to select .sql file"}
                      </span>
                    </div>
                  </div>
                  <button
                    type="submit"
                    disabled={isImporting || !selectedSqlFile}
                    className="w-full py-4 rounded-xl bg-purple-600 hover:bg-purple-500 font-black text-white transition-all transform active:scale-95 disabled:opacity-50 disabled:bg-gray-800 flex items-center justify-center space-x-2"
                  >
                    {isImporting ? (
                      <div className="w-5 h-5 border-2 border-white/20 border-t-white rounded-full animate-spin"></div>
                    ) : (
                      <>
                        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
                        </svg>
                        <span>Restore System State</span>
                      </>
                    )}
                  </button>
                </form>
              </div>
            </div>

            <div className="bg-[#0f0f11] border border-[#1e1e20] p-6 rounded-2xl">
              <h4 className="text-xs font-bold text-gray-500 uppercase tracking-widest mb-4">Auto-Backup Status</h4>
              <div className="flex items-center space-x-3">
                <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></div>
                <span className="text-sm text-gray-400 italic">Cron job active: Backing up every hour to `/db_backups` on host.</span>
              </div>
            </div>
          </div>
        )}

        {/* --- Audit Log Details Modal --- */}
        {selectedAuditLog && (
          <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
            <div className="bg-[#121214] border border-[#1e1e20] w-full max-w-2xl rounded-2xl shadow-2xl overflow-hidden flex flex-col max-h-[80vh]">
              <div className="px-6 py-4 border-b border-[#1e1e20] flex justify-between items-center text-white">
                <h3 className="text-xl font-bold">Audit Log Details</h3>
                <button onClick={() => setSelectedAuditLog(null)} className="text-gray-500 hover:text-white text-2xl transition-colors">&times;</button>
              </div>
              <div className="p-6 overflow-y-auto custom-scrollbar space-y-4">
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <label className="text-gray-500 block">User</label>
                    <span className="font-medium text-white">{selectedAuditLog.username || 'System'}</span>
                  </div>
                  <div>
                    <label className="text-gray-500 block">Action</label>
                    <span className="font-medium text-white">{selectedAuditLog.action}</span>
                  </div>
                  <div>
                    <label className="text-gray-500 block">Table</label>
                    <span className="font-medium text-white capitalize">{selectedAuditLog.target_table?.replace('_', ' ')}</span>
                  </div>
                  <div>
                    <label className="text-gray-500 block">Time</label>
                    <span className="font-medium text-white">{new Date(selectedAuditLog.created_at).toLocaleString()}</span>
                  </div>
                </div>

                {selectedAuditLog.details?.old && (
                  <div>
                    <label className="text-xs font-bold text-gray-500 uppercase mb-2 block">Data Before Action</label>
                    <pre className="custom-scrollbar bg-[#0a0a0b] p-4 rounded-xl text-xs font-mono text-red-400 overflow-x-auto border border-red-500/10 max-h-48">
                      {JSON.stringify(selectedAuditLog.details?.old, null, 2)}
                    </pre>
                  </div>
                )}

                {selectedAuditLog.details?.new && (
                  <div>
                    <label className="text-xs font-bold text-gray-500 uppercase mb-2 block">Data After Action</label>
                    <pre className="custom-scrollbar bg-[#0a0a0b] p-4 rounded-xl text-xs font-mono text-emerald-400 overflow-x-auto border border-emerald-500/10 max-h-48">
                      {JSON.stringify(selectedAuditLog.details?.new, null, 2)}
                    </pre>
                  </div>
                )}

                {getAuditDiff(selectedAuditLog) && (
                  <div className="bg-blue-500/5 border border-blue-500/10 rounded-xl p-4">
                    <label className="text-xs font-bold text-blue-400 uppercase mb-3 block">Detected Changes</label>
                    <div className="space-y-3">
                      {Object.entries(getAuditDiff(selectedAuditLog) || {}).map(([key, val]: [string, any]) => (
                        <div key={key} className="flex flex-col space-y-1">
                          <span className="text-[10px] text-gray-500 font-mono">{key}</span>
                          <div className="flex items-center space-x-2 text-xs">
                            <span className="text-red-400 line-through truncate max-w-[150px]">{String(val.from)}</span>
                            <span className="text-gray-600">→</span>
                            <span className="text-emerald-400 font-bold truncate">{String(val.to)}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
              <div className="px-6 py-4 border-t border-[#1e1e20] flex justify-end">
                <button
                  onClick={() => setSelectedAuditLog(null)}
                  className="px-6 py-2 rounded-xl bg-[#1e1e20] hover:bg-[#2a2a2e] font-bold text-white transition-colors"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        )}

        <ChangePasswordModal isOpen={isPassModalOpen} onClose={() => setIsPassModalOpen(false)} />
      </div>
    </div>
  );
}
