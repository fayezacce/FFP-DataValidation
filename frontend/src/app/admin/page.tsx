"use client";

import React, { useState, useEffect, type FormEvent, type ChangeEvent } from "react";
import { useRouter } from "next/navigation";
import { fetchWithAuth, getBackendUrl, isAdmin, getUser } from "@/lib/auth";
import ChangePasswordModal from "@/components/ChangePasswordModal";
import LocationSelector from "@/components/LocationSelector";
import Link from "next/link";
import LocationsTab from "./components/LocationsTab";
import HeaderAliasTab from "./components/HeaderAliasTab";

export default function AdminPage() {
  const [activeTab, setActiveTab] = useState("users");

  // Users state
  const [users, setUsers] = useState<any[]>([]);
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newRole, setNewRole] = useState("viewer");
  const [newDivisionAccess, setNewDivisionAccess] = useState<string | null>(null);
  const [newDistrictAccess, setNewDistrictAccess] = useState<string | null>(null);
  const [newUpazilaAccess, setNewUpazilaAccess] = useState<string | null>(null);
  
  const [isAddUserModalOpen, setIsAddUserModalOpen] = useState(false);
  const [editingUser, setEditingUser] = useState<any | null>(null);
  const [editDivisionAccess, setEditDivisionAccess] = useState<string | null>(null);
  const [editDistrictAccess, setEditDistrictAccess] = useState<string | null>(null);
  const [editUpazilaAccess, setEditUpazilaAccess] = useState<string | null>(null);
  const [isPassModalOpen, setIsPassModalOpen] = useState(false);
  const [editApiRateLimit, setEditApiRateLimit] = useState<number>(60);
  const [editApiTotalLimit, setEditApiTotalLimit] = useState<string>("");
  const [editApiIpWhitelist, setEditApiIpWhitelist] = useState<string>("");

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

  // Logs State
  const [auditLogs, setAuditLogs] = useState<any[]>([]);
  const [apiLogs, setApiLogs] = useState<any[]>([]);

  // Maintenance State
  const [maintenanceLoading, setMaintenanceLoading] = useState(false);
  const [maintenanceStatus, setMaintenanceStatus] = useState<any>(null);
  const [orphanPreview, setOrphanPreview] = useState<any | null>(null);
  const [cleanupResult, setCleanupResult] = useState<any | null>(null);
  const [deleteResult, setDeleteResult] = useState<any | null>(null);

  const formatAuditSummary = (log: any) => {
    const details = log.details || {};
    const old = details.old || {};
    const latest = details.new || {};

    if (log.action === "LOGIN_FAIL") return `Failed login attempt for user: ${latest.username || 'unknown'}`;
    if (log.action === "LOGIN_SUCCESS") return `Successful login`;

    if (log.target_table === "upazila_full_wipe") {
      return `Fully wiped Upazila: ${details.location || log.target_id}`;
    }

    if (log.action === "RECHECK") {
      return `Fraud Recheck on ${latest.upazila || 'Upazila'}: ${latest.flagged}/${latest.total} suspicious records found`;
    }

    if (log.target_table === "upload_batch") {
      if (log.action === "CREATE") {
        return `Processed upload: ${latest.filename} (${latest.location || ''}) — ${latest.valid}/${latest.total} valid`;
      }
      if (log.action === "DELETE") return `Deleted upload batch history`;
    }

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

    return `${log.action} on ${log.target_table.replace('_', ' ')}`;
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
  const [backups, setBackups] = useState<any[]>([]);
  const [isExporting, setIsExporting] = useState(false);
  const [isImporting, setIsImporting] = useState(false);
  const [selectedSqlFile, setSelectedSqlFile] = useState<File | null>(null);
  const [dbTasks, setDbTasks] = useState<any[]>([]);

  // --- User Management ---
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

    // Setup polling for maintenance and db tasks
    const pollInterval = setInterval(async () => {
      if (activeTab === 'maintenance' || activeTab === 'database') {
        try {
          const res = await fetchWithAuth(`${getBackendUrl()}/admin/maintenance/status`);
          if (res.ok) {
            const statusData = await res.json();
            setMaintenanceStatus(statusData);
            
            // Clear loading state when background task finishes
            const cleanupDone = statusData.cleanup?.status === 'completed' || statusData.cleanup?.status === 'error';
            const deleteDone = statusData.delete?.status !== 'running';
            const repairGeoDone = statusData.repair_geo?.status === 'completed' || statusData.repair_geo?.status === 'error';
            if (cleanupDone && deleteDone && repairGeoDone) {
              setMaintenanceLoading(false);
            }
          }

          // Fetch my tasks for DB progress
          const tasksRes = await fetchWithAuth(`${getBackendUrl()}/tasks/my-tasks`);
          if (tasksRes.ok) {
            const data = await tasksRes.json();
            setDbTasks(data.filter((t: any) => t.task_name === 'db_backup' || t.task_name === 'db_restore'));
          }

          if (activeTab === 'database') {
            loadBackups();
          }
        } catch (e) { console.error("Poll error", e); }
      }
    }, 3000);

    return () => clearInterval(pollInterval);
  }, [router, activeTab]);

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
      if (tzRes.ok) setTzWhitelist(await tzRes.json());

      const [auditRes, apiRes] = await Promise.all([
        fetchWithAuth(`${getBackendUrl()}/audit/logs?limit=50`),
        fetchWithAuth(`${getBackendUrl()}/audit/api-usage?limit=50`),
      ]);

      if (auditRes.ok) { const d = await auditRes.json(); setAuditLogs(d.items || d); }
      if (apiRes.ok) { const d = await apiRes.json(); setApiLogs(d.items || d); }

    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const loadBackups = async () => {
    try {
      const res = await fetchWithAuth(`${getBackendUrl()}/admin/db/backups`);
      if (res.ok) setBackups(await res.json());
    } catch (e) { console.error("Load backups failed", e); }
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
        body: JSON.stringify({ 
          username: newUsername, 
          password: newPassword, 
          role: newRole,
          division_access: newRole === 'admin' ? null : newDivisionAccess,
          district_access: newRole === 'admin' ? null : newDistrictAccess,
          upazila_access: newRole === 'admin' ? null : newUpazilaAccess,
        }),
      });
      if (!response.ok) throw new Error((await response.json()).detail || "Failed to create user");
      setNewUsername(""); 
      setNewPassword(""); 
      setNewDivisionAccess(null);
      setNewDistrictAccess(null);
      setNewUpazilaAccess(null);
      setIsAddUserModalOpen(false);
      showMsg("User created");
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
      const response = await fetchWithAuth(`${getBackendUrl()}/admin/instances`, {
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


  // --- DB Management ---
  const handleExportDB = async () => {
    if (dbTasks.some(t => t.status === 'running')) {
      showMsg("A database task is already running", true);
      return;
    }
    setIsExporting(true);
    try {
      const res = await fetchWithAuth(`${getBackendUrl()}/admin/db/backups/run`, { method: "POST" });
      if (!res.ok) throw new Error("Failed to start backup");
      showMsg("Database backup started in background");
      loadBackups();
    } catch (err: any) {
      showMsg(err.message, true);
    } finally {
      setIsExporting(false);
    }
  };

  const handleImportDB = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedSqlFile) return;
    setIsImporting(true);
    try {
      const formData = new FormData();
      formData.append("file", selectedSqlFile);
      const res = await fetchWithAuth(`${getBackendUrl()}/admin/db/backups/upload`, {
        method: "POST",
        body: formData,
      });
      if (!res.ok) throw new Error("Upload failed");
      showMsg("File uploaded to server. You can now restore it from the list.");
      setSelectedSqlFile(null);
      loadBackups();
    } catch (err: any) {
      showMsg(err.message, true);
    } finally {
      setIsImporting(false);
    }
  };

  const handleRestoreBackup = async (filename: string) => {
    if (!confirm(`WARNING: Restoring ${filename} will overwrite ALL current data.\n\nA safety backup will be created automatically before restore.\n\nProceed?`)) return;
    try {
      const res = await fetchWithAuth(`${getBackendUrl()}/admin/db/backups/${filename}/restore`, { method: "POST" });
      if (!res.ok) throw new Error((await res.json()).detail || "Restore failed to start");
      showMsg("Restore scheduled in background");
    } catch (err: any) { showMsg(err.message, true); }
  };

  const handleDeleteBackup = async (filename: string) => {
    if (!confirm(`Delete backup file ${filename}?`)) return;
    try {
      const res = await fetchWithAuth(`${getBackendUrl()}/admin/db/backups/${filename}`, { method: "DELETE" });
      if (res.ok) {
        showMsg("Backup deleted");
        loadBackups();
      }
    } catch (err: any) { showMsg(err.message, true); }
  };

  const handleDownloadBackup = async (filename: string) => {
    try {
      const res = await fetchWithAuth(`${getBackendUrl()}/admin/db/backups/${filename}/download`);
      if (res.ok) {
        const blob = await res.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
      }
    } catch (e) { showMsg("Download failed", true); }
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

      // Geo tenancy - Admins are always nationwide
      const isAdminRole = newRole === 'admin';
      updateData.division_access = isAdminRole ? null : (editDivisionAccess || null);
      updateData.district_access = isAdminRole ? null : (editDistrictAccess || null);
      updateData.upazila_access = isAdminRole ? null : (editUpazilaAccess || null);

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
          {['users', 'config', 'instances', 'locations', 'aliases', 'database', 'maintenance', 'audit', 'api'].map(tab => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-4 py-3 font-semibold text-xs sm:text-sm capitalize transition-colors border-b-2 ${
                activeTab === tab ? 'border-emerald-500 text-white' : 'border-transparent text-gray-500 hover:text-gray-300'
              } ${tab === 'maintenance' ? 'text-amber-400!' : ''}`}
            >
              {tab === 'api' ? 'API Usage' : tab === 'audit' ? 'Audit Logs' : tab === 'aliases' ? 'Header Variations' : tab}
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
                    if (!confirm('Run geo cleanup in background?\n\nThis will:\n• Trim whitespace from geo name fields\n• Backfill missing geo IDs\n\nNo records will be deleted. Continue?')) return;
                    setMaintenanceLoading(true);
                    setCleanupResult(null);
                    try {
                      const res = await fetchWithAuth(`${getBackendUrl()}/admin/maintenance/run-cleanup`, { method: 'POST' });
                      const data = await res.json();
                      if (!res.ok) throw new Error(data.detail || 'Cleanup failed');
                      showMsg('Cleanup task started in background.');
                    } catch (err: any) { 
                      showMsg(err.message, true); 
                      setMaintenanceLoading(false);
                    }
                  }}
                  disabled={maintenanceLoading || maintenanceStatus?.cleanup?.status === 'running'}
                  className="w-full py-3 rounded-xl bg-amber-600 hover:bg-amber-500 font-bold disabled:opacity-50 transition-colors"
                >
                  {maintenanceStatus?.cleanup?.status === 'running' 
                    ? 'Processing in Background...' 
                    : '🧹 Run Cleanup (Safe & Optimized)'}
                </button>

                {/* Background Task Progress */}
                {maintenanceStatus?.cleanup?.status === 'running' && (
                  <div className="mt-4 p-4 rounded-xl bg-amber-500/10 border border-amber-500/20 animate-pulse">
                    <p className="text-amber-500 font-bold text-sm mb-1">Task Running...</p>
                    <p className="text-gray-400 text-xs">{maintenanceStatus.cleanup.message}</p>
                  </div>
                )}

                {/* Legacy cleanup result display (updated to prefer maintenanceStatus check) */}
                {maintenanceStatus?.cleanup?.status === 'completed' && (
                  <div className="mt-4 space-y-3 text-xs">
                    <div className="p-3 rounded-lg border bg-emerald-500/10 border-emerald-500/20">
                      <p className="font-bold mb-1 text-emerald-400">✅ Cleanup successful</p>
                      <p className="text-gray-400">{maintenanceStatus.cleanup.message}</p>
                    </div>
                  </div>
                )}

                {maintenanceStatus?.cleanup?.status === 'error' && (
                  <div className="mt-4 p-3 rounded-lg border bg-red-500/10 border-red-500/20">
                    <p className="font-bold mb-1 text-red-400">❌ Cleanup failed</p>
                    <p className="text-gray-400">{maintenanceStatus.cleanup.error}</p>
                  </div>
                )}
              </div>
            </div>

            {/* Repair Geo IDs Panel — fixes wrong non-NULL upazila_ids */}
            <div className="bg-[#121214] border border-amber-600/30 p-6 rounded-2xl">
              <div className="flex items-center gap-2 mb-2">
                <span className="text-xl">🔧</span>
                <h2 className="text-xl font-bold text-amber-300">2b. Repair Geo IDs (Force Fix)</h2>
                <span className="ml-auto text-[10px] uppercase font-bold text-amber-400 border border-amber-500/30 px-2 py-0.5 rounded">Admin Only</span>
              </div>
              <p className="text-xs text-gray-500 mb-1">
                The standard cleanup (2) only fills <strong className="text-white">NULL</strong> geo IDs. This step also fixes records with <strong className="text-amber-300">wrong but non-NULL</strong> IDs — the main cause of missing PDFs in district Checked ZIPs.
              </p>
              <ul className="text-xs text-gray-600 list-disc list-inside mb-4 space-y-0.5">
                <li>Force-re-maps <code className="bg-black/30 px-1 rounded">upazila_id</code>, <code className="bg-black/30 px-1 rounded">district_id</code>, <code className="bg-black/30 px-1 rounded">division_id</code> on ALL tables</li>
                <li>Uses text-name match — safe even if IDs were previously wrong</li>
                <li>Does NOT delete any data. Run Refresh Stats after for counts sync.</li>
              </ul>
              <button
                onClick={async () => {
                  if (!confirm('Force-repair all geo IDs across valid_records, invalid_records, summary_stats, and upload_batches?\n\nThis overwrites stale IDs using canonical name matching. No data deleted.\n\nRecommend running "Refresh Stats" afterwards.\n\nProceed?')) return;
                  setMaintenanceLoading(true);
                  try {
                    const res = await fetchWithAuth(`${getBackendUrl()}/admin/maintenance/repair-geo-ids`, { method: 'POST' });
                    const data = await res.json();
                    if (!res.ok) throw new Error(data.detail || data.error || 'Repair failed');
                    showMsg('Repair task started in background.');
                  } catch (err: any) { 
                    showMsg(err.message, true); 
                    setMaintenanceLoading(false);
                  }
                }}
                disabled={maintenanceLoading || maintenanceStatus?.repair_geo?.status === 'running'}
                className="w-full py-3 rounded-xl bg-amber-700 hover:bg-amber-600 font-bold disabled:opacity-50 transition-colors text-white"
              >
                {maintenanceStatus?.repair_geo?.status === 'running' ? 'Repairing in Background...' : '🔧 Repair All Geo IDs (Overwrite Stale)'}
              </button>

              {/* Background Task Progress */}
              {maintenanceStatus?.repair_geo?.status === 'running' && (
                <div className="mt-4 p-4 rounded-xl bg-amber-500/10 border border-amber-500/20 animate-pulse">
                  <div className="flex justify-between items-center mb-1">
                    <p className="text-amber-500 font-bold text-sm">Repair Running...</p>
                    <span className="text-amber-400 font-bold text-sm">{maintenanceStatus.repair_geo.progress}%</span>
                  </div>
                  <div className="w-full bg-[#1a1a1c] h-2 rounded-full overflow-hidden mb-2">
                    <div 
                      className="bg-amber-500 h-full transition-all duration-500" 
                      style={{ width: `${maintenanceStatus.repair_geo.progress}%` }}
                    ></div>
                  </div>
                  <p className="text-gray-400 text-xs">{maintenanceStatus.repair_geo.message}</p>
                </div>
              )}

              {/* Status Display */}
              {maintenanceStatus?.repair_geo?.status === 'completed' && (
                <div className="mt-4 space-y-3 text-xs">
                  <div className="p-3 rounded-lg border bg-emerald-500/10 border-emerald-500/20">
                    <p className="font-bold mb-1 text-emerald-400">✅ Repair successful</p>
                    <p className="text-gray-400">{maintenanceStatus.repair_geo.message}</p>
                  </div>
                </div>
              )}

              {maintenanceStatus?.repair_geo?.status === 'error' && (
                <div className="mt-4 p-3 rounded-lg border bg-red-500/10 border-red-500/20">
                  <p className="font-bold mb-1 text-red-400">❌ Repair failed</p>
                  <p className="text-gray-400">{maintenanceStatus.repair_geo.error}</p>
                </div>
              )}
            </div>

            {/* Refresh All Stats Panel — recalculates from truth tables */}
            <div className="bg-[#121214] border border-emerald-600/30 p-6 rounded-2xl">
              <div className="flex items-center gap-2 mb-2">
                <span className="text-xl">📊</span>
                <h2 className="text-xl font-bold text-emerald-300">3. Refresh All Stats (Truth Sync)</h2>
              </div>
              <p className="text-xs text-gray-500 mb-1">
                Recalculates <strong className="text-white">every</strong> SummaryStats entry by counting actual rows in <code className="bg-black/30 px-1 rounded">valid_records</code> and <code className="bg-black/30 px-1 rounded">invalid_records</code>.
              </p>
              <ul className="text-xs text-gray-600 list-disc list-inside mb-4 space-y-0.5">
                <li>Fixes <strong className="text-amber-300">ghost entries</strong> — stats showing invalid &gt; 0 when no records exist</li>
                <li>Safe to run at any time. Does NOT delete any data.</li>
                <li>Automatically runs after <strong className="text-white">Repair Geo IDs</strong>, but can be triggered manually.</li>
              </ul>
              <button
                onClick={async () => {
                  if (!confirm('Refresh ALL SummaryStats from truth tables?\n\nThis recounts valid/invalid from actual database records.\nGhost entries (stats with no backing data) will be zeroed.\n\nProceed?')) return;
                  setMaintenanceLoading(true);
                  try {
                    const res = await fetchWithAuth(`${getBackendUrl()}/statistics/refresh-all`, { method: 'POST' });
                    const data = await res.json();
                    if (!res.ok) throw new Error(data.detail || data.error || 'Refresh failed');
                    showMsg(`✅ Refreshed ${data.updated || '?'} entries. ${data.ghost_entries_zeroed || 0} ghost entries zeroed.`);
                  } catch (err: any) { showMsg(err.message, true); }
                  finally { setMaintenanceLoading(false); }
                }}
                disabled={maintenanceLoading}
                className="w-full py-3 rounded-xl bg-emerald-700 hover:bg-emerald-600 font-bold disabled:opacity-50 transition-colors text-white"
              >
                {maintenanceLoading ? 'Refreshing...' : '📊 Refresh All Stats from Truth Tables'}
              </button>
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
                    <p className="text-gray-500 uppercase text-[9px] font-bold">Valid Records</p>
                    <p className="text-emerald-400 font-mono font-bold text-lg">{deleteResult.valid_records_deleted?.toLocaleString()}</p>
                  </div>
                  <div className="p-2 rounded bg-[#1a1a1c] border border-[#2a2a2e]">
                    <p className="text-gray-500 uppercase text-[9px] font-bold">Invalid Records</p>
                    <p className="text-red-400 font-mono font-bold text-lg">{deleteResult.invalid_records_deleted?.toLocaleString()}</p>
                  </div>
                  <div className="p-2 rounded bg-[#1a1a1c] border border-[#2a2a2e]">
                    <p className="text-gray-500 uppercase text-[9px] font-bold">Stats Deleted</p>
                    <p className="text-amber-400 font-mono font-bold text-lg">{deleteResult.summary_stats_rows_deleted?.toLocaleString()}</p>
                  </div>
                  <div className="p-2 rounded bg-[#1a1a1c] border border-[#2a2a2e]">
                    <p className="text-gray-500 uppercase text-[9px] font-bold">Batches Archived</p>
                    <p className="text-blue-400 font-mono font-bold text-lg">{deleteResult.upload_batches_archived?.toLocaleString()}</p>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {/* --- USERS TAB --- */}
        {activeTab === 'users' && (
          <div className="space-y-6">
            <div className="flex items-center justify-between pb-4 border-b border-[#1e1e20]">
              <div>
                <h2 className="text-2xl font-bold text-white">Administrative Users</h2>
                <p className="text-sm text-gray-500 mt-1">Manage Directorate General (DG) Food officers and their geographic permissions.</p>
              </div>
              <button 
                onClick={() => setIsAddUserModalOpen(true)}
                className="flex items-center space-x-2 bg-emerald-600 hover:bg-emerald-500 text-white px-5 py-2.5 rounded-xl font-bold transition-all shadow-lg shadow-emerald-900/20"
              >
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
                </svg>
                <span>Add Officer</span>
              </button>
            </div>

            <div className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden shadow-xl">
              <div className="overflow-x-auto">
                <table className="w-full text-left">
                  <thead>
                    <tr className="bg-[#1a1a1c] border-b border-[#1e1e20]">
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">Officer / API Key</th>
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">Designation / Scope</th>
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase tracking-wider text-center">API Usage</th>
                      <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase tracking-wider text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-[#1e1e20]">
                    {users.map((u) => {
                      const getDesignation = () => {
                        if (u.role === 'admin') return { label: 'ADMINISTRATOR', color: 'bg-indigo-500/10 text-indigo-400 border-indigo-500/20' };
                        if (u.upazila_access) return { label: 'UCF (Upazila)', color: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' };
                        if (u.district_access) return { label: 'DCF (District)', color: 'bg-blue-500/10 text-blue-400 border-blue-500/20' };
                        if (u.division_access) return { label: 'RCF (Region)', color: 'bg-amber-500/10 text-amber-400 border-amber-500/20' };
                        return { label: u.role.toUpperCase(), color: 'bg-gray-500/10 text-gray-400 border-gray-500/20' };
                      };
                      const des = getDesignation();
                      
                      return (
                        <tr key={u.id} className="hover:bg-[#161618] group transition-colors">
                          <td className="px-6 py-5">
                            <div className="flex flex-col">
                              <span className="font-bold text-gray-200">{u.username}</span>
                              {u.api_key ? (
                                <div className="flex items-center space-x-2 mt-1.5 animate-in fade-in duration-300">
                                  <span className="text-[10px] text-emerald-500 font-mono bg-emerald-500/5 px-2 py-0.5 rounded border border-emerald-500/10">
                                    {u.api_key.substring(0, 8)}...{u.api_key.substring(u.api_key.length - 4)}
                                  </span>
                                  <button onClick={() => { navigator.clipboard.writeText(u.api_key); showMsg("Copied to clipboard"); }} className="text-[10px] text-gray-500 hover:text-white transition-colors">Copy</button>
                                </div>
                              ) : (
                                <span className="text-[10px] text-gray-600 italic mt-1.5">Cloud Sync Disabled (No Key)</span>
                              )}
                            </div>
                          </td>
                          <td className="px-6 py-5">
                            <div className="flex flex-col space-y-2">
                              <span className={`w-fit px-2 py-0.5 rounded border text-[10px] font-black uppercase tracking-widest ${des.color}`}>
                                {des.label}
                              </span>
                              <div className="flex items-center space-x-1.5 mt-2">
                                <svg className="w-3.5 h-3.5 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" /><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" /></svg>
                                {u.role === 'admin' ? (
                                  <span className="text-[11px] font-bold text-indigo-400 uppercase tracking-tight">Nationwide Access (Global)</span>
                                ) : (
                                  <span className="text-[11px] text-gray-300 font-medium">
                                    {[u.division_access, u.district_access, u.upazila_access].filter(Boolean).join(' > ') || 'No Scope Assigned'}
                                  </span>
                                )}
                              </div>
                            </div>
                          </td>
                          <td className="px-6 py-5">
                            <div className="flex flex-col justify-center items-center">
                              <span className="text-sm font-mono font-bold text-gray-200">{u.api_usage_count?.toLocaleString() || 0}</span>
                              <span className="text-[9px] text-gray-500 uppercase tracking-tighter">API Calls</span>
                              {u.api_key_last_used && (
                                <span className="text-[9px] text-emerald-500/60 mt-1 uppercase font-bold">Active: {new Date(u.api_key_last_used).toLocaleDateString()}</span>
                              )}
                            </div>
                          </td>
                          <td className="px-6 py-5 text-right">
                            <div className="flex items-center justify-end space-x-3">
                              <button
                                onClick={() => handleGenerateApiKey(u.id)}
                                className="text-emerald-500 hover:text-emerald-400 transition-colors text-[10px] font-bold border border-emerald-500/20 px-2.5 py-1.5 rounded-lg bg-emerald-500/5 hover:bg-emerald-500/10"
                                title="Regenerate API Key"
                              >
                                {u.api_key ? 'Regen' : 'Generate'}
                              </button>
                              <button 
                                onClick={() => {
                                  setEditingUser(u);
                                  setNewRole(u.role);
                                  setEditApiRateLimit(u.api_rate_limit || "");
                                  setEditApiTotalLimit(u.api_total_limit !== null && u.api_total_limit !== undefined ? u.api_total_limit.toString() : "");
                                  setEditApiIpWhitelist(u.api_ip_whitelist || "");
                                  setEditDivisionAccess(u.division_access || null);
                                  setEditDistrictAccess(u.district_access || null);
                                  setEditUpazilaAccess(u.upazila_access || null);
                                }} 
                                className="bg-[#1a1a1c] border border-[#2a2a2e] text-gray-400 hover:text-white hover:border-gray-500 transition-all text-xs font-bold px-3 py-1.5 rounded-lg"
                              >
                                Edit
                              </button>
                              <button onClick={() => handleDeleteUser(u.id)} className="text-gray-500 hover:text-red-500 transition-colors text-xs font-bold p-1">Del</button>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>

            {/* --- MODAL: ADD USER --- */}
            {isAddUserModalOpen && (
              <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-300">
                <div className="bg-[#121214] border border-[#1e1e20] w-full max-w-lg rounded-3xl overflow-hidden shadow-2xl animate-in zoom-in-95 duration-200">
                  <div className="p-6 border-b border-[#1e1e20] flex justify-between items-center bg-[#1a1a1c]/50">
                    <h2 className="text-xl font-bold text-white flex items-center space-x-2">
                       <span className="p-2 rounded-xl bg-emerald-500/10 text-emerald-500">
                        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M18 9v3m0 0v3m0-3h3m-3 0h-3m-2-5a4 4 0 11-8 0 4 4 0 018 0zM3 20a6 6 0 0112 0v1H3v-1z" /></svg>
                       </span>
                       <span>Add New Officer</span>
                    </h2>
                    <button onClick={() => setIsAddUserModalOpen(false)} className="text-gray-500 hover:text-white transition-colors">
                      <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
                    </button>
                  </div>
                  <form onSubmit={handleCreateUser} className="p-8 space-y-6">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Username</label>
                        <input type="text" value={newUsername} onChange={(e) => setNewUsername(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 outline-none transition-all" placeholder="Login ID" required />
                      </div>
                      <div>
                        <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Password</label>
                        <input type="password" value={newPassword} onChange={(e) => setNewPassword(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 outline-none transition-all" placeholder="••••••••" required />
                      </div>
                    </div>
                    
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">System Role</label>
                        <select value={newRole} onChange={(e) => setNewRole(e.target.value)} className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:border-emerald-500 outline-none transition-all">
                          <option value="viewer">Viewer (Read Only)</option>
                          <option value="uploader">Officer (Data Ops)</option>
                          <option value="admin">Admin (Full System)</option>
                        </select>
                      </div>
                      <div className="flex items-end">
                        <div className="w-full p-4 rounded-xl bg-indigo-500/5 border border-indigo-500/10 text-[10px] text-gray-400">
                          {newRole === 'admin' ? 'Admins always have Nationwide access.' : 'Set Region/Upazila access below.'}
                        </div>
                      </div>
                    </div>

                    {newRole !== 'admin' && (
                       <LocationSelector 
                          upazilas={upazilas}
                          selectedDivision={newDivisionAccess}
                          selectedDistrict={newDistrictAccess}
                          selectedUpazila={newUpazilaAccess}
                          onSelect={({ division, district, upazila }) => {
                            setNewDivisionAccess(division);
                            setNewDistrictAccess(district);
                            setNewUpazilaAccess(upazila);
                          }}
                       />
                    )}

                    <div className="pt-4 flex space-x-3">
                      <button type="button" onClick={() => setIsAddUserModalOpen(false)} className="flex-1 py-3 rounded-xl border border-[#2a2a2e] text-gray-400 hover:text-white transition-colors font-bold">Cancel</button>
                      <button type="submit" disabled={actionLoading} className="flex-2 w-full py-3 rounded-xl bg-emerald-600 hover:bg-emerald-500 shadow-lg shadow-emerald-900/40 text-white font-bold disabled:opacity-50 transition-all">
                        {actionLoading ? 'Creating...' : 'Create Account'}
                      </button>
                    </div>
                  </form>
                </div>
              </div>
            )}

            {/* --- MODAL: EDIT USER --- */}
            {editingUser && (
              <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-300">
                <div className="bg-[#121214] border border-[#1e1e20] w-full max-w-2xl rounded-3xl overflow-hidden shadow-2xl animate-in zoom-in-95 duration-200">
                  <div className="p-6 border-b border-[#1e1e20] flex justify-between items-center bg-[#1a1a1c]/50">
                    <h2 className="text-xl font-bold text-white flex items-center space-x-3">
                       <span className="p-2 rounded-xl bg-indigo-500/10 text-indigo-500">
                        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" /></svg>
                       </span>
                       <div className="flex flex-col">
                        <span>Edit {editingUser.username}</span>
                        <span className="text-[10px] text-gray-500 uppercase tracking-widest font-normal">Officer Account Management</span>
                       </div>
                    </h2>
                    <button onClick={() => setEditingUser(null)} className="text-gray-500 hover:text-white transition-colors">
                      <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
                    </button>
                  </div>
                  <div className="grid grid-cols-5 h-full">
                    {/* Sidebar inside modal */}
                    <div className="col-span-2 bg-[#1a1a1c]/30 border-r border-[#1e1e20] p-6 space-y-6">
                      <div>
                        <label className="block text-[10px] font-black text-gray-500 uppercase tracking-widest mb-3">Core Credentials</label>
                        <div className="space-y-4">
                          <div>
                            <span className="text-xs text-gray-500">System Role</span>
                            <select value={newRole} onChange={(e) => setNewRole(e.target.value)} className="w-full mt-1 px-3 py-2 rounded-lg bg-[#121214] border border-[#2a2a2e] text-white text-sm outline-none focus:border-indigo-500">
                              <option value="viewer">Viewer</option>
                              <option value="uploader">Officer</option>
                              <option value="admin">Administrator</option>
                            </select>
                          </div>
                          <div>
                            <span className="text-xs text-gray-500">Reset Password</span>
                            <input type="password" value={newPassword} onChange={(e) => setNewPassword(e.target.value)} placeholder="New Password (optional)" className="w-full mt-1 px-3 py-2 rounded-lg bg-[#121214] border border-[#2a2a2e] text-white text-sm outline-none focus:border-indigo-500" />
                          </div>
                        </div>
                      </div>

                      <div className="pt-4 border-t border-[#1e1e20]">
                         <label className="block text-[10px] font-black text-gray-500 uppercase tracking-widest mb-3 text-indigo-400">Security & API</label>
                         <div className="space-y-3">
                            <div className="flex flex-col">
                              <span className="text-[10px] text-gray-500">Rate Limit (/min)</span>
                              <input type="number" value={editApiRateLimit} onChange={(e) => setEditApiRateLimit(parseInt(e.target.value) || 0)} className="bg-[#121214] border border-[#2a2a2e] text-sm rounded px-2 py-1.5 focus:border-indigo-500" />
                            </div>
                            <div className="flex flex-col">
                              <span className="text-[10px] text-gray-500">IP Whitelist</span>
                              <input type="text" value={editApiIpWhitelist} onChange={(e) => setEditApiIpWhitelist(e.target.value)} placeholder="Comma separated IPs" className="bg-[#121214] border border-[#2a2a2e] text-[10px] rounded px-2 py-1.5 focus:border-indigo-500" />
                            </div>
                         </div>
                      </div>
                    </div>

                    {/* Main Content inside modal */}
                    <div className="col-span-3 p-8 space-y-8">
                      <div className="p-5 rounded-2xl bg-indigo-500/5 border border-indigo-500/10">
                        <h4 className="text-sm font-bold text-gray-200 mb-2">Hierarchy Status</h4>
                        <p className="text-xs text-gray-500 leading-relaxed">
                          Assigned officers can only view and process data within their authorized territory. 
                          Changes to geography access will be logged in the system audit trail.
                        </p>
                      </div>

                      <LocationSelector 
                        upazilas={upazilas}
                        selectedDivision={editDivisionAccess}
                        selectedDistrict={editDistrictAccess}
                        selectedUpazila={editUpazilaAccess}
                        onSelect={({ division, district, upazila }) => {
                          setEditDivisionAccess(division);
                          setEditDistrictAccess(district);
                          setEditUpazilaAccess(upazila);
                        }}
                        disabled={newRole === 'admin'}
                      />

                      <div className="pt-8 flex space-x-3">
                        <button onClick={() => setEditingUser(null)} className="px-6 py-3 rounded-xl border border-[#2a2a2e] text-gray-400 font-bold flex-1">Discard</button>
                        <button 
                          onClick={() => handleUpdateUser(editingUser.id)}
                          disabled={actionLoading}
                          className="px-6 py-3 rounded-xl bg-indigo-600 hover:bg-indigo-500 text-white font-bold flex-[2] shadow-lg shadow-indigo-900/40 transition-all disabled:opacity-50"
                        >
                          {actionLoading ? 'Saving...' : 'Save Account Changes'}
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
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

        {/* --- LOCATIONS TAB --- */}
        {activeTab === 'locations' && (
          <LocationsTab />
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

        {activeTab === 'aliases' && (
          <HeaderAliasTab />
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
                  <p className="text-sm text-gray-400">All maintenance tasks run in the background. Restoring will overwrite existing data. Gzip compression (30-day retention) is enabled.</p>
                </div>
              </div>
            </div>

            {/* Task Tracking Cards */}
            {dbTasks.length > 0 && (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {dbTasks.map(task => (
                  <div key={task.id} className={`p-4 rounded-xl border ${task.status === 'running' ? 'bg-blue-500/5 border-blue-500/30 animate-pulse' : task.status === 'error' ? 'bg-red-500/5 border-red-500/30' : 'bg-[#121214] border-[#1e1e20]'}`}>
                    <div className="flex justify-between items-start mb-2">
                      <span className="text-[10px] font-bold uppercase text-gray-500">{task.task_name?.replace('_', ' ')}</span>
                      <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded uppercase ${task.status === 'running' ? 'bg-blue-500 text-white' : task.status === 'completed' ? 'bg-emerald-500 text-white' : 'bg-red-500 text-white'}`}>
                        {task.status}
                      </span>
                    </div>
                    <p className="text-xs font-semibold mb-2 truncate" title={task.message}>{task.message}</p>
                    <div className="w-full bg-gray-800 h-1 rounded-full overflow-hidden">
                      <div className={`h-full transition-all duration-500 ${task.status === 'error' ? 'bg-red-500' : 'bg-emerald-500'}`} style={{ width: `${task.progress || 0}%` }}></div>
                    </div>
                  </div>
                ))}
              </div>
            )}

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
              <div className="lg:col-span-1 space-y-6">
                <div className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl">
                  <h2 className="text-xl font-black mb-4 flex items-center gap-2">
                    <span className="p-1.5 bg-emerald-500/20 rounded-lg text-emerald-500">
                      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" /></svg>
                    </span>
                    Manual Backup
                  </h2>
                  <p className="text-xs text-gray-500 mb-6 leading-relaxed">
                    Triggers a <code>pg_dump</code> piped to <code>gzip</code> in the background. Files are stored in the server's persistent backup volume.
                  </p>
                  <button
                    onClick={handleExportDB}
                    disabled={isExporting || dbTasks.some(t => t.status === 'running')}
                    className="w-full py-3 rounded-xl bg-emerald-600 hover:bg-emerald-500 font-bold text-white transition-all disabled:opacity-50"
                  >
                    {isExporting ? 'Starting...' : 'Run Backup Task'}
                  </button>
                </div>

                <div className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl">
                  <h2 className="text-xl font-black mb-4 flex items-center gap-2">
                    <span className="p-1.5 bg-purple-500/20 rounded-lg text-purple-500">
                      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" /></svg>
                    </span>
                    Upload SQL
                  </h2>
                  <p className="text-xs text-gray-500 mb-6 font-medium">Upload a <code>.sql</code> or <code>.sql.gz</code> file to the history list.</p>
                  <div className="space-y-3">
                    <div className="relative group bg-[#1a1a1c] border border-[#2a2a2e] border-dashed rounded-xl p-4 text-center hover:border-purple-500/50 transition-colors">
                      <input type="file" accept=".sql,.gz" onChange={(e) => setSelectedSqlFile(e.target.files?.[0] || null)} className="absolute inset-0 w-full h-full opacity-0 cursor-pointer z-10" />
                      <span className="text-xs text-gray-500 truncate block">
                        {selectedSqlFile ? selectedSqlFile.name : "Select or drag file"}
                      </span>
                    </div>
                    <button
                      onClick={handleImportDB}
                      disabled={isImporting || !selectedSqlFile}
                      className="w-full py-3 rounded-xl bg-purple-600 hover:bg-purple-500 font-bold text-white transition-all disabled:opacity-50"
                    >
                      {isImporting ? 'Uploading...' : 'Upload to History'}
                    </button>
                  </div>
                </div>
              </div>

              <div className="lg:col-span-2">
                <div className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden flex flex-col h-full">
                  <div className="p-5 border-b border-[#1e1e20] flex justify-between items-center bg-[#1a1a1c]">
                    <h2 className="text-lg font-bold">Backup History</h2>
                    <span className="text-[10px] font-bold text-gray-500 uppercase tracking-widest">{backups.length} Files Available</span>
                  </div>
                  <div className="overflow-x-auto overflow-y-auto max-h-[500px] custom-scrollbar">
                    <table className="w-full text-left">
                      <thead>
                        <tr className="bg-[#1a1a1c] border-b border-[#1e1e20] text-xs font-semibold text-gray-500 uppercase">
                          <th className="px-6 py-3">Filename</th>
                          <th className="px-6 py-3">Date</th>
                          <th className="px-6 py-3">Size</th>
                          <th className="px-6 py-3 text-right">Actions</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-[#1e1e20]">
                        {backups.map((f) => (
                          <tr key={f.filename} className="hover:bg-[#161618] group text-sm">
                            <td className="px-6 py-4 font-mono text-[11px] text-gray-300 truncate max-w-[200px]" title={f.filename}>{f.filename}</td>
                            <td className="px-6 py-4 text-gray-500 whitespace-nowrap text-xs">{new Date(f.created_at).toLocaleString()}</td>
                            <td className="px-6 py-4 text-gray-500 text-xs">{(f.size / (1024 * 1024)).toFixed(2)} MB</td>
                            <td className="px-6 py-4 text-right">
                              <div className="flex justify-end space-x-2">
                                <button onClick={() => handleDownloadBackup(f.filename)} className="p-1.5 text-blue-500 hover:bg-blue-500/10 rounded transition-colors" title="Download">
                                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" /></svg>
                                </button>
                                <button onClick={() => handleRestoreBackup(f.filename)} className="px-2 py-1 text-[10px] font-bold bg-amber-500/10 text-amber-500 border border-amber-500/20 rounded hover:bg-amber-500 hover:text-white transition-colors">
                                  Restore
                                </button>
                                <button onClick={() => handleDeleteBackup(f.filename)} className="p-1.5 text-gray-500 hover:text-red-500 hover:bg-red-500/10 rounded transition-colors">
                                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                                </button>
                              </div>
                            </td>
                          </tr>
                        ))}
                        {backups.length === 0 && (
                          <tr><td colSpan={4} className="p-12 text-center text-gray-600 italic text-sm">No backup files found in storage.</td></tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>
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
