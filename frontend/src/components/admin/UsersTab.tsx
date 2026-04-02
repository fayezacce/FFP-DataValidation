/**
 * FFP Data Validation Platform - Admin Users Tab
 */
import React from 'react';
import { User } from '@/types/ffp';

interface UsersTabProps {
  users: User[];
  editingUser: User | null;
  newUsername: string;
  newPassword: string;
  newRole: string;
  editApiRateLimit: string | number;
  editApiTotalLimit: string;
  editApiIpWhitelist: string;
  actionLoading: boolean;
  setNewUsername: (v: string) => void;
  setNewPassword: (v: string) => void;
  setNewRole: (v: string) => void;
  setEditingUser: (v: User | null) => void;
  setEditApiRateLimit: (v: string | number) => void;
  setEditApiTotalLimit: (v: string) => void;
  setEditApiIpWhitelist: (v: string) => void;
  handleCreateUser: (e: React.FormEvent) => void;
  handleUpdateUser: (id: number) => void;
  handleDeleteUser: (id: number) => void;
  handleGenerateApiKey: (id: number) => void;
  showMsg: (msg: string, isError?: boolean) => void;
}

const UsersTab: React.FC<UsersTabProps> = ({
  users,
  editingUser,
  newUsername,
  newPassword,
  newRole,
  editApiRateLimit,
  editApiTotalLimit,
  editApiIpWhitelist,
  actionLoading,
  setNewUsername,
  setNewPassword,
  setNewRole,
  setEditingUser,
  setEditApiRateLimit,
  setEditApiTotalLimit,
  setEditApiIpWhitelist,
  handleCreateUser,
  handleUpdateUser,
  handleDeleteUser,
  handleGenerateApiKey,
  showMsg
}) => {
  return (
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
                              onClick={() => { navigator.clipboard.writeText(u.api_key || ""); showMsg("Copied to clipboard"); }}
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
                            {u.api_total_limit !== null && u.api_total_limit !== undefined && (
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
                            setEditApiTotalLimit(u.api_total_limit !== null && u.api_total_limit !== undefined ? u.api_total_limit.toString() : "");
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
  );
};

export default UsersTab;
