"use client";

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { fetchWithAuth, getBackendUrl, isAdmin, getUser } from "@/lib/auth";

export default function AdminPage() {
  const [users, setUsers] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newRole, setNewRole] = useState("viewer");
  const [creating, setCreating] = useState(false);
  const router = useRouter();

  useEffect(() => {
    const user = getUser();
    if (!user || user.role !== "admin") {
      router.push("/");
      return;
    }
    loadUsers();
  }, [router]);

  const loadUsers = async () => {
    try {
      const response = await fetchWithAuth(`${getBackendUrl()}/auth/users`);
      if (!response.ok) throw new Error("Failed to load users");
      const data = await response.json();
      setUsers(data);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleCreateUser = async (e: React.FormEvent) => {
    e.preventDefault();
    setCreating(true);
    setError("");

    try {
      const response = await fetchWithAuth(`${getBackendUrl()}/auth/users`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          username: newUsername,
          password: newPassword,
          role: newRole,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || "Failed to create user");
      }

      setNewUsername("");
      setNewPassword("");
      await loadUsers();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setCreating(false);
    }
  };

  const handleDeleteUser = async (id: number) => {
    if (!confirm("Are you sure you want to delete this user?")) return;

    try {
      const response = await fetchWithAuth(`${getBackendUrl()}/auth/users/${id}`, {
        method: "DELETE",
      });
      if (!response.ok) throw new Error("Failed to delete user");
      await loadUsers();
    } catch (err: any) {
      setError(err.message);
    }
  };

  if (loading) return (
    <div className="min-h-screen bg-[#0a0a0b] flex items-center justify-center">
      <div className="w-8 h-8 border-4 border-emerald-500/20 border-t-emerald-500 rounded-full animate-spin"></div>
    </div>
  );

  return (
    <div className="min-h-screen bg-[#0a0a0b] text-white p-8">
      <div className="max-w-6xl mx-auto">
        <header className="mb-12">
          <h1 className="text-4xl font-black tracking-tight mb-2">User Management</h1>
          <p className="text-gray-500">System Administration & Access Control</p>
        </header>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* Create User Section */}
          <div className="lg:col-span-1">
            <div className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl">
              <h2 className="text-xl font-bold mb-6">Create New User</h2>
              <form onSubmit={handleCreateUser} className="space-y-4">
                <div>
                  <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Username</label>
                  <input
                    type="text"
                    value={newUsername}
                    onChange={(e) => setNewUsername(e.target.value)}
                    className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:outline-none focus:border-emerald-500 transition-colors"
                    required
                  />
                </div>
                <div>
                  <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Password</label>
                  <input
                    type="password"
                    value={newPassword}
                    onChange={(e) => setNewPassword(e.target.value)}
                    className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:outline-none focus:border-emerald-500 transition-colors"
                    required
                  />
                </div>
                <div>
                  <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Role</label>
                  <select
                    value={newRole}
                    onChange={(e) => setNewRole(e.target.value)}
                    className="w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border border-[#2a2a2e] text-white focus:outline-none focus:border-emerald-500 transition-colors appearance-none"
                  >
                    <option value="viewer">Viewer</option>
                    <option value="uploader">Uploader</option>
                    <option value="admin">Admin</option>
                  </select>
                </div>
                <button
                  type="submit"
                  disabled={creating}
                  className="w-full py-3 rounded-xl bg-emerald-600 hover:bg-emerald-500 font-bold transition-all disabled:opacity-50"
                >
                  {creating ? "Creating..." : "Add User"}
                </button>
                {error && <p className="text-red-500 text-sm mt-2">{error}</p>}
              </form>
            </div>
          </div>

          {/* User List Section */}
          <div className="lg:col-span-2">
            <div className="bg-[#121214] border border-[#1e1e20] rounded-2xl overflow-hidden">
              <table className="w-full text-left">
                <thead>
                  <tr className="bg-[#1a1a1c] border-b border-[#1e1e20]">
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">User</th>
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase">Role</th>
                    <th className="px-6 py-4 text-xs font-semibold text-gray-500 uppercase text-right">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[#1e1e20]">
                  {users.map((user) => (
                    <tr key={user.id} className="hover:bg-[#161618] transition-colors group">
                      <td className="px-6 py-4">
                        <div className="flex items-center">
                          <div className="w-8 h-8 rounded-full bg-emerald-500/10 flex items-center justify-center text-emerald-500 font-bold text-xs mr-3">
                            {user.username[0].toUpperCase()}
                          </div>
                          <span className="font-medium">{user.username}</span>
                        </div>
                      </td>
                      <td className="px-6 py-4">
                        <span className={`px-2 py-1 rounded-md text-[10px] font-bold uppercase tracking-wider ${
                          user.role === 'admin' ? 'bg-purple-500/10 text-purple-400' :
                          user.role === 'uploader' ? 'bg-blue-500/10 text-blue-400' :
                          'bg-gray-500/10 text-gray-400'
                        }`}>
                          {user.role}
                        </span>
                      </td>
                      <td className="px-6 py-4 text-right">
                        <button
                          onClick={() => handleDeleteUser(user.id)}
                          className="text-gray-500 hover:text-red-500 transition-colors opacity-0 group-hover:opacity-100"
                        >
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {users.length === 0 && (
                <div className="p-12 text-center text-gray-500">No users found.</div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
