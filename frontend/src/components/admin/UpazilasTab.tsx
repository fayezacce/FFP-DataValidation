/**
 * FFP Data Validation Platform - Admin Upazilas Tab
 */
import React from 'react';

interface Upazila {
  id: number;
  division_name: string;
  district_name: string;
  name: string;
  quota?: number;
}

interface UpazilasTabProps {
  upazilas: Upazila[];
  upzDiv: string;
  upzDist: string;
  upzName: string;
  renameLevel: string;
  renameParentName: string;
  renameOldName: string;
  renameNewName: string;
  actionLoading: boolean;
  editingUpzId: number | null;
  editingUpzName: string;
  editingUpzQuota: string | number;
  setUpzDiv: (v: string) => void;
  setUpzDist: (v: string) => void;
  setUpzName: (v: string) => void;
  setRenameLevel: (v: string) => void;
  setRenameParentName: (v: string) => void;
  setRenameOldName: (v: string) => void;
  setRenameNewName: (v: string) => void;
  setEditingUpzId: (v: number | null) => void;
  setEditingUpzName: (v: string) => void;
  setEditingUpzQuota: (v: string | number) => void;
  handleCreateUpz: (e: React.FormEvent) => void;
  handleRenameLocation: (e: React.FormEvent) => void;
  handleSaveUpazilaInline: (u: Upazila) => void;
  handleDeleteUpz: (id: number) => void;
}

const UpazilasTab: React.FC<UpazilasTabProps> = ({
  upazilas,
  upzDiv,
  upzDist,
  upzName,
  renameLevel,
  renameParentName,
  renameOldName,
  renameNewName,
  actionLoading,
  editingUpzId,
  editingUpzName,
  editingUpzQuota,
  setUpzDiv,
  setUpzDist,
  setUpzName,
  setRenameLevel,
  setRenameParentName,
  setRenameOldName,
  setRenameNewName,
  setEditingUpzId,
  setEditingUpzName,
  setEditingUpzQuota,
  handleCreateUpz,
  handleRenameLocation,
  handleSaveUpazilaInline,
  handleDeleteUpz
}) => {
  return (
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
                    <td colSpan={5} className="p-8 text-center text-gray-500">No database upazilas added. System will use default hardcoded list.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
};

export default UpazilasTab;
