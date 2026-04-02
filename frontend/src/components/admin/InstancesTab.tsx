/**
 * FFP Data Validation Platform - Admin Instances Tab
 */
import React from 'react';

interface Instance {
  id: number;
  name: string;
  url: string;
  is_active: boolean;
  last_synced_at: string | null;
}

interface InstancesTabProps {
  instances: Instance[];
  instName: string;
  instUrl: string;
  instApiKey: string;
  actionLoading: boolean;
  testingInstanceId: number | null;
  setInstName: (v: string) => void;
  setInstUrl: (v: string) => void;
  setInstApiKey: (v: string) => void;
  handleCreateInstance: (e: React.FormEvent) => void;
  handleTestInstance: (id: number) => void;
  handleSyncInstance: (id: number) => void;
  handleDeleteInstance: (id: number) => void;
}

const InstancesTab: React.FC<InstancesTabProps> = ({
  instances,
  instName,
  instUrl,
  instApiKey,
  actionLoading,
  testingInstanceId,
  setInstName,
  setInstUrl,
  setInstApiKey,
  handleCreateInstance,
  handleTestInstance,
  handleSyncInstance,
  handleDeleteInstance
}) => {
  return (
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
  );
};

export default InstancesTab;
