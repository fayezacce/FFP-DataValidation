/**
 * FFP Data Validation Platform - Admin Config Tab
 */
import React from 'react';

interface TzWhitelistItem {
  nid: string;
  added_by: string;
}

interface ConfigTabProps {
  rateLimit: string;
  trailingZeroLimit: string;
  tzWhitelist: TzWhitelistItem[];
  newTzNid: string;
  actionLoading: boolean;
  setRateLimit: (v: string) => void;
  setTrailingZeroLimit: (v: string) => void;
  setNewTzNid: (v: string) => void;
  handleUpdateConfig: (e: React.FormEvent) => void;
  handleAddTzWhitelist: (e: React.FormEvent) => void;
  handleRemoveTzWhitelist: (nid: string) => void;
}

const ConfigTab: React.FC<ConfigTabProps> = ({
  rateLimit,
  trailingZeroLimit,
  tzWhitelist,
  newTzNid,
  actionLoading,
  setRateLimit,
  setTrailingZeroLimit,
  setNewTzNid,
  handleUpdateConfig,
  handleAddTzWhitelist,
  handleRemoveTzWhitelist
}) => {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
      {/* System Config */}
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

      {/* Trailing Zero Whitelist */}
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
  );
};

export default ConfigTab;
