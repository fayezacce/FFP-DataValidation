/**
 * FFP Data Validation Platform - Admin API Logs Tab
 */
import React from 'react';

interface ApiLogsTabProps {
  apiLogs: any[];
}

const ApiLogsTab: React.FC<ApiLogsTabProps> = ({ apiLogs }) => {
  return (
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
  );
};

export default ApiLogsTab;
