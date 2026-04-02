/**
 * FFP Data Validation Platform - Admin Audit Logs Tab
 */
import React from 'react';

interface AuditLogsTabProps {
  auditLogs: any[];
  setSelectedAuditLog: (log: any) => void;
  formatAuditSummary: (log: any) => string;
}

const AuditLogsTab: React.FC<AuditLogsTabProps> = ({ auditLogs, setSelectedAuditLog, formatAuditSummary }) => {
  return (
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
  );
};

export default AuditLogsTab;
