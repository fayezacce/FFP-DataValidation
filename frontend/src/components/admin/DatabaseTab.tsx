/**
 * FFP Data Validation Platform - Admin Database Tab
 */
import React from 'react';

interface DatabaseTabProps {
  isExporting: boolean;
  isImporting: boolean;
  selectedSqlFile: File | null;
  handleExportDB: () => void;
  handleImportDB: (e: React.FormEvent) => void;
  setSelectedSqlFile: (file: File | null) => void;
}

const DatabaseTab: React.FC<DatabaseTabProps> = ({
  isExporting,
  isImporting,
  selectedSqlFile,
  handleExportDB,
  handleImportDB,
  setSelectedSqlFile
}) => {
  return (
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
  );
};

export default DatabaseTab;
