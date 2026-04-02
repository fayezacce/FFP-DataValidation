/**
 * FFP Data Validation Platform - Admin Maintenance Tab
 */
import React from 'react';

interface MaintenanceTabProps {
  maintenanceLoading: boolean;
  maintenanceStatus: any;
  orphanPreview: any;
  cleanupResult: any;
  deleteResult: any;
  onScanOrphans: () => void;
  onRunCleanup: () => void;
  onDeleteUnresolved: () => void;
}

const MaintenanceTab: React.FC<MaintenanceTabProps> = ({
  maintenanceLoading,
  maintenanceStatus,
  orphanPreview,
  cleanupResult,
  deleteResult,
  onScanOrphans,
  onRunCleanup,
  onDeleteUnresolved
}) => {
  return (
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
            onClick={onScanOrphans}
            disabled={maintenanceLoading}
            className="w-full py-3 rounded-xl bg-blue-600 hover:bg-blue-500 font-bold disabled:opacity-50 transition-colors text-white"
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
            onClick={onRunCleanup}
            disabled={maintenanceLoading || maintenanceStatus?.cleanup?.status === 'running'}
            className="w-full py-3 rounded-xl bg-amber-600 hover:bg-amber-500 font-bold disabled:opacity-50 transition-colors text-white"
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
            onClick={onDeleteUnresolved}
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
  );
};

export default MaintenanceTab;
