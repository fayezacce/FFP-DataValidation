"use client";
import React, { useState } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
  PieChart, Pie, ComposedChart, Legend,
} from 'recharts';
import { Target, CheckCircle2, AlertTriangle, TrendingDown, MapPin, Shield, Zap, BarChart3 } from 'lucide-react';

interface DivNode {
  name: string;
  total: number;
  valid: number;
  invalid: number;
  quota: number;
  districts: Record<string, {
    name: string;
    invalid: number;
    valid: number;
    quota: number;
    total: number;
    upazilas: any[];
  }>;
}

interface AnalyticsChartsProps {
  hierarchy: DivNode[];
}

// ── Color Palette ─────────────────────────────────────────────────────────
const PALETTE = {
  emerald: '#34d399',
  emeraldDark: '#059669',
  red: '#f87171',
  redDark: '#dc2626',
  amber: '#fbbf24',
  amberDark: '#d97706',
  cyan: '#22d3ee',
  cyanDark: '#0891b2',
  blue: '#60a5fa',
  blueDark: '#2563eb',
  slate: '#475569',
  slateDark: '#1e293b',
  white: '#f8fafc',
};

const GRADE_COLORS: Record<string, string> = {
  'A+': '#10b981', 'A': '#34d399', 'B': '#60a5fa', 'C': '#fbbf24', 'D': '#f97316', 'F': '#ef4444',
};

function getGrade(errorRate: number): string {
  if (errorRate <= 1) return 'A+';
  if (errorRate <= 3) return 'A';
  if (errorRate <= 5) return 'B';
  if (errorRate <= 10) return 'C';
  if (errorRate <= 20) return 'D';
  return 'F';
}

// ── Custom Tooltip ────────────────────────────────────────────────────────
const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-[#0f172a] border border-slate-600/50 rounded-xl px-4 py-3 shadow-2xl text-sm">
      <p className="font-bold text-white mb-2">{label}</p>
      {payload.map((p: any, i: number) => (
        <div key={i} className="flex items-center gap-2 py-0.5">
          <span className="w-2.5 h-2.5 rounded-full" style={{ background: p.color || p.fill }} />
          <span className="text-slate-400">{p.name}:</span>
          <span className="font-bold text-white">
            {typeof p.value === 'number'
              ? p.name?.includes('%') || p.name?.includes('Rate')
                ? `${p.value.toFixed(1)}%`
                : p.value.toLocaleString('en-IN')
              : p.value}
          </span>
        </div>
      ))}
    </div>
  );
};

const AnalyticsCharts: React.FC<AnalyticsChartsProps> = ({ hierarchy }) => {
  const [activeTab, setActiveTab] = useState<'overview' | 'problems' | 'coverage'>('overview');

  // ── Flatten and compute ─────────────────────────────────────────────────
  const allDistricts: any[] = [];
  const allUpazilas: any[] = [];
  let globalValid = 0, globalInvalid = 0, globalQuota = 0, globalTotal = 0;
  let startedUpazilas = 0, completedUpazilas = 0, notStartedUpazilas = 0;

  hierarchy.forEach(div => {
    globalValid += div.valid;
    globalInvalid += div.invalid;
    globalQuota += (div.quota || 0);
    globalTotal += div.total;

    Object.values(div.districts).forEach(dist => {
      const distTotal = dist.valid + dist.invalid;
      const distErrorRate = distTotal > 0 ? (dist.invalid / distTotal) * 100 : 0;
      allDistricts.push({
        name: dist.name,
        invalid: dist.invalid,
        valid: dist.valid,
        total: distTotal,
        quota: dist.quota || 0,
        errorRate: parseFloat(distErrorRate.toFixed(1)),
        remaining: Math.max(0, (dist.quota || 0) - dist.valid),
        completionPct: (dist.quota || 0) > 0 ? parseFloat(((dist.valid / dist.quota) * 100).toFixed(1)) : 0,
        upazilaCount: dist.upazilas.length,
      });

      dist.upazilas.forEach((upz: any) => {
        const upzTotal = upz.valid + upz.invalid;
        allUpazilas.push({
          name: upz.upazila,
          district: upz.district,
          division: upz.division,
          valid: upz.valid,
          invalid: upz.invalid,
          total: upzTotal,
          quota: upz.quota || 0,
          errorRate: upzTotal > 0 ? parseFloat(((upz.invalid / upzTotal) * 100).toFixed(1)) : 0,
        });
        if (upzTotal === 0) notStartedUpazilas++;
        else if ((upz.quota || 0) > 0 && upz.valid >= upz.quota && upz.invalid === 0) completedUpazilas++;
        else startedUpazilas++;
      });
    });
  });

  const globalErrorRate = globalTotal > 0 ? (globalInvalid / globalTotal) * 100 : 0;
  const globalGrade = getGrade(globalErrorRate);
  const globalRemaining = Math.max(0, globalQuota - globalValid);
  const globalCompletionPct = globalQuota > 0 ? (globalValid / globalQuota) * 100 : 0;

  // ── Division-level data ─────────────────────────────────────────────────
  const divisionData = hierarchy.map(div => {
    const quota = div.quota || 0;
    const remaining = Math.max(0, quota - div.valid);
    const completionPct = quota > 0 ? (div.valid / quota) * 100 : 0;
    const errorRate = div.total > 0 ? (div.invalid / div.total) * 100 : 0;
    return {
      name: div.name,
      valid: div.valid,
      remaining,
      invalid: div.invalid,
      quota,
      completionPct: parseFloat(completionPct.toFixed(1)),
      errorRate: parseFloat(errorRate.toFixed(1)),
      grade: getGrade(errorRate),
    };
  }).sort((a, b) => b.quota - a.quota);

  // ── Problem areas ───────────────────────────────────────────────────────
  const topErrorDistricts = [...allDistricts]
    .filter(d => d.total > 0)
    .sort((a, b) => b.errorRate - a.errorRate)
    .slice(0, 10);

  const topRemainingDistricts = [...allDistricts]
    .filter(d => d.remaining > 0)
    .sort((a, b) => b.remaining - a.remaining)
    .slice(0, 8);

  // ── Coverage pie ────────────────────────────────────────────────────────
  const coveragePie = [
    { name: 'Completed', value: completedUpazilas, fill: PALETTE.emerald },
    { name: 'In Progress', value: startedUpazilas, fill: PALETTE.blue },
    { name: 'Not Started', value: notStartedUpazilas, fill: PALETTE.slate },
  ].filter(d => d.value > 0);

  const qualityPie = [
    { name: 'Valid', value: globalValid, fill: PALETTE.emerald },
    { name: 'Invalid', value: globalInvalid, fill: PALETTE.red },
  ].filter(d => d.value > 0);

  // ── Radial gauge for completion ─────────────────────────────────────────
  const completionGauge = [{ name: 'Completion', value: parseFloat(globalCompletionPct.toFixed(1)), fill: PALETTE.cyan }];

  if (allDistricts.length === 0) return null;

  const tabs = [
    { key: 'overview' as const, label: 'Overview', icon: <BarChart3 className="w-3.5 h-3.5" /> },
    { key: 'problems' as const, label: 'Problem Areas', icon: <AlertTriangle className="w-3.5 h-3.5" /> },
    { key: 'coverage' as const, label: 'Coverage', icon: <MapPin className="w-3.5 h-3.5" /> },
  ];

  return (
    <div className="space-y-6 mb-12">
      {/* Tab Navigation */}
      <div className="flex items-center gap-2 border-b border-slate-700/50 pb-3">
        <span className="text-sm font-bold text-slate-500 mr-2 uppercase tracking-widest">Analytics</span>
        {tabs.map(tab => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold transition-all ${
              activeTab === tab.key
                ? 'bg-cyan-600/20 text-cyan-300 border border-cyan-500/30'
                : 'text-slate-500 hover:text-slate-300 hover:bg-slate-800/50'
            }`}
          >
            {tab.icon} {tab.label}
          </button>
        ))}
      </div>

      {/* ═══════════════════════════════════════════════════════════════════ */}
      {/* TAB 1: OVERVIEW                                                   */}
      {/* ═══════════════════════════════════════════════════════════════════ */}
      {activeTab === 'overview' && (
        <div className="space-y-6 animate-in fade-in duration-300">
          {/* MIS Cards Row */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <MISCard
              icon={<Target className="w-5 h-5 text-cyan-400" />}
              label="National Quota"
              value={globalQuota.toLocaleString('en-IN')}
              subText="Distribution target"
              accent="cyan"
            />
            <MISCard
              icon={<CheckCircle2 className="w-5 h-5 text-emerald-400" />}
              label="Records Verified"
              value={globalValid.toLocaleString('en-IN')}
              subText={`${globalCompletionPct.toFixed(1)}% of quota`}
              accent="emerald"
            />
            <MISCard
              icon={<Shield className="w-5 h-5" style={{ color: GRADE_COLORS[globalGrade] }} />}
              label="Data Quality"
              value={globalGrade}
              subText={`${globalErrorRate.toFixed(1)}% error rate`}
              accent={globalErrorRate <= 5 ? 'emerald' : globalErrorRate <= 10 ? 'amber' : 'red'}
            />
            <MISCard
              icon={<TrendingDown className="w-5 h-5 text-amber-400" />}
              label="Remaining"
              value={globalRemaining.toLocaleString('en-IN')}
              subText={globalRemaining === 0 ? 'Target achieved ✓' : 'Records to reach target'}
              accent="amber"
            />
          </div>

          {/* Division Progress + Quality */}
          <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
            {/* Division Progress Bars — Stacked: Valid / Remaining */}
            <div className="lg:col-span-3 glass-panel p-6 rounded-2xl border border-slate-700/50 bg-[#1a1a1c]/40 backdrop-blur-md">
              <h3 className="text-base font-bold text-slate-300 mb-5 flex items-center gap-2">
                <span className="w-1.5 h-5 bg-cyan-500 rounded-full shadow-[0_0_10px_rgba(34,211,238,0.4)]" />
                Division Quota Progress
                <span className="ml-auto text-[10px] text-slate-600 uppercase tracking-wider font-medium">Valid vs Remaining</span>
              </h3>
              <div className="h-[320px]">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={divisionData} layout="vertical" margin={{ top: 0, right: 30, left: 5, bottom: 0 }}>
                    <XAxis type="number" tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false}
                      tickFormatter={(v: number) => v >= 1000000 ? `${(v/1000000).toFixed(1)}M` : v >= 1000 ? `${(v/1000).toFixed(0)}K` : `${v}`}
                    />
                    <YAxis dataKey="name" type="category" axisLine={false} tickLine={false}
                      tick={{ fill: '#e2e8f0', fontSize: 12, fontWeight: 600 }} width={105}
                    />
                    <Tooltip content={<CustomTooltip />} />
                    <Legend verticalAlign="top" height={32} iconType="circle"
                      formatter={(v: string) => <span className="text-slate-400 text-xs font-medium">{v}</span>}
                    />
                    <Bar dataKey="valid" name="Verified" stackId="a" fill={PALETTE.emerald} radius={[0, 0, 0, 0]} barSize={22} />
                    <Bar dataKey="remaining" name="Remaining" stackId="a" fill="#1e293b" radius={[0, 4, 4, 0]} barSize={22} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>

            {/* Quality Donut + Grade Cards */}
            <div className="lg:col-span-2 glass-panel p-6 rounded-2xl border border-slate-700/50 bg-[#1a1a1c]/40 backdrop-blur-md flex flex-col">
              <h3 className="text-base font-bold text-slate-300 mb-4 flex items-center gap-2">
                <span className="w-1.5 h-5 bg-emerald-500 rounded-full shadow-[0_0_10px_rgba(52,211,153,0.4)]" />
                Record Quality
              </h3>
              <div className="h-[180px] relative">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={qualityPie}
                      cx="50%"
                      cy="50%"
                      innerRadius={55}
                      outerRadius={80}
                      dataKey="value"
                      startAngle={90}
                      endAngle={-270}
                      stroke="none"
                    >
                      {qualityPie.map((entry, i) => (
                        <Cell key={i} fill={entry.fill} />
                      ))}
                    </Pie>
                    <Tooltip content={<CustomTooltip />} />
                  </PieChart>
                </ResponsiveContainer>
                <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
                  <div className="text-center">
                    <p className="text-2xl font-black text-white">{(100 - globalErrorRate).toFixed(1)}%</p>
                    <p className="text-[10px] text-slate-500 uppercase tracking-wider">Accuracy</p>
                  </div>
                </div>
              </div>
              {/* Division Quality Grades */}
              <div className="mt-auto grid grid-cols-2 gap-2">
                {divisionData.slice(0, 4).map(div => (
                  <div key={div.name} className="bg-slate-800/30 rounded-lg px-3 py-2 flex items-center justify-between border border-slate-700/20">
                    <span className="text-[11px] text-slate-400 font-medium truncate mr-2">{div.name}</span>
                    <span className="text-xs font-black px-1.5 py-0.5 rounded" style={{
                      color: GRADE_COLORS[div.grade],
                      background: `${GRADE_COLORS[div.grade]}15`,
                    }}>{div.grade}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ═══════════════════════════════════════════════════════════════════ */}
      {/* TAB 2: PROBLEM AREAS                                              */}
      {/* ═══════════════════════════════════════════════════════════════════ */}
      {activeTab === 'problems' && (
        <div className="space-y-6 animate-in fade-in duration-300">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Error Rate by District — Top 10 */}
            <div className="glass-panel p-6 rounded-2xl border border-slate-700/50 bg-[#1a1a1c]/40 backdrop-blur-md">
              <h3 className="text-base font-bold text-slate-300 mb-1 flex items-center gap-2">
                <span className="w-1.5 h-5 bg-red-500 rounded-full shadow-[0_0_10px_rgba(239,68,68,0.4)]" />
                Highest Error Rate Districts
              </h3>
              <p className="text-[10px] text-slate-600 mb-5">Top 10 districts by percentage of invalid records (not absolute count)</p>
              {topErrorDistricts.length > 0 ? (
                <div className="h-[350px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={topErrorDistricts} layout="vertical" margin={{ top: 0, right: 30, left: 5, bottom: 0 }}>
                      <XAxis type="number" tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false}
                        tickFormatter={(v: number) => `${v}%`}
                      />
                      <YAxis dataKey="name" type="category" axisLine={false} tickLine={false}
                        tick={{ fill: '#e2e8f0', fontSize: 11, fontWeight: 500 }} width={110}
                      />
                      <Tooltip content={<CustomTooltip />} />
                      <Bar dataKey="errorRate" name="Error Rate %" radius={[0, 4, 4, 0]} barSize={20}>
                        {topErrorDistricts.map((entry, index) => (
                          <Cell key={index} fill={
                            entry.errorRate > 20 ? PALETTE.red
                              : entry.errorRate > 10 ? PALETTE.amberDark
                              : entry.errorRate > 5 ? PALETTE.amber
                              : PALETTE.emerald
                          } />
                        ))}
                      </Bar>
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              ) : (
                <div className="h-[350px] flex items-center justify-center text-slate-600">
                  <p>No error data available</p>
                </div>
              )}
            </div>

            {/* Remaining Quota — Where effort is needed */}
            <div className="glass-panel p-6 rounded-2xl border border-slate-700/50 bg-[#1a1a1c]/40 backdrop-blur-md">
              <h3 className="text-base font-bold text-slate-300 mb-1 flex items-center gap-2">
                <span className="w-1.5 h-5 bg-amber-500 rounded-full shadow-[0_0_10px_rgba(245,158,11,0.4)]" />
                Largest Quota Gaps
              </h3>
              <p className="text-[10px] text-slate-600 mb-5">Districts that need the most additional records to meet quota target</p>
              {topRemainingDistricts.length > 0 ? (
                <div className="h-[350px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={topRemainingDistricts} layout="vertical" margin={{ top: 0, right: 30, left: 5, bottom: 0 }}>
                      <XAxis type="number" tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false}
                        tickFormatter={(v: number) => v >= 1000 ? `${(v/1000).toFixed(0)}K` : `${v}`}
                      />
                      <YAxis dataKey="name" type="category" axisLine={false} tickLine={false}
                        tick={{ fill: '#e2e8f0', fontSize: 11, fontWeight: 500 }} width={110}
                      />
                      <Tooltip content={<CustomTooltip />} />
                      <Bar dataKey="remaining" name="Remaining" radius={[0, 4, 4, 0]} barSize={20} fill={PALETTE.amber}>
                        {topRemainingDistricts.map((entry, index) => (
                          <Cell key={index} fill={
                            entry.completionPct < 50 ? PALETTE.red
                              : entry.completionPct < 80 ? PALETTE.amber
                              : PALETTE.blue
                          } />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              ) : (
                <div className="h-[350px] flex items-center justify-center text-emerald-400 font-bold">
                  <p>All districts have met their quotas ✓</p>
                </div>
              )}
            </div>
          </div>

          {/* Division Error Rate comparison */}
          <div className="glass-panel p-6 rounded-2xl border border-slate-700/50 bg-[#1a1a1c]/40 backdrop-blur-md">
            <h3 className="text-base font-bold text-slate-300 mb-1 flex items-center gap-2">
              <span className="w-1.5 h-5 bg-amber-500 rounded-full shadow-[0_0_10px_rgba(245,158,11,0.4)]" />
              Division Error Rate Comparison
            </h3>
            <p className="text-[10px] text-slate-600 mb-5">Invalid records as percentage of total — lower is better</p>
            <div className="h-[300px]">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={divisionData.sort((a, b) => b.errorRate - a.errorRate)} layout="vertical" margin={{ top: 0, right: 30, left: 5, bottom: 0 }}>
                  <XAxis type="number" tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false}
                    tickFormatter={(v: number) => `${v}%`}
                  />
                  <YAxis dataKey="name" type="category" axisLine={false} tickLine={false}
                    tick={{ fill: '#e2e8f0', fontSize: 13, fontWeight: 600 }} width={120}
                  />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="errorRate" name="Error Rate %" radius={[0, 6, 6, 0]} barSize={26}>
                    {divisionData.map((entry, index) => (
                      <Cell key={index} fill={
                        entry.errorRate > 10 ? PALETTE.red
                          : entry.errorRate > 5 ? PALETTE.amber
                          : PALETTE.emerald
                      } />
                    ))}
                  </Bar>
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      )}

      {/* ═══════════════════════════════════════════════════════════════════ */}
      {/* TAB 3: COVERAGE                                                   */}
      {/* ═══════════════════════════════════════════════════════════════════ */}
      {activeTab === 'coverage' && (
        <div className="space-y-6 animate-in fade-in duration-300">
          {/* Coverage Summary Cards */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <MISCard
              icon={<MapPin className="w-5 h-5 text-slate-400" />}
              label="Total Upazilas"
              value={allUpazilas.length.toLocaleString('en-IN')}
              subText="In the system"
              accent="cyan"
            />
            <MISCard
              icon={<CheckCircle2 className="w-5 h-5 text-emerald-400" />}
              label="Fully Completed"
              value={completedUpazilas.toLocaleString('en-IN')}
              subText="Quota met + zero errors"
              accent="emerald"
            />
            <MISCard
              icon={<Zap className="w-5 h-5 text-blue-400" />}
              label="In Progress"
              value={startedUpazilas.toLocaleString('en-IN')}
              subText="Data uploaded"
              accent="blue"
            />
            <MISCard
              icon={<AlertTriangle className="w-5 h-5 text-slate-500" />}
              label="Not Started"
              value={notStartedUpazilas.toLocaleString('en-IN')}
              subText="No data yet"
              accent="slate"
            />
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Coverage Donut */}
            <div className="glass-panel p-6 rounded-2xl border border-slate-700/50 bg-[#1a1a1c]/40 backdrop-blur-md">
              <h3 className="text-base font-bold text-slate-300 mb-4 flex items-center gap-2">
                <span className="w-1.5 h-5 bg-blue-500 rounded-full shadow-[0_0_10px_rgba(59,130,246,0.4)]" />
                Upazila Coverage
              </h3>
              <div className="h-[280px] relative">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={coveragePie}
                      cx="50%"
                      cy="50%"
                      innerRadius={65}
                      outerRadius={100}
                      dataKey="value"
                      startAngle={90}
                      endAngle={-270}
                      stroke="#0f172a"
                      strokeWidth={3}
                      paddingAngle={2}
                    >
                      {coveragePie.map((entry, i) => (
                        <Cell key={i} fill={entry.fill} />
                      ))}
                    </Pie>
                    <Tooltip content={<CustomTooltip />} />
                    <Legend
                      verticalAlign="bottom"
                      iconType="circle"
                      formatter={(v: string) => <span className="text-slate-400 text-xs">{v}</span>}
                    />
                  </PieChart>
                </ResponsiveContainer>
                <div className="absolute inset-0 flex items-center justify-center pointer-events-none" style={{ marginBottom: 32 }}>
                  <div className="text-center">
                    <p className="text-3xl font-black text-white">
                      {allUpazilas.length > 0 ? ((startedUpazilas + completedUpazilas) / allUpazilas.length * 100).toFixed(0) : 0}%
                    </p>
                    <p className="text-[10px] text-slate-500 uppercase tracking-wider">Coverage</p>
                  </div>
                </div>
              </div>
            </div>

            {/* Division Completion Comparison */}
            <div className="glass-panel p-6 rounded-2xl border border-slate-700/50 bg-[#1a1a1c]/40 backdrop-blur-md">
              <h3 className="text-base font-bold text-slate-300 mb-4 flex items-center gap-2">
                <span className="w-1.5 h-5 bg-cyan-500 rounded-full shadow-[0_0_10px_rgba(34,211,238,0.4)]" />
                Division Completion %
              </h3>
              <div className="space-y-3">
                {divisionData.sort((a, b) => b.completionPct - a.completionPct).map((div, i) => {
                  const pct = Math.min(100, div.completionPct);
                  const color = pct >= 100 ? PALETTE.emerald : pct >= 70 ? PALETTE.cyan : pct >= 40 ? PALETTE.amber : PALETTE.red;
                  return (
                    <div key={div.name} className="group">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-sm font-semibold text-slate-300">{div.name}</span>
                        <div className="flex items-center gap-2">
                          <span className="text-xs text-slate-500">{div.valid.toLocaleString('en-IN')} / {div.quota.toLocaleString('en-IN')}</span>
                          <span className="text-xs font-bold tabular-nums min-w-[45px] text-right" style={{ color }}>
                            {div.completionPct.toFixed(1)}%
                          </span>
                        </div>
                      </div>
                      <div className="h-2.5 bg-slate-800 rounded-full overflow-hidden">
                        <div
                          className="h-full rounded-full transition-all duration-500 group-hover:brightness-125"
                          style={{
                            width: `${pct}%`,
                            background: `linear-gradient(90deg, ${color}80, ${color})`,
                          }}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// ── MIS Summary Card ──────────────────────────────────────────────────────
const MISCard = ({ icon, label, value, subText, accent = 'cyan' }: {
  icon: any; label: string; value: string; subText: string; accent?: string;
}) => {
  const borderColors: Record<string, string> = {
    cyan: 'border-cyan-800/30 hover:border-cyan-600/40',
    emerald: 'border-emerald-800/30 hover:border-emerald-600/40',
    amber: 'border-amber-800/30 hover:border-amber-600/40',
    red: 'border-red-800/30 hover:border-red-600/40',
    blue: 'border-blue-800/30 hover:border-blue-600/40',
    slate: 'border-slate-700/30 hover:border-slate-500/40',
  };
  return (
    <div className={`bg-[#1a1a1c]/60 border ${borderColors[accent] || borderColors.cyan} p-4 rounded-2xl backdrop-blur-sm group transition-all duration-300`}>
      <div className="flex items-center gap-3">
        <div className="p-2.5 bg-slate-800/60 rounded-xl group-hover:scale-110 transition-transform duration-300">
          {icon}
        </div>
        <div className="min-w-0">
          <p className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-0.5">{label}</p>
          <p className="text-xl font-black text-white leading-none">{value}</p>
          <p className="text-[10px] text-slate-500 mt-1 truncate">{subText}</p>
        </div>
      </div>
    </div>
  );
};

export default AnalyticsCharts;
