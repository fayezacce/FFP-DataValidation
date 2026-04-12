import React from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, PieChart, Pie, Legend } from 'recharts';
import { Target, CheckCircle2, AlertCircle, Percent, MapPin } from 'lucide-react';

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
  }>;
}

interface AnalyticsChartsProps {
  hierarchy: DivNode[];
}

const COLORS = ['#ef4444', '#f97316', '#f59e0b', '#eab308', '#84cc16']; // Red to yellow/green

const AnalyticsCharts: React.FC<AnalyticsChartsProps> = ({ hierarchy }) => {

  // Flatten data for analytics
  const allDistricts: any[] = [];
  const allUpazilas: any[] = [];
  let globalValid = 0;
  let globalInvalid = 0;
  let globalQuota = 0;
  let completedUpazilasCount = 0;

  hierarchy.forEach(div => {
    globalValid += div.valid;
    globalInvalid += div.invalid;
    globalQuota += (div.quota || 0);

    Object.values(div.districts).forEach(dist => {
      allDistricts.push({
        name: dist.name,
        invalid: dist.invalid,
        valid: dist.valid,
        quota: dist.quota || 0,
      });

      dist.upazilas.forEach((upz: any) => {
        allUpazilas.push({
          name: upz.upazila,
          district: upz.district,
          division: upz.division,
          valid: upz.valid,
          invalid: upz.invalid,
          quota: upz.quota || 0,
        });
        if ((upz.quota || 0) > 0 && upz.valid >= upz.quota && upz.invalid === 0) {
          completedUpazilasCount++;
        }
      });
    });
  });

  // Top 5 districts by invalid count
  const topDistricts = [...allDistricts]
    .sort((a, b) => b.invalid - a.invalid)
    .slice(0, 5);

  // Top 5 upazilas by valid count
  const topUpazilas = [...allUpazilas]
    .sort((a, b) => b.valid - a.valid)
    .slice(0, 5);

  // Division-wise data
  const divisionData = hierarchy.map(div => {
    const quota = div.quota || 0;
    const invalidRatio = quota > 0 ? (div.invalid / quota) * 100 : 0;
    return {
      name: div.name,
      valid: div.valid,
      target: quota,
      invalid: div.invalid,
      invalidRatio: parseFloat(invalidRatio.toFixed(2)),
      remaining: Math.max(0, quota - div.valid)
    };
  }).sort((a, b) => b.target - a.target);

  const completionRate = globalQuota > 0 ? (globalValid / globalQuota) * 100 : 0;
  const remainingTarget = Math.max(0, globalQuota - globalValid);

  const pieData = [
    { name: 'Valid Records', value: globalValid, fill: '#34d399' },
    { name: 'Invalid Records', value: globalInvalid, fill: '#f87171' }
  ];

  if (allDistricts.length === 0) return null;

  return (
    <div className="space-y-8 mb-12">
      {/* 1. MIS Overview Panel */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
        <MISCard 
          icon={<Target className="w-5 h-5 text-blue-400" />}
          label="National Quota"
          value={globalQuota.toLocaleString('en-IN')}
          subText="Distribution Target"
        />
        <MISCard 
          icon={<CheckCircle2 className="w-5 h-5 text-emerald-400" />}
          label="Total Valid"
          value={globalValid.toLocaleString('en-IN')}
          subText="Successfully Verified"
        />
        <MISCard 
          icon={<AlertCircle className="w-5 h-5 text-amber-400" />}
          label="Completed Regions"
          value={completedUpazilasCount.toLocaleString('en-IN')}
          subText="Target Met + 0 Errors"
        />
        <MISCard 
          icon={<Percent className="w-5 h-5 text-purple-400" />}
          label="Completion %"
          value={`${completionRate.toFixed(1)}%`}
          subText="Current nationwide status"
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 2. Top 5 Districts by Invalid Count */}
        <div className="glass-panel p-6 rounded-2xl border border-slate-700/50 bg-[#1a1a1c]/40 backdrop-blur-md shadow-xl">
          <h3 className="text-lg font-bold text-slate-300 mb-6 flex items-center gap-2">
            <span className="w-1.5 h-6 bg-red-500 rounded-full shadow-[0_0_12px_rgba(239,68,68,0.5)]"></span>
            Top 5 Districts by Invalid Errors
          </h3>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={topDistricts} layout="vertical" margin={{ top: 0, right: 20, left: 20, bottom: 0 }}>
                <XAxis type="number" hide />
                <YAxis dataKey="name" type="category" axisLine={false} tickLine={false} tick={{ fill: '#94a3b8', fontSize: 12 }} width={100} />
                <Tooltip 
                  cursor={{ fill: '#334155', opacity: 0.4 }}
                  contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: '8px', color: '#f8fafc' }}
                  itemStyle={{ color: '#f8fafc' }}
                />
                <Bar dataKey="invalid" radius={[0, 4, 4, 0]} barSize={24}>
                  {topDistricts.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* 3. Top Performing Upazilas */}
        <div className="glass-panel p-6 rounded-2xl border border-slate-700/50 bg-[#1a1a1c]/40 backdrop-blur-md shadow-xl">
          <h3 className="text-lg font-bold text-slate-300 mb-6 flex items-center gap-2">
            <span className="w-1.5 h-6 bg-cyan-500 rounded-full shadow-[0_0_12px_rgba(34,211,238,0.5)]"></span>
            Top Performance (Upazilas)
          </h3>
          <div className="space-y-4">
            {topUpazilas.map((upz, idx) => (
              <div key={idx} className="flex items-center justify-between p-3 bg-slate-800/30 rounded-xl border border-slate-700/30">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 rounded-full bg-slate-800 flex items-center justify-center text-xs font-bold text-slate-400 border border-slate-700">
                    {idx + 1}
                  </div>
                  <div>
                    <h4 className="text-sm font-bold text-slate-200">{upz.name}</h4>
                    <p className="text-[10px] text-slate-500 font-medium">{upz.district}, {upz.division}</p>
                  </div>
                </div>
                <div className="text-right">
                  <p className="text-sm font-black text-emerald-400">{upz.valid.toLocaleString('en-IN')}</p>
                  <p className="text-[10px] text-slate-500 font-bold tracking-tighter uppercase">Valid</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 4. Division-wise Quota Completion */}
        <div className="glass-panel p-6 rounded-2xl border border-slate-700/50 bg-[#1a1a1c]/40 backdrop-blur-md shadow-xl">
          <h3 className="text-lg font-bold text-slate-300 mb-8 flex items-center gap-2">
            <span className="w-1.5 h-6 bg-blue-500 rounded-full shadow-[0_0_12px_rgba(59,130,246,0.5)]"></span>
            Division-wise Quota Completion (Target vs Actual)
          </h3>
          <div className="h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={divisionData} layout="vertical" margin={{ top: 0, right: 30, left: 40, bottom: 20 }}>
                <XAxis type="number" tick={{ fill: '#64748b', fontSize: 11 }} axisLine={{ stroke: '#334155' }} tickLine={false} />
                <YAxis dataKey="name" type="category" axisLine={false} tickLine={false} tick={{ fill: '#f8fafc', fontSize: 13, fontWeight: 600 }} width={120} />
                <Tooltip 
                  cursor={{ fill: '#334155', opacity: 0.4 }}
                  contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: '8px', color: '#f8fafc' }}
                />
                <Legend verticalAlign="top" height={36} iconType="circle" />
                <Bar dataKey="target" name="Quota Target" fill="#1e293b" radius={[0, 4, 4, 0]} barSize={34} />
                <Bar dataKey="valid" name="Current Valid" fill="#10b981" radius={[0, 4, 4, 0]} barSize={34} style={{ marginTop: -42 }} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* 5. Division-wise Invalid Intensity */}
        <div className="glass-panel p-6 rounded-2xl border border-slate-700/50 bg-[#1a1a1c]/40 backdrop-blur-md shadow-xl">
          <h3 className="text-lg font-bold text-slate-300 mb-8 flex items-center gap-2">
            <span className="w-1.5 h-6 bg-amber-500 rounded-full shadow-[0_0_12px_rgba(245,158,11,0.5)]"></span>
            Invalid Intensity % (Errors per Target)
          </h3>
          <div className="h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={[...divisionData].sort((a,b) => b.invalidRatio - a.invalidRatio)} layout="vertical" margin={{ top: 0, right: 30, left: 40, bottom: 20 }}>
                <XAxis type="number" hide />
                <YAxis dataKey="name" type="category" axisLine={false} tickLine={false} tick={{ fill: '#f8fafc', fontSize: 13, fontWeight: 600 }} width={120} />
                <Tooltip 
                  cursor={{ fill: '#334155', opacity: 0.4 }}
                  contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: '8px', color: '#f8fafc' }}
                  formatter={(val: number) => [`${val}%`, 'Intensity']}
                />
                <Bar dataKey="invalidRatio" name="Invalid %" radius={[0, 4, 4, 0]} barSize={24}>
                  {divisionData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.invalidRatio > 10 ? '#ef4444' : entry.invalidRatio > 5 ? '#f59e0b' : '#34d399'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
};

const MISCard = ({ icon, label, value, subText }: { icon: any, label: string, value: string, subText: string }) => (
  <div className="bg-[#1a1a1c]/60 border border-slate-700/50 p-5 rounded-2xl shadow-lg backdrop-blur-sm group hover:border-slate-500/50 transition-all duration-300">
    <div className="flex items-center gap-4">
      <div className="p-3 bg-slate-800/50 rounded-xl group-hover:scale-110 transition-transform duration-300">
        {icon}
      </div>
      <div>
        <p className="text-xs font-bold text-slate-500 uppercase tracking-widest mb-1">{label}</p>
        <p className="text-2xl font-black text-white">{value}</p>
        <p className="text-[10px] text-slate-400 mt-1">{subText}</p>
      </div>
    </div>
  </div>
);

export default AnalyticsCharts;
