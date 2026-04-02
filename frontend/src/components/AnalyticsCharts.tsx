import React from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, PieChart, Pie } from 'recharts';

interface DivNode {
  name: string;
  total: number;
  valid: number;
  invalid: number;
  districts: Record<string, {
    name: string;
    invalid: number;
    valid: number;
  }>;
}

interface AnalyticsChartsProps {
  hierarchy: DivNode[];
}

const COLORS = ['#ef4444', '#f97316', '#f59e0b', '#eab308', '#84cc16']; // Red to yellow/green

const AnalyticsCharts: React.FC<AnalyticsChartsProps> = ({ hierarchy }) => {

  // Flatten districts to find top error districts
  const allDistricts: any[] = [];
  hierarchy.forEach(div => {
    Object.values(div.districts).forEach(dist => {
      allDistricts.push({
        name: dist.name,
        invalid: dist.invalid,
        valid: dist.valid,
        total: dist.invalid + dist.valid
      });
    });
  });

  // Top 5 districts by invalid count
  const topDistricts = [...allDistricts]
    .sort((a, b) => b.invalid - a.invalid)
    .slice(0, 5);

  let globalValid = 0;
  let globalInvalid = 0;
  hierarchy.forEach(d => {
    globalValid += d.valid;
    globalInvalid += d.invalid;
  });

  const pieData = [
    { name: 'Valid Records', value: globalValid, fill: '#34d399' },
    { name: 'Invalid Records', value: globalInvalid, fill: '#f87171' }
  ];

  if (allDistricts.length === 0) return null;

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
      {/* Top 5 Districts by Invalid Count */}
      <div className="glass-panel p-6 rounded-2xl border border-slate-700/50">
        <h3 className="text-lg font-bold text-slate-300 mb-6">Top 5 Districts by Invalid Errors</h3>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={topDistricts} layout="vertical" margin={{ top: 0, right: 0, left: 20, bottom: 0 }}>
              <XAxis type="number" hide />
              <YAxis dataKey="name" type="category" axisLine={false} tickLine={false} tick={{ fill: '#94a3b8' }} width={100} />
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

      {/* Global Validation Distribution */}
      <div className="glass-panel p-6 rounded-2xl border border-slate-700/50">
        <h3 className="text-lg font-bold text-slate-300 mb-6">Global Data Integrity</h3>
        <div className="h-64 relative">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={pieData}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={80}
                paddingAngle={5}
                dataKey="value"
                stroke="none"
              >
                {pieData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.fill} />
                ))}
              </Pie>
              <Tooltip 
                 contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #475569', borderRadius: '8px', color: '#f8fafc' }}
                 itemStyle={{ color: '#f8fafc' }}
              />
            </PieChart>
          </ResponsiveContainer>
          <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
            <span className="text-3xl font-black text-white">
               {globalValid + globalInvalid > 0 ? Math.round((globalValid / (globalValid + globalInvalid)) * 100) : 0}%
            </span>
            <span className="text-xs text-slate-400">Valid</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AnalyticsCharts;
