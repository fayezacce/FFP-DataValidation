import React from "react";
import { Target, CheckCircle2, Info, Percent } from "lucide-react";
import { useTranslation } from "@/lib/useTranslation";

interface DashboardCardsProps {
  data: {
    totalQuota: number;
    totalValid: number;
    remaining: number;
    completionPct: number;
  };
  loading?: boolean;
}

const DashboardCards: React.FC<DashboardCardsProps> = ({ data, loading }) => {
  const { t } = useTranslation();

  const cards = [
    {
      label: t("national_quota"),
      subLabel: t("distribution_target"),
      value: data.totalQuota.toLocaleString("en-IN"),
      icon: <Target className="w-5 h-5 text-blue-400" />,
      bg: "bg-blue-500/10",
      border: "border-blue-500/20"
    },
    {
      label: t("total_valid"),
      subLabel: t("successfully_verified"),
      value: data.totalValid.toLocaleString("en-IN"),
      icon: <CheckCircle2 className="w-5 h-5 text-emerald-400" />,
      bg: "bg-emerald-500/10",
      border: "border-emerald-500/20"
    },
    {
      label: t("remaining"),
      subLabel: t("pending_quota"),
      value: data.remaining.toLocaleString("en-IN"),
      icon: <Info className="w-5 h-5 text-amber-400" />,
      bg: "bg-amber-500/10",
      border: "border-amber-500/20"
    },
    {
      label: t("completion_pct"),
      subLabel: t("nationwide_status"),
      value: data.completionPct.toFixed(1) + "%",
      icon: <Percent className="w-5 h-5 text-purple-400" />,
      bg: "bg-purple-500/10",
      border: "border-purple-500/20"
    }
  ];

  if (loading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        {[1, 2, 3, 4].map((i) => (
          <div key={i} className="bg-[#121214]/50 border border-slate-800/50 p-6 rounded-2xl animate-pulse h-32"></div>
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
      {cards.map((card, i) => (
        <div key={i} className={`bg-[#121214] border ${card.border} p-6 rounded-xl relative overflow-hidden group hover:bg-[#161618] transition-all duration-300 shadow-xl`}>
          <div className="relative z-10 flex flex-col justify-between h-full">
            <div className="flex justify-between items-start">
              <div>
                <span className="text-gray-500 text-[10px] font-black uppercase tracking-[0.2em]">{card.label}</span>
                <div className="text-3xl font-black text-white tracking-tight mt-1">{card.value}</div>
                <div className="text-[10px] text-slate-500 font-medium mt-1">{card.subLabel}</div>
              </div>
              <div className={`p-3 rounded-xl ${card.bg} border border-white/5`}>{card.icon}</div>
            </div>
          </div>
          {/* Subtle glow effect */}
          <div className={`absolute -bottom-10 -right-10 w-32 h-32 ${card.bg} rounded-full blur-[60px] opacity-20 group-hover:opacity-40 transition-opacity duration-700`}></div>
        </div>
      ))}
    </div>
  );
};

export default DashboardCards;
