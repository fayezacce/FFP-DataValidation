/**
 * FFP Data Validation Platform - Dashboard Analytics Cards
 */

import React from "react";
import { BarChart3, Hash, CheckCircle2, FileWarning, Clock } from "lucide-react";
import { useTranslation } from "@/lib/useTranslation";

interface DashboardCardsProps {
  grandTotal: {
    total: number;
    valid: number;
    invalid: number;
  };
  loading?: boolean;
}

const DashboardCards: React.FC<DashboardCardsProps> = ({ grandTotal, loading }) => {
  const { t } = useTranslation();

  const cards = [
    {
      label: t("total_unique"),
      value: grandTotal.total.toLocaleString("en-IN"),
      icon: <Hash className="w-5 h-5 text-blue-400" />,
      bg: "bg-blue-500/10",
      border: "border-blue-500/20"
    },
    {
      label: t("validated_unique"),
      value: grandTotal.valid.toLocaleString("en-IN"),
      icon: <CheckCircle2 className="w-5 h-5 text-emerald-400" />,
      bg: "bg-emerald-500/10",
      border: "border-emerald-500/20"
    },
    {
      label: t("invalid_records"),
      value: grandTotal.invalid.toLocaleString("en-IN"),
      icon: <FileWarning className="w-5 h-5 text-red-400" />,
      bg: "bg-red-500/10",
      border: "border-red-500/20"
    },
    {
      label: t("data_integrity"),
      value: grandTotal.total > 0 
        ? ((grandTotal.valid / grandTotal.total) * 100).toFixed(1) + "%" 
        : "0%",
      icon: <BarChart3 className="w-5 h-5 text-amber-400" />,
      bg: "bg-amber-500/10",
      border: "border-amber-500/20"
    }
  ];

  if (loading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        {[1, 2, 3, 4].map((i) => (
          <div key={i} className="bg-[#121214] border border-[#1e1e20] p-6 rounded-2xl animate-pulse h-32"></div>
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
      {cards.map((card, i) => (
        <div key={i} className={`bg-[#121214] border ${card.border} p-6 rounded-2xl relative overflow-hidden group hover:bg-[#161618] transition-all`}>
          <div className="relative z-10 flex flex-col justify-between h-full">
            <div className="flex justify-between items-start mb-4">
              <span className="text-gray-500 text-xs font-bold uppercase tracking-wider">{card.label}</span>
              <div className={`p-2 rounded-xl ${card.bg}`}>{card.icon}</div>
            </div>
            <div>
              <div className="text-3xl font-black text-white tracking-tight">{card.value}</div>
            </div>
          </div>
          <div className={`absolute -bottom-6 -right-6 w-24 h-24 ${card.bg} rounded-full blur-2xl group-hover:scale-150 transition-all duration-700`}></div>
        </div>
      ))}
    </div>
  );
};

export default DashboardCards;
