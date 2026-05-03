"use client";

import { Globe } from "lucide-react";
import { useTranslation } from "@/lib/useTranslation";

export default function LanguageToggle() {
  const { lang, toggleLang } = useTranslation();

  return (
    <button
      onClick={toggleLang}
      className="flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg border border-[#2a2a2e] bg-[#1a1a1c] text-gray-400 hover:text-white hover:border-emerald-500/50 transition-all text-xs font-bold select-none shadow-sm"
      title={`Switch to ${lang === "en" ? "Bangla" : "English"}`}
    >
      <Globe className="w-3.5 h-3.5 text-emerald-500/70" />
      <span>{lang === "en" ? "EN" : "BN"}</span>
    </button>
  );
}
