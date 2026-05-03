"use client";

import { useEffect, useState } from "react";
import { Globe } from "lucide-react";

export default function LanguageToggle() {
  const [lang, setLang] = useState<"EN" | "BN">("EN");

  useEffect(() => {
    const saved = localStorage.getItem("ffp_lang") as "EN" | "BN";
    if (saved) setLang(saved);
  }, []);

  const toggleLang = () => {
    const newLang = lang === "EN" ? "BN" : "EN";
    setLang(newLang);
    localStorage.setItem("ffp_lang", newLang);
    window.dispatchEvent(new Event("languageChange"));
  };

  return (
    <button
      onClick={toggleLang}
      className="flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg border border-[#2a2a2e] bg-[#1a1a1c] text-gray-400 hover:text-white hover:border-emerald-500/50 transition-all text-xs font-bold select-none shadow-sm"
      title={`Switch to ${lang === "EN" ? "Bangla" : "English"}`}
    >
      <Globe className="w-3.5 h-3.5 text-emerald-500/70" />
      <span>{lang}</span>
    </button>
  );
}
