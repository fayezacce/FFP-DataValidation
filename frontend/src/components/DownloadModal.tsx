"use client";

import React, { useState, useMemo, useEffect } from "react";
import {
  X, Download, ChevronUp, ChevronDown, GripVertical, MapPin, CheckCircle2,
  FileWarning, FileSpreadsheet, FileText, Square, CheckSquare
} from "lucide-react";
import { fetchWithAuth } from "@/lib/auth";

const COLUMN_OPTIONS = [
  { value: "division", label: "Division" },
  { value: "district", label: "District" },
  { value: "upazila", label: "Upazila" },
  { value: "union_name", label: "Union Name" },
  { value: "serial_no", label: "Serial No" },
  { value: "name_bn", label: "Name (BN)" },
  { value: "name_en", label: "Name (EN)" },
  { value: "father_husband_name", label: "Father/Husband Name" },
  { value: "dob", label: "DOB" },
  { value: "occupation", label: "Occupation" },
  { value: "address", label: "Village Name" },
  { value: "ward", label: "Ward No" },
  { value: "nid_17", label: "NID (17 digit)" },
  { value: "nid_10", label: "NID (10 digit)" },
  { value: "mobile", label: "Mobile No" },
  { value: "gender", label: "Gender" },
  { value: "religion", label: "Religion" },
  { value: "spouse_name", label: "Spouse Name" },
  { value: "spouse_nid", label: "Spouse NID" },
  { value: "spouse_dob", label: "Spouse DOB" },
];

const DEFAULT_COLUMNS = [
  "division", "district", "upazila", "union_name", "serial_no",
  "name_bn", "name_en", "father_husband_name", "dob", "occupation",
  "address", "ward", "nid_17", "nid_10", "mobile", "gender",
  "religion", "spouse_name", "spouse_nid", "spouse_dob",
];

type DownloadLevel = "country" | "division" | "district" | "upazila" | "custom";
type ExportMode = "checked" | "valid" | "invalid";
type ExportFormat = "xlsx" | "csv" | "pdf";

interface DownloadModalProps {
  isOpen: boolean;
  onClose: () => void;
  hierarchy: any[];
  initialDivisions?: Set<string>;
  initialDistricts?: Set<string>;
}

export default function DownloadModal({
  isOpen,
  onClose,
  hierarchy,
  initialDivisions,
  initialDistricts,
}: DownloadModalProps) {
  // Step 1: Location
  const [level, setLevel] = useState<DownloadLevel>("country");
  const [selectedDivs, setSelectedDivs] = useState<Set<string>>(initialDivisions || new Set());
  const [selectedDists, setSelectedDists] = useState<Set<string>>(initialDistricts || new Set());
  const [expandedDivs, setExpandedDivs] = useState<Set<string>>(new Set());
  const [locationSearch, setLocationSearch] = useState("");

  // Step 2: Mode & Format
  const [mode, setMode] = useState<ExportMode>("valid");
  const [format, setFormat] = useState<ExportFormat>("xlsx");

  // Step 3: Columns
  const [columns, setColumns] = useState<string[]>(DEFAULT_COLUMNS);

  // Step 4: Filename template
  const DEFAULT_TEMPLATE = "%District%_Approved_NID_NotVerified_FFP_List";
  const [filenameTemplate, setFilenameTemplate] = useState(DEFAULT_TEMPLATE);

  // Download state
  const [loading, setLoading] = useState(false);

  // Sync initial selections
  useEffect(() => {
    if (initialDivisions) setSelectedDivs(new Set(initialDivisions));
    if (initialDistricts) setSelectedDists(new Set(initialDistricts));
  }, [initialDivisions, initialDistricts]);

  const levelLabels: Record<DownloadLevel, string> = {
    country: "Country Wide",
    division: "Division Wise",
    district: "District Wise",
    upazila: "Upazila Wise",
    custom: "Custom Selection",
  };

  const formatLabels: Record<ExportFormat, string> = {
    xlsx: "Excel (.xlsx)",
    csv: "CSV (.csv)",
    pdf: "PDF (.pdf)",
  };

  const formatIcons: Record<ExportFormat, React.ReactNode> = {
    xlsx: <FileSpreadsheet className="w-4 h-4" />,
    csv: <FileText className="w-4 h-4" />,
    pdf: <FileText className="w-4 h-4" />,
  };

  const groupByMap: Record<DownloadLevel, string> = {
    country: "none",
    division: "division",
    district: "district",
    upazila: "upazila",
    custom: "upazila",
  };

  // Compute selection from level
  const computedSelection = useMemo(() => {
    const divs = new Set<string>();
    const dists = new Set<string>();

    if (level === "country" || level === "division" || level === "district" || level === "upazila") {
      hierarchy.forEach(div => {
        divs.add(div.name);
        Object.values(div.districts).forEach((d: any) => {
          dists.add(`${div.name}|${d.name}`);
        });
      });
    } else if (level === "custom") {
      selectedDivs.forEach(d => divs.add(d));
      selectedDists.forEach(d => dists.add(d));
    }

    return { divisions: Array.from(divs), districts: Array.from(dists).map(k => k.split("|")[1]) };
  }, [level, selectedDivs, selectedDists, hierarchy]);

  const selectionCount = computedSelection.divisions.length + computedSelection.districts.length;

  // Filtered / expanded location tree
  const filteredHierarchy = useMemo(() => {
    if (!locationSearch) return hierarchy;
    const q = locationSearch.toLowerCase();
    return hierarchy
      .map((div: any) => {
        const districts = Object.values(div.districts)
          .map((d: any) => {
            const upazilas = d.upazilas.filter((u: any) => u.upazila.toLowerCase().includes(q));
            const nameMatch = d.name.toLowerCase().includes(q);
            if (nameMatch || upazilas.length > 0) {
              return { ...d, upazilas: nameMatch ? d.upazilas : upazilas };
            }
            return null;
          })
          .filter(Boolean);
        const nameMatch = div.name.toLowerCase().includes(q);
        if (nameMatch || districts.length > 0) {
          return { ...div, districts: Object.fromEntries(districts.map((d: any) => [d.name, d])) };
        }
        return null;
      })
      .filter(Boolean) as any[];
  }, [hierarchy, locationSearch]);

  useEffect(() => {
    if (level === "custom" && filteredHierarchy.length > 0) {
      const names = new Set(filteredHierarchy.map((d: any) => d.name));
      const divs = new Set(Array.from(selectedDivs).filter(d => names.has(d)));
      const dists = new Set(Array.from(selectedDists));
      setExpandedDivs(new Set([...divs]));
    } else if (level === "custom") {
      setExpandedDivs(new Set(hierarchy.map((d: any) => d.name)));
    }
  }, [level, filteredHierarchy]);

  const toggleDiv = (name: string) => {
    setSelectedDivs(prev => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
    setExpandedDivs(prev => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const toggleDist = (divName: string, distName: string) => {
    const key = `${divName}|${distName}`;
    setSelectedDists(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const selectAll = () => {
    hierarchy.forEach((div: any) => selectedDivs.add(div.name));
    setSelectedDivs(new Set(selectedDivs));
  };

  const deselectAll = () => {
    setSelectedDivs(new Set());
    setSelectedDists(new Set());
  };

  // Column management
  const checkAllColumns = () => setColumns(COLUMN_OPTIONS.map(c => c.value));
  const uncheckAllColumns = () => setColumns([]);
  const resetColumns = () => setColumns(DEFAULT_COLUMNS);

  const toggleColumn = (val: string) => {
    setColumns(prev => prev.includes(val) ? prev.filter(c => c !== val) : [...prev, val]);
  };

  const moveColumn = (val: string, dir: "up" | "down") => {
    setColumns(prev => {
      const idx = prev.indexOf(val);
      if (idx === -1) return prev;
      const next = [...prev];
      if (dir === "up" && idx > 0) {
        [next[idx - 1], next[idx]] = [next[idx], next[idx - 1]];
      } else if (dir === "down" && idx < next.length - 1) {
        [next[idx], next[idx + 1]] = [next[idx + 1], next[idx]];
      }
      return next;
    });
  };

  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const [dragOverIndex, setDragOverIndex] = useState<number | null>(null);

  const handleDragStart = (idx: number) => {
    setDragIndex(idx);
  };

  const handleDragOver = (e: React.DragEvent, idx: number) => {
    e.preventDefault();
    setDragOverIndex(idx);
  };

  const handleDrop = (idx: number) => {
    if (dragIndex === null || dragIndex === idx) {
      setDragIndex(null);
      setDragOverIndex(null);
      return;
    }
    setColumns(prev => {
      const next = [...prev];
      const [moved] = next.splice(dragIndex, 1);
      next.splice(idx, 0, moved);
      return next;
    });
    setDragIndex(null);
    setDragOverIndex(null);
  };

  const handleDragEnd = () => {
    setDragIndex(null);
    setDragOverIndex(null);
  };

  // Template variable helpers
  const TEMPLATE_VARS = [
    { label: "Division", var: "%Division%" },
    { label: "District", var: "%District%" },
    { label: "Upazila", var: "%Upazila%" },
    { label: "Date", var: "%Date%" },
    { label: "Time", var: "%Time%" },
  ];

  /** Convert %Var% / {Var} syntax to backend {var} format */
  const normalizeTemplate = (tpl: string): string => {
    let s = tpl;
    // %Var% → {var}
    s = s.replace(/%(\w+)%/g, (_, name) => `{${name.toLowerCase()}}`);
    // {Var} → {var}
    s = s.replace(/\{(\w+)\}/g, (_, name) => `{${name.toLowerCase()}}`);
    // automatic extension if missing
    if (!s.includes(".")) s += ".{ext}";
    return s;
  };

  /** Render a preview filename from a template + sample data */
  const renderPreview = (tpl: string, sample: { division: string; district: string; upazila: string }): string => {
    const now = new Date();
    const dateStr = now.toISOString().slice(0, 10);
    const timeStr = now.toTimeString().slice(0, 8).replace(/:/g, "-");
    let s = tpl;
    s = s
      .replace(/%Division%/gi, sample.division)
      .replace(/%District%/gi, sample.district)
      .replace(/%Upazila%/gi, sample.upazila)
      .replace(/%Date%/gi, dateStr)
      .replace(/%Time%/gi, timeStr)
      .replace(/%Ext%/gi, format);
    // Also handle {var} syntax
    s = s
      .replace(/\{division\}/gi, sample.division)
      .replace(/\{district\}/gi, sample.district)
      .replace(/\{upazila\}/gi, sample.upazila)
      .replace(/\{date\}/gi, dateStr)
      .replace(/\{time\}/gi, timeStr)
      .replace(/\{ext\}/gi, format);
    // Remove invalid filename chars
    s = s.replace(/[\\/:*?"<>|]/g, "_").replace(/\s+/g, "_");
    if (!s.includes(".")) s += `.${format}`;
    return s;
  };

  /** Get a sample location for preview (first selected or first in hierarchy) */
  const sampleLocation = useMemo(() => {
    let div = "", dist = "", upz = "";
    if (level === "custom") {
      const firstDiv = hierarchy.find((d: any) => selectedDivs.has(d.name));
      if (firstDiv) {
        div = firstDiv.name;
        const firstDist = Object.values(firstDiv.districts)[0] as any;
        if (firstDist) {
          dist = firstDist.name;
          upz = firstDist.upazilas?.[0]?.upazila || "";
        }
      }
    } else {
      const firstDiv = hierarchy[0];
      if (firstDiv) {
        div = firstDiv.name;
        const firstDist = Object.values(firstDiv.districts)[0] as any;
        if (firstDist) {
          dist = firstDist.name;
          upz = firstDist.upazilas?.[0]?.upazila || "";
        }
      }
    }
    return { division: div || "Division", district: dist || "District", upazila: upz || "Upazila" };
  }, [hierarchy, level, selectedDivs]);

  const previewFilename = renderPreview(filenameTemplate, sampleLocation);

  const handleDownload = async () => {
    if (columns.length === 0) { alert("Please select at least one column."); return; }
    if (computedSelection.divisions.length === 0 && computedSelection.districts.length === 0) {
      alert("No locations selected."); return;
    }

    setLoading(true);
    try {
      const body: Record<string, any> = {
        mode,
        divisions: computedSelection.divisions,
        districts: computedSelection.districts,
        columns,
        column_order: columns,
        fmt: format,
        group_by: groupByMap[level],
      };
      const normalized = normalizeTemplate(filenameTemplate);
      if (normalized !== normalizeTemplate(DEFAULT_TEMPLATE)) {
        body.filename_template = normalized;
      }
      const res = await fetchWithAuth("/api/export/zip-selected", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (res.ok) {
        alert(`Export started! Check the Task Tray in the bottom right corner.`);
        onClose();
      } else {
        const err = await res.json().catch(() => ({ detail: "Unknown error" }));
        alert("Failed: " + (err.detail || "Unknown error"));
      }
    } catch (e) {
      console.error(e);
      alert("Export failed. Check console for details.");
    } finally {
      setLoading(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-300">
      <div className="bg-[#121214] border border-[#1e1e20] w-full max-w-3xl max-h-[90vh] rounded-3xl overflow-hidden shadow-[0_0_50px_rgba(0,0,0,0.5)] flex flex-col">
        {/* Header */}
        <div className="p-6 border-b border-[#1e1e20] flex justify-between items-center bg-[#161618] shrink-0">
          <div className="flex items-center gap-4">
            <div className="w-12 h-12 rounded-2xl bg-cyan-500/10 flex items-center justify-center">
              <Download className="w-6 h-6 text-cyan-500" />
            </div>
            <div>
              <h2 className="text-xl font-black text-white tracking-tight">Custom Export</h2>
              <p className="text-xs text-slate-500">Configure and download your data</p>
            </div>
          </div>
          <button onClick={onClose} className="p-2 rounded-lg hover:bg-slate-800/50 text-slate-400 hover:text-white transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="overflow-y-auto p-6 space-y-6 flex-1">
          {/* Step 1: Location Level */}
          <section>
            <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3">1. Location Level</h3>
            <div className="flex flex-wrap gap-2">
              {(Object.entries(levelLabels) as [DownloadLevel, string][]).map(([key, label]) => (
                <button
                  key={key}
                  onClick={() => setLevel(key as DownloadLevel)}
                  className={`px-4 py-2 rounded-lg text-sm font-medium transition-all border ${
                    level === key
                      ? "bg-cyan-600/30 border-cyan-500/50 text-cyan-300"
                      : "bg-slate-800/50 border-slate-700/50 text-slate-400 hover:bg-slate-700/50"
                  }`}
                >
                  {label}
                </button>
              ))}
            </div>
          </section>

          {/* Step 1b: Location Tree (Custom only) */}
          {level === "custom" && (
            <section>
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider">Select Locations</h3>
                <div className="flex gap-2">
                  <button onClick={selectAll} className="text-xs text-cyan-400 hover:text-cyan-300">Select All</button>
                  <button onClick={deselectAll} className="text-xs text-slate-500 hover:text-slate-300">Deselect All</button>
                </div>
              </div>

              <input
                type="text"
                placeholder="Search locations..."
                value={locationSearch}
                onChange={e => setLocationSearch(e.target.value)}
                className="w-full mb-3 px-3 py-2 bg-slate-800/50 border border-slate-700 rounded-lg text-sm text-slate-200 focus:outline-none focus:border-cyan-500/50"
              />

              <div className="max-h-60 overflow-y-auto space-y-1 custom-scrollbar">
                {filteredHierarchy.map((div: any) => (
                  <div key={div.name}>
                    <button
                      onClick={() => toggleDiv(div.name)}
                      className="flex items-center gap-2 w-full px-3 py-2 rounded-lg hover:bg-slate-800/50 text-left transition-colors"
                    >
                      {selectedDivs.has(div.name) ? (
                        <CheckSquare className="w-4 h-4 text-emerald-500 shrink-0" />
                      ) : (
                        <Square className="w-4 h-4 text-slate-600 shrink-0" />
                      )}
                      <MapPin className="w-3.5 h-3.5 text-emerald-500/50 shrink-0" />
                      <span className="text-sm font-bold text-emerald-400">{div.name}</span>
                    </button>

                    {(expandedDivs.has(div.name) || locationSearch) && (
                      <div className="ml-6 space-y-0.5">
                        {Object.values(div.districts).map((d: any) => (
                          <div key={d.name}>
                            <button
                              onClick={() => {
                                if (!selectedDivs.has(div.name)) toggleDist(div.name, d.name);
                              }}
                              disabled={selectedDivs.has(div.name)}
                              className={`flex items-center gap-2 w-full px-3 py-1.5 rounded-lg text-left transition-colors ${
                                selectedDivs.has(div.name)
                                  ? "opacity-40 cursor-not-allowed"
                                  : "hover:bg-slate-800/50"
                              }`}
                            >
                              {selectedDivs.has(div.name) || selectedDists.has(`${div.name}|${d.name}`) ? (
                                <CheckSquare className="w-3.5 h-3.5 text-blue-500 shrink-0" />
                              ) : (
                                <Square className="w-3.5 h-3.5 text-slate-700 shrink-0" />
                              )}
                              <span className="text-sm text-blue-400">{d.name}</span>
                            </button>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </section>
          )}

          {/* Step 2: Record Type & Format */}
          <section className="grid grid-cols-2 gap-6">
            <div>
              <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3">2. Record Type</h3>
              <div className="flex flex-col gap-2">
                {(["checked", "valid", "invalid"] as ExportMode[]).map(m => (
                  <button
                    key={m}
                    onClick={() => setMode(m)}
                    className={`flex items-center gap-3 px-4 py-3 rounded-lg border text-sm font-medium transition-all ${
                      mode === m
                        ? m === "valid" ? "bg-emerald-600/20 border-emerald-500/50 text-emerald-300"
                        : m === "invalid" ? "bg-red-600/20 border-red-500/50 text-red-300"
                        : "bg-cyan-600/20 border-cyan-500/50 text-cyan-300"
                        : "bg-slate-800/50 border-slate-700/50 text-slate-400 hover:bg-slate-700/50"
                    }`}
                  >
                    {m === "valid" ? <CheckCircle2 className="w-4 h-4" /> :
                     m === "invalid" ? <FileWarning className="w-4 h-4" /> :
                     <FileSpreadsheet className="w-4 h-4" />}
                    {m === "checked" ? "Checked (All + Highlighted)" : m === "valid" ? "Valid Only" : "Invalid Only"}
                  </button>
                ))}
              </div>
            </div>

            <div>
              <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3">3. File Format</h3>
              <div className="flex flex-col gap-2">
                {(["xlsx", "csv", "pdf"] as ExportFormat[]).map(f => (
                  <button
                    key={f}
                    onClick={() => setFormat(f)}
                    className={`flex items-center gap-3 px-4 py-3 rounded-lg border text-sm font-medium transition-all ${
                      format === f
                        ? "bg-indigo-600/20 border-indigo-500/50 text-indigo-300"
                        : "bg-slate-800/50 border-slate-700/50 text-slate-400 hover:bg-slate-700/50"
                    }`}
                  >
                    {formatIcons[f]}
                    {formatLabels[f]}
                  </button>
                ))}
              </div>
            </div>
          </section>

          {/* Step 3: Columns */}
          <section>
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider">4. Columns & Order</h3>
              <div className="flex gap-3">
                <button onClick={checkAllColumns} className="text-xs text-cyan-400 hover:text-cyan-300">Select All</button>
                <button onClick={uncheckAllColumns} className="text-xs text-slate-500 hover:text-slate-300">Deselect All</button>
                <button onClick={resetColumns} className="text-xs text-amber-400 hover:text-amber-300">Reset</button>
              </div>
            </div>

            <div className="border border-[#1e1e20] rounded-xl overflow-hidden">
              <div className="max-h-48 overflow-y-auto custom-scrollbar divide-y divide-[#1e1e20]">
                {COLUMN_OPTIONS.map((col) => {
                  const idx = columns.indexOf(col.value);
                  const checked = idx !== -1;
                  const isDragging = dragIndex === idx;
                  const isDragOver = dragOverIndex === idx;
                  return (
                    <div
                      key={col.value}
                      draggable={checked}
                      onDragStart={() => handleDragStart(idx)}
                      onDragOver={(e) => handleDragOver(e, idx)}
                      onDrop={() => handleDrop(idx)}
                      onDragEnd={handleDragEnd}
                      className={`flex items-center gap-2 px-4 py-2.5 transition-colors ${
                        checked ? "bg-slate-800/30" : "bg-transparent"
                      } ${isDragging ? "opacity-40" : ""} ${
                        isDragOver && checked ? "border-t-2 border-cyan-400" : ""
                      }`}
                    >
                      <label className="flex items-center gap-3 flex-1 cursor-pointer min-w-0">
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() => toggleColumn(col.value)}
                          className="accent-cyan-400"
                        />
                        <span className={`text-sm leading-tight ${checked ? "text-slate-200" : "text-slate-500"}`}>
                          {col.label}
                        </span>
                      </label>

                      {checked && (
                        <div className="flex items-center gap-1 shrink-0">
                          <span className="cursor-grab active:cursor-grabbing text-slate-500 hover:text-slate-300 transition-colors" title="Drag to reorder">
                            <GripVertical className="w-3.5 h-3.5" />
                          </span>
                          <button
                            onClick={() => moveColumn(col.value, "up")}
                            disabled={idx === 0}
                            className="p-1 rounded hover:bg-slate-700/50 disabled:opacity-20 disabled:cursor-not-allowed text-slate-400 hover:text-white transition-colors"
                          >
                            <ChevronUp className="w-3.5 h-3.5" />
                          </button>
                          <button
                            onClick={() => moveColumn(col.value, "down")}
                            disabled={idx === columns.length - 1}
                            className="p-1 rounded hover:bg-slate-700/50 disabled:opacity-20 disabled:cursor-not-allowed text-slate-400 hover:text-white transition-colors"
                          >
                            <ChevronDown className="w-3.5 h-3.5" />
                          </button>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
            <p className="text-xs text-slate-600 mt-2">
              {columns.length} of {COLUMN_OPTIONS.length} columns selected
            </p>
          </section>

          {/* Step 4: Filename Template */}
          <section>
            <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3">5. File Name Format</h3>
            <div className="flex flex-wrap gap-1.5 mb-3">
              {TEMPLATE_VARS.map(v => (
                <button
                  key={v.var}
                  onClick={() => setFilenameTemplate(prev => prev + v.var)}
                  className="px-2.5 py-1 rounded-md bg-slate-800/60 border border-slate-700/50 text-xs text-cyan-300 hover:bg-slate-700/60 transition-colors font-mono"
                >
                  {v.var}
                </button>
              ))}
              <button
                onClick={() => setFilenameTemplate(DEFAULT_TEMPLATE)}
                className="px-2.5 py-1 rounded-md bg-slate-800/60 border border-slate-700/50 text-xs text-slate-400 hover:bg-slate-700/60 transition-colors"
              >
                Reset
              </button>
            </div>
            <input
              type="text"
              value={filenameTemplate}
              onChange={e => setFilenameTemplate(e.target.value)}
              className="w-full px-3 py-2 bg-slate-900/60 border border-slate-700 rounded-lg text-sm text-slate-200 font-mono focus:outline-none focus:border-cyan-500/50"
              placeholder="e.g. %District%_%Upazila%_FFP_Data"
            />
            <div className="mt-2 flex items-center gap-2 text-xs">
              <span className="text-slate-600">Preview:</span>
              <span className="text-slate-300 font-mono bg-slate-800/40 px-2 py-0.5 rounded truncate max-w-full">
                {previewFilename}
              </span>
            </div>
          </section>
        </div>

        {/* Footer */}
        <div className="p-6 border-t border-[#1e1e20] bg-[#161618] shrink-0">
          <div className="flex items-center justify-between gap-4">
            <div className="text-sm text-slate-400">
              {level === "country" ? "All locations" : (
                <>
                  {computedSelection.divisions.length} division{computedSelection.divisions.length !== 1 ? "s" : ""}
                  {computedSelection.districts.length > 0 && `, ${computedSelection.districts.length} district${computedSelection.districts.length !== 1 ? "s" : ""}`}
                  {" "}· {mode} · {formatLabels[format]}
                </>
              )}
            </div>
            <button
              onClick={handleDownload}
              disabled={loading || columns.length === 0}
              className="flex items-center gap-2 px-6 py-3 rounded-xl bg-cyan-600 text-white font-semibold text-sm hover:bg-cyan-500 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-500 transition-all"
            >
              {loading ? (
                <>Starting…</>
              ) : (
                <><Download className="w-4 h-4" /> Start Export</>
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
