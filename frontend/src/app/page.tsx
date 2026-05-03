"use client";
/**
 * FFP Data Validator - Home Page
 * Author: Fayez Ahmed, Assistant Programmer, DG Food
 */

import React, { useState, useCallback, useRef, useEffect } from "react";
import Link from "next/link";
import { useDropzone } from "react-dropzone";
import * as XLSX from "xlsx";
import { useRouter } from "next/navigation";
import { UploadCloud, FileSpreadsheet, AlertCircle, CheckCircle2, FileWarning, Play, Download, MapPin, BarChart3, Database, Loader2, CheckCircle } from "lucide-react";
import { fetchWithAuth, getBackendUrl, downloadFileWithAuth, isAuthenticated } from "@/lib/auth";
import { useTranslation } from "@/lib/useTranslation";

export default function Home() {
  const router = useRouter();
  const [file, setFile] = useState<File | null>(null);

  useEffect(() => {
    if (!isAuthenticated()) {
      router.push("/login");
    }
  }, [router]);

  const [previewData, setPreviewData] = useState<any[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [dobColumn, setDobColumn] = useState("");
  const [nidColumn, setNidColumn] = useState("");
  const [headerRow, setHeaderRow] = useState<number>(1);
  const [sheets, setSheets] = useState<string[]>([]);
  const [selectedSheet, setSelectedSheet] = useState<string>("");
  const [workbook, setWorkbook] = useState<XLSX.WorkBook | null>(null);
  const [additionalColumns, setAdditionalColumns] = useState<string[]>([]);
  const [showAdditionalColumns, setShowAdditionalColumns] = useState(false);
  const [isCorrection, setIsCorrection] = useState(false);
  const [wipeBeforeUpload, setWipeBeforeUpload] = useState(false);
  const [loading, setLoading] = useState(false);
  const [pollingStatus, setPollingStatus] = useState<{ status: string; valid: number; invalid: number; new_records: number; total: number; message?: string } | null>(null);
  const [geoData, setGeoData] = useState<{
    divisions: string[];
    districts: Record<string, string[]>;
    upazilas: Record<string, string[]>;
  } | null>(null);
  const [selectedDivision, setSelectedDivision] = useState("");
  const [selectedDistrict, setSelectedDistrict] = useState("");
  const [selectedUpazila, setSelectedUpazila] = useState("");
  const [results, setResults] = useState<{
    summary: { total_rows: number; issues: number; converted_nid: number };
    geo: { division: string; district: string; upazila: string };
    valid_count: number;
    invalid_count: number;
    new_records: number;
    updated_records: number;
    cross_upazila_duplicates: Array<{
      nid: string;
      name: string;
      previous_district: string;
      previous_upazila: string;
      new_district: string;
      new_upazila: string;
    }>;
    pdf_url: string;
    pdf_invalid_url?: string;
    excel_url: string;
    excel_valid_url: string;
    excel_invalid_url: string;
    preview_data: any[];
  } | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewRows, setPreviewRows] = useState<any[] | null>(null);
  const [previewBlocked, setPreviewBlocked] = useState(false);
  const [previewInvalidPct, setPreviewInvalidPct] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const mapColumnsRef = useRef<HTMLDivElement>(null);
  const resultsRef = useRef<HTMLDivElement>(null);
  const errorRef = useRef<HTMLDivElement>(null);
  const { lang, toggleLang, t } = useTranslation();

  useEffect(() => {
    if (error) {
      setTimeout(() => {
        (errorRef.current as HTMLDivElement)?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }, 100);
    }
  }, [error]);

  useEffect(() => {
    if (file && !results && previewData.length > 0) {
      setTimeout(() => {
        (mapColumnsRef.current as HTMLDivElement)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }, 100);
    }
  }, [file, results, previewData.length]);

  useEffect(() => {
    if (results) {
      setTimeout(() => {
        (resultsRef.current as HTMLDivElement)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }, 100);
    }
  }, [results]);

  useEffect(() => {
    const fetchGeoInfo = async () => {
      try {
        const res = await fetchWithAuth(`${getBackendUrl()}/geo/info`);
        if (res.ok) {
          const data = await res.json();
          setGeoData(data);
        }
      } catch (err) {
        console.error("Failed to fetch geo info:", err);
      }
    };
    fetchGeoInfo();
  }, []);

  const onDrop = useCallback((acceptedFiles: File[]) => {
    const selected = acceptedFiles[0];
    if (selected) {
      setFile(selected);
      setResults(null);
      setError(null);
      parseExcel(selected);

      const fetchGuess = async () => {
        try {
          const res = await fetchWithAuth(`${getBackendUrl()}/geo/guess?filename=${encodeURIComponent(selected.name)}`);
          if (res.ok) {
            const data = await res.json();
            if (data.division && data.division !== "Unknown") setSelectedDivision(data.division);
            if (data.district && data.district !== "Unknown") setSelectedDistrict(data.district);
            if (data.upazila && data.upazila !== "Unknown") setSelectedUpazila(data.upazila);
          }
        } catch (e) { }
      };
      fetchGuess();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": [".xlsx"], "application/vnd.ms-excel": [".xls"] },
    maxFiles: 1,
  });

  const updateMapping = (json: any[][], rowIndex: number) => {
    try {
      if (!json || rowIndex <= 0 || rowIndex > json.length) return;

      const rawCols = json[rowIndex - 1] || [];
      if (rawCols.length === 0) {
        setColumns([]);
        setPreviewData([]);
        setDobColumn("");
        setNidColumn("");
        return;
      }

      const cols: string[] = [];
      const counts: Record<string, number> = {};

      rawCols.forEach((c: any) => {
        let name = String(c || "").trim();
        if (!name) name = "Unnamed";

        if (counts[name]) {
          counts[name]++;
          cols.push(`${name} (${counts[name]})`);
        } else {
          counts[name] = 1;
          cols.push(name);
        }
      });

      setColumns(cols);
      setHeaderRow(rowIndex);

      // Auto-detect DOB and NID
      const dobMatch = cols.find(c => {
        if (!c) return false;
        const s = String(c).toLowerCase().replace(/\s+/g, '');
        return s.includes("dob") || s.includes("date") || s.includes("জম্মতারিখ") || s.includes("জন্মতারিখ");
      });
      const nidMatch = cols.find(c => {
        if (!c) return false;
        const s = String(c).toLowerCase().replace(/\s+/g, '');
        return s.includes("পরিচয়") || s.includes("national") || s.includes("জাতীয়পরিচয়পত্র");
      });

      setDobColumn(dobMatch || "");
      setNidColumn(nidMatch || "");
      setAdditionalColumns([]); // Reset when header row changes

      // Show first 15 rows of the sheet so user can pick any as header
      const dataRows = json.slice(0, 15).map((row: any[]) => {
        const obj: any = {};
        cols.forEach((col: string, idx: number) => {
          obj[col] = row[idx];
        });
        return obj;
      });
      setPreviewData(dataRows);
    } catch (err) {
      console.error("Mapping update failed:", err);
      setError("Failed to update column mapping for this row.");
    }
  };

  const parseExcel = async (file: File) => {
    try {
      const buffer = await file.arrayBuffer();
      let wb: XLSX.WorkBook;
      try {
        wb = XLSX.read(new Uint8Array(buffer), { type: "array" });
      } catch (err1) {
        console.warn("Binary parse failed, trying as text string...", err1);
        try {
          const text = await file.text();
          wb = XLSX.read(text, { type: "string" });
        } catch (err2) {
          throw new Error("Could not parse file as binary or text.");
        }
      }

      setWorkbook(wb);

      // Setup sheets
      setSheets(wb.SheetNames);
      const firstSheetName = wb.SheetNames[0];
      setSelectedSheet(firstSheetName);

      loadSheetData(wb, firstSheetName, 1);
    } catch (err) {
      console.error("Excel parse error:", err);
      setError("Failed to parse the Excel file. It may be corrupted or in an unsupported format.");
    }
  };

  const loadSheetData = (wb: XLSX.WorkBook, sheetName: string, headerIdxOffset: number) => {
    const worksheet = wb.Sheets[sheetName];
    
    // Force spreadsheet range to start at A1 so that indexing exactly matches pandas regardless of empty first rows
    const ref = worksheet['!ref'];
    if (ref) {
      const range = XLSX.utils.decode_range(ref);
      if (range.s.r > 0) {
        range.s.r = 0;
        worksheet['!ref'] = XLSX.utils.encode_range(range);
      }
    }
    
    // Ensure blankrows: true is set to count completely empty rows correctly
    const json = XLSX.utils.sheet_to_json(worksheet, { header: 1, blankrows: true }) as any[][];
    (window as any).__raw_wb_json = json;

    if (json.length > 0) {
      let initialHeaderIdx = 0;
      // Search for first row that isn't completely empty
      while (initialHeaderIdx < json.length && (!json[initialHeaderIdx] || json[initialHeaderIdx].filter(x => x !== undefined && x !== null && x !== "").length === 0)) {
        initialHeaderIdx++;
      }
      if (initialHeaderIdx >= json.length) initialHeaderIdx = 0;

      updateMapping(json, initialHeaderIdx + 1);
    }
  };

  const runPreview = async () => {
    if (!file || !dobColumn || !nidColumn) return;

    setPreviewLoading(true);
    setPreviewRows(null);
    setPreviewBlocked(false);
    setPreviewInvalidPct(0);
    setError(null);

    const formData = new FormData();
    formData.append("file", file);
    formData.append("dob_column", dobColumn);
    formData.append("nid_column", nidColumn);
    formData.append("header_row", headerRow.toString());
    formData.append("sheet_name", selectedSheet);

    try {
      const res = await fetchWithAuth(`${getBackendUrl()}/upload/preview`, {
        method: "POST",
        body: formData,
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.detail || "Preview failed.");
      }

      const data = await res.json();
      setPreviewRows(data.preview);
      setPreviewBlocked(data.blocked || false);
      setPreviewInvalidPct(data.invalid_pct || 0);
    } catch (err: any) {
      setError(err.message || "Preview failed.");
    } finally {
      setPreviewLoading(false);
    }
  };

  useEffect(() => {
    if (file && dobColumn && nidColumn && !results) {
      runPreview();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [file, dobColumn, nidColumn, headerRow, selectedSheet]);

    const runValidation = async () => {
    if (!file || !dobColumn || !nidColumn) {
      setError("Please select a file and map both DOB and NID columns.");
      return;
    }

    setLoading(true);
    setPollingStatus(null);
    setError(null);

    const formData = new FormData();
    formData.append("file", file);
    formData.append("dob_column", dobColumn);
    formData.append("nid_column", nidColumn);
    formData.append("header_row", headerRow.toString());
    formData.append("additional_columns", additionalColumns.join(","));
    formData.append("sheet_name", selectedSheet);
    formData.append("is_correction", isCorrection.toString());
    formData.append("wipe_before_upload", wipeBeforeUpload.toString());
    if (selectedDivision) formData.append("division", selectedDivision);
    if (selectedDistrict) formData.append("district", selectedDistrict);
    if (selectedUpazila) formData.append("upazila", selectedUpazila);

    try {
      const res = await fetchWithAuth(`${getBackendUrl()}/upload/validate`, {
        method: "POST",
        body: formData,
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.detail || "Validation failed on the server.");
      }

      const data = await res.json();
      if (data.status === "queued" && data.task_id) {
          // start polling
          setPollingStatus({ status: "processing", valid: 0, invalid: 0, new_records: 0, total: 0 });
          pollTask(data.task_id);
      } else {
          setResults(data);
          setLoading(false);
      }
    } catch (err: any) {
      setError(err.message || "An unexpected error occurred.");
      setLoading(false);
    } 
  };

  const pollTask = async (taskId: number) => {
      try {
          const res = await fetchWithAuth(`${getBackendUrl()}/upload/validate/status/${taskId}`);
          if (!res.ok) throw new Error("Status check failed");
          const data = await res.json();
          
          setPollingStatus({ 
              status: data.status, 
              valid: data.valid_count, 
              invalid: data.invalid_count, 
              new_records: data.new_records,
              total: data.total_rows 
          });

          if (data.status === "processing") {
              setTimeout(() => pollTask(taskId), 2000);
          } else if (data.status === "completed") {
              setResults({
                  ...data,
                  // Keep earlier preview behavior if anything
                  preview_data: previewRows || []
              });
              setLoading(false);
              setPollingStatus(null);
          } else if (data.status === "failed") {
              setError("Background processing failed.");
              setLoading(false);
              setPollingStatus(null);
          }
      } catch (err: any) {
          setError(err.message || "Polling failed.");
          setLoading(false);
          setPollingStatus(null);
      }
  };


  // All file download URLs go through nginx /downloads proxy (same origin, no CORS)
  const getDownloadUrl = (path: string) => path;

  return (
    <main className="min-h-screen p-4 md:p-8 lg:p-12 text-slate-200 flex flex-col">
      <div className="max-w-6xl mx-auto space-y-8 flex-1 w-full">

        {/* Header */}
        <header className="flex flex-col sm:flex-row items-center justify-between space-y-4 sm:space-y-0 text-center sm:text-left">
          <div className="flex-1" />
          <div className="flex-none text-center space-y-4">
            <h1 className="text-4xl md:text-5xl font-extrabold tracking-tight bg-gradient-to-r from-blue-400 to-indigo-400 bg-clip-text text-transparent">
              {t("title")}
            </h1>
            <p className="text-lg text-slate-400 max-w-2xl mx-auto">
              {t("subtitle")}
            </p>
            <Link
              href="/statistics"
              className="inline-flex items-center gap-2 text-sm text-cyan-400 hover:text-cyan-300 bg-cyan-500/10 hover:bg-cyan-500/20 border border-cyan-500/20 px-4 py-2 rounded-lg transition-all"
            >
              <BarChart3 className="w-4 h-4" />
              {t("view_stats")}
            </Link>
          </div>
          <div className="flex-1 flex justify-end items-start pt-2">
            <button onClick={toggleLang} className="px-3 py-1 bg-slate-800 rounded-md text-sm border border-slate-700 hover:bg-slate-700">
              {lang === 'en' ? 'বাংলা' : 'English'}
            </button>
          </div>
        </header>

        {/* Upload Zone */}
        {!results && (
          <div className="glass-panel p-8 rounded-2xl transition-all duration-300">
            <div
              {...getRootProps()}
              className={`border-2 border-dashed rounded-xl p-12 text-center cursor-pointer transition-all ${isDragActive ? "border-blue-500 bg-blue-500/10" : "border-slate-600 hover:border-slate-500 hover:bg-slate-800/50"
                }`}
            >
              <input {...getInputProps()} />
              <UploadCloud className="w-16 h-16 mx-auto mb-4 text-slate-400" />
              {isDragActive ? (
                <p className="text-xl font-medium text-blue-400">{t("drop_here")}</p>
              ) : (
                <div>
                  <p className="text-xl font-medium">{t("drag_drop")}</p>
                  <p className="text-sm text-slate-500 mt-2">{t("click_browse")}</p>
                  <p className="text-xs text-slate-600 mt-4">{t("supported_formats")}</p>
                </div>
              )}
            </div>

            {error && (
              <div ref={errorRef} className="mt-6 animate-in fade-in slide-in-from-top-4 duration-300">
                <div className="bg-red-500/10 border-l-4 border-l-red-500 rounded-r-xl border-y border-r border-red-500/20 overflow-hidden">
                  <div className="p-4 md:p-5">
                    <div className="flex items-start gap-4">
                      <div className="bg-red-500/20 p-2 rounded-lg shrink-0 mt-1">
                        <AlertCircle className="w-6 h-6 text-red-400" />
                      </div>
                      <div className="space-y-1.5 flex-1">
                        <h3 className="text-lg font-semibold text-red-400">Processing Failed</h3>
                        <p className="text-slate-300 leading-relaxed text-sm md:text-base">
                          {error}
                        </p>
                        <div className="mt-4 pt-4 border-t border-red-500/20">
                          <h4 className="text-sm font-medium text-red-300 uppercase tracking-wider mb-2">Common Solutions:</h4>
                          <ul className="text-sm text-slate-400 space-y-1.5 list-disc list-inside marker:text-red-500/50">
                            <li>Check if you selected the correct <strong className="text-slate-200">Date of Birth</strong> and <strong className="text-slate-200">NID</strong> columns.</li>
                            <li>Ensure your column names match the data (Double check the <strong className="text-slate-200">Header Row</strong> setting).</li>
                            <li>Make sure there are no <strong className="text-slate-200">Merged Cells</strong> in your Excel data rows.</li>
                            <li>Save your file as <strong className="text-slate-200">.xlsx</strong> if it&apos;s currently an older format.</li>
                          </ul>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Mapping & Preview */}
        {file && !results && previewData.length > 0 && (
          <div ref={mapColumnsRef} className="glass-panel p-6 md:p-8 rounded-2xl space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-500">
            <div className="flex items-center gap-3">
              <FileSpreadsheet className="w-6 h-6 text-indigo-400" />
              <h2 className="text-2xl font-semibold">Map Columns</h2>
            </div>

            <div className="grid md:grid-cols-2 gap-6 mb-6">
              {sheets.length > 1 && (
                <div className="space-y-2 col-span-1 md:col-span-2 border-b border-slate-700/50 pb-6">
                  <label className="text-sm font-medium text-slate-300">Select Excel Sheet</label>
                  <select
                    className="w-full bg-slate-800 border border-slate-700 rounded-lg p-3 text-slate-200 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none transition-all"
                    value={selectedSheet}
                    onChange={(e) => {
                      setSelectedSheet(e.target.value);
                      if (workbook) {
                        loadSheetData(workbook, e.target.value, 1);
                      }
                    }}
                  >
                    {sheets.map(s => <option key={s} value={s}>{s}</option>)}
                  </select>
                </div>
              )}

              <div className="space-y-2 col-span-1 md:col-span-2 border-b border-slate-700/50 pb-6">
                <label className="text-sm font-medium text-slate-300">Header Row (1-indexed)</label>
                <div className="text-xs text-slate-400 mb-2">Change this if your column names start on a lower row.</div>
                <input
                  type="number"
                  min={1}
                  className="w-full md:w-1/3 bg-slate-800 border border-slate-700 rounded-lg p-3 text-slate-200 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none transition-all"
                  value={headerRow}
                  onChange={(e) => {
                    const val = parseInt(e.target.value);
                    const rawJson = (window as any).__raw_wb_json;
                    if (rawJson) {
                      updateMapping(rawJson, val);
                    } else {
                      setHeaderRow(val);
                    }
                  }}
                />
              </div>
            </div>

            <div className="grid md:grid-cols-2 gap-6">
              <div className="space-y-2">
                <label className="text-sm font-medium text-slate-300">Date of Birth Column</label>
                <select
                  className="w-full bg-slate-800 border border-slate-700 rounded-lg p-3 text-slate-200 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none transition-all"
                  value={dobColumn}
                  onChange={(e) => setDobColumn(e.target.value)}
                >
                  <option value="">Select DOB column...</option>
                  {columns.map(col => <option key={col} value={col}>{col}</option>)}
                </select>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium text-slate-300">NID Column</label>
                <select
                  className="w-full bg-slate-800 border border-slate-700 rounded-lg p-3 text-slate-200 focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none transition-all"
                  value={nidColumn}
                  onChange={(e) => setNidColumn(e.target.value)}
                >
                  <option value="">Select NID column...</option>
                  {columns.map(col => <option key={col} value={col}>{col}</option>)}
                </select>
              </div>

              {/* Manual Location Selection */}
              <div className="space-y-4 col-span-1 md:col-span-2 border-t border-slate-700/50 pt-6 mt-4">
                <div className="flex items-center gap-2">
                  <MapPin className="w-5 h-5 text-cyan-400" />
                  <h3 className="text-lg font-medium text-slate-200">Manual Location Selection (Optional)</h3>
                </div>
                <p className="text-xs text-slate-400">If you don&apos;t select these, we&apos;ll try to detect them from the filename.</p>
                <div className="grid md:grid-cols-3 gap-4">
                  <div className="space-y-2">
                    <label className="text-xs font-medium text-slate-400 uppercase">Division</label>
                    <select
                      className="w-full bg-slate-800 border border-slate-700 rounded-lg p-3 text-slate-200 focus:ring-2 focus:ring-cyan-500/50 outline-none transition-all text-sm"
                      value={selectedDivision}
                      onChange={(e) => {
                        setSelectedDivision(e.target.value);
                        setSelectedDistrict("");
                        setSelectedUpazila("");
                      }}
                    >
                      <option value="">Select Division...</option>
                      {geoData?.divisions.map(d => <option key={d} value={d}>{d}</option>)}
                    </select>
                  </div>
                  <div className="space-y-2">
                    <label className="text-xs font-medium text-slate-400 uppercase">District</label>
                    <select
                      className="w-full bg-slate-800 border border-slate-700 rounded-lg p-3 text-slate-200 focus:ring-2 focus:ring-cyan-500/50 outline-none transition-all text-sm disabled:opacity-50"
                      disabled={!selectedDivision}
                      value={selectedDistrict}
                      onChange={(e) => {
                        setSelectedDistrict(e.target.value);
                        setSelectedUpazila("");
                      }}
                    >
                      <option value="">Select District...</option>
                      {selectedDivision && geoData?.districts[selectedDivision]?.map(d => <option key={d} value={d}>{d}</option>)}
                    </select>
                  </div>
                  <div className="space-y-2">
                    <label className="text-xs font-medium text-slate-400 uppercase">Upazila</label>
                    <select
                      className="w-full bg-slate-800 border border-slate-700 rounded-lg p-3 text-slate-200 focus:ring-2 focus:ring-cyan-500/50 outline-none transition-all text-sm disabled:opacity-50"
                      disabled={!selectedDistrict}
                      value={selectedUpazila}
                      onChange={(e) => setSelectedUpazila(e.target.value)}
                    >
                      <option value="">Select Upazila...</option>
                      {selectedDistrict && geoData?.upazilas[selectedDistrict]?.map(u => <option key={u} value={u}>{u}</option>)}
                    </select>
                  </div>
                </div>
              </div>
              <div
                className="space-y-2 col-span-1 md:col-span-2 font-medium text-slate-300 mt-4 mb-2 flex items-center gap-3 cursor-pointer select-none"
                onClick={() => setShowAdditionalColumns(!showAdditionalColumns)}
              >
                <input
                  type="checkbox"
                  className="rounded border-slate-600 bg-slate-900 text-indigo-500 pointer-events-none"
                  checked={showAdditionalColumns}
                  readOnly
                />
                <span>Include Additional Columns In Reports</span>
              </div>

              {/* Wipe Before Upload */}
              <div
                className="space-y-2 col-span-1 md:col-span-2 font-medium text-slate-300 mb-2 flex items-center gap-3 cursor-pointer select-none bg-orange-500/10 p-4 rounded-xl border border-orange-500/20"
                onClick={() => setWipeBeforeUpload(!wipeBeforeUpload)}
              >
                <input
                  type="checkbox"
                  className="w-5 h-5 rounded border-orange-500/50 bg-slate-900 text-orange-500 pointer-events-none"
                  checked={wipeBeforeUpload}
                  readOnly
                />
                <div>
                  <span className="text-orange-300 font-bold block">Wipe Upazila Before Upload</span>
                  <p className="text-xs text-slate-400 font-normal mt-0.5">Clears ALL existing records for this upazila first, then inserts the new batch. Ensures the statistics page exactly matches this upload&apos;s results. Use for re-uploads.</p>
                </div>
              </div>

              <div
                className="space-y-2 col-span-1 md:col-span-2 font-medium text-slate-300 mb-6 flex items-center gap-3 cursor-pointer select-none bg-indigo-500/10 p-4 rounded-xl border border-indigo-500/20"
                onClick={() => setIsCorrection(!isCorrection)}
              >
                <input
                  type="checkbox"
                  className="w-5 h-5 rounded border-indigo-500/50 bg-slate-900 text-indigo-500 pointer-events-none"
                  checked={isCorrection}
                  readOnly
                />
                <div>
                  <span className="text-indigo-300 font-bold block">This is a Correction Upload</span>
                  <p className="text-xs text-slate-400 font-normal mt-0.5">Check this if you are uploading fixes for previously invalid records. This will prevent total count inflation.</p>
                </div>
              </div>
              {showAdditionalColumns && (
                <div className="col-span-1 md:col-span-2 flex flex-wrap gap-3 p-4 bg-slate-800/20 rounded-lg border border-slate-700/50 transition-all">
                  {columns.filter(c => c !== dobColumn && c !== nidColumn).map(col => (
                    <label key={col} className="flex items-center gap-2 text-sm text-slate-400 bg-slate-800/50 px-3 py-2 rounded-lg border border-slate-700 cursor-pointer hover:bg-slate-700/50 transition-colors">
                      <input
                        type="checkbox"
                        className="rounded border-slate-600 text-indigo-500 focus:ring-indigo-500 bg-slate-900"
                        checked={additionalColumns.includes(col)}
                        onChange={(e) => {
                          if (e.target.checked) setAdditionalColumns([...additionalColumns, col]);
                          else setAdditionalColumns(additionalColumns.filter(c => c !== col));
                        }}
                      />
                      {col}
                    </label>
                  ))}
                </div>
              )}
            </div>

            {/* Pre-validation Preview Warning */}
            {previewRows && previewRows.length > 0 && (
              <div className="space-y-4 border-t border-slate-700/50 pt-8 animate-in fade-in duration-500">
                <div className="flex items-center gap-3">
                  <AlertCircle className="w-6 h-6 text-yellow-500" />
                  <div>
                    <h2 className="text-xl font-semibold text-yellow-500">Validation Check (First 10 Rows)</h2>
                    <p className="text-sm text-slate-400">Verifying columns and date formats for the first 10 rows...</p>
                  </div>
                </div>

                {/* 50% THRESHOLD BLOCKING BANNER */}
                {previewBlocked && (
                  <div className="p-4 rounded-xl bg-red-500/15 border-2 border-red-500/50 flex items-start gap-4">
                    <div className="p-2 bg-red-500/30 rounded-lg shrink-0">
                      <AlertCircle className="w-6 h-6 text-red-400" />
                    </div>
                    <div>
                      <p className="font-bold text-red-400 text-lg">Upload Blocked — {previewInvalidPct}% Invalid</p>
                      <p className="text-sm text-slate-300 mt-1">More than 50% of the first 10 rows are invalid. This usually means the <strong className="text-white">column mapping is wrong</strong>.</p>
                      <ul className="text-xs text-slate-400 mt-2 space-y-1 list-disc list-inside marker:text-red-500/50">
                        <li>Double-check the <strong className="text-slate-200">Date of Birth</strong> and <strong className="text-slate-200">NID</strong> column selection</li>
                        <li>Verify the <strong className="text-slate-200">Header Row</strong> number is correct</li>
                        <li>Make sure there are no merged cells in the data area</li>
                      </ul>
                    </div>
                  </div>
                )}

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {previewRows.some(r => r.Status === 'error') && !previewBlocked ? (
                    <div className="p-4 rounded-xl bg-red-500/10 border border-red-500/30 flex items-start gap-4">
                      <div className="p-2 bg-red-500/20 rounded-lg shrink-0">
                        <FileWarning className="w-5 h-5 text-red-400" />
                      </div>
                      <div>
                        <p className="font-semibold text-red-400">Some Errors Detected ({previewInvalidPct}%)</p>
                        <p className="text-xs text-slate-400 mt-1">Some rows have invalid data, but within acceptable range. You can still proceed with validation.</p>
                      </div>
                    </div>
                  ) : !previewBlocked ? (
                    <div className="p-4 rounded-xl bg-emerald-500/10 border border-emerald-500/30 flex items-start gap-4">
                      <div className="p-2 bg-emerald-500/20 rounded-lg shrink-0">
                        <CheckCircle2 className="w-5 h-5 text-emerald-400" />
                      </div>
                      <div>
                        <p className="font-semibold text-emerald-400">Columns Look Good</p>
                        <p className="text-xs text-slate-400 mt-1">First 10 rows were parsed successfully. You can proceed with full validation.</p>
                      </div>
                    </div>
                  ) : null}
                </div>

                <div className="overflow-x-auto rounded-xl border border-slate-700/50 bg-slate-900/30">
                  <table className="w-full text-xs text-left">
                    <thead className="bg-slate-800/80 text-slate-400 uppercase">
                      <tr>
                        <th className="px-4 py-2">Status</th>
                        <th className="px-4 py-2">Original DOB</th>
                        <th className="px-4 py-2">Cleaned DOB</th>
                        <th className="px-4 py-2">Original NID</th>
                        <th className="px-4 py-2">Cleaned NID</th>
                        <th className="px-4 py-2">Result</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/50">
                      {previewRows.map((row, i) => (
                        <tr key={i}>
                          <td className="px-4 py-2">
                            {row.Status === 'success' ? <span className="text-emerald-500">✓</span> : <span className="text-red-500">✗</span>}
                          </td>
                          <td className="px-4 py-2 text-slate-500">{row[dobColumn]}</td>
                          <td className="px-4 py-2 font-mono">{row.Cleaned_DOB || "—"}</td>
                          <td className="px-4 py-2 text-slate-500">{row[nidColumn]}</td>
                          <td className="px-4 py-2 font-mono">{row.Cleaned_NID || "—"}</td>
                          <td className="px-4 py-2 text-slate-400">{row.Message}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            <div className="flex justify-end pt-4 gap-4">
              {previewRows && (
                <button
                  onClick={() => { setFile(null); setPreviewRows(null); }}
                  className="px-6 py-3 rounded-xl font-medium text-slate-400 hover:text-slate-200 transition-colors"
                >
                  Cancel
                </button>
              )}
              <button
              onClick={runValidation}
              disabled={loading || previewBlocked}
              className={`w-full py-5 rounded-xl font-bold tracking-widest uppercase transition-all shadow-xl shadow-indigo-500/20 active:translate-y-1 mt-8 mb-4 border border-transparent 
                ${loading
                  ? 'bg-indigo-600/50 text-indigo-200 cursor-wait'
                  : previewBlocked
                    ? 'bg-slate-700/50 text-slate-500 cursor-not-allowed border-red-500/20'
                    : 'bg-gradient-to-r from-indigo-600 to-blue-600 hover:from-indigo-500 hover:to-blue-500 text-white'}`}
            >
              <div className="flex items-center justify-center gap-3">
                {loading ? (
                  <>
                    <Loader2 className="w-6 h-6 animate-spin" />
                    {pollingStatus ? `Processing... (${pollingStatus.valid} Valid | ${pollingStatus.invalid} Invalid)` : 'Validating Data...'}
                  </>
                ) : (
                  <>
                    <CheckCircle className="w-6 h-6" />
                    Start Full Validation
                  </>
                )}
              </div>
            </button>
            </div>
          </div>
        )}

        {/* Results */}
        {results && (
          <div ref={resultsRef} className="space-y-8 animate-in fade-in slide-in-from-bottom-8 duration-700">

            {/* Geo Location Info */}
            {results.geo && results.geo.division !== "Unknown" && (
              <div className="glass-panel p-5 rounded-2xl flex items-center gap-4 border-l-4 border-l-cyan-500">
                <div className="p-2.5 bg-cyan-500/20 rounded-xl">
                  <MapPin className="w-6 h-6 text-cyan-400" />
                </div>
                <div className="flex flex-wrap gap-x-8 gap-y-1">
                  <div><span className="text-xs text-slate-500 uppercase tracking-wider">Division</span><p className="text-lg font-semibold text-slate-100">{results.geo?.division || "—"}</p></div>
                  <div><span className="text-xs text-slate-500 uppercase tracking-wider">District</span><p className="text-lg font-semibold text-slate-100">{results.geo?.district || "—"}</p></div>
                  <div><span className="text-xs text-slate-500 uppercase tracking-wider">Upazila</span><p className="text-lg font-semibold text-slate-100">{results.geo?.upazila || "—"}</p></div>
                </div>
              </div>
            )}

            {/* NID Dedup Summary */}
            {(results.new_records !== undefined || results.updated_records !== undefined) && (
              <div className="glass-panel p-5 rounded-2xl border border-slate-700/50 bg-slate-800/30">
                <h3 className="text-sm font-bold text-slate-300 uppercase tracking-wider mb-3 flex items-center gap-2">
                  <Database className="w-4 h-4 text-indigo-400" />
                  Database Update Summary
                </h3>
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 text-center">
                  <div className="bg-emerald-500/10 border border-emerald-500/20 rounded-xl p-3">
                    <p className="text-2xl font-bold text-emerald-400 font-mono">{results.new_records ?? 0}</p>
                    <p className="text-[10px] text-emerald-500 uppercase tracking-widest font-bold mt-1">New NIDs Added</p>
                  </div>
                  <div className="bg-amber-500/10 border border-amber-500/20 rounded-xl p-3">
                    <p className="text-2xl font-bold text-amber-400 font-mono">{results.updated_records ?? 0}</p>
                    <p className="text-[10px] text-amber-500 uppercase tracking-widest font-bold mt-1">Existing Updated</p>
                  </div>
                  <div className="bg-blue-500/10 border border-blue-500/20 rounded-xl p-3">
                    <p className="text-2xl font-bold text-blue-400 font-mono">{results.cross_upazila_duplicates?.length ?? 0}</p>
                    <p className="text-[10px] text-blue-500 uppercase tracking-widest font-bold mt-1">Cross-Upazila</p>
                  </div>
                </div>

                {/* Cross-Upazila Duplicate Warning */}
                {results.cross_upazila_duplicates && results.cross_upazila_duplicates.length > 0 && (
                  <div className="mt-4 bg-amber-500/10 border border-amber-500/20 rounded-xl p-3 sm:p-4">
                    <p className="text-[10px] sm:text-xs font-bold text-amber-400 uppercase tracking-wider mb-2">⚠️ Cross-Upazila NIDs (Moved)</p>
                    <div className="space-y-2 max-h-40 overflow-y-auto custom-scrollbar">
                      {results.cross_upazila_duplicates.map((dup, i) => (
                        <div key={i} className="text-[10px] sm:text-xs text-slate-300 flex flex-col sm:flex-row sm:items-center gap-1 sm:gap-2 bg-slate-900/50 px-3 py-2 rounded-lg">
                          <span className="font-mono text-amber-300">{dup.nid}</span>
                          <span className="text-slate-500 hidden sm:inline">—</span>
                          <div className="flex items-center gap-2">
                            <span className="text-slate-400">{dup.previous_district}/{dup.previous_upazila}</span>
                            <span className="text-slate-500">→</span>
                            <span className="text-emerald-400">{dup.new_district}/{dup.new_upazila}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Statistics Table */}
            <div className="glass-panel rounded-2xl overflow-hidden">
              <div className="p-5 border-b border-slate-700/50">
                <h3 className="text-xl font-semibold">File Statistics</h3>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm text-left">
                  <thead className="bg-slate-800/80 text-slate-300 uppercase text-xs">
                    <tr>
                      <th className="px-5 py-3 font-medium">Division</th>
                      <th className="px-5 py-3 font-medium">District</th>
                      <th className="px-5 py-3 font-medium">Upazila</th>
                      <th className="px-5 py-3 font-medium text-right">Total</th>
                      <th className="px-5 py-3 font-medium text-right">Valid</th>
                      <th className="px-5 py-3 font-medium text-right">Invalid</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-700/50">
                    <tr className="bg-slate-900/50 hover:bg-slate-800/50 transition-colors">
                      <td className="px-5 py-4 font-medium text-slate-200">{results.geo?.division || "—"}</td>
                      <td className="px-5 py-4 font-medium text-slate-200">{results.geo?.district || "—"}</td>
                      <td className="px-5 py-4 font-medium text-slate-200">{results.geo?.upazila || "—"}</td>
                      <td className="px-5 py-4 text-right font-mono text-blue-400 font-semibold">{results.summary?.total_rows ?? 0}</td>
                      <td className="px-5 py-4 text-right font-mono text-emerald-400 font-semibold">{results.valid_count}</td>
                      <td className="px-5 py-4 text-right font-mono text-red-400 font-semibold">{results.invalid_count}</td>
                    </tr>
                    {/* Grand Total */}
                    <tr className="bg-slate-800/80 font-bold">
                      <td colSpan={3} className="px-5 py-3 text-slate-300 uppercase text-xs tracking-wider">Grand Total</td>
                      <td className="px-5 py-3 text-right font-mono text-blue-300">{results.summary?.total_rows ?? 0}</td>
                      <td className="px-5 py-3 text-right font-mono text-emerald-300">{results.valid_count ?? 0}</td>
                      <td className="px-5 py-3 text-right font-mono text-red-300">{results.invalid_count ?? 0}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Summary Cards */}
            <div className="grid md:grid-cols-4 gap-5">
              <div className="glass-panel p-5 rounded-2xl flex items-center gap-4 border-t-4 border-t-blue-500">
                <div className="p-3 bg-blue-500/20 rounded-xl">
                  <FileSpreadsheet className="w-7 h-7 text-blue-400" />
                </div>
                <div>
                  <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Total Rows</p>
                  <p className="text-2xl font-bold text-slate-100">{results.summary?.total_rows ?? 0}</p>
                </div>
              </div>

              <div className="glass-panel p-5 rounded-2xl flex items-center gap-4 border-t-4 border-t-emerald-500">
                <div className="p-3 bg-emerald-500/20 rounded-xl">
                  <CheckCircle2 className="w-7 h-7 text-emerald-400" />
                </div>
                <div>
                  <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Valid</p>
                  <p className="text-2xl font-bold text-slate-100">{results.valid_count ?? 0}</p>
                </div>
              </div>

              <div className="glass-panel p-5 rounded-2xl flex items-center gap-4 border-t-4 border-t-red-500">
                <div className="p-3 bg-red-500/20 rounded-xl">
                  <AlertCircle className="w-7 h-7 text-red-400" />
                </div>
                <div>
                  <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Invalid</p>
                  <p className="text-2xl font-bold text-slate-100">{results.invalid_count ?? 0}</p>
                </div>
              </div>

              <div className="glass-panel p-5 rounded-2xl flex items-center gap-4 border-t-4 border-t-yellow-500">
                <div className="p-3 bg-yellow-500/20 rounded-xl">
                  <FileWarning className="w-7 h-7 text-yellow-500" />
                </div>
                <div>
                  <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">NID Converted</p>
                  <p className="text-2xl font-bold text-slate-100">{results.summary?.converted_nid ?? 0}</p>
                </div>
              </div>
            </div>

            {/* Actions */}
            <div className="flex flex-wrap justify-between items-center bg-slate-800/50 p-4 rounded-xl border border-slate-700 gap-4">
              <button
                onClick={() => { setResults(null); setFile(null); setPreviewData([]); }}
                className="text-slate-400 hover:text-white transition-colors text-sm font-medium"
              >
                ← Upload Another File
              </button>

              <div className="flex flex-wrap gap-3">
                {(() => {
                  const allValid = results.invalid_count === 0 && results.summary.issues === 0;
                  return (
                    <>
                      <button
                        onClick={() => downloadFileWithAuth(results.excel_url, results.excel_url.split('/').pop() || "all_rows.xlsx")}
                        disabled={allValid}
                        className={`bg-blue-600 hover:bg-blue-500 text-white px-5 py-2.5 rounded-lg font-medium flex items-center gap-2 transition-all text-sm ${allValid ? "opacity-50 cursor-not-allowed" : "shadow-lg shadow-blue-500/20"}`}
                      >
                        <Download className="w-4 h-4" />
                        All Rows
                      </button>

                      <button
                        onClick={() => downloadFileWithAuth(results.excel_valid_url, results.excel_valid_url.split('/').pop() || "valid_rows.xlsx")}
                        className="bg-emerald-600 hover:bg-emerald-500 text-white px-5 py-2.5 rounded-lg font-medium flex items-center gap-2 transition-all shadow-lg shadow-emerald-500/20 text-sm"
                      >
                        <Download className="w-4 h-4" />
                        Valid Only
                      </button>

                      <button
                        onClick={() => downloadFileWithAuth(results.excel_invalid_url, results.excel_invalid_url.split('/').pop() || "invalid_rows.xlsx")}
                        disabled={allValid}
                        className={`bg-red-600 hover:bg-red-500 text-white px-5 py-2.5 rounded-lg font-medium flex items-center gap-2 transition-all text-sm ${allValid ? "opacity-50 cursor-not-allowed" : "shadow-lg shadow-red-500/20"}`}
                      >
                        <Download className="w-4 h-4" />
                        Invalid Only
                      </button>
                    </>
                  );
                })()}

                <button
                  onClick={() => downloadFileWithAuth(results.pdf_url, results.pdf_url.split('/').pop() || "validation_report.pdf")}
                  className="bg-indigo-600 hover:bg-indigo-500 text-white px-5 py-2.5 rounded-lg font-medium flex items-center gap-2 transition-all shadow-lg shadow-indigo-500/20 text-sm"
                >
                  <Download className="w-4 h-4" />
                  PDF Report
                </button>

                {results.pdf_invalid_url && results.invalid_count > 0 && (
                  <button
                    onClick={() => {
                      if (results.pdf_invalid_url) {
                        downloadFileWithAuth(results.pdf_invalid_url, results.pdf_invalid_url.split('/').pop() || "invalid_report.pdf");
                      }
                    }}
                    className="bg-orange-600 hover:bg-orange-500 text-white px-5 py-2.5 rounded-lg font-medium flex items-center gap-2 transition-all shadow-lg shadow-orange-500/20 text-sm"
                  >
                    <Download className="w-4 h-4" />
                    Invalid PDF
                  </button>
                )}
              </div>
            </div>

            {/* Results Table */}
            <div className="glass-panel rounded-2xl overflow-hidden">
              <div className="p-6 border-b border-slate-700/50">
                <h3 className="text-xl font-semibold">Processed Data Preview</h3>
                <p className="text-sm text-slate-400">Showing first 50 rows</p>
              </div>
              <div className="overflow-x-auto max-h-[500px]">
                <table className="w-full text-sm text-left">
                  <thead className="bg-slate-800/80 text-slate-300 uppercase sticky top-0 backdrop-blur-md">
                    <tr>
                      <th className="px-4 py-3 font-medium">Status</th>
                      <th className="px-4 py-3 font-medium">Cleaned DOB</th>
                      <th className="px-4 py-3 font-medium">Cleaned NID</th>
                      <th className="px-4 py-3 font-medium">Message</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-700/50">
                    {results.preview_data.map((row, i) => (
                      <tr key={i} className="hover:bg-slate-800/30 transition-colors">
                        <td className="px-4 py-3">
                          {row.Status === 'success' && <span className="inline-flex items-center gap-1.5 py-1 px-2.5 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20"><CheckCircle2 className="w-3.5 h-3.5" /> Valid</span>}
                          {row.Status === 'error' && <span className="inline-flex items-center gap-1.5 py-1 px-2.5 rounded-full text-xs font-medium bg-red-500/10 text-red-400 border border-red-500/20"><AlertCircle className="w-3.5 h-3.5" /> Error</span>}
                          {row.Status === 'warning' && <span className="inline-flex items-center gap-1.5 py-1 px-2.5 rounded-full text-xs font-medium bg-yellow-500/10 text-yellow-500 border border-yellow-500/20"><FileWarning className="w-3.5 h-3.5" /> Converted</span>}
                        </td>
                        <td className="px-4 py-3 font-mono text-slate-300">{row.Cleaned_DOB}</td>
                        <td className="px-4 py-3 font-mono text-slate-300">{row.Cleaned_NID}</td>
                        <td className="px-4 py-3 text-slate-400 max-w-xs truncate" title={row.Message}>{row.Message}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

      </div>

      {/* Footer */}
      <footer className="mt-12 py-6 border-t border-slate-800/50">
        <div className="max-w-6xl mx-auto text-center">
          <p className="text-sm text-slate-500">
            © {new Date().getFullYear()} Computer Network Unit | Directorate General of Food
          </p>
        </div>
      </footer>
    </main>
  );
}
