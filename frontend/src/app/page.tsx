"use client";

import React, { useState, useCallback, useRef, useEffect } from "react";
import Link from "next/link";
import { useDropzone } from "react-dropzone";
import * as XLSX from "xlsx";
import { useRouter } from "next/navigation";
import { UploadCloud, FileSpreadsheet, AlertCircle, CheckCircle2, FileWarning, Play, Download, MapPin, BarChart3, Database } from "lucide-react";
import { fetchWithAuth, getBackendUrl, downloadFileWithAuth, isAuthenticated } from "@/lib/auth";

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
  const [loading, setLoading] = useState(false);
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
    excel_url: string;
    excel_valid_url: string;
    excel_invalid_url: string;
    preview_data: any[];
  } | null>(null);
  const [error, setError] = useState<string | null>(null);

  const mapColumnsRef = useRef<HTMLDivElement>(null);
  const resultsRef = useRef<HTMLDivElement>(null);
  const errorRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (error) {
      setTimeout(() => {
        errorRef.current?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }, 100);
    }
  }, [error]);

  useEffect(() => {
    if (file && !results && previewData.length > 0) {
      setTimeout(() => {
        mapColumnsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }, 100);
    }
  }, [file, results, previewData.length]);

  useEffect(() => {
    if (results) {
      setTimeout(() => {
        resultsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }, 100);
    }
  }, [results]);

  useEffect(() => {
    const fetchGeoInfo = async () => {
      try {
        const res = await fetchWithAuth(`${getBackendUrl()}/geo-info`);
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
          const res = await fetchWithAuth(`${getBackendUrl()}/guess-location?filename=${encodeURIComponent(selected.name)}`);
          if (res.ok) {
            const data = await res.json();
            if (data.division && data.division !== "Unknown") setSelectedDivision(data.division);
            if (data.district && data.district !== "Unknown") setSelectedDistrict(data.district);
            if (data.upazila && data.upazila !== "Unknown") setSelectedUpazila(data.upazila);
          }
        } catch (e) {}
      };
      fetchGuess();
    }
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": [".xlsx"], "application/vnd.ms-excel": [".xls"] },
    maxFiles: 1,
  });

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
      const json = XLSX.utils.sheet_to_json(worksheet, { header: 1 }) as any[][];
      (window as any).__raw_wb_json = json;
        if (json.length > 0) {
          let headerIdx = 0;
          while (headerIdx < json.length && (!json[headerIdx] || json[headerIdx].length === 0)) {
              headerIdx++;
          }
          if (headerIdx < json.length) {
              const rawCols = json[headerIdx] || [];
              const cols = rawCols.map((c: any) => String(c || ""));
              setColumns(cols);
              setHeaderRow(headerIdx + 1);
              
          const dobMatch = cols.find(c => {
             if (!c) return false;
             const s = String(c).toLowerCase().replace(/\s+/g, '');
             return s.includes("dob") || s.includes("date") || s.includes("জম্মতারিখ") || s.includes("জন্মতারিখ");
          });
          const nidMatch = cols.find(c => {
             if (!c) return false;
             const s = String(c).toLowerCase().replace(/\s+/g, '');
             return s.includes("nid") || s.includes("national") || s.includes("জাতীয়পরিচয়পত্র");
          });
          
          if (dobMatch) setDobColumn(dobMatch);
          if (nidMatch) setNidColumn(nidMatch);
          setAdditionalColumns([]);
          
          const dataRows = json.slice(headerIdx + 1, headerIdx + 6).map((row: any[]) => {
            const obj: any = {};
            cols.forEach((col: string, idx: number) => {
              obj[col] = row[idx];
            });
            return obj;
          });
          setPreviewData(dataRows);
        } else {
            setColumns([]);
            setPreviewData([]);
        }
      }
  };

  const runValidation = async () => {
    if (!file || !dobColumn || !nidColumn) {
      setError("Please select a file and map both DOB and NID columns.");
      return;
    }
    
    setLoading(true);
    setError(null);
    
    const formData = new FormData();
    formData.append("file", file);
    formData.append("dob_column", dobColumn);
    formData.append("nid_column", nidColumn);
    formData.append("header_row", headerRow.toString());
    formData.append("additional_columns", additionalColumns.join(","));
    formData.append("sheet_name", selectedSheet);
    if (selectedDivision) formData.append("division", selectedDivision);
    if (selectedDistrict) formData.append("district", selectedDistrict);
    if (selectedUpazila) formData.append("upazila", selectedUpazila);
    
    try {
      const res = await fetchWithAuth(`${getBackendUrl()}/validate`, {
        method: "POST",
        body: formData,
      });
      
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.detail || "Validation failed on the server.");
      }
      
      const data = await res.json();
      setResults(data);
    } catch (err: any) {
      setError(err.message || "An unexpected error occurred.");
    } finally {
      setLoading(false);
    }
  };

  // All file download URLs go through nginx /downloads proxy (same origin, no CORS)
  const getDownloadUrl = (path: string) => path;

  return (
    <main className="min-h-screen p-4 md:p-8 lg:p-12 text-slate-200 flex flex-col">
      <div className="max-w-6xl mx-auto space-y-8 flex-1 w-full">
        
        {/* Header */}
        <header className="text-center space-y-4">
          <h1 className="text-4xl md:text-5xl font-extrabold tracking-tight bg-gradient-to-r from-blue-400 to-indigo-400 bg-clip-text text-transparent">
            Data Validator
          </h1>
          <p className="text-lg text-slate-400 max-w-2xl mx-auto">
            Upload your excel files to automatically normalize digits, clean dates, and validate NID numbers.
          </p>
          <Link
            href="/statistics"
            className="inline-flex items-center gap-2 text-sm text-cyan-400 hover:text-cyan-300 bg-cyan-500/10 hover:bg-cyan-500/20 border border-cyan-500/20 px-4 py-2 rounded-lg transition-all"
          >
            <BarChart3 className="w-4 h-4" />
            View Statistics Dashboard
          </Link>
        </header>

        {/* Upload Zone */}
        {!results && (
          <div className="glass-panel p-8 rounded-2xl transition-all duration-300">
            <div 
              {...getRootProps()} 
              className={`border-2 border-dashed rounded-xl p-12 text-center cursor-pointer transition-all ${
                isDragActive ? "border-blue-500 bg-blue-500/10" : "border-slate-600 hover:border-slate-500 hover:bg-slate-800/50"
              }`}
            >
              <input {...getInputProps()} />
              <UploadCloud className="w-16 h-16 mx-auto mb-4 text-slate-400" />
              {isDragActive ? (
                <p className="text-xl font-medium text-blue-400">Drop the Excel file here...</p>
              ) : (
                <div>
                  <p className="text-xl font-medium">Drag & drop your Excel file here</p>
                  <p className="text-sm text-slate-500 mt-2">or click to browse from your computer</p>
                  <p className="text-xs text-slate-600 mt-4">Supported formats: .xlsx, .xls</p>
                </div>
              )}
            </div>

            {error && (
              <div ref={errorRef} className="mt-4 p-4 rounded-lg bg-red-500/10 border border-red-500/50 flex items-start gap-3 text-red-400">
                <AlertCircle className="w-5 h-5 shrink-0 mt-0.5" />
                <p>{error}</p>
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
                      setHeaderRow(val);
                      const rawJson = (window as any).__raw_wb_json;
                      if (rawJson && val > 0 && val <= rawJson.length) {
                          const rawCols = rawJson[val - 1] || [];
                          const newCols = rawCols.map((c: any) => String(c || ""));
                          setColumns(newCols);
                          
                          const dataRows = rawJson.slice(val, val + 5).map((row: any[]) => {
                            const obj: any = {};
                            newCols.forEach((col: string, idx: number) => {
                              obj[col] = row[idx];
                            });
                            return obj;
                          });
                          setPreviewData(dataRows);
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
                <p className="text-xs text-slate-400">If you don't select these, we'll try to detect them from the filename.</p>
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

            <div className="space-y-4">
              <h3 className="text-lg font-medium text-slate-300">Data Preview (First 5 rows)</h3>
              <div className="overflow-x-auto rounded-xl border border-slate-700/50">
                <table className="w-full text-sm text-left">
                  <thead className="bg-slate-800/80 text-slate-300 uppercase">
                    <tr>
                      {columns.map(col => (
                        <th key={col} className={`px-4 py-3 font-medium ${col === dobColumn || col === nidColumn ? 'text-indigo-400' : ''}`}>
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-700/50">
                    {previewData.map((row, i) => (
                      <tr key={i} className="bg-slate-900/50 hover:bg-slate-800/50 transition-colors">
                        {columns.map(col => (
                          <td key={`${i}-${col}`} className="px-4 py-3 whitespace-nowrap overflow-hidden text-ellipsis max-w-[200px]">
                            {String(row[col] ?? "")}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="flex justify-end pt-4">
              <button
                onClick={runValidation}
                disabled={loading || !dobColumn || !nidColumn}
                className="bg-indigo-600 hover:bg-indigo-500 text-white px-8 py-3 rounded-xl font-medium flex items-center gap-2 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-lg shadow-indigo-500/20 active:scale-95"
              >
                {loading ? (
                  <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                ) : (
                  <Play className="w-5 h-5" />
                )}
                {loading ? "Processing..." : "Run Validation"}
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
                  <div><span className="text-xs text-slate-500 uppercase tracking-wider">Division</span><p className="text-lg font-semibold text-slate-100">{results.geo.division}</p></div>
                  <div><span className="text-xs text-slate-500 uppercase tracking-wider">District</span><p className="text-lg font-semibold text-slate-100">{results.geo.district}</p></div>
                  <div><span className="text-xs text-slate-500 uppercase tracking-wider">Upazila</span><p className="text-lg font-semibold text-slate-100">{results.geo.upazila}</p></div>
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
                <div className="grid grid-cols-3 gap-4 text-center">
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
                  <div className="mt-4 bg-amber-500/10 border border-amber-500/20 rounded-xl p-4">
                    <p className="text-xs font-bold text-amber-400 uppercase tracking-wider mb-2">⚠️ Cross-Upazila NIDs (Moved)</p>
                    <div className="space-y-2 max-h-40 overflow-y-auto custom-scrollbar">
                      {results.cross_upazila_duplicates.map((dup, i) => (
                        <div key={i} className="text-xs text-slate-300 flex items-center gap-2 bg-slate-900/50 px-3 py-2 rounded-lg">
                          <span className="font-mono text-amber-300">{dup.nid}</span>
                          <span className="text-slate-500">—</span>
                          <span className="text-slate-400">{dup.previous_district}/{dup.previous_upazila}</span>
                          <span className="text-slate-500">→</span>
                          <span className="text-emerald-400">{dup.new_district}/{dup.new_upazila}</span>
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
                      <td className="px-5 py-4 text-right font-mono text-blue-400 font-semibold">{results.summary.total_rows}</td>
                      <td className="px-5 py-4 text-right font-mono text-emerald-400 font-semibold">{results.valid_count}</td>
                      <td className="px-5 py-4 text-right font-mono text-red-400 font-semibold">{results.invalid_count}</td>
                    </tr>
                    {/* Grand Total */}
                    <tr className="bg-slate-800/80 font-bold">
                      <td colSpan={3} className="px-5 py-3 text-slate-300 uppercase text-xs tracking-wider">Grand Total</td>
                      <td className="px-5 py-3 text-right font-mono text-blue-300">{results.summary.total_rows}</td>
                      <td className="px-5 py-3 text-right font-mono text-emerald-300">{results.valid_count}</td>
                      <td className="px-5 py-3 text-right font-mono text-red-300">{results.invalid_count}</td>
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
                  <p className="text-2xl font-bold text-slate-100">{results.summary.total_rows}</p>
                </div>
              </div>
              
              <div className="glass-panel p-5 rounded-2xl flex items-center gap-4 border-t-4 border-t-emerald-500">
                <div className="p-3 bg-emerald-500/20 rounded-xl">
                  <CheckCircle2 className="w-7 h-7 text-emerald-400" />
                </div>
                <div>
                  <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Valid</p>
                  <p className="text-2xl font-bold text-slate-100">{results.valid_count}</p>
                </div>
              </div>

              <div className="glass-panel p-5 rounded-2xl flex items-center gap-4 border-t-4 border-t-red-500">
                <div className="p-3 bg-red-500/20 rounded-xl">
                  <AlertCircle className="w-7 h-7 text-red-400" />
                </div>
                <div>
                  <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Invalid</p>
                  <p className="text-2xl font-bold text-slate-100">{results.invalid_count}</p>
                </div>
              </div>
              
              <div className="glass-panel p-5 rounded-2xl flex items-center gap-4 border-t-4 border-t-yellow-500">
                <div className="p-3 bg-yellow-500/20 rounded-xl">
                  <FileWarning className="w-7 h-7 text-yellow-500" />
                </div>
                <div>
                  <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">NID Converted</p>
                  <p className="text-2xl font-bold text-slate-100">{results.summary.converted_nid}</p>
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
                        onClick={() => downloadFileWithAuth(results.excel_url, "all_rows.xlsx")}
                        disabled={allValid}
                        className={`bg-blue-600 hover:bg-blue-500 text-white px-5 py-2.5 rounded-lg font-medium flex items-center gap-2 transition-all text-sm ${allValid ? "opacity-50 cursor-not-allowed" : "shadow-lg shadow-blue-500/20"}`}
                      >
                        <Download className="w-4 h-4" />
                        All Rows
                      </button>
                      
                      <button 
                        onClick={() => downloadFileWithAuth(results.excel_valid_url, "valid_rows.xlsx")}
                        className="bg-emerald-600 hover:bg-emerald-500 text-white px-5 py-2.5 rounded-lg font-medium flex items-center gap-2 transition-all shadow-lg shadow-emerald-500/20 text-sm"
                      >
                        <Download className="w-4 h-4" />
                        Valid Only
                      </button>
                      
                      <button 
                        onClick={() => downloadFileWithAuth(results.excel_invalid_url, "invalid_rows.xlsx")}
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
                  onClick={() => downloadFileWithAuth(results.pdf_url, "validation_report.pdf")}
                  className="bg-indigo-600 hover:bg-indigo-500 text-white px-5 py-2.5 rounded-lg font-medium flex items-center gap-2 transition-all shadow-lg shadow-indigo-500/20 text-sm"
                >
                  <Download className="w-4 h-4" />
                  PDF Report
                </button>
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
                          {row.Status === 'success' && <span className="inline-flex items-center gap-1.5 py-1 px-2.5 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20"><CheckCircle2 className="w-3.5 h-3.5"/> Valid</span>}
                          {row.Status === 'error' && <span className="inline-flex items-center gap-1.5 py-1 px-2.5 rounded-full text-xs font-medium bg-red-500/10 text-red-400 border border-red-500/20"><AlertCircle className="w-3.5 h-3.5"/> Error</span>}
                          {row.Status === 'warning' && <span className="inline-flex items-center gap-1.5 py-1 px-2.5 rounded-full text-xs font-medium bg-yellow-500/10 text-yellow-500 border border-yellow-500/20"><FileWarning className="w-3.5 h-3.5"/> Converted</span>}
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
