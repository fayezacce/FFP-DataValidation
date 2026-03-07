"use client";

import React, { useState, useCallback, useRef, useEffect } from "react";
import { useDropzone } from "react-dropzone";
import * as XLSX from "xlsx";
import { UploadCloud, FileSpreadsheet, AlertCircle, CheckCircle2, FileWarning, Play, Download } from "lucide-react";

export default function Home() {
  const [file, setFile] = useState<File | null>(null);
  const [previewData, setPreviewData] = useState<any[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [dobColumn, setDobColumn] = useState("");
  const [nidColumn, setNidColumn] = useState("");
  const [headerRow, setHeaderRow] = useState<number>(1);
  const [sheets, setSheets] = useState<string[]>([]);
  const [selectedSheet, setSelectedSheet] = useState<string>("");
  const [workbook, setWorkbook] = useState<XLSX.WorkBook | null>(null);
  const [additionalColumns, setAdditionalColumns] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<{
    summary: { total_rows: number; issues: number; converted_nid: number };
    pdf_url: string;
    excel_url: string;
    excel_valid_url: string;
    preview_data: any[];
  } | null>(null);
  const [error, setError] = useState<string | null>(null);

  const mapColumnsRef = useRef<HTMLDivElement>(null);
  const resultsRef = useRef<HTMLDivElement>(null);

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

  const onDrop = useCallback((acceptedFiles: File[]) => {
    const selected = acceptedFiles[0];
    if (selected) {
      setFile(selected);
      setResults(null);
      setError(null);
      parseExcel(selected);
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
      // Output format as array of arrays
      const json = XLSX.utils.sheet_to_json(worksheet, { header: 1 }) as any[][];
      (window as any).__raw_wb_json = json;
        if (json.length > 0) {
          // Re-parse with the selected header row later if they change it
          // For initial column options, we just look at the first non-empty row or just row 0
          let headerIdx = 0;
          while (headerIdx < json.length && (!json[headerIdx] || json[headerIdx].length === 0)) {
              headerIdx++;
          }
          if (headerIdx < json.length) {
              const rawCols = json[headerIdx] || [];
              const cols = rawCols.map((c: any) => String(c || ""));
              setColumns(cols);
              setHeaderRow(headerIdx + 1); // 1-indexed for the user
              
              // Try to auto-detect columns
          const dobMatch = cols.find(c => {
             if (!c) return false;
             const s = String(c).toLowerCase();
             return s.includes("dob") || s.includes("date");
          });
          const nidMatch = cols.find(c => {
             if (!c) return false;
             const s = String(c).toLowerCase();
             return s.includes("nid") || s.includes("national");
          });
          
          if (dobMatch) setDobColumn(dobMatch);
          if (nidMatch) setNidColumn(nidMatch);
          setAdditionalColumns([]); // clear on new sheet
          
          // Preview first 5 data rows
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
    
    try {
      // Use window.location.hostname for local LAN access fallback
      const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || `http://${window.location.hostname}:8000`;
      const res = await fetch(`${backendUrl}/validate`, {
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

  return (
    <main className="min-h-screen p-4 md:p-8 lg:p-12 text-slate-200">
      <div className="max-w-6xl mx-auto space-y-8">
        
        {/* Header */}
        <header className="text-center space-y-4">
          <h1 className="text-4xl md:text-5xl font-extrabold tracking-tight bg-gradient-to-r from-blue-400 to-indigo-400 bg-clip-text text-transparent">
            Data Validator
          </h1>
          <p className="text-lg text-slate-400 max-w-2xl mx-auto">
            Upload your excel files to automatically normalize digits, clean dates, and validate NID numbers.
          </p>
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
              <div className="mt-4 p-4 rounded-lg bg-red-500/10 border border-red-500/50 flex items-start gap-3 text-red-400">
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
                          
                          // Update preview data using new offset
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
              <div className="space-y-2 col-span-1 md:col-span-2 font-medium text-slate-300 mt-4 mb-2">
                Include Additional Columns In Reports
              </div>
              <div className="col-span-1 md:col-span-2 flex flex-wrap gap-3">
                {columns.filter(c => c !== dobColumn && c !== nidColumn).map(col => (
                  <label key={col} className="flex items-center gap-2 text-sm text-slate-400 bg-slate-800/50 px-3 py-2 rounded-lg border border-slate-700 cursor-pointer hover:bg-slate-700/50">
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
            {/* Summary Cards */}
            <div className="grid md:grid-cols-3 gap-6">
              <div className="glass-panel p-6 rounded-2xl flex items-center gap-4 border-t-4 border-t-blue-500">
                <div className="p-3 bg-blue-500/20 rounded-xl">
                  <FileSpreadsheet className="w-8 h-8 text-blue-400" />
                </div>
                <div>
                  <p className="text-sm font-medium text-slate-400">Total Rows Processed</p>
                  <p className="text-3xl font-bold text-slate-100">{results.summary.total_rows}</p>
                </div>
              </div>
              
              <div className="glass-panel p-6 rounded-2xl flex items-center gap-4 border-t-4 border-t-red-500">
                <div className="p-3 bg-red-500/20 rounded-xl">
                  <AlertCircle className="w-8 h-8 text-red-400" />
                </div>
                <div>
                  <p className="text-sm font-medium text-slate-400">Validation Errors</p>
                  <p className="text-3xl font-bold text-slate-100">{results.summary.issues}</p>
                </div>
              </div>
              
              <div className="glass-panel p-6 rounded-2xl flex items-center gap-4 border-t-4 border-t-yellow-500">
                <div className="p-3 bg-yellow-500/20 rounded-xl">
                  <FileWarning className="w-8 h-8 text-yellow-500" />
                </div>
                <div>
                  <p className="text-sm font-medium text-slate-400">NIDs Auto-Converted</p>
                  <p className="text-3xl font-bold text-slate-100">{results.summary.converted_nid}</p>
                </div>
              </div>
            </div>

            {/* Actions */}
            <div className="flex justify-between items-center bg-slate-800/50 p-4 rounded-xl border border-slate-700">
              <button 
                onClick={() => { setResults(null); setFile(null); setPreviewData([]); }}
                className="text-slate-400 hover:text-white transition-colors text-sm font-medium"
              >
                ← Upload Another File
              </button>
              
              <div className="flex gap-4">
                <a 
                  href={`${process.env.NEXT_PUBLIC_BACKEND_URL || `http://${window.location.hostname}:8000`}${results.excel_url}`}
                  download
                  target="_blank"
                  rel="noreferrer"
                  className="bg-blue-600 hover:bg-blue-500 text-white px-6 py-2.5 rounded-lg font-medium flex items-center gap-2 transition-all shadow-lg shadow-blue-500/20"
                >
                  <Download className="w-4 h-4" />
                  All Rows Excel
                </a>
                
                <a 
                  href={`${process.env.NEXT_PUBLIC_BACKEND_URL || `http://${window.location.hostname}:8000`}${results.excel_valid_url}`}
                  download
                  target="_blank"
                  rel="noreferrer"
                  className="bg-emerald-600 hover:bg-emerald-500 text-white px-6 py-2.5 rounded-lg font-medium flex items-center gap-2 transition-all shadow-lg shadow-emerald-500/20"
                >
                  <Download className="w-4 h-4" />
                  Valid Rows Only
                </a>
                
                <a 
                  href={`${process.env.NEXT_PUBLIC_BACKEND_URL || `http://${window.location.hostname}:8000`}${results.pdf_url}`}
                  download
                  target="_blank"
                  rel="noreferrer"
                  className="bg-indigo-600 hover:bg-indigo-500 text-white px-6 py-2.5 rounded-lg font-medium flex items-center gap-2 transition-all shadow-lg shadow-indigo-500/20"
                >
                  <Download className="w-4 h-4" />
                  PDF Report
                </a>
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
    </main>
  );
}
