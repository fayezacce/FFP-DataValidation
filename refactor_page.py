import re

with open('c:/FFP-DataValidation/frontend/src/app/page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add state variables
state_vars = """  const [pollingStatus, setPollingStatus] = useState<{ status: string; valid: number; invalid: number; new_records: number; total: number; message?: string } | null>(null);"""
content = re.sub(r'const \[loading, setLoading\] = useState\(false\);', r'const [loading, setLoading] = useState(false);\n' + state_vars, content)

# 2. Rewrite runValidation
new_run_validation = """  const runValidation = async () => {
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
"""
content = re.sub(r'const runValidation = async \(\) => \{.+?finally \{\s*setLoading\(false\);\s*\}\s*\};', new_run_validation, content, flags=re.DOTALL)

# 3. Add polling UI update just below mapping button section
ui_update = """            <button
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
"""
content = re.sub(r'<button\s*onClick=\{runValidation\}(.*?)</button>', ui_update.strip(), content, flags=re.DOTALL)

with open('c:/FFP-DataValidation/frontend/src/app/page.tsx', 'w', encoding='utf-8') as f:
    f.write(content)
print("page.tsx refactored successfully for polling")
