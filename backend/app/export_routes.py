"""
FFP Data Validator — Export Routes
Handles live exports, ZIP generation, downloads, recheck, and trailing-zeros reports.
"""
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
import pandas as pd
import os
import uuid
import urllib.parse
import zipfile
import logging

from .database import get_db, SessionLocal
from .models import (
    User, SummaryStats, ValidRecord, InvalidRecord, BackgroundTask,
)
from .auth import get_current_user
from .rbac import PermissionChecker
from .audit import log_audit
from .pdf_generator import generate_pdf_report

logger = logging.getLogger("ffp")
router = APIRouter(tags=["export"])


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS — shared by live export, zip generation, and recheck
# ─────────────────────────────────────────────────────────────────────────────

def get_live_records_df(
    db: Session,
    division: str = None,
    district: str = None,
    upazila: str = None,
    is_invalid: bool = False,
    upazila_id: int = None,
):
    """Fetch live records and return a formatted DataFrame. Optimized for national scale."""
    table_name = "invalid_records" if is_invalid else "valid_records"

    query_sql = f"""
        SELECT 
            nid as "NID", nid as "Cleaned_NID",
            dob as "DOB", dob as "Cleaned_DOB",
            name as "Name", division as "Division", district as "District",
            upazila as "Upazila", batch_id as "Batch_ID", source_file as "Source_File",
            data,
            {"'error' as \"Status\", error_message as \"Message\"" if is_invalid else "'valid' as \"Status\", '' as \"Message\""}
        FROM {table_name}
    """

    params = {}
    use_id = False
    if upazila_id:
        query_sql += " WHERE upazila_id = :uid"
        params["uid"] = upazila_id
        use_id = True
    elif division and district and upazila:
        query_sql += " WHERE LOWER(TRIM(division)) = LOWER(TRIM(:div)) AND LOWER(TRIM(district)) = LOWER(TRIM(:dist)) AND LOWER(TRIM(upazila)) = LOWER(TRIM(:upz))"
        params.update({"div": division, "dist": district, "upz": upazila})
    else:
        return None

    if is_invalid:
        query_sql += " ORDER BY id DESC"
    else:
        query_sql += " ORDER BY nid ASC"

    df = pd.read_sql(text(query_sql), db.connection(), params=params)

    if df.empty and use_id and division and district and upazila:
        fallback_sql = f"""
            SELECT 
                nid as "NID", nid as "Cleaned_NID",
                dob as "DOB", dob as "Cleaned_DOB",
                name as "Name", division as "Division", district as "District",
                upazila as "Upazila", batch_id as "Batch_ID", source_file as "Source_File",
                data,
                {"'error' as \"Status\", error_message as \"Message\"" if is_invalid else "'valid' as \"Status\", '' as \"Message\""}
            FROM {table_name}
            WHERE LOWER(TRIM(division)) = LOWER(TRIM(:div)) 
              AND LOWER(TRIM(district)) = LOWER(TRIM(:dist)) 
              AND LOWER(TRIM(upazila)) = LOWER(TRIM(:upz))
        """
        if is_invalid:
            fallback_sql += " ORDER BY id DESC"
        else:
            fallback_sql += " ORDER BY nid ASC"
        df = pd.read_sql(text(fallback_sql), db.connection(), params={"div": division, "dist": district, "upz": upazila})

    if df.empty:
        return None

    if "data" in df.columns and not df["data"].isnull().all():
        data_df = pd.DataFrame(df["data"].tolist())
        df = df.drop(columns=["data"])
        existing_cols = set(df.columns)
        cols_to_add = [c for c in data_df.columns if c not in existing_cols and c not in ["Status", "Message", "Excel_Row", "Cleaned_DOB", "Cleaned_NID"]]
        if cols_to_add:
            df = pd.concat([df, data_df[cols_to_add]], axis=1)
        if "Excel_Row" in data_df.columns:
            df["Excel_Row"] = data_df["Excel_Row"]
    else:
        if "data" in df.columns:
            df = df.drop(columns=["data"])
        df["Excel_Row"] = ""

    return df


# Helper for Excel output
def _get_export_df(df: pd.DataFrame) -> pd.DataFrame:
    """Strip system columns and prepare for display."""
    from .export_routes import _SYSTEM_COLS  # Ensure we use the global set
    keep = [c for c in df.columns if c not in _SYSTEM_COLS]
    return df[keep].copy() if keep else df


# ─────────────────────────────────────────────────────────────────────────────
# LIVE EXPORT ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/live-invalid", dependencies=[Depends(PermissionChecker("view_stats"))])
def upazila_live_export_invalid(
    division: str = None,
    district: str = None,
    upazila: str = None,
    upazila_id: int = None,
    fmt: str = "pdf",
    db: Session = Depends(get_db),
):
    """Stream a freshly generated export of ALL invalid records for the upazila."""
    df = get_live_records_df(db, division, district, upazila, is_invalid=True, upazila_id=upazila_id)
    if df is None:
        raise HTTPException(status_code=404, detail="No invalid records found for this upazila")

    safe_name = f"{district}_{upazila}".replace(" ", "_").replace("/", "_")
    os.makedirs("downloads/live", exist_ok=True)

    if fmt == "pdf":
        stats = {"total_rows": len(df), "issues": len(df), "converted_nid": 0}
        geo = {"division": division, "district": district, "upazila": upazila}
        path = generate_pdf_report(
            df, stats,
            additional_columns=[c for c in df.columns if c not in _SYSTEM_COLS],
            output_dir="downloads/live",
            original_filename=safe_name + "_live_invalid",
            geo=geo,
            invalid_only=True,
        )
        media = "application/pdf"
        dl_name = f"{safe_name}_live_invalid.pdf"
    else:
        path = os.path.join("downloads", "live", f"{safe_name}_live_invalid.xlsx")
        save_live_excel_nikosh(df, path, "Invalid Records", is_valid=False)
        media = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        dl_name = f"{safe_name}_live_invalid.xlsx"

    return FileResponse(path, media_type=media, filename=dl_name)


@router.get("/live", dependencies=[Depends(PermissionChecker("view_stats"))])
def upazila_live_export(
    division: str = None,
    district: str = None,
    upazila: str = None,
    upazila_id: int = None,
    fmt: str = "xlsx",
    db: Session = Depends(get_db),
):
    """Stream a freshly generated export of ALL valid records for the upazila."""
    df = get_live_records_df(db, division, district, upazila, is_invalid=False, upazila_id=upazila_id)
    if df is None:
        raise HTTPException(status_code=404, detail="No valid records found for this upazila")

    safe_name = f"{district}_{upazila}".replace(" ", "_").replace("/", "_")
    os.makedirs("downloads/live", exist_ok=True)

    if fmt == "pdf":
        stats = {"total_rows": len(df), "issues": 0, "converted_nid": 0}
        geo = {"division": division, "district": district, "upazila": upazila}
        df["Status"] = "success"
        df["Message"] = "Valid record"
        df["Excel_Row"] = range(2, len(df) + 2)
        df["Cleaned_DOB"] = df["DOB"]
        df["Cleaned_NID"] = df["NID"]
        path = generate_pdf_report(
            df, stats,
            additional_columns=[c for c in df.columns if c not in _SYSTEM_COLS],
            output_dir="downloads/live",
            original_filename=safe_name + "_live_valid",
            geo=geo,
        )
        media = "application/pdf"
        dl_name = f"{safe_name}_live_valid.pdf"
    else:
        path = os.path.join("downloads", "live", f"{safe_name}_live_valid.xlsx")
        save_live_excel_nikosh(df, path, "Valid Records", is_valid=True)
        media = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        dl_name = f"{safe_name}_live_valid.xlsx"

    return FileResponse(path, media_type=media, filename=dl_name)


# ─────────────────────────────────────────────────────────────────────────────
# RECHECK — fraud detection on stored valid records
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/recheck", dependencies=[Depends(PermissionChecker("view_admin"))])
async def upazila_recheck(
    division: str,
    district: str,
    upazila: str,
    fmt: str = "xlsx",
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """Re-run NID fraud checks on every stored valid record for the upazila."""
    from .validator import check_fake_nid

    records = db.query(ValidRecord).filter(
        ValidRecord.division == division,
        ValidRecord.district == district,
        ValidRecord.upazila == upazila,
    ).all()

    if not records:
        raise HTTPException(status_code=404, detail="No records found for this upazila")

    flagged = []
    for r in records:
        is_fake, reason = check_fake_nid(r.nid or "")
        if is_fake:
            row = {
                "NID": r.nid,
                "DOB": r.dob,
                "Name": r.name,
                "Batch_ID": r.batch_id,
                "Source_File": r.source_file,
                "Fraud_Reason": reason,
            }
            if r.data and isinstance(r.data, dict):
                for k, v in r.data.items():
                    if k not in row:
                        row[k] = v
            flagged.append(row)

    log_audit(db, current_user, "RECHECK", "valid_records", None, new_data={"upazila": upazila, "total": len(records), "flagged": len(flagged)})

    if fmt == "json":
        return {"total_checked": len(records), "flagged_count": len(flagged), "flagged": flagged}

    safe_name = f"{district}_{upazila}".replace(" ", "_").replace("/", "_")
    os.makedirs("downloads/recheck", exist_ok=True)

    if not flagged:
        return {"total_checked": len(records), "flagged_count": 0, "message": "No suspicious NIDs found in stored records"}

    df = pd.DataFrame(flagged)

    if fmt == "pdf":
        df["Status"] = "error"
        df["Message"] = df["Fraud_Reason"]
        df["Excel_Row"] = range(2, len(df) + 2)
        df["Cleaned_DOB"] = df["DOB"]
        df["Cleaned_NID"] = df["NID"]
        stats = {"total_rows": len(records), "issues": len(flagged), "converted_nid": 0}
        geo = {"division": division, "district": district, "upazila": upazila}
        path = generate_pdf_report(
            df, stats,
            additional_columns=["Fraud_Reason"],
            output_dir="downloads/recheck",
            original_filename=safe_name + "_recheck",
            geo=geo,
            invalid_only=False,
        )
        media = "application/pdf"
        dl_name = f"{safe_name}_fraud_report.pdf"
    else:
        path = os.path.join("downloads", "recheck", f"{safe_name}_recheck.xlsx")
        exclude = [
            "Excel_Row", "NID", "Cleaned_NID", "DOB", "Cleaned_DOB",
            "Name", "Status", "Message", "Division", "District",
            "Upazila", "Batch_ID", "Source_File", "Extracted_Name",
            "Card_No", "Master_Serial", "Mobile", "Fraud_Reason",
        ]
        export_df = df.drop(columns=[c for c in exclude if c in df.columns])
        export_df.to_excel(path, index=False)
        media = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        dl_name = f"{safe_name}_fraud_report.xlsx"

    return FileResponse(path, media_type=media, filename=dl_name)


# ─────────────────────────────────────────────────────────────────────────────
# TRAILING ZEROS PDF
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/trailing-zeros-pdf", dependencies=[Depends(PermissionChecker("view_stats"))])
async def download_trailing_zeros_pdf(
    division: str,
    district: str,
    upazila: str,
    db: Session = Depends(get_db),
):
    """Generate and download a PDF of records with 2+ trailing zeros."""
    valid_records = db.query(ValidRecord).filter(
        ValidRecord.division == division,
        ValidRecord.district == district,
        ValidRecord.upazila == upazila,
    ).order_by(ValidRecord.created_at.desc()).all()

    seen_nids = {}
    for r in valid_records:
        nid = str(r.nid or "").strip()
        if nid and nid not in seen_nids:
            seen_nids[nid] = r

    trailing_records = [r for nid, r in seen_nids.items() if len(nid) == 17 and nid.endswith("00")]

    if not trailing_records:
        raise HTTPException(status_code=404, detail="No records found with 17 digits and 2+ trailing zeros in this upazila")

    data = []
    for r in trailing_records:
        row_data = r.data if isinstance(r.data, dict) else {}
        row = {
            "Cleaned_NID": r.nid,
            "Cleaned_DOB": r.dob,
            "Extracted_Name": r.name,
            "Card_No": getattr(r, "card_no", ""),
            "Master_Serial": getattr(r, "master_serial", row_data.get("master_serial", "")),
            "Mobile": getattr(r, "mobile", row_data.get("mobile", "")),
            "Status": "valid",
            "Message": "Trailing 2+ zeros (Validated)",
        }
        for k, v in row_data.items():
            if k not in row:
                row[k] = v
        data.append(row)

    df = pd.DataFrame(data)
    geo = {"division": division, "district": district, "upazila": upazila}
    stats = {"total_rows": len(df), "issues": len(df), "converted_nid": 0}

    safe_name = f"{district}_{upazila}".replace(" ", "_").replace("/", "_") + "_trailing_zeros"
    os.makedirs("downloads/trailing_zeros", exist_ok=True)

    path = generate_pdf_report(
        df, stats,
        additional_columns=[],
        output_dir="downloads/trailing_zeros",
        original_filename=safe_name,
        geo=geo,
        invalid_only=True,
        custom_title="Food Friendly Program — Trailing Zeros Record Report",
        issues_label="Records with 2+ Zeros",
        status_filter="valid",
    )

    return FileResponse(path, media_type="application/pdf", filename=f"{safe_name}.pdf")


# ─────────────────────────────────────────────────────────────────────────────
# FILE DOWNLOAD — authenticated static file serving
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/download/{filename}", dependencies=[Depends(PermissionChecker("view_stats"))])
async def download_file(filename: str):
    """Serve a file from the downloads directory with path traversal protection."""
    safe_name = os.path.basename(filename)
    file_path = os.path.join("downloads", safe_name)
    downloads_dir = os.path.abspath("downloads")
    abs_path = os.path.abspath(file_path)

    if not abs_path.startswith(downloads_dir + os.sep):
        raise HTTPException(status_code=400, detail="Invalid file path")
    if not os.path.exists(abs_path):
        raise HTTPException(status_code=404, detail="File not found")

    if safe_name.endswith(".pdf"):
        media_type = "application/pdf"
    elif safe_name.endswith(".zip"):
        media_type = "application/zip"
    else:
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    return FileResponse(abs_path, media_type=media_type, filename=safe_name)


# ─────────────────────────────────────────────────────────────────────────────
# BULK DATA HELPERS — optimized for national-scale exports
# ─────────────────────────────────────────────────────────────────────────────

# Column exclusion list used across all export functions
_SYSTEM_COLS = frozenset([
    "Excel_Row", "NID", "Cleaned_NID", "DOB", "Cleaned_DOB",
    "Name", "Status", "Message", "Division", "District",
    "Upazila", "Batch_ID", "Source_File", "Extracted_Name",
    "Card_No", "Master_Serial", "Mobile", "Fraud_Reason",
    "upazila_id", "division_id", "district_id",
])


def _bulk_fetch_records(db: Session, is_invalid: bool = False,
                        divisions: list = None, districts: list = None,
                        upazila_ids: list = None) -> pd.DataFrame:
    """Fetch records in a SINGLE bulk query. Returns DataFrame with upazila_id for partitioning.
    
    Filters (all optional, applied as AND):
      - divisions: list of division names
      - districts: list of district names
      - upazila_ids: list of integer upazila_ids
    If no filter given, fetches ALL records.
    """
    table = "invalid_records" if is_invalid else "valid_records"
    cols = (
        "upazila_id, nid, dob, name, division, district, upazila, "
        "batch_id, source_file, data"
    )
    if is_invalid:
        cols += ", error_message"

    sql = f"SELECT {cols} FROM {table} WHERE upazila_id IS NOT NULL"
    params = {}

    if upazila_ids:
        sql += " AND upazila_id = ANY(:uids)"
        params["uids"] = upazila_ids
    elif districts:
        sql += " AND LOWER(TRIM(district)) = ANY(:dists)"
        params["dists"] = [d.strip().lower() for d in districts]
    elif divisions:
        sql += " AND LOWER(TRIM(division)) = ANY(:divs)"
        params["divs"] = [d.strip().lower() for d in divisions]

    if is_invalid:
        sql += " ORDER BY upazila_id, id DESC"
    else:
        sql += " ORDER BY upazila_id, nid ASC"

    df = pd.read_sql(text(sql), db.connection(), params=params)
    return df


def _expand_data_col(df: pd.DataFrame) -> pd.DataFrame:
    """Expand the JSON 'data' column and merge into the DataFrame."""
    if df.empty:
        return df
    if "data" in df.columns and not df["data"].isnull().all():
        data_df = pd.DataFrame(df["data"].tolist())
        df = df.drop(columns=["data"])
        existing = set(df.columns)
        cols_to_add = [c for c in data_df.columns if c not in existing and c not in _SYSTEM_COLS]
        if cols_to_add:
            df = pd.concat([df, data_df[cols_to_add]], axis=1)
        if "Excel_Row" in data_df.columns:
            df["Excel_Row"] = data_df["Excel_Row"]
    elif "data" in df.columns:
        df = df.drop(columns=["data"])
    return df


def _export_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Strip system columns from a DataFrame, keeping only user-visible data."""
    keep = [c for c in df.columns if c not in _SYSTEM_COLS]
    return df[keep].copy() if keep else df


def _write_checked_xlsx(export_df: pd.DataFrame, invalid_mask, path: str):
    """Write a single Excel file with Nikosh font and red-highlighted invalid rows.
    Uses xlsxwriter for maximum speed (C-optimized, single-pass).
    """
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        export_df.to_excel(writer, index=False, sheet_name="All Records")
        workbook = writer.book
        worksheet = writer.sheets["All Records"]

        fmt_nikosh = workbook.add_format({"font_name": "Nikosh", "font_size": 11})
        fmt_red = workbook.add_format({
            "font_name": "Nikosh", "font_size": 11,
            "bg_color": "#FFCCCC",
        })

        num_cols = len(export_df.columns)
        # Apply Nikosh font to ALL columns in one call
        worksheet.set_column(0, num_cols - 1, None, fmt_nikosh)

        # Highlight invalid rows — one set_row call per invalid row (not per cell)
        if invalid_mask is not None:
            for idx in invalid_mask:
                worksheet.set_row(idx + 1, None, fmt_red)  # +1 for header


def _safe_name(s: str) -> str:
    return str(s or "Unknown").replace(" ", "_").replace("/", "_")


def save_live_excel_nikosh(df: pd.DataFrame, path: str, sheet_name: str, is_valid: bool = True):
    """Utility function for live exports and background zips using high-speed xlsxwriter."""
    mask = []
    # If invalid-only, and we want to highlight all rows as red, we can pass a mask of all rows
    if not is_valid:
        mask = list(range(len(df)))
    
    _write_checked_xlsx(df, mask, path)


# ─────────────────────────────────────────────────────────────────────────────
# ZIP GENERATION — background tasks for bulk exports (OPTIMIZED)
# ─────────────────────────────────────────────────────────────────────────────

def _generate_zip_common(task_id: str, mode: str, filter_divisions: list = None,
                         filter_districts: list = None, filter_upazila_ids: list = None):
    """Unified ZIP generator for all export types.
    
    mode: 'checked' | 'valid' | 'invalid'
    
    Optimizations applied:
    1. Bulk DB fetch (2 queries max instead of N*2)
    2. xlsxwriter single-pass Excel generation
    3. Pre-partitioned data, workers do CPU-only file gen (no DB calls)
    4. Standardized 6 workers
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import shutil

    db = SessionLocal()
    task = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
    if not task:
        db.close()
        return

    task.status = "running"
    task.message = "Fetching records from database..."
    db.commit()

    try:
        # ── Step 1: Determine which upazilas to export ──
        stats_query = db.query(SummaryStats)
        if filter_upazila_ids:
            stats_query = stats_query.filter(SummaryStats.upazila_id.in_(filter_upazila_ids))
        elif filter_districts:
            stats_query = stats_query.filter(
                SummaryStats.district.in_(filter_districts)
            )
        elif filter_divisions:
            stats_query = stats_query.filter(
                SummaryStats.division.in_(filter_divisions)
            )

        if mode == "valid":
            stats_query = stats_query.filter(SummaryStats.valid > 0)
        elif mode == "invalid":
            stats_query = stats_query.filter(SummaryStats.invalid > 0)
        else:
            stats_query = stats_query.filter(SummaryStats.total > 0)

        entries = stats_query.order_by(
            SummaryStats.division, SummaryStats.district, SummaryStats.upazila
        ).all()

        if not entries:
            task.status = "error"
            task.error_details = "No records found matching the criteria"
            db.commit()
            db.close()
            return

        total_entries = len(entries)
        upazila_id_list = [e.upazila_id for e in entries if e.upazila_id]

        # ── Step 2: Bulk fetch ALL records in 1-2 queries ──
        task.message = f"Loading {total_entries} upazilas from database..."
        db.commit()

        need_valid = mode in ("checked", "valid")
        need_invalid = mode in ("checked", "invalid")

        df_all_valid = pd.DataFrame()
        df_all_invalid = pd.DataFrame()

        if need_valid:
            df_all_valid = _bulk_fetch_records(
                db, is_invalid=False, upazila_ids=upazila_id_list
            )
            df_all_valid = _expand_data_col(df_all_valid)

        if need_invalid:
            df_all_invalid = _bulk_fetch_records(
                db, is_invalid=True, upazila_ids=upazila_id_list
            )
            df_all_invalid = _expand_data_col(df_all_invalid)

        task.message = f"Data loaded. Generating files for {total_entries} upazilas..."
        db.commit()

        # ── Step 3: Partition data and prepare work units ──
        temp_dir = os.path.join("downloads", f"temp_zip_{task_id}")
        os.makedirs(temp_dir, exist_ok=True)

        mode_label = {"checked": "checked", "valid": "valid", "invalid": "invalid"}[mode]
        zip_filename = f"all_{mode_label}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.zip"
        zip_path = os.path.join("downloads", zip_filename)

        work_units = []
        for entry in entries:
            uid = entry.upazila_id
            div_name = _safe_name(entry.division)
            dist_name = _safe_name(entry.district)
            upz_name = _safe_name(entry.upazila)

            v_df = df_all_valid[df_all_valid["upazila_id"] == uid] if (need_valid and uid and not df_all_valid.empty) else pd.DataFrame()
            i_df = df_all_invalid[df_all_invalid["upazila_id"] == uid] if (need_invalid and uid and not df_all_invalid.empty) else pd.DataFrame()

            work_units.append({
                "div": div_name, "dist": dist_name, "upz": upz_name,
                "division": entry.division, "district": entry.district, "upazila": entry.upazila,
                "valid_df": v_df, "invalid_df": i_df,
            })

        # ── Step 4: Process in parallel (CPU-only, no DB calls) ──
        def process_unit(unit):
            """Generate Excel/PDF files for a single upazila. No DB access needed."""
            try:
                div = unit["div"]
                safe_base = f"{unit['dist']}_{unit['upz']}"
                results = []

                if mode == "checked":
                    # Combine valid + invalid for tested.xlsx
                    frames = []
                    if not unit["valid_df"].empty:
                        vdf = unit["valid_df"].copy()
                        vdf["Status"] = "valid"
                        frames.append(vdf)
                    if not unit["invalid_df"].empty:
                        idf = unit["invalid_df"].copy()
                        idf["Status"] = "error"
                        frames.append(idf)

                    if not frames:
                        return None

                    combined = pd.concat(frames, ignore_index=True)
                    export_df = _export_cols(combined)
                    # Find invalid row indices for highlighting
                    invalid_indices = []
                    if "Status" in combined.columns:
                        invalid_indices = combined.index[combined["Status"] == "error"].tolist()

                    tested_path = os.path.join(temp_dir, f"{safe_base}_tested.xlsx")
                    _write_checked_xlsx(export_df, invalid_indices, tested_path)
                    results.append((tested_path, f"{div}/{safe_base}_tested.xlsx"))

                    # PDF for invalid only
                    if not unit["invalid_df"].empty:
                        idf = unit["invalid_df"].copy()
                        idf["Status"] = "error"
                        idf["Message"] = idf.get("error_message", "Invalid")
                        if "Cleaned_NID" not in idf.columns and "nid" in idf.columns:
                            idf["Cleaned_NID"] = idf["nid"]
                        if "Cleaned_DOB" not in idf.columns and "dob" in idf.columns:
                            idf["Cleaned_DOB"] = idf["dob"]
                        if "Excel_Row" not in idf.columns:
                            idf["Excel_Row"] = range(2, len(idf) + 2)

                        pdf_stats = {"total_rows": len(idf), "issues": len(idf), "converted_nid": 0}
                        pdf_geo = {"division": unit["division"], "district": unit["district"], "upazila": unit["upazila"]}
                        try:
                            pdf_path = generate_pdf_report(
                                idf, pdf_stats, additional_columns=[],
                                output_dir=temp_dir, original_filename=f"{safe_base}_Invalid_Report",
                                geo=pdf_geo, invalid_only=True,
                            )
                            if pdf_path:
                                results.append((pdf_path, f"{div}/{safe_base}_Invalid_Report.pdf"))
                        except Exception as pdf_err:
                            logger.warning(f"PDF generation failed for {safe_base}: {pdf_err}")

                elif mode == "valid":
                    if unit["valid_df"].empty:
                        return None
                    export_df = _export_cols(unit["valid_df"])
                    t_file = os.path.join(temp_dir, f"{safe_base}_valid.xlsx")
                    save_live_excel_nikosh(export_df if not export_df.empty else unit["valid_df"], t_file, "Valid Records", is_valid=True)
                    results.append((t_file, f"{div}/{safe_base}_valid.xlsx"))

                elif mode == "invalid":
                    if unit["invalid_df"].empty:
                        return None
                    idf = unit["invalid_df"].copy()
                    # Excel
                    t_file = os.path.join(temp_dir, f"{safe_base}_invalid.xlsx")
                    save_live_excel_nikosh(idf, t_file, "Invalid Records", is_valid=False)
                    results.append((t_file, f"{div}/{safe_base}_invalid.xlsx"))

                    # PDF
                    idf["Status"] = "error"
                    idf["Message"] = idf.get("error_message", "Invalid")
                    if "Cleaned_NID" not in idf.columns and "nid" in idf.columns:
                        idf["Cleaned_NID"] = idf["nid"]
                    if "Cleaned_DOB" not in idf.columns and "dob" in idf.columns:
                        idf["Cleaned_DOB"] = idf["dob"]
                    if "Excel_Row" not in idf.columns:
                        idf["Excel_Row"] = range(2, len(idf) + 2)

                    pdf_s = {"total_rows": len(idf), "issues": len(idf), "converted_nid": 0}
                    pdf_g = {"division": unit["division"], "district": unit["district"], "upazila": unit["upazila"]}
                    try:
                        t_pdf = generate_pdf_report(
                            idf, pdf_s, additional_columns=[], output_dir=temp_dir,
                            original_filename=f"{safe_base}_invalid", geo=pdf_g, invalid_only=True,
                        )
                        if t_pdf:
                            results.append((t_pdf, f"{div}/{safe_base}_invalid.pdf"))
                    except Exception:
                        pass

                return results
            except Exception as entry_err:
                logger.error(f"Error generating files for {unit.get('dist')}_{unit.get('upz')}: {entry_err}")
                return None

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
            processed = 0
            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = {executor.submit(process_unit, u): u for u in work_units}
                for future in as_completed(futures):
                    processed += 1
                    try:
                        file_list = future.result()
                        if file_list:
                            for file_path, arc_name in file_list:
                                zipf.write(file_path, arcname=arc_name)
                                try:
                                    os.remove(file_path)
                                except OSError:
                                    pass
                    except Exception as loop_e:
                        logger.error(f"Error in zip thread: {loop_e}")

                    if processed % 10 == 0 or processed == total_entries:
                        task.progress = int((processed / total_entries) * 100)
                        task.message = f"Processing {processed}/{total_entries} upazilas..."
                        db.commit()

        if not os.path.exists(zip_path) or os.path.getsize(zip_path) < 100:
            if os.path.exists(zip_path):
                os.remove(zip_path)
            raise Exception("Zip file generation failed or produced an empty file")

        task.progress = 100
        task.status = "completed"
        task.message = f"{mode_label.title()} zip generated successfully"
        task.result_url = f"/api/export/download/{urllib.parse.quote(zip_filename)}"
        db.commit()

    except Exception as e:
        logger.error(f"{mode}-zip generation failed: {str(e)}")
        task.status = "error"
        task.error_details = str(e)[:500]
        task.message = f"Failed to create {mode} zip"
        db.commit()
    finally:
        temp_dir = os.path.join("downloads", f"temp_zip_{task_id}")
        if os.path.exists(temp_dir):
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
        db.close()


# ── Endpoint wrappers ────────────────────────────────────────────────────────

@router.post("/zip-checked", dependencies=[Depends(PermissionChecker("view_stats"))])
def start_checked_zip_task(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Start background task to generate a ZIP with tested Excel + Invalid PDF per upazila."""
    task_id = str(uuid.uuid4())
    task = BackgroundTask(id=task_id, task_name="export_checked_zip", user_id=current_user.id, status="pending", progress=0, message="Starting generation...")
    db.add(task)
    db.commit()
    background_tasks.add_task(_generate_zip_common, task_id, "checked")
    return {"task_id": task_id, "status": "started"}


@router.post("/zip-valid", dependencies=[Depends(PermissionChecker("view_stats"))])
def start_valid_zip_task(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    task_id = str(uuid.uuid4())
    task = BackgroundTask(id=task_id, task_name="export_valid_zip", user_id=current_user.id, status="pending", progress=0, message="Starting generation...")
    db.add(task)
    db.commit()
    background_tasks.add_task(_generate_zip_common, task_id, "valid")
    return {"task_id": task_id, "status": "started"}


@router.post("/zip-invalid", dependencies=[Depends(PermissionChecker("view_stats"))])
def start_invalid_zip_task(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    task_id = str(uuid.uuid4())
    task = BackgroundTask(id=task_id, task_name="export_invalid_zip", user_id=current_user.id, status="pending", progress=0, message="Starting generation...")
    db.add(task)
    db.commit()
    background_tasks.add_task(_generate_zip_common, task_id, "invalid")
    return {"task_id": task_id, "status": "started"}


# ─────────────────────────────────────────────────────────────────────────────
# SELECTIVE DOWNLOAD — export selected divisions/districts/upazilas
# ─────────────────────────────────────────────────────────────────────────────

from pydantic import BaseModel
from typing import List, Optional

class SelectiveExportRequest(BaseModel):
    mode: str = "checked"  # checked | valid | invalid
    divisions: Optional[List[str]] = None
    districts: Optional[List[str]] = None
    upazila_ids: Optional[List[int]] = None


@router.post("/zip-selected", dependencies=[Depends(PermissionChecker("view_stats"))])
def start_selected_zip_task(
    req: SelectiveExportRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Start background task to generate a ZIP for selected divisions/districts/upazilas only."""
    if not req.divisions and not req.districts and not req.upazila_ids:
        raise HTTPException(status_code=400, detail="At least one filter (divisions, districts, or upazila_ids) is required")

    label_parts = []
    if req.divisions:
        label_parts.append(f"{len(req.divisions)} div")
    if req.districts:
        label_parts.append(f"{len(req.districts)} dist")
    if req.upazila_ids:
        label_parts.append(f"{len(req.upazila_ids)} upz")
    label = ", ".join(label_parts)

    task_id = str(uuid.uuid4())
    task = BackgroundTask(
        id=task_id, task_name=f"export_selected_{req.mode}",
        user_id=current_user.id, status="pending", progress=0,
        message=f"Starting {req.mode} export for {label}...",
    )
    db.add(task)
    db.commit()
    background_tasks.add_task(
        _generate_zip_common, task_id, req.mode,
        filter_divisions=req.divisions,
        filter_districts=req.districts,
        filter_upazila_ids=req.upazila_ids,
    )
    return {"task_id": task_id, "status": "started"}
