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
    ExportHistory, ExportTracking, SystemConfig, TrailingZeroWhitelist,
)
from .auth import get_current_user
from .rbac import PermissionChecker
from .audit import log_audit
from .pdf_generator import generate_pdf_report
from .validator import ensure_dob_format

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
    # New competitive filters for district-level batching
    upazila_ids: list = None,
    districts: list = None,
    divisions: list = None,
):
    """Fetch live records and return a formatted DataFrame. Optimized for national scale."""
    table_name = "invalid_records" if is_invalid else "valid_records"

    # Select native columns instead of hitting JSONB for core identifiers
    query_sql = f"""
        SELECT 
            nid as "NID", nid as "Cleaned_NID",
            dob as "DOB", dob as "Cleaned_DOB",
            name as "Name", division as "Division", district as "District",
            upazila as "Upazila", batch_id as "Batch_ID", source_file as "Source_File",
            mobile as "Mobile", card_no as "Card_No",
            data,
            {"'error' as \"Status\", error_message as \"Message\"" if is_invalid else "'valid' as \"Status\", '' as \"Message\""}
        FROM {table_name}
        WHERE 1=1
    """

    params = {}
    # Priority 1: Indexed integer ID lookup — fastest path (uses ix_*_upazila_id)
    if upazila_id:
        query_sql += " AND upazila_id = :uid"
        params["uid"] = upazila_id
    elif upazila_ids:
        query_sql += " AND upazila_id = ANY(:uids)"
        params["uids"] = upazila_ids
    elif districts:
        query_sql += " AND district = ANY(:dists)"
        params["dists"] = districts
    elif divisions:
        query_sql += " AND division = ANY(:divs)"
        params["divs"] = divisions
    elif division and district and upazila:
        # Exact match — allows B-tree index on (division, district, upazila) to be used
        query_sql += " AND division = :div AND district = :dist AND upazila = :upz"
        params.update({"div": division, "dist": district, "upz": upazila})
    else:
        query_sql += " AND 1=0"

    if is_invalid:
        query_sql += " ORDER BY id DESC"
    else:
        query_sql += " ORDER BY nid ASC"

    # streaming with chunksize if needed, but for district-level ~500k is okay for memory in most 16GB+ VMs
    df = pd.read_sql(text(query_sql), db.connection(), params=params)

    if df.empty:
        return None

    # Performance optimization: Don't expand JSON unless we absolutely have to.
    # We now have native columns for core fields.
    if "data" in df.columns and not df["data"].isnull().all():
        # Only expand the FIRST row to see what extra fields we might need
        # This is a compromise to keep memory usage low
        try:
            sample_data = df.iloc[0]["data"]
            if isinstance(sample_data, dict):
                # If we need and extra fields beyond our native ones, we'll expand.
                # Standard CSV headers we care about often are in data.
                # At nationwide scale, we should actually avoid this unless it's a small set.
                if len(df) < 50000:
                    data_df = pd.DataFrame(df["data"].tolist())
                    df = df.drop(columns=["data"])
                    existing_cols = set(df.columns)
                    cols_to_add = [c for c in data_df.columns if c not in existing_cols and c not in ["Status", "Message", "Excel_Row", "Cleaned_DOB", "Cleaned_NID"]]
                    if cols_to_add:
                        df = pd.concat([df, data_df[cols_to_add]], axis=1)
                    if "Excel_Row" in data_df.columns:
                        df["Excel_Row"] = data_df["Excel_Row"]
                else:
                    # Large dataset: Just drop data to save RAM
                    df = df.drop(columns=["data"])
        except:
             df = df.drop(columns=["data"])
    else:
        if "data" in df.columns:
            df = df.drop(columns=["data"])

    if "Excel_Row" not in df.columns:
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
            additional_columns=[],  # Fixed 5-column layout: Row, DOB, NID, Status, Message
            output_dir="downloads/live",
            original_filename=safe_name + "_live_invalid",
            geo=geo,
            invalid_only=True,
        )
        media = "application/pdf"
        dl_name = f"{safe_name}_live_invalid.pdf"
    else:
        df = ensure_dob_format(df)
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
        df = ensure_dob_format(df)
        path = os.path.join("downloads", "live", f"{safe_name}_live_valid.xlsx")
        save_live_excel_nikosh(df, path, "Valid Records", is_valid=True)
        media = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        dl_name = f"{safe_name}_live_valid.xlsx"

    return FileResponse(path, media_type=media, filename=dl_name)


# ── Internal columns added by validator/upload, NOT present in the original uploaded file ──
_UPLOAD_INTERNAL_COLS = frozenset({
    "Status", "Message",
    "Cleaned_NID", "Cleaned_DOB", "DOB_Year",
    "Extracted_Name", "Card_No", "Master_Serial",
    "Mobile", "Fraud_Reason", "Excel_Row",
    "division_id", "district_id", "upazila_id",
    # Canonical keys that used to leak into old data JSON
    "NID", "DOB", "Name", "Division", "District", "Upazila",
    "Batch_ID", "Source_File", "Cleaned_DOB", "DOB_Year",
})

# ── Fallback column order if original headers are missing ──
_FALLBACK_COLUMN_ORDER = [
    "serial_no", "card_no", "name_bn", "father_husband_name", "dob",
    "occupation", "address", "ward", "nid_number", "mobile",
    "spouse_name", "spouse_nid", "spouse_dob", "dealer_name",
    "dealer_nid", "dealer_mobile", "name_en", "gender", "religion"
]

# removed _get_reverse_header_map


def _restore_original_headers(df: pd.DataFrame, stored_headers: list, forward_map: dict) -> pd.DataFrame:
    """
    Restore exact column names and order from the original uploaded Excel file.

    The DB data JSON uses canonical keys ('nid_number', 'name_bn', etc.).
    stored_headers has the EXACT original Bangla strings from the uploaded file.

    Correct direction:
      For each stored_header string, find its canonical key in the FORWARD map,
      then rename that canonical column back to the stored_header.

      stored_header = 'জাতীয় পরিচয়পত্র নম্বর'
      forward_map['জাতীয় পরিচয়পত্র নম্বর'] = 'nid_number'
      df['nid_number'] → renamed to 'জাতীয় পরিচয়পত্র নম্বর'
    """
    if not stored_headers:
        return df

    # Build rename: {canonical_col_in_df → stored_header_bangla}
    # Use .strip() on the lookup key to handle trailing whitespace in stored headers
    rename = {}
    used_canonicals = set()
    for stored_header in stored_headers:
        canonical = forward_map.get(stored_header) or forward_map.get(stored_header.strip())
        if canonical and canonical in df.columns and canonical not in used_canonicals:
            rename[canonical] = stored_header
            used_canonicals.add(canonical)

    if rename:
        df = df.rename(columns=rename)

    # Reorder + pad to exactly match stored_headers; all system cols dropped silently
    result_cols = {}
    for h in stored_headers:
        result_cols[h] = df[h] if h in df.columns else pd.Series([""] * len(df), dtype=str)

    return pd.DataFrame(result_cols)



def _sort_by_fallback(df: pd.DataFrame) -> pd.DataFrame:
    """Sort columns by a predefined fallback order for cases where original headers are missing."""
    existing = set(df.columns)
    ordered = []
    
    # 1. Add columns from fallback order that exist in the DF
    for col in _FALLBACK_COLUMN_ORDER:
        if col in existing:
            ordered.append(col)
            existing.remove(col)
            
    # 2. Add any remaining columns (system columns or extra data)
    ordered.extend(sorted(list(existing)))
    
    return df[ordered]


def _build_original_checked_df(valid_data: list, invalid_data: list):
    """
    Build a DataFrame from raw data dicts (original JSONB from uploaded files).
    Rows sorted by Excel_Row to restore original file order.

    Returns: (df, invalid_mask, warning_mask)
      invalid_mask  — 0-based row indices to highlight RED  (Status == 'error')
      warning_mask  — 0-based row indices to highlight YELLOW (Status == 'warning')
    """
    if not valid_data and not invalid_data:
        return None, [], []

    all_data = list(valid_data) + list(invalid_data)
    df = pd.DataFrame(all_data)
    
    # STRICT DOB FORMATTING: Ensure all original DOB-like columns are formatted as YYYY-MM-DD
    df = ensure_dob_format(df)

    def _extract_masks(frame: pd.DataFrame):
        """Build red/yellow index lists from Status column."""
        red, yellow = [], []
        if "Status" in frame.columns:
            statuses = frame["Status"].tolist()
            for i, s in enumerate(statuses):
                if s == "error":
                    red.append(i)
                elif s == "warning":
                    yellow.append(i)
        else:
            # Fall back: anything from invalid_data → red
            n_valid = len(valid_data)
            red = list(range(n_valid, len(frame)))
        return red, yellow

    # ── Sort globally by Excel_Row to restore original upload order ──
    if "Excel_Row" in df.columns:
        try:
            df["Excel_Row"] = pd.to_numeric(df["Excel_Row"], errors="coerce")
            df = df.sort_values("Excel_Row", na_position="last").reset_index(drop=True)
        except Exception:
            pass

    invalid_mask, warning_mask = _extract_masks(df)

    # ── Drop every internal column we injected — not in the original file ──
    drop_cols = [c for c in df.columns if c in _UPLOAD_INTERNAL_COLS]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    return df, invalid_mask, warning_mask


def _fetch_raw_data_dicts(db: Session, table: str, where: str, params: dict) -> list:
    """Fetch data JSONB as a list of dicts from a table."""
    import json as _json
    rows = db.execute(text(f"SELECT data FROM {table} WHERE {where} ORDER BY id ASC"), params).fetchall()
    result = []
    for (d,) in rows:
        if isinstance(d, str):
            try:
                d = _json.loads(d)
            except Exception:
                d = {}
        result.append(d or {})
    return result


@router.get("/live-checked", dependencies=[Depends(PermissionChecker("view_stats"))])
def upazila_live_export_checked(
    division: str = None,
    district: str = None,
    upazila: str = None,
    upazila_id: int = None,
    db: Session = Depends(get_db),
):
    """
    Download ALL records for the upazila with the ORIGINAL uploaded columns.
    Invalid rows are highlighted red, converted-NID rows yellow.
    """
    # Determine WHERE clause (priority: indexed ID > string names)
    if upazila_id:
        where = "upazila_id = :uid"
        params = {"uid": upazila_id}
    elif division and district and upazila:
        where = "division = :div AND district = :dist AND upazila = :upz"
        params = {"div": division, "dist": district, "upz": upazila}
    else:
        raise HTTPException(status_code=400, detail="Must provide upazila_id or full location names")

    # Fetch stored original column headers for this upazila
    from sqlalchemy import func as _func
    stored_headers = None
    if upazila_id:
        ss = db.query(SummaryStats).filter(SummaryStats.upazila_id == upazila_id).first()
    elif district and upazila:
        ss = db.query(SummaryStats).filter(
            _func.lower(_func.trim(SummaryStats.district)) == _func.lower(district.strip()),
            _func.lower(_func.trim(SummaryStats.upazila)) == _func.lower(upazila.strip()),
        ).first()
    else:
        ss = None
    if ss and ss.column_headers:
        stored_headers = ss.column_headers

    valid_data = _fetch_raw_data_dicts(db, "valid_records", where, params)
    invalid_data = _fetch_raw_data_dicts(db, "invalid_records", where, params)

    df, invalid_mask, warning_mask = _build_original_checked_df(valid_data, invalid_data)

    if df is None or df.empty:
        raise HTTPException(status_code=404, detail="No records found for this upazila")

    # Restore exact original headers (Bangla column names + original order)
    if stored_headers:
        from .models import HeaderAlias
        aliases = db.query(HeaderAlias).all()
        forward_map = {a.original_header: a.canonical_key for a in aliases}
        df = _restore_original_headers(df, stored_headers, forward_map)
    else:
        logger.warning("No stored column_headers for upazila_id=%s — exporting with fallback sort", upazila_id)
        df = _sort_by_fallback(df)

    df = ensure_dob_format(df)

    safe_name = f"{district}_{upazila}".replace(" ", "_").replace("/", "_") if district and upazila else "upazila"
    os.makedirs("downloads/live", exist_ok=True)
    path = os.path.join("downloads", "live", f"{safe_name}_checked.xlsx")

    _write_checked_xlsx(df, invalid_mask, path, warning_mask=warning_mask)

    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"{safe_name}_checked.xlsx",
    )



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

    tz_limit_conf = db.query(SystemConfig).filter(SystemConfig.key == "trailing_zero_limit").first()
    # Default to 2 if missing/invalid, but allow user to set 0 to disable
    if tz_limit_conf and tz_limit_conf.value and tz_limit_conf.value.strip().isdigit():
        tz_limit = int(tz_limit_conf.value.strip())
    else:
        tz_limit = 2

    tz_whitelist_records = db.query(TrailingZeroWhitelist.nid).all()
    tz_whitelist = {r[0] for r in tz_whitelist_records}

    flagged = []
    for r in records:
        is_fake, reason = check_fake_nid(r.nid or "", tz_limit=tz_limit, tz_whitelist=tz_whitelist)
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
    """Serve a file from the downloads directory with path traversal protection and subdirectory check."""
    safe_name = os.path.basename(filename)
    file_path = os.path.join("downloads", safe_name)
    downloads_dir = os.path.abspath("downloads")
    abs_path = os.path.abspath(file_path)

    # Check subdirectories if not directly in downloads/
    if not os.path.exists(abs_path):
        for sub in ["live", "recheck", "archives"]:
            candidate = os.path.join(downloads_dir, sub, safe_name)
            if os.path.exists(candidate):
                abs_path = candidate
                break

    if not abs_path.startswith(downloads_dir):
        raise HTTPException(status_code=400, detail="Invalid file path")
    return FileResponse(abs_path)


_SYSTEM_COLS = frozenset([
    "Excel_Row", "NID", "Cleaned_NID", "DOB", "Cleaned_DOB",
    "Name", "Status", "Message", "Division", "District",
    "Upazila", "Batch_ID", "Source_File", "Extracted_Name",
    "Card_No", "Master_Serial", "Mobile", "Fraud_Reason",
    "upazila_id", "division_id", "district_id",
    # Aliases
    "Mobile", "Card_No",
])


def _export_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Strip system columns from a DataFrame, keeping only user-visible data."""
    keep = [c for c in df.columns if c not in _SYSTEM_COLS]
    return df[keep].copy() if keep else df


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

        from .models import HeaderAlias
        aliases = db.query(HeaderAlias).all()
        forward_map = {a.original_header: a.canonical_key for a in aliases}

        # ── Step 2 & 3: Group by District and Batch Fetch ──
        temp_dir = os.path.join("downloads", f"temp_zip_{task_id}")
        os.makedirs(temp_dir, exist_ok=True)

        mode_label = {"checked": "checked", "valid": "valid", "invalid": "invalid"}[mode]
        zip_filename = f"all_{mode_label}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.zip"
        zip_path = os.path.join("downloads", zip_filename)
        
        need_valid = mode in ("checked", "valid")
        need_invalid = mode in ("checked", "invalid")

        # Group entries by (division, district) for batching
        from collections import defaultdict
        grouped_work = defaultdict(list)
        for entry in entries:
            grouped_work[(entry.division, entry.district)].append(entry)

        # Worker for CPU-heavy tasks (Excel/PDF generation)
        def process_upazila_files(unit_data, u_v, u_i):
            """Generates files for a single upazila. No DB calls inside."""
            div_name = _safe_name(unit_data["division"])
            safe_base = f"{_safe_name(unit_data['district'])}_{_safe_name(unit_data['upazila'])}"
            results = []

            if mode == "checked":
                # u_v and u_i are lists of original data dicts (JSONB)
                df, invalid_mask, warning_mask = _build_original_checked_df(u_v, u_i)
                if df is None or df.empty:
                    return None

                # Restore original headers if available
                stored_headers = unit_data.get("column_headers")
                if stored_headers:
                    df = _restore_original_headers(df, stored_headers, forward_map)

                df = ensure_dob_format(df)
                tested_path = os.path.join(temp_dir, f"{safe_base}_tested.xlsx")
                _write_checked_xlsx(df, invalid_mask, tested_path, warning_mask=warning_mask)
                results.append((tested_path, f"{div_name}/{safe_base}_tested.xlsx"))

                # Also include the invalid PDF report in the ZIP
                if u_i:
                    try:
                        idf_pdf = pd.DataFrame(u_i)
                        idf_pdf["Status"] = "error"
                        nid_col = next((c for c in ["NID", "Cleaned_NID"] if c in idf_pdf.columns), None)
                        dob_col = next((c for c in ["DOB", "Cleaned_DOB"] if c in idf_pdf.columns), None)
                        if nid_col and "Cleaned_NID" not in idf_pdf.columns:
                            idf_pdf["Cleaned_NID"] = idf_pdf[nid_col]
                        if dob_col and "Cleaned_DOB" not in idf_pdf.columns:
                            idf_pdf["Cleaned_DOB"] = idf_pdf[dob_col]
                        if "Excel_Row" not in idf_pdf.columns:
                            idf_pdf["Excel_Row"] = range(2, len(idf_pdf) + 2)
                        if "Message" not in idf_pdf.columns:
                            idf_pdf["Message"] = "Invalid record"
                        pdf_stats = {"total_rows": len(idf_pdf), "issues": len(idf_pdf), "converted_nid": 0}
                        pdf_geo = {"division": unit_data["division"], "district": unit_data["district"], "upazila": unit_data["upazila"]}
                        pdf_path = generate_pdf_report(idf_pdf, pdf_stats, additional_columns=[], output_dir=temp_dir, original_filename=f"{safe_base}_Invalid", geo=pdf_geo, invalid_only=True)
                        if pdf_path:
                            results.append((pdf_path, f"{div_name}/{safe_base}_Invalid_Report.pdf"))
                    except Exception as e:
                        logger.warning(f"PDF generation skipped for {safe_base}: {e}")

            elif mode == "valid":
                if u_v is None or (hasattr(u_v, 'empty') and u_v.empty): return None
                from .export_routes import _export_cols
                export_df = _export_cols(u_v)
                export_df = ensure_dob_format(export_df)
                t_file = os.path.join(temp_dir, f"{safe_base}_valid.xlsx")
                save_live_excel_nikosh(export_df, t_file, "Valid Records", is_valid=True)
                results.append((t_file, f"{div_name}/{safe_base}_valid.xlsx"))

            elif mode == "invalid":
                if u_i is None or (hasattr(u_i, 'empty') and u_i.empty): return None
                u_i_formatted = ensure_dob_format(u_i)
                t_file = os.path.join(temp_dir, f"{safe_base}_invalid.xlsx")
                save_live_excel_nikosh(u_i_formatted, t_file, "Invalid Records", is_valid=False)
                results.append((t_file, f"{div_name}/{safe_base}_invalid.xlsx"))
                # PDF
                idf_pdf = u_i.copy()
                idf_pdf["Status"] = "error"
                idf_pdf["Message"] = idf_pdf.get("error_message", "Invalid")
                idf_pdf["Excel_Row"] = range(2, len(idf_pdf) + 2)
                pdf_stats = {"total_rows": len(idf_pdf), "issues": len(idf_pdf), "converted_nid": 0}
                pdf_geo = {"division": unit_data["division"], "district": unit_data["district"], "upazila": unit_data["upazila"]}
                try:
                    t_pdf = generate_pdf_report(idf_pdf, pdf_stats, additional_columns=[], output_dir=temp_dir, original_filename=f"{safe_base}_invalid", geo=pdf_geo, invalid_only=True)
                    if t_pdf: results.append((t_pdf, f"{div_name}/{safe_base}_invalid.pdf"))
                except: pass

            return results

        # ── Step 4: Grouped Processing ──
        processed = 0
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
            with ThreadPoolExecutor(max_workers=6) as executor:
                for (div, dist), sub_entries in grouped_work.items():
                    task.message = f"Fetching batch for {dist}..."
                    db.commit()

                    # 1. Bulk Fetch for whole district
                    uids = [e.upazila_id for e in sub_entries if e.upazila_id]
                    no_ids = [e for e in sub_entries if not e.upazila_id]
                    dist_v_df = pd.DataFrame()
                    dist_i_df = pd.DataFrame()

                    # For checked mode: fetch raw data JSONB (original columns)
                    dist_raw_valid = {}   
                    dist_raw_invalid = {} 

                    if mode == "checked":
                        import json as _json
                        def _parse(d):
                            if isinstance(d, str):
                                try: return _json.loads(d)
                                except: return {}
                            return d or {}

                        # Bulk fetch for perfectly mapped upazilas
                        if uids:
                            raw_v = db.execute(
                                text("SELECT upazila_id, data FROM valid_records WHERE upazila_id = ANY(:uids)"),
                                {"uids": uids}
                            ).fetchall()
                            raw_i = db.execute(
                                text("SELECT upazila_id, data FROM invalid_records WHERE upazila_id = ANY(:uids)"),
                                {"uids": uids}
                            ).fetchall()
                            for uid, d in raw_v:
                                dist_raw_valid.setdefault(uid, []).append(_parse(d))
                            for uid, d in raw_i:
                                dist_raw_invalid.setdefault(uid, []).append(_parse(d))
                        
                        # Fallback query for orphaned unmapped upazilas using entry.id as a pseudo-key
                        if no_ids:
                            for entry in no_ids:
                                raw_v = db.execute(
                                    text("SELECT :uid, data FROM valid_records WHERE division = :div AND district = :dist AND upazila = :upz"),
                                    {"uid": entry.id, "div": entry.division, "dist": entry.district, "upz": entry.upazila}
                                ).fetchall()
                                raw_i = db.execute(
                                    text("SELECT :uid, data FROM invalid_records WHERE division = :div AND district = :dist AND upazila = :upz"),
                                    {"uid": entry.id, "div": entry.division, "dist": entry.district, "upz": entry.upazila}
                                ).fetchall()
                                for uid, d in raw_v:
                                    dist_raw_valid.setdefault(uid, []).append(_parse(d))
                                for uid, d in raw_i:
                                    dist_raw_invalid.setdefault(uid, []).append(_parse(d))

                    if mode in ("valid", "invalid"):  # checked uses raw path above
                        if need_valid:
                            dist_v_df = get_live_records_df(db, is_invalid=False, upazila_ids=uids if uids else None, division=div if not uids else None, district=dist if not uids else None)
                        if need_invalid:
                            dist_i_df = get_live_records_df(db, is_invalid=True, upazila_ids=uids if uids else None, division=div if not uids else None, district=dist if not uids else None)

                    # 2. Slice and parallelize upazilas in this district
                    futures = []
                    for entry in sub_entries:
                        # For checked mode: use raw data dicts keyed by upazila_id or entry.id (fallback)
                        if mode == "checked":
                            key = entry.upazila_id if entry.upazila_id else entry.id
                            raw_v_slice = dist_raw_valid.get(key, [])
                            raw_i_slice = dist_raw_invalid.get(key, [])

                            # ── upazila_id mismatch fallback ─────────────────────────────────
                            # The bulk fetch above uses integer upazila_id. It silently misses
                            # records where upazila_id is either NULL (never backfilled) or holds
                            # a WRONG value (uploaded with stale/renamed geo, then backfill was
                            # not run, or a rename changed the canonical ID).
                            # If the ID-based slice is empty but SummaryStats says data exists,
                            # re-query by text name (no upazila_id filter) as the authoritative
                            # source of truth. Deduplication is not needed because the ID path
                            # already returned nothing for this upazila.
                            if not raw_i_slice and entry.invalid and entry.invalid > 0:
                                fallback_i = db.execute(
                                    text("SELECT data FROM invalid_records"
                                         " WHERE LOWER(TRIM(district)) = LOWER(TRIM(:dist))"
                                         " AND LOWER(TRIM(upazila)) = LOWER(TRIM(:upz))"),
                                    {"dist": entry.district, "upz": entry.upazila}
                                ).fetchall()
                                raw_i_slice = [_parse(d) for (d,) in fallback_i]
                                if raw_i_slice:
                                    logger.info("ZIP checked fallback: found %d invalid rows for %s via text-match (upazila_id mismatch)",
                                                len(raw_i_slice), entry.upazila)
                                else:
                                    logger.warning("ZIP checked: STALE STATS — SummaryStats.invalid=%d for %s/%s (upazila_id=%s) but 0 rows found in invalid_records by both ID and text-match. Run 'Refresh All Stats' to fix.",
                                                   entry.invalid, entry.district, entry.upazila, entry.upazila_id)

                            if not raw_v_slice and entry.valid and entry.valid > 0:
                                fallback_v = db.execute(
                                    text("SELECT data FROM valid_records"
                                         " WHERE LOWER(TRIM(district)) = LOWER(TRIM(:dist))"
                                         " AND LOWER(TRIM(upazila)) = LOWER(TRIM(:upz))"),
                                    {"dist": entry.district, "upz": entry.upazila}
                                ).fetchall()
                                raw_v_slice = [_parse(d) for (d,) in fallback_v]
                                if raw_v_slice:
                                    logger.info("ZIP checked fallback: found %d valid rows for %s via text-match (upazila_id mismatch)",
                                                len(raw_v_slice), entry.upazila)

                            u_v = raw_v_slice
                            u_i = raw_i_slice
                        else:
                            # Slice DataFrames for valid/invalid modes
                            if dist_v_df is not None and not dist_v_df.empty:
                                u_v = dist_v_df[dist_v_df["Upazila"] == entry.upazila]
                            else:
                                u_v = pd.DataFrame()
                            if dist_i_df is not None and not dist_i_df.empty:
                                u_i = dist_i_df[dist_i_df["Upazila"] == entry.upazila]
                            else:
                                u_i = pd.DataFrame()

                        unit_data = {
                            "division": entry.division,
                            "district": entry.district,
                            "upazila": entry.upazila,
                            "upazila_id": entry.upazila_id,
                            "column_headers": entry.column_headers
                        }
                        futures.append(executor.submit(process_upazila_files, unit_data, u_v, u_i))

                    # 3. Collect from workers and write to ZIP
                    for future in as_completed(futures):
                        processed += 1
                        file_res = future.result()
                        if file_res:
                            for f_path, arc_name in file_res:
                                zipf.write(f_path, arcname=arc_name)
                                try: os.remove(f_path)
                                except: pass

                    # Update progress
                    task.progress = int((processed / total_entries) * 100)
                    task.message = f"Processed {processed}/{total_entries} upazilas..."
                    db.commit()

        if not os.path.exists(zip_path) or os.path.getsize(zip_path) < 100:
            if os.path.exists(zip_path): os.remove(zip_path)
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
# ─────────────────────────────────────────────────────────────────────────────
# STANDARD CSV EXPORT — Standardized headers + pipe-separated (|)
# ─────────────────────────────────────────────────────────────────────────────

STANDARD_HEADERS = [
    "card_no", "serial_no", "name_bn", "name_en", "father_husband_name", "dob",
    "nid_number", "mobile", "gender", "religion", "occupation", "spouse_name",
    "spouse_nid", "spouse_dob", "address", "ward", "union_name", "dealer_name",
    "dealer_nid", "dealer_mobile", "beneficiary_type", "division", "district", "upazila"
]

class StandardCSVExportRequest(BaseModel):
    divisions: Optional[List[str]] = None
    districts: Optional[List[str]] = None
    upazila_ids: Optional[List[int]] = None
    only_new: bool = False

def _generate_standard_csv_task(task_id: str, filter_divisions: list = None,
                                filter_districts: list = None, filter_upazila_ids: list = None,
                                only_new: bool = False):
    """Background task to generate standard CSV for national-scale reports."""
    import csv
    db = SessionLocal()
    task = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
    if not task:
        db.close()
        return

    task.status = "running"
    task.message = "Preparing standardized CSV generation..."
    db.commit()

    try:
        # Determine tracking scope
        scope = "all"
        scope_value = None
        if filter_upazila_ids and len(filter_upazila_ids) == 1:
            scope = "upazila"
            scope_value = str(filter_upazila_ids[0])
        elif filter_districts and len(filter_districts) == 1:
            scope = "district"
            scope_value = filter_districts[0]
        elif filter_divisions and len(filter_divisions) == 1:
            scope = "division"
            scope_value = filter_divisions[0]

        last_timestamp = None
        if only_new:
            tracking = db.query(ExportTracking).filter(
                ExportTracking.scope == scope,
                ExportTracking.scope_value == scope_value
            ).first()
            if tracking:
                last_timestamp = tracking.last_exported_at

        # Build Query with Multiselection support (OR logic)
        sql = "SELECT data, division, district, upazila, created_at FROM valid_records WHERE 1=1"
        where_clauses = []
        params = {}
        
        if filter_upazila_ids:
            where_clauses.append("upazila_id = ANY(:uids)")
            params["uids"] = filter_upazila_ids
        
        if filter_districts:
            where_clauses.append("LOWER(TRIM(district)) = ANY(:dists)")
            params["dists"] = [d.strip().lower() for d in filter_districts]
            
        if filter_divisions:
            where_clauses.append("LOWER(TRIM(division)) = ANY(:divs)")
            params["divs"] = [d.strip().lower() for d in filter_divisions]

        if where_clauses:
            sql += " AND (" + " OR ".join(where_clauses) + ")"

        if last_timestamp:
            sql += " AND created_at > :ts"
            params["ts"] = last_timestamp
        
        sql += " ORDER BY created_at ASC"

        filename = f"standard_{scope}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = os.path.join("downloads", filename)
        os.makedirs("downloads", exist_ok=True)

        count = 0
        new_last_timestamp = last_timestamp

        with open(filepath, 'w', encoding='utf-8-sig', newline='') as f:
            # Use Pipe (|) separator as requested
            writer = csv.DictWriter(f, fieldnames=STANDARD_HEADERS, delimiter='|')
            writer.writeheader()

            # Execute via connection for streaming/memory efficiency
            result = db.execute(text(sql), params)
            for row in result:
                data = row.data if isinstance(row.data, dict) else {}
                
                # Combine DB-level geo with JSON data
                out_row = {h: data.get(h, "") for h in STANDARD_HEADERS}
                
                # STRICT DOB FORMATTING for standard CSV
                for dob_key in ["dob", "spouse_dob"]:
                    if dob_key in out_row and out_row[dob_key]:
                        from .validator import clean_dob
                        cleaned, _ = clean_dob(str(out_row[dob_key]))
                        if cleaned:
                            out_row[dob_key] = cleaned
                
                out_row["division"] = row.division
                out_row["district"] = row.district
                out_row["upazila"] = row.upazila
                
                writer.writerow(out_row)
                count += 1
                new_last_timestamp = row.created_at

                if count % 10000 == 0:
                    task.message = f"Exported {count} records..."
                    db.commit()

        if count == 0:
            if os.path.exists(filepath):
                os.remove(filepath)
            task.status = "completed"
            task.message = "No new records found for this scope."
            task.progress = 100
            db.commit()
            return

        # Update Tracking (System-wide as requested)
        if count > 0:
            tracking = db.query(ExportTracking).filter(
                ExportTracking.scope == scope,
                ExportTracking.scope_value == scope_value
            ).first()
            if not tracking:
                tracking = ExportTracking(scope=scope, scope_value=scope_value)
                db.add(tracking)
            
            tracking.last_exported_at = new_last_timestamp
            tracking.updated_at = datetime.now(timezone.utc)
            
            # Fetch username for history
            user = db.query(User).filter(User.id == task.user_id).first()
            username = user.username if user else f"User {task.user_id}"

            # Log to history
            history = ExportHistory(
                user_id=task.user_id,
                username=username,
                scope=scope,
                scope_value=scope_value,
                export_type="new" if only_new else "all",
                record_count=count,
                filename=filename
            )
            db.add(history)
            db.commit()

        task.status = "completed"
        task.progress = 100
        task.message = f"Standard CSV exported ({count} records)"
        task.result_url = f"/api/export/download/{urllib.parse.quote(filename)}"
        db.commit()

    except Exception as e:
        logger.error(f"Standard CSV export failed: {e}")
        task.status = "error"
        task.error_details = str(e)
        db.commit()
    finally:
        db.close()

@router.post("/standard-csv", dependencies=[Depends(PermissionChecker("view_stats"))])
def export_standard_csv(
    req: StandardCSVExportRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Start standard pipe-separated CSV export."""
    task_id = str(uuid.uuid4())
    task = BackgroundTask(
        id=task_id, 
        task_name="standard_csv_export", 
        user_id=current_user.id, 
        status="pending", 
        progress=0, 
        message="Initializing standard CSV export..."
    )
    db.add(task)
    db.commit()
    
    background_tasks.add_task(
        _generate_standard_csv_task,
        task_id,
        filter_divisions=req.divisions,
        filter_districts=req.districts,
        filter_upazila_ids=req.upazila_ids,
        only_new=req.only_new
    )
    
    # Audit Logging
    log_audit(db, current_user, "EXPORT_CSV", "valid_records", None, {
        "filters": req.dict(),
        "task_id": task_id
    })
    
    return {"task_id": task_id, "status": "started"}


def _write_checked_xlsx(export_df: pd.DataFrame, invalid_mask, path: str, warning_mask=None):
    """Write a single Excel file with Nikosh font and highlighted rows.
    Headers use the original uploaded column names (Bangla).
    Red rows: Invalid records. Yellow rows: Converted NID / warnings.
    Uses xlsxwriter for high performance.
    """
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        export_df.to_excel(writer, index=False, sheet_name="All Records")
        workbook = writer.book
        worksheet = writer.sheets["All Records"]

        # Base Nikosh format for all data cells
        fmt_nikosh = workbook.add_format({"font_name": "Nikosh", "font_size": 11})
        # Header row format — bold Nikosh with a subtle background
        fmt_header = workbook.add_format({
            "font_name": "Nikosh", "font_size": 11, "bold": True,
            "bg_color": "#D9E1F2", "border": 1,
            "text_wrap": True, "valign": "vcenter",
        })
        fmt_red = workbook.add_format({
            "font_name": "Nikosh", "font_size": 11,
            "bg_color": "#FFCCCC",  # Light Red
        })
        fmt_yellow = workbook.add_format({
            "font_name": "Nikosh", "font_size": 11,
            "bg_color": "#FFFF99",  # Light Yellow
        })

        num_cols = len(export_df.columns)

        # Apply Nikosh to all data columns
        worksheet.set_column(0, num_cols - 1, 18, fmt_nikosh)

        # Explicitly write the header row with the Nikosh header format
        for col_idx, col_name in enumerate(export_df.columns):
            worksheet.write(0, col_idx, col_name, fmt_header)

        # Set header row height for wrapped Bangla text
        worksheet.set_row(0, 30, fmt_header)

        # Highlight invalid rows in Red
        if invalid_mask:
            for row_idx in invalid_mask:
                worksheet.set_row(row_idx + 1, None, fmt_red)

        # Highlight warnings in Yellow
        if warning_mask:
            for row_idx in warning_mask:
                worksheet.set_row(row_idx + 1, None, fmt_yellow)

        # Freeze header row
        worksheet.freeze_panes(1, 0)
