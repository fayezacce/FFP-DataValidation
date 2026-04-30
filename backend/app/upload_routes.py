"""
FFP Data Validator — Upload Routes
Handles Excel file upload, validation, preview, and DB persistence.
"""
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, BackgroundTasks
from fastapi.requests import Request
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timezone
import pandas as pd
import io
import os
import asyncio
import urllib.parse
import logging
from openpyxl.styles import PatternFill, Font
import openpyxl

from .database import get_db, SessionLocal
from .models import (
    User, SystemConfig, SummaryStats, ValidRecord, InvalidRecord,
    UploadedFile, UploadBatch, TrailingZeroWhitelist
)
from .auth import get_current_user
from .rbac import PermissionChecker
from .audit import log_audit
from .validator import process_dataframe, ensure_dob_format
from .pdf_generator import generate_pdf_report
from .bd_geo import fuzzy_match_location, get_division_for_district
from .stats_utils import refresh_summary_stats


logger = logging.getLogger("ffp")
router = APIRouter(tags=["upload"])

MAX_UPLOAD_SIZE = int(os.environ.get("MAX_UPLOAD_SIZE", 20 * 1024 * 1024))



@router.get("/validate/status/{task_id}")
async def get_validation_status(task_id: int, db: Session = Depends(get_db)):
    batch = db.query(UploadBatch).filter(UploadBatch.id == task_id).first()
    if not batch:
        raise HTTPException(status_code=404, detail="Task not found")
        
    response = {
        "status": batch.status,
        "batch_id": batch.id,
        "total_rows": batch.total_rows or 0,
        "valid_count": batch.valid_count or 0,
        "invalid_count": batch.invalid_count or 0,
        "new_records": batch.new_records or 0,
        "updated_records": batch.updated_records or 0,
        "geo": {
            "division": batch.division,
            "district": batch.district,
            "upazila": batch.upazila
        },
        "summary": {
            "total_rows": batch.total_rows or 0,
            "converted_nid": batch.updated_records or 0
        }
    }
    
    if batch.status == "completed":
        summary = db.query(SummaryStats).filter(
            SummaryStats.district == batch.district,
            SummaryStats.upazila == batch.upazila
        ).first()
        if summary:
            response["pdf_url"] = summary.pdf_url
            response["excel_url"] = summary.excel_url
            response["excel_valid_url"] = summary.excel_valid_url
            response["excel_invalid_url"] = summary.excel_invalid_url
            response["pdf_invalid_url"] = summary.pdf_invalid_url
    
    return response

@router.post("/validate", dependencies=[Depends(PermissionChecker("upload_data"))])
async def validate_excel(
    request: Request,
    file: UploadFile = File(...),
    dob_column: str = Form(...),
    nid_column: str = Form(...),
    header_row: int = Form(1),
    additional_columns: str = Form(""),
    sheet_name: str = Form(None),
    division: str = Form(None),
    district: str = Form(None),
    upazila: str = Form(None),
    is_correction: bool = Form(False),
    wipe_before_upload: bool = Form(False),
    db: Session = Depends(get_db),
    background_tasks: BackgroundTasks = None,
    current_user: User = Depends(get_current_user),
):
    """Upload and validate an Excel file. Persists valid/invalid records to DB."""
    if getattr(request.app.state, "security_lockout", False):
        raise HTTPException(
            status_code=503,
            detail="Security lockout active. Change the default admin password via /auth/change-password.",
        )

    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are allowed.")

    contents = await file.read()
    if len(contents) > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=400, detail="File too large. Max 20MB allowed.")

    import uuid

    safe_filename = f"{uuid.uuid4().hex}_{os.path.basename(file.filename)}"
    upload_path = os.path.join("uploads", safe_filename)
    with open(upload_path, "wb") as f:
        f.write(contents)

    db_file = UploadedFile(filename=safe_filename, original_name=file.filename, filepath=upload_path)
    db.add(db_file)
    db.commit()

    # Geo-match from filename or use manual overrides
    if division and district and upazila:
        geo = {"division": division.strip(), "district": district.strip(), "upazila": upazila.strip()}
    elif district and upazila:
        geo = {
            "division": get_division_for_district(district.strip()),
            "district": district.strip(),
            "upazila": upazila.strip(),
        }
    else:
        geo = fuzzy_match_location(file.filename)
        
    # Check geo authorization
    if current_user.role != "admin":
        doc_div = geo.get("division")
        doc_dist = geo.get("district")
        doc_upz = geo.get("upazila")
        
        if getattr(current_user, "division_access", None) and doc_div != current_user.division_access:
            raise HTTPException(status_code=403, detail=f"Access denied: You are restricted to {current_user.division_access} division.")
        if getattr(current_user, "district_access", None) and doc_dist != current_user.district_access:
            raise HTTPException(status_code=403, detail=f"Access denied: You are restricted to {current_user.district_access} district.")
        if getattr(current_user, "upazila_access", None) and doc_upz != current_user.upazila_access:
            raise HTTPException(status_code=403, detail=f"Access denied: You are restricted to {current_user.upazila_access} upazila.")

    try:
        # Create a "processing" batch record immediately
        # CRITICAL FIX: Use the 'geo' values which include matched locations if Form values were empty
        batch = UploadBatch(
            filename=file.filename,
            original_name=file.filename,
            uploader_id=current_user.id,
            username=current_user.username,
            division=geo.get("division"),
            district=geo.get("district"),
            upazila=geo.get("upazila"),
            status="processing",
            total_rows=0,
            valid_count=0,
            invalid_count=0,
        )
        db.add(batch)
        db.commit()
        db.refresh(batch)

        # Enqueue the heavy work
        background_tasks.add_task(
            run_validation_task,
            batch.id,
            upload_path,
            dob_column,
            nid_column,
            header_row,
            additional_columns,
            sheet_name,
            is_correction,
            current_user.id,
            wipe_before_upload,
        )

        return {
            "status": "queued",
            "task_id": batch.id,
            "message": "Validation started in background"
        }
    except Exception as e:
        logger.error(f"Failed to start validation: {e}")
        raise HTTPException(status_code=500, detail="Failed to start validation process")


def run_validation_task(
    batch_id: int,
    upload_path: str,
    dob_column: str,
    nid_column: str,
    header_row: int,
    additional_columns: str,
    sheet_name: str,
    is_correction: bool,
    user_id: int,
    wipe_before_upload: bool = False,
):
    """Background task for processing the full Excel validation."""
    db = SessionLocal()
    try:
        batch = db.query(UploadBatch).filter(UploadBatch.id == batch_id).first()
        if not batch:
            return

        with open(upload_path, "rb") as f:
            contents = f.read()

        tz_limit_conf = db.query(SystemConfig).filter(SystemConfig.key == "trailing_zero_limit").first()
        # Default to 2 if missing/invalid, but allow the user to set 0 to disable
        if tz_limit_conf and tz_limit_conf.value and tz_limit_conf.value.strip().isdigit():
            tz_limit = int(tz_limit_conf.value.strip())
        else:
            tz_limit = 2

        tz_whitelist_records = db.query(TrailingZeroWhitelist.nid).all()
        tz_whitelist = {r[0] for r in tz_whitelist_records}

        if sheet_name and sheet_name.strip():
            df = pd.read_excel(io.BytesIO(contents), sheet_name=sheet_name.strip(), header=header_row - 1, dtype=str)
        else:
            df = pd.read_excel(io.BytesIO(contents), header=header_row - 1, dtype=str)
        
        # Capture original headers BEFORE process_dataframe renames columns to canonical keys
        original_headers = [str(c) for c in df.columns.tolist()]
        
        # STRICT DOB FORMATTING: Ensure all DOB-like columns in the original DF are formatted as YYYY-MM-DD
        df = ensure_dob_format(df, primary_dob_col=dob_column)

        # ── CRITICAL: Save raw copy with original Bangla column names BEFORE normalization ──
        # process_dataframe() renames df.columns in-place to canonical keys.
        # We need the original Bangla-keyed data to reconstruct the original file during export.
        raw_df_copy = df.copy()
        
        # Fetch dynamic mapping from DB
        from .models import HeaderAlias
        aliases = db.query(HeaderAlias).all()
        header_mapping = {a.original_header: a.canonical_key for a in aliases}
        
        processed_df, stats = process_dataframe(df, dob_col=dob_column, nid_col=nid_column, header_row=header_row, tz_limit=tz_limit, tz_whitelist=tz_whitelist, header_mapping=header_mapping)
        # After this call: df.columns == canonical keys (e.g. nid_number, name_bn)
        #                  raw_df_copy.columns == original Bangla headers

        add_cols = [c.strip() for c in additional_columns.split(",")] if additional_columns else []
        
        # Geo-match for base filename
        geo = {"division": batch.division, "district": batch.district, "upazila": batch.upazila}
        base_filename = f"{geo['district']}_{geo['upazila']}".replace(" ", "_").replace("/", "_") if geo['district'] else "upload"

        # ── Wipe existing data if requested (single-version mode) ──
        if wipe_before_upload and geo.get("district") and geo.get("upazila"):
            logger.info("Wiping existing data for %s / %s before upload", geo["district"], geo["upazila"])
            db.query(ValidRecord).filter(
                ValidRecord.district == geo["district"],
                ValidRecord.upazila == geo["upazila"]
            ).delete(synchronize_session=False)
            db.query(InvalidRecord).filter(
                InvalidRecord.district == geo["district"],
                InvalidRecord.upazila == geo["upazila"]
            ).delete(synchronize_session=False)
            db.query(SummaryStats).filter(
                SummaryStats.district == geo["district"],
                SummaryStats.upazila == geo["upazila"]
            ).delete(synchronize_session=False)
            db.commit()

        # Generate Reports
        pdf_path = generate_pdf_report(processed_df, stats, additional_columns=add_cols, original_filename=base_filename, geo=geo)
        pdf_filename = os.path.basename(pdf_path)

        pdf_invalid_path = generate_pdf_report(processed_df, stats, additional_columns=add_cols, original_filename=base_filename, geo=geo, invalid_only=True)
        pdf_invalid_filename = os.path.basename(pdf_invalid_path)

        # ── Build upload-time Excel exports using ORIGINAL Bangla headers ──
        excel_filename = f"{base_filename}_tested.xlsx"
        excel_path = os.path.join("downloads", excel_filename)
        excel_valid_filename = f"{base_filename}_valid.xlsx"
        excel_valid_path = os.path.join("downloads", excel_valid_filename)
        excel_invalid_filename = f"{base_filename}_invalid.xlsx"
        excel_invalid_path = os.path.join("downloads", excel_invalid_filename)

        # Align validator output columns to raw_df rows using the index
        status_series  = processed_df["Status"]
        error_mask   = status_series == "error"
        warning_mask = status_series == "warning"

        # Keep positional integer lists (row+1 because header occupies row 0 in Excel)
        error_rows_pos   = [i for i, v in enumerate(status_series) if v == "error"]
        warning_rows_pos = [i for i, v in enumerate(status_series) if v == "warning"]

        # Tested (checked) Excel — original headers + red/yellow highlights, no internal cols
        tested_export_df = raw_df_copy.copy()
        from .export_routes import _write_checked_xlsx
        _write_checked_xlsx(tested_export_df, error_rows_pos, excel_path, warning_mask=warning_rows_pos)

        # Valid-only export — rows where Status != error (original headers, no highlights)
        valid_raw_df = raw_df_copy[~error_mask].copy()
        _save_optimized_excel(valid_raw_df, excel_valid_path)

        # Invalid-only export — rows where Status == error (original headers, all red)
        invalid_raw_df = raw_df_copy[error_mask].copy()
        _save_optimized_excel(invalid_raw_df, excel_invalid_path, mask_all_red=True)

        error_count = int(stats["issues"])
        valid_count = stats["total_rows"] - error_count

        # Map Geo IDs
        from .models import Division, District, Upazila
        from sqlalchemy import func
        div_obj = db.query(Division).filter(func.lower(Division.name) == geo["division"].lower().strip()).first()
        dist_obj = db.query(District).filter(func.lower(District.name) == geo["district"].lower().strip()).first()
        upz_obj = db.query(Upazila).filter(
            func.lower(Upazila.name) == geo["upazila"].lower().strip(),
            func.lower(Upazila.district_name) == geo["district"].lower().strip()
        ).first()

        div_id = div_obj.id if div_obj else None
        dist_id = dist_obj.id if dist_obj else None
        upazila_id = upz_obj.id if upz_obj else None

        # Bulk check existing NIDs
        valid_rows = processed_df[processed_df["Status"] != "error"]
        valid_nids = [str(row["Cleaned_NID"]).strip() for _, row in valid_rows.iterrows() if pd.notna(row["Cleaned_NID"]) and str(row["Cleaned_NID"]).strip()]
        existing_map = {}
        if valid_nids:
            for i in range(0, len(valid_nids), 5000):
                chunk = valid_nids[i : i + 5000]
                recs = db.query(ValidRecord.nid, ValidRecord.district, ValidRecord.upazila).filter(ValidRecord.nid.in_(chunk)).all()
                for r in recs: existing_map[r.nid] = r

        new_count = 0
        updated_count = 0
        cross_upazila_duplicates = []
        insert_data = []

        summary = db.query(SummaryStats).filter(SummaryStats.district == geo["district"], SummaryStats.upazila == geo["upazila"]).first()
        current_version = (summary.version + 1) if summary else 1

        # ── Build index → original-row lookup from raw_df_copy for O(1) access ──
        raw_index_map = {idx: raw_df_copy.loc[idx].where(pd.notna(raw_df_copy.loc[idx]), None).to_dict()
                         for idx in raw_df_copy.index}

        for idx, row in valid_rows.iterrows():
            nid = str(row["Cleaned_NID"]).strip()
            if not nid: continue

            if nid in existing_map:
                existing = existing_map[nid]
                if existing.upazila != geo["upazila"] or existing.district != geo["district"]:
                    cross_upazila_duplicates.append({"nid": nid, "name": row.get("Extracted_Name", "Unknown"), "previous_district": existing.district, "previous_upazila": existing.upazila, "new_district": geo["district"], "new_upazila": geo["upazila"]})
                updated_count += 1
            else:
                new_count += 1
                existing_map[nid] = True

            # Build data dict using original Bangla keys + internal validator fields.
            data_dict = dict(raw_index_map.get(idx, {}))
            data_dict["Status"]         = row.get("Status", "success")
            data_dict["Cleaned_NID"]    = row.get("Cleaned_NID", "")
            data_dict["Cleaned_DOB"]    = row.get("Cleaned_DOB", "")
            data_dict["Excel_Row"]      = row.get("Excel_Row", "")
            data_dict["Message"]        = row.get("Message", "")
            data_dict["Extracted_Name"] = row.get("Extracted_Name", "")

            # ── Extract Canonical Fields Directly from Mapped Row ──
            # row is a Series with index = canonical keys (from process_dataframe renaming)
            def _get_val(k, fallback=""):
                v = row.get(k)
                return str(v).strip() if v is not None and pd.notna(v) else fallback

            insert_data.append({
                "nid": nid, "dob": row.get("Cleaned_DOB", ""), "name": row.get("Extracted_Name", "Unknown"),
                "division": geo["division"], "district": geo["district"], "upazila": geo["upazila"],
                "division_id": div_id, "district_id": dist_id, "upazila_id": upazila_id,
                "card_no": _get_val("card_no", str(row.get("Card_No", ""))), 
                "mobile": _get_val("mobile", str(row.get("Mobile", ""))),
                "source_file": batch.filename, "batch_id": batch.id, "upload_batch": current_version,
                "data": data_dict, "updated_at": datetime.now(timezone.utc),
                # Promoted columns
                "father_husband_name": _get_val("father_husband_name"),
                "name_bn":     _get_val("name_bn"),
                "name_en":     _get_val("name_en"),
                "ward":        _get_val("ward"),
                "union_name":  _get_val("union_name"),
                "occupation":  _get_val("occupation"),
                "gender":      _get_val("gender"),
                "religion":    _get_val("religion"),
                "address":     _get_val("address"),
                "spouse_name": _get_val("spouse_name"),
                "spouse_nid":  _get_val("spouse_nid"),
                "spouse_dob":  _get_val("spouse_dob"),
                "verification_status": "unverified",
                # Temporary storage for dealer info to be processed in next step
                "_dealer_nid": _get_val("dealer_nid"),
                "_dealer_name": _get_val("dealer_name", "Unknown"),
                "_dealer_mobile": _get_val("dealer_mobile"),
            })

        if insert_data:
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            # ── Collect unique dealers from this upload ──
            from .models import Dealer

            dealer_nid_to_id: dict[tuple, int] = {}
            unique_dealers: dict[tuple, dict] = {}
            for row_data in insert_data:
                d_nid = row_data.get("_dealer_nid")
                if d_nid and upazila_id:
                    key = (d_nid, upazila_id)
                    if key not in unique_dealers:
                        unique_dealers[key] = {
                            "nid":        d_nid,
                            "name":       row_data.get("_dealer_name") or "Unknown",
                            "mobile":     row_data.get("_dealer_mobile"),
                            "division":   geo["division"], "district": geo["district"], "upazila": geo["upazila"],
                            "division_id": div_id, "district_id": dist_id, "upazila_id": upazila_id,
                        }

            # Upsert dealers (update name + mobile on conflict)
            if unique_dealers:
                dealer_stmt = pg_insert(Dealer).values(list(unique_dealers.values()))
                dealer_stmt = dealer_stmt.on_conflict_do_update(
                    constraint="uix_dealer_nid_upazila",
                    set_={"name": dealer_stmt.excluded.name, "mobile": dealer_stmt.excluded.mobile,
                          "updated_at": dealer_stmt.excluded.updated_at}
                ).returning(Dealer.id, Dealer.nid, Dealer.upazila_id)
                dealer_rows = db.execute(dealer_stmt).fetchall()
                db.commit()
                dealer_nid_to_id = {(r.nid, r.upazila_id): r.id for r in dealer_rows}

            # Map dealer_id to records and cleanup temp fields
            for row_data in insert_data:
                d_nid = row_data.pop("_dealer_nid", None)
                row_data.pop("_dealer_name", None)
                row_data.pop("_dealer_mobile", None)
                if d_nid and upazila_id and (d_nid, upazila_id) in dealer_nid_to_id:
                    row_data["dealer_id"] = dealer_nid_to_id[(d_nid, upazila_id)]

            for i in range(0, len(insert_data), 2000):
                stmt = pg_insert(ValidRecord).values(insert_data[i : i + 2000])
                stmt = stmt.on_conflict_do_update(index_elements=["nid"], set_={k: getattr(stmt.excluded, k) for k in insert_data[0].keys() if k != "nid"})
                db.execute(stmt)


        invalid_rows_final = processed_df[processed_df["Status"] == "error"]
        invalid_insert = []
        for idx, row in invalid_rows_final.iterrows():
            # Same pattern: original Bangla keys + internal fields
            data_dict = dict(raw_index_map.get(idx, {}))
            data_dict["Status"]         = row.get("Status", "error")
            data_dict["Cleaned_NID"]    = row.get("Cleaned_NID", "")
            data_dict["Cleaned_DOB"]    = row.get("Cleaned_DOB", "")
            data_dict["Excel_Row"]      = row.get("Excel_Row", "")
            data_dict["Message"]        = row.get("Message", "")
            data_dict["Extracted_Name"] = row.get("Extracted_Name", "")

            # ── Promote Columns for InvalidRecord as well ──
            def _get_val(k, fallback=""):
                v = row.get(k)
                return str(v).strip() if v is not None and pd.notna(v) else fallback

            invalid_insert.append({
                "nid": str(row.get("Cleaned_NID", "")).strip(), "dob": row.get("Cleaned_DOB", ""), "name": row.get("Extracted_Name", "Unknown"),
                "division": geo["division"], "district": geo["district"], "upazila": geo["upazila"],
                "division_id": div_id, "district_id": dist_id, "upazila_id": upazila_id,
                "card_no": _get_val("card_no", str(row.get("Card_No", ""))), 
                "master_serial": _get_val("master_serial", str(row.get("Master_Serial", ""))), 
                "mobile": _get_val("mobile", str(row.get("Mobile", ""))),
                "source_file": batch.filename, "batch_id": batch.id, "upload_batch": current_version, "error_message": str(row.get("Message", "Unknown")),
                "data": data_dict,
                # Promoted columns
                "father_husband_name": _get_val("father_husband_name"),
                "name_bn":     _get_val("name_bn"),
                "name_en":     _get_val("name_en"),
                "ward":        _get_val("ward"),
                "union_name":  _get_val("union_name"),
                "occupation":  _get_val("occupation"),
                "gender":      _get_val("gender"),
                "religion":    _get_val("religion"),
                "address":     _get_val("address"),
                "spouse_name": _get_val("spouse_name"),
                "spouse_nid":  _get_val("spouse_nid"),
                "spouse_dob":  _get_val("spouse_dob"),
            })
        if invalid_insert:
            # ── Delete existing invalid records for this upazila before re-inserting ──
            # This ensures the upload page count always matches the statistics page
            # (single-version-per-upazila model: no stacking of old invalid rows).
            # Skip if wipe_before_upload already cleared them above.
            if not wipe_before_upload:
                db.query(InvalidRecord).filter(
                    InvalidRecord.district == geo["district"],
                    InvalidRecord.upazila == geo["upazila"]
                ).delete(synchronize_session=False)
                db.commit()
            for i in range(0, len(invalid_insert), 2000):
                db.bulk_insert_mappings(InvalidRecord, invalid_insert[i : i + 2000])

        if is_correction:
            # Logic similar to original (delete from invalid if corrected)
            from sqlalchemy import or_
            v_nids = [r["nid"] for r in insert_data]
            v_cards = [r["card_no"] for r in insert_data if r["card_no"]]
            if v_nids or v_cards:
                conds = []
                if v_nids: conds.append(InvalidRecord.nid.in_(v_nids))
                if v_cards: conds.append(InvalidRecord.card_no.in_(v_cards))
                db.query(InvalidRecord).filter(InvalidRecord.upazila == geo["upazila"], InvalidRecord.district == geo["district"], or_(*conds)).delete(synchronize_session=False)

        batch.status = "completed"
        batch.total_rows = stats["total_rows"]
        batch.valid_count = valid_count
        batch.invalid_count = error_count
        batch.new_records = new_count
        batch.updated_records = updated_count
        batch.column_headers = original_headers
        db.commit()

        # Refresh summary
        refresh_summary_stats(db, geo["division"], geo["district"], geo["upazila"], filename=batch.filename, stats_source={"total_rows": stats["total_rows"], "valid_count": valid_count, "error_count": error_count, "new_count": new_count, "updated_count": updated_count}, current_version=current_version, column_headers=original_headers)
        
        # Log successful completion
        uploader = db.query(User).filter(User.id == user_id).first()
        log_audit(
            db, uploader, 
            "CREATE", "upload_batch", batch.id, 
            new_data={
                "filename": batch.filename,
                "location": f"{geo['division']} > {geo['district']} > {geo['upazila']}",
                "total": stats["total_rows"],
                "valid": valid_count,
                "invalid": error_count
            }
        )
        
        # Sync summary urls
        summary = db.query(SummaryStats).filter(SummaryStats.district == geo["district"], SummaryStats.upazila == geo["upazila"]).first()
        if summary:
            summary.pdf_url = f"/api/export/download/{urllib.parse.quote(pdf_filename)}"
            summary.pdf_invalid_url = f"/api/export/download/{urllib.parse.quote(pdf_invalid_filename)}"
            summary.excel_url = f"/api/export/download/{urllib.parse.quote(excel_filename)}"
            summary.excel_valid_url = f"/api/export/download/{urllib.parse.quote(excel_valid_filename)}"
            summary.excel_invalid_url = f"/api/export/download/{urllib.parse.quote(excel_invalid_filename)}"
            db.commit()

    except Exception as e:
        logger.error(f"Validation task failed: {e}")
        import traceback
        traceback.print_exc()
        if 'db' in locals():
            batch = db.query(UploadBatch).filter(UploadBatch.id == batch_id).first()
            if batch:
                batch.status = "failed"
                db.commit()
    finally:
        if 'db' in locals():
            db.close()


def _save_optimized_excel(df: pd.DataFrame, path: str, red_rows: list = None, yellow_rows: list = None, mask_all_red: bool = False):
    """High-performance Excel export using xlsxwriter with Nikosh font support and row highlighting."""
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
        workbook = writer.book
        worksheet = writer.sheets["Sheet1"]
        
        fmt_nikosh = workbook.add_format({"font_name": "Nikosh", "font_size": 11})
        fmt_red = workbook.add_format({"font_name": "Nikosh", "font_size": 11, "bg_color": "#FFCCCC"})
        fmt_yellow = workbook.add_format({"font_name": "Nikosh", "font_size": 11, "bg_color": "#FFFFCC"})
        
        num_cols = len(df.columns)
        worksheet.set_column(0, num_cols - 1, 18, fmt_nikosh)
        
        if mask_all_red:
            for r in range(len(df)):
                worksheet.set_row(r + 1, None, fmt_red)
        else:
            if red_rows:
                for r in red_rows:
                    worksheet.set_row(r + 1, None, fmt_red)
            if yellow_rows:
                for r in yellow_rows:
                    worksheet.set_row(r + 1, None, fmt_yellow)
        
        worksheet.freeze_panes(1, 0)


@router.post("/preview", dependencies=[Depends(PermissionChecker("upload_data"))])
async def preview_validation(
    file: UploadFile = File(...),
    dob_column: str = Form(...),
    nid_column: str = Form(...),
    header_row: int = Form(1),
    sheet_name: str = Form(None),
    db: Session = Depends(get_db),
):
    """Dry-run validation of the first 10 rows to catch column mismatches early.
    Returns blocked=true if >50% of preview rows are invalid."""
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are allowed.")

    contents = await file.read()
    try:
        tz_limit_conf = db.query(SystemConfig).filter(SystemConfig.key == "trailing_zero_limit").first()
        # Default to 2 if missing/invalid, but allow the user to set 0 to disable
        if tz_limit_conf and tz_limit_conf.value and tz_limit_conf.value.strip().isdigit():
            tz_limit = int(tz_limit_conf.value.strip())
        else:
            tz_limit = 2

        tz_whitelist_records = db.query(TrailingZeroWhitelist.nid).all()
        tz_whitelist = {r[0] for r in tz_whitelist_records}

        def read_and_process():
            from .models import HeaderAlias
            aliases = db.query(HeaderAlias).all()
            header_mapping = {a.original_header: a.canonical_key for a in aliases}
            
            if sheet_name and sheet_name.strip():
                df = pd.read_excel(io.BytesIO(contents), sheet_name=sheet_name.strip(), header=header_row - 1, nrows=10, dtype=str)
            else:
                df = pd.read_excel(io.BytesIO(contents), header=header_row - 1, nrows=10, dtype=str)
            return process_dataframe(df, dob_col=dob_column, nid_col=nid_column, header_row=header_row, tz_limit=tz_limit, tz_whitelist=tz_whitelist, header_mapping=header_mapping)

        processed_df, stats = await asyncio.to_thread(read_and_process)
        preview_data = processed_df.replace({float("nan"): None}).to_dict(orient="records")

        total = len(preview_data)
        invalid = sum(1 for r in preview_data if r.get("Status") == "error")
        invalid_pct = round((invalid / total) * 100, 1) if total > 0 else 0
        blocked = invalid_pct > 50

        return {"preview": preview_data, "summary": stats, "invalid_pct": invalid_pct, "blocked": blocked}
    except Exception as e:
        import traceback
        logger.error(f"Preview calculation failed for file {file.filename}: {traceback.format_exc()}")
        raise HTTPException(status_code=400, detail=str(e))
