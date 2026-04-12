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
from .validator import process_dataframe
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
        "updated_records": batch.updated_records or 0
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
    db: Session = Depends(get_db),
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
        tz_limit_conf = db.query(SystemConfig).filter(SystemConfig.key == "trailing_zero_limit").first()
        tz_limit = int(tz_limit_conf.value) if tz_limit_conf and tz_limit_conf.value.isdigit() else 0

        tz_whitelist_records = db.query(TrailingZeroWhitelist.nid).all()
        tz_whitelist = {r[0] for r in tz_whitelist_records}

        def read_and_process():
            if sheet_name and sheet_name.strip():
                df = pd.read_excel(io.BytesIO(contents), sheet_name=sheet_name.strip(), header=header_row - 1, dtype=str)
            else:
                df = pd.read_excel(io.BytesIO(contents), header=header_row - 1, dtype=str)
            # Capture original headers BEFORE process_dataframe renames them via header_mapping
            original_headers = [str(c) for c in df.columns.tolist()]
            processed, s = process_dataframe(df, dob_col=dob_column, nid_col=nid_column, header_row=header_row, tz_limit=tz_limit, tz_whitelist=tz_whitelist)
            return processed, s, original_headers

        processed_df, stats, original_headers = await asyncio.to_thread(read_and_process)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except KeyError as ke:
        raise HTTPException(status_code=400, detail=f"Column not found in the uploaded file: {str(ke)}. Please verify your column selection.")
    except Exception:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error. Please contact support.")

    add_cols = [c.strip() for c in additional_columns.split(",")] if additional_columns else []
    original_filename_no_ext = os.path.splitext(file.filename)[0]

    if geo and geo.get("district") and geo.get("upazila") and geo.get("district") != "Unknown" and geo.get("upazila") != "Unknown":
        base_filename = f"{geo['district']}_{geo['upazila']}".replace(" ", "_").replace("/", "_")
    else:
        base_filename = original_filename_no_ext

    pdf_path = generate_pdf_report(processed_df, stats, additional_columns=add_cols, original_filename=base_filename, geo=geo)
    filename = os.path.basename(pdf_path)

    pdf_invalid_path = generate_pdf_report(processed_df, stats, additional_columns=add_cols, original_filename=base_filename, geo=geo, invalid_only=True)
    pdf_invalid_filename = os.path.basename(pdf_invalid_path)

    # Generate Excel exports
    red_fill = PatternFill(start_color="FFFFCCCC", end_color="FFFFCCCC", fill_type="solid")
    yellow_fill = PatternFill(start_color="FFFFFF99", end_color="FFFFFF99", fill_type="solid")

    excel_filename = f"{base_filename}_tested.xlsx"
    excel_path = os.path.join("downloads", excel_filename)
    excel_valid_filename = f"{base_filename}_valid.xlsx"
    excel_valid_path = os.path.join("downloads", excel_valid_filename)
    excel_invalid_filename = f"{base_filename}_invalid.xlsx"
    excel_invalid_path = os.path.join("downloads", excel_invalid_filename)

    dob_col_idx = None
    nid_col_idx = None

    if file.filename.endswith(".xls") and not file.filename.endswith(".xlsx"):
        # Fallback for strict .xls
        processed_df[dob_column] = processed_df[dob_column].astype(object)
        processed_df[nid_column] = processed_df[nid_column].astype(object)

        for idx in range(len(processed_df)):
            processed_df.at[idx, dob_column] = processed_df.at[idx, "Cleaned_DOB"]
            processed_df.at[idx, nid_column] = processed_df.at[idx, "Cleaned_NID"]

        cols_to_drop = ["Cleaned_DOB", "Cleaned_NID", "DOB_Year", "Status", "Message", "Excel_Row", "Extracted_Name", "Card_No", "Master_Serial", "Mobile", "Fraud_Reason"]
        export_df = processed_df.drop(columns=[c for c in cols_to_drop if c in processed_df.columns])
        export_df.to_excel(excel_path, index=False, engine="openpyxl")

        wb = openpyxl.load_workbook(excel_path)
        ws = wb.active
        for idx, row in processed_df.iterrows():
            r = idx + 2
            status = row["Status"]
            if status == "error":
                for c in range(1, ws.max_column + 1):
                    ws.cell(row=r, column=c).fill = red_fill
            elif status == "warning":
                for c in range(1, ws.max_column + 1):
                    ws.cell(row=r, column=c).fill = yellow_fill
        wb.save(excel_path)

        nikosh_font = Font(name="Nikosh", size=11)
        valid_mask = processed_df["Status"] != "error"
        with pd.ExcelWriter(excel_valid_path, engine="openpyxl") as writer:
            export_df[valid_mask].to_excel(writer, index=False, sheet_name="Valid Records")
            ws = writer.sheets["Valid Records"]
            for row in ws.iter_rows():
                for cell in row:
                    cell.font = nikosh_font

        invalid_mask = processed_df["Status"] == "error"
        with pd.ExcelWriter(excel_invalid_path, engine="openpyxl") as writer:
            export_df[invalid_mask].to_excel(writer, index=False, sheet_name="Invalid Records")
            ws = writer.sheets["Invalid Records"]
            for row in ws.iter_rows():
                for cell in row:
                    cell.font = nikosh_font
    else:
        # Standard .xlsx handling
        wb = openpyxl.load_workbook(io.BytesIO(contents))
        ws = wb[sheet_name.strip()] if sheet_name and sheet_name.strip() in wb.sheetnames else wb.active

        for col_idx in range(1, ws.max_column + 1):
            val = ws.cell(row=header_row, column=col_idx).value
            if str(val).strip() == dob_column.strip():
                dob_col_idx = col_idx
            if str(val).strip() == nid_column.strip():
                nid_col_idx = col_idx

        for row in processed_df.itertuples(index=False):
            r = int(row.Excel_Row)
            status = row.Status

            if dob_col_idx:
                c_dob = ws.cell(row=r, column=dob_col_idx)
                if type(c_dob).__name__ != "MergedCell":
                    c_dob.value = row.Cleaned_DOB
                    c_dob.number_format = "@"

            if nid_col_idx:
                c_nid = ws.cell(row=r, column=nid_col_idx)
                if type(c_nid).__name__ != "MergedCell":
                    c_nid.value = row.Cleaned_NID
                    c_nid.number_format = "@"

            if status == "error":
                for c in range(1, ws.max_column + 1):
                    cell = ws.cell(row=r, column=c)
                    if type(cell).__name__ != "MergedCell":
                        cell.fill = red_fill
            elif status == "warning":
                for c in range(1, ws.max_column + 1):
                    cell = ws.cell(row=r, column=c)
                    if type(cell).__name__ != "MergedCell":
                        cell.fill = yellow_fill

        nikosh_font = Font(name="Nikosh", size=11)
        for row in ws.iter_rows():
            for cell in row:
                cell.font = nikosh_font

        wb.save(excel_path)

        export_df = processed_df.copy()
        export_df[dob_column] = export_df["Cleaned_DOB"]
        export_df[nid_column] = export_df["Cleaned_NID"]

        cols_to_drop = ["Cleaned_DOB", "Cleaned_NID", "DOB_Year", "Status", "Message", "Excel_Row", "Extracted_Name", "Card_No", "Master_Serial", "Mobile", "Fraud_Reason"]
        export_df = export_df.drop(columns=[c for c in cols_to_drop if c in export_df.columns])

        valid_mask = processed_df["Status"] != "error"
        with pd.ExcelWriter(excel_valid_path, engine="openpyxl") as writer:
            export_df[valid_mask].to_excel(writer, index=False, sheet_name="Valid Records")
            ws = writer.sheets["Valid Records"]
            nikosh_font = Font(name="Nikosh", size=11)
            for row in ws.iter_rows():
                for cell in row:
                    cell.font = nikosh_font

        invalid_mask = processed_df["Status"] == "error"
        with pd.ExcelWriter(excel_invalid_path, engine="openpyxl") as writer:
            export_df[invalid_mask].to_excel(writer, index=False, sheet_name="Invalid Records")
            ws = writer.sheets["Invalid Records"]
            for row in ws.iter_rows():
                for cell in row:
                    cell.font = nikosh_font

    # Prepare preview data
    preview_df = processed_df.head(50).replace({float("nan"): None})
    preview_data = preview_df.to_dict(orient="records")

    error_count = int((processed_df["Status"] == "error").sum())
    valid_count = stats["total_rows"] - error_count

    # ── Map Geo Names to IDs ──
    from sqlalchemy import func
    from .models import Division, District, Upazila

    div_obj = db.query(Division).filter(func.lower(Division.name) == geo["division"].lower().strip()).first()
    dist_obj = db.query(District).filter(func.lower(District.name) == geo["district"].lower().strip()).first()
    upz_obj = db.query(Upazila).filter(
        func.lower(Upazila.name) == geo["upazila"].lower().strip(),
        func.lower(Upazila.district_name) == geo["district"].lower().strip()
    ).first()

    div_id = div_obj.id if div_obj else None
    dist_id = dist_obj.id if dist_obj else None
    upazila_id = upz_obj.id if upz_obj else None

    # ── Database Persistence (NID-Aware Upsert) ──
    batch = UploadBatch(
        filename=file.filename,
        original_name=file.filename,
        uploader_id=current_user.id,
        username=current_user.username,
        division=geo["division"],
        district=geo["district"],
        upazila=geo["upazila"],
        division_id=div_id,
        district_id=dist_id,
        upazila_id=upazila_id,
        total_rows=stats["total_rows"],
        valid_count=valid_count,
        invalid_count=error_count,
        status="completed",
        column_headers=original_headers,  # Store original Bangla headers
    )
    db.add(batch)
    db.commit()
    db.refresh(batch)

    summary = db.query(SummaryStats).filter(
        SummaryStats.district == geo["district"],
        SummaryStats.upazila == geo["upazila"],
    ).first()
    current_version = (summary.version + 1) if summary else 1

    # Bulk check existing NIDs
    valid_rows = processed_df[processed_df["Status"] != "error"]
    valid_nids = [str(row["Cleaned_NID"]).strip() for _, row in valid_rows.iterrows() if pd.notna(row["Cleaned_NID"]) and str(row["Cleaned_NID"]).strip()]
    existing_records = []
    if valid_nids:
        for i in range(0, len(valid_nids), 5000):
            chunk = valid_nids[i : i + 5000]
            existing_records.extend(db.query(ValidRecord.nid, ValidRecord.district, ValidRecord.upazila).filter(ValidRecord.nid.in_(chunk)).all())
    existing_map = {r.nid: r for r in existing_records}

    new_count = 0
    updated_count = 0
    cross_upazila_duplicates = []
    insert_data = []

    for _, row in valid_rows.iterrows():
        nid = str(row["Cleaned_NID"]).strip()
        if not nid:
            continue

        row_dict = row.to_dict()
        row_dict = {k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in row_dict.items()}

        name_val = row.get("Extracted_Name", "Unknown")
        if pd.isna(name_val):
            name_val = "Unknown"
        dob_val = row.get("Cleaned_DOB", "")
        if pd.isna(dob_val):
            dob_val = ""

        if nid in existing_map:
            existing = existing_map[nid]
            if existing.upazila != geo["upazila"] or existing.district != geo["district"]:
                cross_upazila_duplicates.append({
                    "nid": nid,
                    "name": name_val,
                    "previous_district": existing.district,
                    "previous_upazila": existing.upazila,
                    "new_district": geo["district"],
                    "new_upazila": geo["upazila"],
                })
            updated_count += 1
        else:
            new_count += 1
            existing_map[nid] = True

        insert_data.append({
            "nid": nid,
            "dob": dob_val,
            "name": name_val,
            "division": geo["division"],
            "district": geo["district"],
            "upazila": geo["upazila"],
            "division_id": div_id,
            "district_id": dist_id,
            "upazila_id": upazila_id,
            "card_no": row.get("Card_No", ""),
            "mobile": str(row.get("Mobile", "")).strip() if pd.notna(row.get("Mobile")) else "",
            "source_file": file.filename,
            "batch_id": batch.id,
            "upload_batch": current_version,
            "data": row_dict,
            "updated_at": datetime.now(timezone.utc),
        })

    if insert_data:
        for i in range(0, len(insert_data), 2000):
            chunk = insert_data[i : i + 2000]
            stmt = insert(ValidRecord).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=["nid"],
                set_={
                    "dob": stmt.excluded.dob,
                    "name": stmt.excluded.name,
                    "division": stmt.excluded.division,
                    "district": stmt.excluded.district,
                    "upazila": stmt.excluded.upazila,
                    "division_id": stmt.excluded.division_id,
                    "district_id": stmt.excluded.district_id,
                    "upazila_id": stmt.excluded.upazila_id,
                    "card_no": stmt.excluded.card_no,
                    "mobile": stmt.excluded.mobile,
                    "source_file": stmt.excluded.source_file,
                    "batch_id": stmt.excluded.batch_id,
                    "upload_batch": stmt.excluded.upload_batch,
                    "data": stmt.excluded.data,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            db.execute(stmt)

    # --- InvalidRecord inserting ---
    invalid_rows = processed_df[processed_df["Status"] == "error"]
    invalid_insert_data = []
    for _, row in invalid_rows.iterrows():
        nid_val = str(row.get("Cleaned_NID", "")).strip()
        if pd.isna(nid_val):
            nid_val = ""
        row_dict = row.to_dict()
        row_dict = {k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in row_dict.items()}

        name_val = row.get("Extracted_Name", "Unknown")
        if pd.isna(name_val):
            name_val = "Unknown"
        dob_val = row.get("Cleaned_DOB", "")
        if pd.isna(dob_val):
            dob_val = ""

        invalid_insert_data.append({
            "nid": nid_val,
            "dob": dob_val,
            "name": name_val,
            "division": geo["division"],
            "district": geo["district"],
            "upazila": geo["upazila"],
            "division_id": div_id,
            "district_id": dist_id,
            "upazila_id": upazila_id,
            "card_no": row.get("Card_No", ""),
            "master_serial": row.get("Master_Serial", ""),
            "mobile": row.get("Mobile", ""),
            "source_file": file.filename,
            "batch_id": batch.id,
            "upload_batch": current_version,
            "error_message": str(row.get("Message", "Unknown Error")),
            "data": row_dict,
        })

    if invalid_insert_data:
        for i in range(0, len(invalid_insert_data), 2000):
            chunk = invalid_insert_data[i : i + 2000]
            db.bulk_insert_mappings(InvalidRecord, chunk)

    # --- Iterative Correction Logic ---
    if is_correction:
        from sqlalchemy import or_

        valid_nids_corr = [str(r["nid"]) for r in insert_data if r.get("nid")]
        valid_cards = [str(r["card_no"]) for r in insert_data if r.get("card_no")]
        valid_names = [str(r["name"]) for r in insert_data if r.get("name") and r["name"] != "Unknown"]
        valid_mobiles = []
        for r in insert_data:
            mob = r.get("data", {}).get("Mobile", "") if isinstance(r.get("data"), dict) else ""
            if mob and str(mob).strip():
                valid_mobiles.append(str(mob).strip())

        match_conditions = []
        if valid_nids_corr:
            match_conditions.append(InvalidRecord.nid.in_(valid_nids_corr))
        if valid_cards:
            match_conditions.append(InvalidRecord.card_no.in_(valid_cards))
        if valid_names:
            match_conditions.append(InvalidRecord.name.in_(valid_names))
        if valid_mobiles:
            match_conditions.append(InvalidRecord.mobile.in_(valid_mobiles))

        if match_conditions:
            db.query(InvalidRecord).filter(
                InvalidRecord.upazila == geo["upazila"],
                InvalidRecord.district == geo["district"],
                or_(*match_conditions),
            ).delete(synchronize_session=False)
            db.commit()

    batch.new_records = new_count
    batch.updated_records = updated_count
    db.commit()

    summary = refresh_summary_stats(
        db,
        geo["division"],
        geo["district"],
        geo["upazila"],
        filename=file.filename,
        stats_source={
            "total_rows": stats["total_rows"],
            "valid_count": valid_count,
            "error_count": error_count,
            "new_count": new_count,
            "updated_count": updated_count,
        },
        current_version=current_version,
        column_headers=original_headers,  # Persist original Bangla column headers
    )

    summary.pdf_url = f"/api/export/download/{urllib.parse.quote(filename)}"
    summary.pdf_invalid_url = f"/api/export/download/{urllib.parse.quote(pdf_invalid_filename)}" if pdf_invalid_filename else ""
    summary.excel_url = f"/api/export/download/{urllib.parse.quote(excel_filename)}" if excel_filename else ""
    summary.excel_valid_url = f"/api/export/download/{urllib.parse.quote(excel_valid_filename)}" if excel_valid_filename else ""
    summary.excel_invalid_url = f"/api/export/download/{urllib.parse.quote(excel_invalid_filename)}" if excel_invalid_filename else ""
    db.commit()
    db.refresh(summary)

    log_audit(
        db,
        current_user,
        "CREATE",
        "upload_batch",
        batch.id,
        new_data={
            "action": "file_upload_validation",
            "filename": file.filename,
            "total_rows": stats["total_rows"],
            "valid": valid_count,
            "invalid": error_count,
            "new": new_count,
            "updated": updated_count,
            "geo": geo,
            "summary_id": summary.id,
        },
    )

    return {
        "summary": stats,
        "geo": geo,
        "valid_count": valid_count,
        "invalid_count": error_count,
        "new_records": new_count,
        "updated_records": updated_count,
        "cross_upazila_duplicates": cross_upazila_duplicates,
        "batch_id": batch.id,
        "version": summary.version,
        "updated_at": summary.updated_at.isoformat() + "Z",
        "pdf_url": f"/api/export/download/{urllib.parse.quote(filename)}",
        "pdf_invalid_url": f"/api/export/download/{urllib.parse.quote(pdf_invalid_filename)}",
        "excel_url": f"/api/export/download/{urllib.parse.quote(excel_filename)}",
        "excel_valid_url": f"/api/export/download/{urllib.parse.quote(excel_valid_filename)}",
        "excel_invalid_url": f"/api/export/download/{urllib.parse.quote(excel_invalid_filename)}",
        "preview_data": preview_data,
    }


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
        tz_limit = int(tz_limit_conf.value) if tz_limit_conf and tz_limit_conf.value.isdigit() else 0

        tz_whitelist_records = db.query(TrailingZeroWhitelist.nid).all()
        tz_whitelist = {r[0] for r in tz_whitelist_records}

        def read_and_process():
            if sheet_name and sheet_name.strip():
                df = pd.read_excel(io.BytesIO(contents), sheet_name=sheet_name.strip(), header=header_row - 1, nrows=10, dtype=str)
            else:
                df = pd.read_excel(io.BytesIO(contents), header=header_row - 1, nrows=10, dtype=str)
            return process_dataframe(df, dob_col=dob_column, nid_col=nid_column, header_row=header_row, tz_limit=tz_limit, tz_whitelist=tz_whitelist)

        processed_df, stats = await asyncio.to_thread(read_and_process)
        preview_data = processed_df.replace({float("nan"): None}).to_dict(orient="records")

        total = len(preview_data)
        invalid = sum(1 for r in preview_data if r.get("Status") == "error")
        invalid_pct = round((invalid / total) * 100, 1) if total > 0 else 0
        blocked = invalid_pct > 50

        return {"preview": preview_data, "summary": stats, "invalid_pct": invalid_pct, "blocked": blocked}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
