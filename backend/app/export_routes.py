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


def save_live_excel_nikosh(df: pd.DataFrame, path: str, sheet_name: str, is_valid: bool = True):
    """Save DataFrame to Excel with Nikosh font and column filtering (xlsxwriter)."""
    exclude = [
        "Excel_Row", "NID", "Cleaned_NID", "DOB", "Cleaned_DOB",
        "Name", "Status", "Message", "Division", "District",
        "Upazila", "Batch_ID", "Source_File", "Extracted_Name",
        "Card_No", "Master_Serial", "Mobile", "Fraud_Reason",
    ]
    export_df = df.drop(columns=[c for c in exclude if c in df.columns])

    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        export_df.to_excel(writer, index=False, sheet_name=sheet_name)
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        format_nikosh = workbook.add_format({"font_name": "Nikosh", "font_size": 11})
        worksheet.set_column(0, len(export_df.columns) - 1, None, format_nikosh)


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
            additional_columns=[c for c in df.columns if c not in ["Status", "Message", "Excel_Row", "Cleaned_DOB", "Cleaned_NID", "NID", "DOB", "Batch_ID", "Source_File", "Division", "District", "Upazila"]],
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
            additional_columns=[c for c in df.columns if c not in ["Status", "Message", "Excel_Row", "Cleaned_DOB", "Cleaned_NID"]],
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
# ZIP GENERATION — background tasks for bulk exports
# ─────────────────────────────────────────────────────────────────────────────

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
    background_tasks.add_task(_generate_valid_zip_bg, task_id)
    return {"task_id": task_id, "status": "started"}


def _generate_valid_zip_bg(task_id: str):
    db = SessionLocal()
    task = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
    if not task:
        db.close()
        return

    task.status = "running"
    db.commit()

    try:
        entries = db.query(SummaryStats).filter(SummaryStats.valid > 0).order_by(SummaryStats.division, SummaryStats.district, SummaryStats.upazila).all()
        if not entries:
            task.status = "error"
            task.error_details = "No valid records found in system"
            db.commit()
            db.close()
            return

        total_entries = len(entries)
        temp_dir = os.path.join("downloads", f"temp_bulk_{task_id}")
        os.makedirs(temp_dir, exist_ok=True)
        zip_filename = f"all_live_valid_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.zip"
        zip_path = os.path.join("downloads", zip_filename)

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def process_valid_entry(loc):
            local_db = SessionLocal()
            try:
                df_local = get_live_records_df(local_db, loc["division"], loc["district"], loc["upazila"], is_invalid=False)
                if df_local is None or df_local.empty:
                    return None
                div_name = str(loc["division"] or "Unknown").replace(" ", "_").replace("/", "_")
                dist_name = str(loc["district"] or "Unknown").replace(" ", "_").replace("/", "_")
                upz_name = str(loc["upazila"] or "Unknown").replace(" ", "_").replace("/", "_")
                t_file = os.path.join(temp_dir, f"{dist_name}_{upz_name}_valid.xlsx")
                save_live_excel_nikosh(df_local, t_file, "Valid Records", is_valid=True)
                a_name = f"{div_name}/{dist_name}_{upz_name}_valid.xlsx"
                return (t_file, a_name)
            finally:
                local_db.close()

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
            processed = 0
            locations = [{"division": e.division, "district": e.district, "upazila": e.upazila} for e in entries]

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = {executor.submit(process_valid_entry, loc): loc for loc in locations}
                for future in as_completed(futures):
                    processed += 1
                    try:
                        res = future.result()
                        if res:
                            t_file, a_name = res
                            zipf.write(t_file, arcname=a_name)
                            if os.path.exists(t_file):
                                os.remove(t_file)
                    except Exception as loop_e:
                        logger.error(f"Error processing upazila in zip thread: {loop_e}")

                    if processed % 5 == 0:
                        task.progress = int((processed / total_entries) * 100)
                        task.message = f"Zipping {processed}/{total_entries} locations..."
                        db.commit()

        if not os.path.exists(zip_path) or os.path.getsize(zip_path) < 100:
            if os.path.exists(zip_path):
                os.remove(zip_path)
            raise Exception("Zip file generation failed or empty")

        task.progress = 100
        task.status = "completed"
        task.message = "Zip generated successfully"
        task.result_url = f"/api/export/download/{urllib.parse.quote(zip_filename)}"
        db.commit()

    except Exception as e:
        logger.error(f"Valid-zip generation failed: {str(e)}")
        task.status = "error"
        task.error_details = str(e)
        task.message = "Failed to create live valid zip"
        db.commit()
    finally:
        import shutil
        temp_dir = os.path.join("downloads", f"temp_bulk_{task_id}")
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        db.close()


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
    background_tasks.add_task(_generate_invalid_zip_bg, task_id)
    return {"task_id": task_id, "status": "started"}


def _generate_invalid_zip_bg(task_id: str):
    db = SessionLocal()
    task = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
    if not task:
        db.close()
        return

    task.status = "running"
    db.commit()

    try:
        entries = db.query(SummaryStats).filter(SummaryStats.invalid > 0).order_by(SummaryStats.division, SummaryStats.district, SummaryStats.upazila).all()
        if not entries:
            task.status = "error"
            task.error_details = "No invalid records found in system"
            db.commit()
            db.close()
            return

        total_entries = len(entries)
        temp_dir = os.path.join("downloads", f"temp_bulk_invalid_{task_id}")
        os.makedirs(temp_dir, exist_ok=True)
        zip_filename = f"all_live_invalid_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.zip"
        zip_path = os.path.join("downloads", zip_filename)

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def process_invalid_entry(loc):
            local_db = SessionLocal()
            try:
                df_local = get_live_records_df(local_db, loc["division"], loc["district"], loc["upazila"], is_invalid=True)
                if df_local is None or df_local.empty:
                    return None

                div_name = str(loc["division"] or "Unknown").replace(" ", "_").replace("/", "_")
                dist_name = str(loc["district"] or "Unknown").replace(" ", "_").replace("/", "_")
                upz_name = str(loc["upazila"] or "Unknown").replace(" ", "_").replace("/", "_")

                t_file = os.path.join(temp_dir, f"{dist_name}_{upz_name}_invalid.xlsx")
                save_live_excel_nikosh(df_local, t_file, "Invalid Records", is_valid=False)

                pdf_s = {"total_rows": len(df_local), "issues": len(df_local), "converted_nid": 0}
                pdf_g = {"division": loc["division"], "district": loc["district"], "upazila": loc["upazila"]}
                t_pdf = generate_pdf_report(df_local, pdf_s, additional_columns=[], output_dir=temp_dir, original_filename=f"{dist_name}_{upz_name}_invalid", geo=pdf_g, invalid_only=True)

                return (
                    (t_file, f"{div_name}/{dist_name}_{upz_name}_invalid.xlsx"),
                    (t_pdf, f"{div_name}/{dist_name}_{upz_name}_invalid.pdf") if t_pdf else None,
                )
            finally:
                local_db.close()

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
            processed = 0
            locations = [{"division": e.division, "district": e.district, "upazila": e.upazila} for e in entries]

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = {executor.submit(process_invalid_entry, loc): loc for loc in locations}
                for future in as_completed(futures):
                    processed += 1
                    try:
                        res = future.result()
                        if res:
                            excel_tuple, pdf_tuple = res
                            zipf.write(excel_tuple[0], arcname=excel_tuple[1])
                            if os.path.exists(excel_tuple[0]):
                                os.remove(excel_tuple[0])
                            if pdf_tuple:
                                zipf.write(pdf_tuple[0], arcname=pdf_tuple[1])
                                if os.path.exists(pdf_tuple[0]):
                                    os.remove(pdf_tuple[0])
                    except Exception as loop_e:
                        logger.error(f"Error processing upazila in zip thread: {loop_e}")

                    if processed % 3 == 0:
                        task.progress = int((processed / total_entries) * 100)
                        task.message = f"Zipping {processed}/{total_entries} locations..."
                        db.commit()

        if not os.path.exists(zip_path) or os.path.getsize(zip_path) < 100:
            if os.path.exists(zip_path):
                os.remove(zip_path)
            raise Exception("Zip file generation failed or empty")

        task.progress = 100
        task.status = "completed"
        task.message = "Zip generated successfully"
        task.result_url = f"/api/export/download/{urllib.parse.quote(zip_filename)}"
        db.commit()

    except Exception as e:
        logger.error(f"Invalid-zip generation failed: {str(e)}")
        task.status = "error"
        task.error_details = str(e)
        task.message = "Failed to create live invalid zip"
        db.commit()
    finally:
        import shutil
        temp_dir = os.path.join("downloads", f"temp_bulk_invalid_{task_id}")
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        db.close()
