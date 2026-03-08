"""
FFP Data Validator API
Author: Fayez Ahmed
"""
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import io
import os
import uvicorn
from contextlib import asynccontextmanager
import openpyxl
from openpyxl.styles import PatternFill
import json
from datetime import datetime
from sqlalchemy.orm import Session
from .database import engine, Base, get_db, SessionLocal
from .models import SummaryStats, ValidRecord, UploadedFile, User
from sqlalchemy import or_
from .validator import process_dataframe
from .pdf_generator import generate_pdf_report
from .bd_geo import fuzzy_match_location, get_division_for_district
from .auth import get_current_user, require_role, hash_password
from . import auth_routes
import urllib.parse

@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs("downloads", exist_ok=True)
    os.makedirs("uploads", exist_ok=True)
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    # Run safe column migration (add last_upload_* columns if they don't exist)
    # Run migrations and seeding
    db = SessionLocal()
    try:
        migrate_schema(db)
        migrate_json_to_db(db)
        # Seed default admin if no users exist
        if db.query(User).count() == 0:
            admin_user = User(
                username="admin",
                hashed_password=hash_password("admin123"),
                role="admin"
            )
            db.add(admin_user)
            db.commit()
            print("Default admin user created: admin / admin123")
    finally:
        db.close()
    yield

def migrate_schema(db: Session):
    """Safely add any missing columns to summary_stats table."""
    from sqlalchemy import text
    new_columns = [
        ("last_upload_total", "INTEGER DEFAULT 0"),
        ("last_upload_valid", "INTEGER DEFAULT 0"),
        ("last_upload_invalid", "INTEGER DEFAULT 0"),
        ("last_upload_new", "INTEGER DEFAULT 0"),
        ("last_upload_updated", "INTEGER DEFAULT 0"),
        ("last_upload_duplicate", "INTEGER DEFAULT 0"),
        ("excel_valid_url", "VARCHAR"),
        ("excel_invalid_url", "VARCHAR"),
    ]
    for col_name, col_def in new_columns:
        try:
            db.execute(text(
                f"ALTER TABLE summary_stats ADD COLUMN IF NOT EXISTS {col_name} {col_def}"
            ))
            db.commit()
        except Exception:
            db.rollback()


def migrate_json_to_db(db: Session):
    stats_file = os.path.join("downloads", "validation_stats.json")
    if os.path.exists(stats_file):
        try:
            with open(stats_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                entries = data.get("entries", {})
                for key, val in entries.items():
                    # Check if already exists in DB
                    existing = db.query(SummaryStats).filter(
                        SummaryStats.district == val["district"],
                        SummaryStats.upazila == val["upazila"]
                    ).first()
                    if not existing:
                        # Convert ISO strings to datetime objects
                        created_at = datetime.fromisoformat(val["created_at"].rstrip("Z"))
                        updated_at = datetime.fromisoformat(val["updated_at"].rstrip("Z"))
                        
                        summary = SummaryStats(
                            division=val["division"],
                            district=val["district"],
                            upazila=val["upazila"],
                            total=val["total"],
                            valid=val["valid"],
                            invalid=val["invalid"],
                            version=val.get("version", 1),
                            filename=val.get("filename", ""),
                            pdf_url=val.get("pdf_url", ""),
                            excel_url=val.get("excel_url", ""),
                            excel_valid_url=val.get("excel_valid_url", ""),
                            excel_invalid_url=val.get("excel_invalid_url", ""),
                            created_at=created_at,
                            updated_at=updated_at,
                        )
                        db.add(summary)
                db.commit()
        except Exception as e:
            print(f"Migration error: {e}")

app = FastAPI(title="FFP Data Validator API", lifespan=lifespan)

# Allow CORS for all origins (development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_routes.router)

MAX_UPLOAD_SIZE = int(os.environ.get("MAX_UPLOAD_SIZE", 20 * 1024 * 1024))  # 20MB

@app.post("/validate")
async def validate_file(
    file: UploadFile = File(...),
    dob_column: str = Form(...),
    nid_column: str = Form(...),
    header_row: int = Form(1),
    additional_columns: str = Form(""),
    sheet_name: str = Form(None),
    division: str = Form(None),
    district: str = Form(None),
    upazila: str = Form(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role(["admin", "uploader"]))
):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are allowed.")
        
    # Read file content
    contents = await file.read()
    if len(contents) > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=400, detail="File too large. Max 20MB allowed.")
    
    # Save original file
    upload_path = os.path.join("uploads", file.filename)
    with open(upload_path, "wb") as f:
        f.write(contents)
    
    # Track uploaded file
    db_file = UploadedFile(filename=file.filename, original_name=file.filename, filepath=upload_path)
    db.add(db_file)
    db.commit()

    # Geo-match from filename or use manual overrides
    if division and district and upazila:
        geo = {
            "division": division.strip(),
            "district": district.strip(),
            "upazila": upazila.strip()
        }
    elif district and upazila:
        # Division not provided but district+upazila are
        from .bd_geo import get_division_for_district
        geo = {
            "division": get_division_for_district(district.strip()),
            "district": district.strip(),
            "upazila": upazila.strip()
        }
    else:
        geo = fuzzy_match_location(file.filename)
        # If fuzzy match failed, use Unknown — don't block the user from validating

    try:
        if sheet_name and sheet_name.strip():
            df = pd.read_excel(io.BytesIO(contents), sheet_name=sheet_name.strip(), header=header_row - 1, dtype=str)
        else:
            df = pd.read_excel(io.BytesIO(contents), header=header_row - 1, dtype=str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read Excel file: {str(e)}")
        
    try:
        processed_df, stats = process_dataframe(df, dob_col=dob_column, nid_col=nid_column, header_row=header_row)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")
        
    # Parse additional columns
    add_cols = [c.strip() for c in additional_columns.split(",")] if additional_columns else []
        
    # Generate PDF
    original_filename_no_ext = os.path.splitext(file.filename)[0]
    pdf_path = generate_pdf_report(processed_df, stats, additional_columns=add_cols, original_filename=original_filename_no_ext, geo=geo)
    filename = os.path.basename(pdf_path)
    
    # Generate Excel exports
    red_fill = PatternFill(start_color='FFFFCCCC', end_color='FFFFCCCC', fill_type='solid')
    yellow_fill = PatternFill(start_color='FFFFFF99', end_color='FFFFFF99', fill_type='solid')
    
    # 1) Full tested output with coloring
    excel_filename = f"{original_filename_no_ext}_tested.xlsx"
    excel_path = os.path.join("downloads", excel_filename)
    
    # 2) Valid-only clean output
    excel_valid_filename = f"{original_filename_no_ext}_valid.xlsx"
    excel_valid_path = os.path.join("downloads", excel_valid_filename)
    
    # 3) Invalid-only output (NEW)
    excel_invalid_filename = f"{original_filename_no_ext}_invalid.xlsx"
    excel_invalid_path = os.path.join("downloads", excel_invalid_filename)
    
    # Find column indices in original workbook
    dob_col_idx = None
    nid_col_idx = None
    
    if file.filename.endswith('.xls') and not file.filename.endswith('.xlsx'):
        # Fallback for strict .xls
        processed_df[dob_column] = processed_df[dob_column].astype(object)
        processed_df[nid_column] = processed_df[nid_column].astype(object)
        
        for idx, row in processed_df.iterrows():
            processed_df.at[idx, dob_column] = row['Cleaned_DOB']
            processed_df.at[idx, nid_column] = row['Cleaned_NID']
            
        cols_to_drop = ['Cleaned_DOB', 'Cleaned_NID', 'Status', 'Message', 'Excel_Row']
        export_df = processed_df.drop(columns=[c for c in cols_to_drop if c in processed_df.columns])
        export_df.to_excel(excel_path, index=False, engine='openpyxl')
        
        # Color the rows in the generated .xlsx
        wb = openpyxl.load_workbook(excel_path)
        ws = wb.active
        for idx, row in processed_df.iterrows():
            r = idx + 2
            status = row['Status']
            if status == 'error':
                for c in range(1, ws.max_column + 1):
                    ws.cell(row=r, column=c).fill = red_fill
            elif status == 'warning':
                for c in range(1, ws.max_column + 1):
                    ws.cell(row=r, column=c).fill = yellow_fill
        wb.save(excel_path)
        
        # Valid-only for .xls
        valid_mask = processed_df['Status'] != 'error'
        valid_df = export_df[valid_mask]
        valid_df.to_excel(excel_valid_path, index=False, engine='openpyxl')
        
        # Invalid-only for .xls
        invalid_mask = processed_df['Status'] == 'error'
        invalid_df = export_df[invalid_mask]
        invalid_df.to_excel(excel_invalid_path, index=False, engine='openpyxl')
    else:
        wb = openpyxl.load_workbook(io.BytesIO(contents))
        if sheet_name and sheet_name.strip() in wb.sheetnames:
            ws = wb[sheet_name.strip()]
        else:
            ws = wb.active
            
        for col_idx in range(1, ws.max_column + 1):
            val = ws.cell(row=header_row, column=col_idx).value
            if str(val).strip() == dob_column.strip():
                dob_col_idx = col_idx
            if str(val).strip() == nid_column.strip():
                nid_col_idx = col_idx
                
        for idx, row in processed_df.iterrows():
            r = int(row['Excel_Row'])
            status = row['Status']
            
            if dob_col_idx:
                cell = ws.cell(row=r, column=dob_col_idx, value=row['Cleaned_DOB'])
                cell.number_format = '@'
            if nid_col_idx:
                cell = ws.cell(row=r, column=nid_col_idx, value=row['Cleaned_NID'])
                cell.number_format = '@'
            
            if status == 'error':
                for c in range(1, ws.max_column + 1):
                    ws.cell(row=r, column=c).fill = red_fill
            elif status == 'warning':
                for c in range(1, ws.max_column + 1):
                    ws.cell(row=r, column=c).fill = yellow_fill
                    
        wb.save(excel_path)
        
        # ── Valid-only workbook ──
        wb_valid = openpyxl.load_workbook(io.BytesIO(contents))
        if sheet_name and sheet_name.strip() in wb_valid.sheetnames:
            ws_valid = wb_valid[sheet_name.strip()]
        else:
            ws_valid = wb_valid.active
            
        rows_to_delete_valid = []
        for idx, row in processed_df.iterrows():
            r = int(row['Excel_Row'])
            status = row['Status']
            
            if status == 'error':
                rows_to_delete_valid.append(r)
            else:
                if dob_col_idx:
                    cell = ws_valid.cell(row=r, column=dob_col_idx, value=row['Cleaned_DOB'])
                    cell.number_format = '@'
                if nid_col_idx:
                    cell = ws_valid.cell(row=r, column=nid_col_idx, value=row['Cleaned_NID'])
                    cell.number_format = '@'
                    
        rows_to_delete_valid.sort(reverse=True)
        for r in rows_to_delete_valid:
            ws_valid.delete_rows(r)
            
        wb_valid.save(excel_valid_path)
        
        # ── Invalid-only workbook (NEW) ──
        wb_invalid = openpyxl.load_workbook(io.BytesIO(contents))
        if sheet_name and sheet_name.strip() in wb_invalid.sheetnames:
            ws_invalid = wb_invalid[sheet_name.strip()]
        else:
            ws_invalid = wb_invalid.active
            
        rows_to_delete_invalid = []
        for idx, row in processed_df.iterrows():
            r = int(row['Excel_Row'])
            status = row['Status']
            
            if status != 'error':
                rows_to_delete_invalid.append(r)
            else:
                # Write cleaned data into invalid rows too
                if dob_col_idx:
                    cell = ws_invalid.cell(row=r, column=dob_col_idx, value=row['Cleaned_DOB'])
                    cell.number_format = '@'
                if nid_col_idx:
                    cell = ws_invalid.cell(row=r, column=nid_col_idx, value=row['Cleaned_NID'])
                    cell.number_format = '@'
                # Highlight invalid rows red
                for c in range(1, ws_invalid.max_column + 1):
                    ws_invalid.cell(row=r, column=c).fill = red_fill
                    
        rows_to_delete_invalid.sort(reverse=True)
        for r in rows_to_delete_invalid:
            ws_invalid.delete_rows(r)
            
        wb_invalid.save(excel_invalid_path)
    
    # Prepare preview data (first 50 rows)
    preview_df = processed_df.head(50).replace({float('nan'): None})
    preview_data = preview_df.to_dict(orient="records")
    
    # Calculate valid/invalid counts
    error_count = int((processed_df['Status'] == 'error').sum())
    valid_count = stats['total_rows'] - error_count
    
    # ── Database Persistence (NID-Aware Upsert) ──
    
    # 1. Upsert ValidRecords — NID is unique across all of Bangladesh
    valid_rows = processed_df[processed_df['Status'] != 'error']
    new_count = 0
    updated_count = 0
    cross_upazila_duplicates = []  # Track NIDs found in different upazilas
    
    # Get the current version for this district+upazila
    summary = db.query(SummaryStats).filter(
        SummaryStats.district == geo["district"],
        SummaryStats.upazila == geo["upazila"]
    ).first()
    current_version = (summary.version + 1) if summary else 1
    
    for _, row in valid_rows.iterrows():
        nid = row['Cleaned_NID']
        if not nid or nid.strip() == '':
            continue
            
        row_dict = row.to_dict()
        # Sanitize NaN values for JSON storage
        row_dict = {k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in row_dict.items()}
        
        existing = db.query(ValidRecord).filter(ValidRecord.nid == nid).first()
        
        if existing:
            # NID already exists — check if from a different upazila
            if existing.upazila != geo["upazila"] or existing.district != geo["district"]:
                cross_upazila_duplicates.append({
                    "nid": nid,
                    "name": row.get('Name', row.get('name', 'Unknown')),
                    "previous_district": existing.district,
                    "previous_upazila": existing.upazila,
                    "new_district": geo["district"],
                    "new_upazila": geo["upazila"]
                })
            # Update existing record with latest data
            existing.dob = row['Cleaned_DOB']
            existing.name = row.get('Name', row.get('name', 'Unknown'))
            existing.division = geo["division"]
            existing.district = geo["district"]
            existing.upazila = geo["upazila"]
            existing.source_file = file.filename
            existing.data = row_dict
            existing.upload_batch = current_version
            existing.updated_at = datetime.utcnow()
            updated_count += 1
        else:
            # Brand new NID — insert
            db.add(ValidRecord(
                nid=nid,
                dob=row['Cleaned_DOB'],
                name=row.get('Name', row.get('name', 'Unknown')),
                division=geo["division"],
                district=geo["district"],
                upazila=geo["upazila"],
                source_file=file.filename,
                upload_batch=current_version,
                data=row_dict
            ))
            new_count += 1
    
    # 2. Accumulate SummaryStats (only unique valid NIDs count)
    new_unique_valid = new_count  # Only genuinely new NIDs
    
    if summary:
        summary.valid += new_unique_valid  # Accumulate unique valid
        summary.invalid += error_count
        summary.total = summary.valid + summary.invalid
        summary.last_upload_total = stats['total_rows']
        summary.last_upload_valid = valid_count
        summary.last_upload_invalid = error_count
        summary.last_upload_new = new_count
        summary.last_upload_updated = updated_count
        summary.last_upload_duplicate = len(cross_upazila_duplicates)
        summary.version = current_version
        summary.filename = file.filename
        summary.pdf_url = f"/downloads/{urllib.parse.quote(filename)}"
        summary.excel_url = f"/downloads/{urllib.parse.quote(excel_filename)}" if excel_filename else ""
        summary.excel_valid_url = f"/downloads/{urllib.parse.quote(excel_valid_filename)}" if excel_valid_filename else ""
        summary.excel_invalid_url = f"/downloads/{urllib.parse.quote(excel_invalid_filename)}" if excel_invalid_filename else ""
    else:
        summary = SummaryStats(
            division=geo["division"],
            district=geo["district"],
            upazila=geo["upazila"],
            total=new_unique_valid + error_count,
            valid=new_unique_valid,
            invalid=error_count,
            last_upload_total=stats['total_rows'],
            last_upload_valid=valid_count,
            last_upload_invalid=error_count,
            last_upload_new=new_count,
            last_upload_updated=updated_count,
            last_upload_duplicate=len(cross_upazila_duplicates),
            filename=file.filename,
            pdf_url=f"/downloads/{urllib.parse.quote(filename)}",
            excel_url=f"/downloads/{urllib.parse.quote(excel_filename)}" if excel_filename else "",
            excel_valid_url=f"/downloads/{urllib.parse.quote(excel_valid_filename)}" if excel_valid_filename else "",
            excel_invalid_url=f"/downloads/{urllib.parse.quote(excel_invalid_filename)}" if excel_invalid_filename else "",
        )
        db.add(summary)
    
    db.commit()
    db.refresh(summary)
    
    return {
        "summary": stats,
        "geo": geo,
        "valid_count": valid_count,
        "invalid_count": error_count,
        "new_records": new_count,
        "updated_records": updated_count,
        "cross_upazila_duplicates": cross_upazila_duplicates,
        "version": summary.version,
        "updated_at": summary.updated_at.isoformat() + "Z",
        "pdf_url": f"/downloads/{urllib.parse.quote(filename)}",
        "excel_url": f"/downloads/{urllib.parse.quote(excel_filename)}",
        "excel_valid_url": f"/downloads/{urllib.parse.quote(excel_valid_filename)}",
        "excel_invalid_url": f"/downloads/{urllib.parse.quote(excel_invalid_filename)}",
        "preview_data": preview_data
    }

@app.get("/downloads/{filename}")
async def download_file(filename: str):
    file_path = os.path.join("downloads", filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
        
    media_type = "application/pdf" if filename.endswith('.pdf') else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return FileResponse(file_path, media_type=media_type, filename=filename)

@app.get("/statistics")
async def get_statistics(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Return all accumulated validation statistics from DB."""
    entries = db.query(SummaryStats).order_by(SummaryStats.division, SummaryStats.district, SummaryStats.upazila).all()
    
    grand_total = {
        "total": sum(e.total for e in entries),
        "valid": sum(e.valid for e in entries),
        "invalid": sum(e.invalid for e in entries),
    }
    
    return {
        "entries": entries,
        "grand_total": grand_total,
    }

@app.get("/search")
async def search_records(
    query: str, 
    type: str = "nid", 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Search for valid records by NID, DOB, or Name."""
    query = query.strip()
    if not query:
        return []
    
    if type == "dob":
        results = db.query(ValidRecord).filter(ValidRecord.dob == query).limit(200).all()
    elif type == "name":
        results = db.query(ValidRecord).filter(
            ValidRecord.name.ilike(f"%{query}%")
        ).limit(200).all()
    else:  # default: nid
        results = db.query(ValidRecord).filter(
            ValidRecord.nid.contains(query)
        ).limit(200).all()
    return results

@app.get("/nid/{nid}")
async def check_nid(
    nid: str, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Public API: Check if an NID exists in the database. Returns the full record if found."""
    record = db.query(ValidRecord).filter(ValidRecord.nid == nid.strip()).first()
    if not record:
        raise HTTPException(status_code=404, detail=f"NID {nid} not found in database")
    return {
        "found": True,
        "nid": record.nid,
        "dob": record.dob,
        "name": record.name,
        "division": record.division,
        "district": record.district,
        "upazila": record.upazila,
        "source_file": record.source_file,
        "data": record.data,
        "created_at": record.created_at.isoformat() + "Z" if record.created_at else None,
        "updated_at": record.updated_at.isoformat() + "Z" if record.updated_at else None,
    }

@app.delete("/record/{record_id}")
async def delete_record(
    record_id: int, 
    db: Session = Depends(get_db),
    admin: User = Depends(require_role(["admin"]))
):
    """Delete a single ValidRecord by ID. Decrements the related SummaryStats."""
    record = db.query(ValidRecord).filter(ValidRecord.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    
    # Decrement SummaryStats for this record's location
    summary = db.query(SummaryStats).filter(
        SummaryStats.district == record.district,
        SummaryStats.upazila == record.upazila
    ).first()
    
    if summary:
        summary.valid = max(0, summary.valid - 1)
        summary.total = max(0, summary.total - 1)
    
    db.delete(record)
    db.commit()
    
    return {"deleted": True, "id": record_id, "nid": record.nid}

class ManualStatsUpdate(BaseModel):
    old_district: str
    old_upazila: str
    new_district: str
    new_upazila: str
    total: int
    valid: int
    invalid: int

@app.put("/statistics/update")
async def update_statistics(
    update: ManualStatsUpdate, 
    db: Session = Depends(get_db),
    admin: User = Depends(require_role(["admin"]))
):
    """Manually update the statistics and location for a specific entry."""
    summary = db.query(SummaryStats).filter(
        SummaryStats.district == update.old_district,
        SummaryStats.upazila == update.old_upazila
    ).first()

    if not summary:
        raise HTTPException(status_code=404, detail="Statistic entry not found")

    summary.district = update.new_district
    summary.upazila = update.new_upazila
    summary.division = get_division_for_district(update.new_district)
    summary.total = update.total
    summary.valid = update.valid
    summary.invalid = update.invalid
    summary.version += 1
    
    db.commit()
    db.refresh(summary)
    return summary

@app.get("/geo-info")
async def get_geo_info():
    """Return the hierarchy of divisions, districts, and upazilas."""
    from .bd_geo import DIVISIONS, DISTRICTS, UPAZILAS
    divisions = list(DIVISIONS.values())
    districts = {}
    for d in DISTRICTS:
        div_name = DIVISIONS.get(d["division_id"], "Unknown")
        if div_name not in districts:
            districts[div_name] = []
        districts[div_name].append(d["name"])
    
    upazilas = {}
    district_map = {d["id"]: d["name"] for d in DISTRICTS}
    for u in UPAZILAS:
        dist_name = district_map.get(u["district_id"], "Unknown")
        if dist_name not in upazilas:
            upazilas[dist_name] = []
        upazilas[dist_name].append(u["name"])
        
    return {
        "divisions": divisions,
        "districts": districts,
        "upazilas": upazilas
    }

@app.get("/guess-location")
async def guess_location(filename: str):
    """Guess the location from the filename."""
    from .bd_geo import fuzzy_match_location
    return fuzzy_match_location(filename)

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
