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
import asyncio
from contextlib import asynccontextmanager
import openpyxl
from openpyxl.styles import PatternFill
import json
from datetime import datetime
from sqlalchemy.orm import Session
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi.requests import Request
import openpyxl
from openpyxl.styles import PatternFill
import json
from datetime import datetime
from sqlalchemy.orm import Session
from .database import engine, Base, get_db, SessionLocal
from .models import SummaryStats, ValidRecord, UploadedFile, User
from sqlalchemy import or_
from sqlalchemy.dialects.postgresql import insert
from .validator import process_dataframe
from .pdf_generator import generate_pdf_report
from .bd_geo import fuzzy_match_location, get_division_for_district
from .auth import get_current_user, require_role, hash_password, get_api_key
from . import auth_routes
from . import admin_routes
import urllib.parse
import zipfile
import tempfile

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
app.include_router(admin_routes.router)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

def get_db_rate_limit(request: Request):
    db: Session = next(get_db())
    config = db.query(SystemConfig).filter(SystemConfig.key == "rate_limit_value").first()
    limit = config.value if config else "60/minute"
    return limit

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
        def read_and_process():
            if sheet_name and sheet_name.strip():
                df = pd.read_excel(io.BytesIO(contents), sheet_name=sheet_name.strip(), header=header_row - 1, dtype=str)
            else:
                df = pd.read_excel(io.BytesIO(contents), header=header_row - 1, dtype=str)
            return process_dataframe(df, dob_col=dob_column, nid_col=nid_column, header_row=header_row)
            
        processed_df, stats = await asyncio.to_thread(read_and_process)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading or processing data: {str(e)}")
        
    # Parse additional columns
    add_cols = [c.strip() for c in additional_columns.split(",")] if additional_columns else []
        
    # Generate PDF
    original_filename_no_ext = os.path.splitext(file.filename)[0]
    
    # Create proper naming base: districtname_upazilaName
    if geo and geo.get("district") and geo.get("upazila") and geo.get("district") != "Unknown" and geo.get("upazila") != "Unknown":
        base_filename = f"{geo['district']}_{geo['upazila']}".replace(" ", "_").replace("/", "_")
    else:
        base_filename = original_filename_no_ext

    pdf_path = generate_pdf_report(processed_df, stats, additional_columns=add_cols, original_filename=base_filename, geo=geo)
    filename = os.path.basename(pdf_path)
    
    # Generate Excel exports
    red_fill = PatternFill(start_color='FFFFCCCC', end_color='FFFFCCCC', fill_type='solid')
    yellow_fill = PatternFill(start_color='FFFFFF99', end_color='FFFFFF99', fill_type='solid')
    
    # 1) Full tested output with coloring
    excel_filename = f"{base_filename}_tested.xlsx"
    excel_path = os.path.join("downloads", excel_filename)
    
    # 2) Valid-only clean output
    excel_valid_filename = f"{base_filename}_valid.xlsx"
    excel_valid_path = os.path.join("downloads", excel_valid_filename)
    
    # 3) Invalid-only output (NEW)
    excel_invalid_filename = f"{base_filename}_invalid.xlsx"
    excel_invalid_path = os.path.join("downloads", excel_invalid_filename)
    
    # Find column indices in original workbook
    dob_col_idx = None
    nid_col_idx = None
    
    if file.filename.endswith('.xls') and not file.filename.endswith('.xlsx'):
        # Fallback for strict .xls
        processed_df[dob_column] = processed_df[dob_column].astype(object)
        processed_df[nid_column] = processed_df[nid_column].astype(object)
        
        for idx in range(len(processed_df)):
            processed_df.at[idx, dob_column] = processed_df.at[idx, 'Cleaned_DOB']
            processed_df.at[idx, nid_column] = processed_df.at[idx, 'Cleaned_NID']
            
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
        
        valid_mask = processed_df['Status'] != 'error'
        export_df[valid_mask].to_excel(excel_valid_path, index=False, engine='openpyxl')
        
        invalid_mask = processed_df['Status'] == 'error'
        export_df[invalid_mask].to_excel(excel_invalid_path, index=False, engine='openpyxl')
        
    else:
        # Standard .xlsx handling
        # 1. Tested Workbook (Retain Original Formatting)
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
            
            # Update cells safely (skip MergedCells which are read-only)
            if dob_col_idx:
                c_dob = ws.cell(row=r, column=dob_col_idx)
                if type(c_dob).__name__ != 'MergedCell':
                    c_dob.value = row.Cleaned_DOB
                    c_dob.number_format = '@'
                    
            if nid_col_idx:
                c_nid = ws.cell(row=r, column=nid_col_idx)
                if type(c_nid).__name__ != 'MergedCell':
                    c_nid.value = row.Cleaned_NID
                    c_nid.number_format = '@'
            
            # Apply highlighting safely
            if status == 'error':
                for c in range(1, ws.max_column + 1):
                    cell = ws.cell(row=r, column=c)
                    if type(cell).__name__ != 'MergedCell':
                        cell.fill = red_fill
            elif status == 'warning':
                for c in range(1, ws.max_column + 1):
                    cell = ws.cell(row=r, column=c)
                    if type(cell).__name__ != 'MergedCell':
                        cell.fill = yellow_fill
                        
        wb.save(excel_path)
        
        # 2. Valid and Invalid Workbooks (Fast Pandas Export instead of openpyxl row deletion)
        # Note: We swap the raw DOB/NID data with the cleaned versions for the final downloads
        export_df = processed_df.copy()
        export_df[dob_column] = export_df['Cleaned_DOB']
        export_df[nid_column] = export_df['Cleaned_NID']
        
        cols_to_drop = ['Cleaned_DOB', 'Cleaned_NID', 'DOB_Year', 'Status', 'Message', 'Excel_Row']
        export_df = export_df.drop(columns=[c for c in cols_to_drop if c in export_df.columns])
        
        valid_mask = processed_df['Status'] != 'error'
        export_df[valid_mask].to_excel(excel_valid_path, index=False, engine='openpyxl')
        
        invalid_mask = processed_df['Status'] == 'error'
        export_df[invalid_mask].to_excel(excel_invalid_path, index=False, engine='openpyxl')
    
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
    
    # Bulk check existing NIDs
    valid_nids = [str(row['Cleaned_NID']).strip() for _, row in valid_rows.iterrows() if pd.notna(row['Cleaned_NID']) and str(row['Cleaned_NID']).strip()]
    existing_records = []
    if valid_nids:
        # Query in chunks if too many NIDs
        for i in range(0, len(valid_nids), 5000):
            chunk = valid_nids[i:i+5000]
            existing_records.extend(db.query(ValidRecord.nid, ValidRecord.district, ValidRecord.upazila).filter(ValidRecord.nid.in_(chunk)).all())
    existing_map = {r.nid: r for r in existing_records}

    insert_data = []    
    for _, row in valid_rows.iterrows():
        nid = str(row['Cleaned_NID']).strip()
        if not nid:
            continue
            
        row_dict = row.to_dict()
        # Sanitize NaN values for JSON storage
        row_dict = {k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in row_dict.items()}
        
        name_val = row.get('Name', row.get('name', 'Unknown'))
        if pd.isna(name_val): name_val = 'Unknown'
        dob_val = row.get('Cleaned_DOB', '')
        if pd.isna(dob_val): dob_val = ''
        
        if nid in existing_map:
            existing = existing_map[nid]
            if existing.upazila != geo["upazila"] or existing.district != geo["district"]:
                cross_upazila_duplicates.append({
                    "nid": nid,
                    "name": name_val,
                    "previous_district": existing.district,
                    "previous_upazila": existing.upazila,
                    "new_district": geo["district"],
                    "new_upazila": geo["upazila"]
                })
            updated_count += 1
        else:
            new_count += 1
            existing_map[nid] = existing_records # prevent counting twice if duplicate inside the sheet itself
            
        insert_data.append({
            "nid": nid,
            "dob": dob_val,
            "name": name_val,
            "division": geo["division"],
            "district": geo["district"],
            "upazila": geo["upazila"],
            "source_file": file.filename,
            "upload_batch": current_version,
            "data": row_dict,
            "updated_at": datetime.utcnow()
        })
        
    if insert_data:
        # Chunked bulk upsert
        for i in range(0, len(insert_data), 2000):
            chunk = insert_data[i:i+2000]
            stmt = insert(ValidRecord).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=['nid'],
                set_={
                    'dob': stmt.excluded.dob,
                    'name': stmt.excluded.name,
                    'division': stmt.excluded.division,
                    'district': stmt.excluded.district,
                    'upazila': stmt.excluded.upazila,
                    'source_file': stmt.excluded.source_file,
                    'upload_batch': stmt.excluded.upload_batch,
                    'data': stmt.excluded.data,
                    'updated_at': stmt.excluded.updated_at
                }
            )
            # Execute async so other users aren't blocked completely out of db connections
            db.execute(stmt)
    
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

@app.get("/downloads/valid-zip")
async def download_all_valid_zip(db: Session = Depends(get_db)):
    """Create a zip archive containing all valid Excel files and return it."""
    entries = db.query(SummaryStats).filter(SummaryStats.excel_valid_url != "").all()
    
    if not entries:
        raise HTTPException(status_code=404, detail="No valid Excel files found to download")
        
    # Create a temporary file to store the zip
    tmp_dir = "downloads" # Keep it in the downloads dir for ease
    os.makedirs(tmp_dir, exist_ok=True)
    zip_filename = f"all_valid_files_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    zip_path = os.path.join(tmp_dir, zip_filename)
    
    try:
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for entry in entries:
                # excel_valid_url is like "/downloads/Filename_valid.xlsx"
                # We need the local path
                filename = urllib.parse.unquote(os.path.basename(entry.excel_valid_url))
                local_file_path = os.path.join("downloads", filename)
                
                if os.path.exists(local_file_path):
                    # Naming convention: Division_District_upazila_valid.xlsx
                    # Replace spaces and special chars with underscores
                    div = str(entry.division or "Unknown").replace(" ", "_")
                    dist = str(entry.district or "Unknown").replace(" ", "_")
                    upz = str(entry.upazila or "Unknown").replace(" ", "_")
                    
                    zip_entry_name = f"{div}_{dist}_{upz}_valid.xlsx"
                    zipf.write(local_file_path, arcname=zip_entry_name)
        
        if os.path.getsize(zip_path) == 0:
            os.remove(zip_path)
            raise HTTPException(status_code=404, detail="Zip file is empty")
            
        return FileResponse(
            zip_path, 
            media_type="application/zip", 
            filename="All_Valid_Files.zip"
        )
    except Exception as e:
        if os.path.exists(zip_path):
            os.remove(zip_path)
        raise HTTPException(status_code=500, detail=f"Failed to create zip: {str(e)}")

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
    page: int = 1,
    limit: int = 50,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Search for valid records by NID, DOB, or Name with pagination."""
    query = query.strip()
    if not query:
        return []
    
    # Cap limit for safety in 100+ concurrent requests
    limit = min(limit, 200)
    offset = (page - 1) * limit
    
    if type == "dob":
        results = db.query(ValidRecord).filter(ValidRecord.dob == query).offset(offset).limit(limit).all()
    elif type == "name":
        # Using ilike works great with GiST/B-tree pg_trgm index
        results = db.query(ValidRecord).filter(
            ValidRecord.name.ilike(f"%{query}%")
        ).offset(offset).limit(limit).all()
    else:  # default: nid
        results = db.query(ValidRecord).filter(
            ValidRecord.nid.contains(query)
        ).offset(offset).limit(limit).all()
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
async def get_geo_info(db: Session = Depends(get_db)):
    """Return the hierarchy of divisions, districts, and upazilas."""
    from .bd_geo import DIVISIONS, DISTRICTS, get_dynamic_upazilas
    divisions = list(DIVISIONS.values())
    districts = {}
    for d in DISTRICTS:
        div_name = DIVISIONS.get(d["division_id"], "Unknown")
        if div_name not in districts:
            districts[div_name] = []
        districts[div_name].append(d["name"])
    
    upazilas = get_dynamic_upazilas(db)
        
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

# --- NID Verification & Sync ---

@app.get("/ibas/nid-verify")
@limiter.limit(get_db_rate_limit)
async def ibas_verify_nid(
    request: Request,
    id: str,
    db: Session = Depends(get_db),
    api_user: User = Depends(get_api_key)
):
    """Secure, rate-limited endpoint for IBAS to verify NID."""
    if not id or not id.strip():
        raise HTTPException(status_code=400, detail="Missing NID")
    
    # Simple check, we don't return PII for security, just boolean
    exists = db.query(ValidRecord).filter(ValidRecord.nid == id.strip()).first() is not None
    return {"found": exists}

@app.get("/sync/export")
async def sync_export(
    since: datetime = None,
    db: Session = Depends(get_db),
    api_user: User = Depends(get_api_key)
):
    """Export ValidRecords modified after the provided timestamp."""
    query = db.query(ValidRecord)
    if since:
        query = query.filter(ValidRecord.updated_at > since)
    
    # We might want to limit the payload size in production, but for now we export all requested
    records = query.all()
    out = []
    for r in records:
        out.append({
            "nid": r.nid,
            "dob": r.dob,
            "name": r.name,
            "division": r.division,
            "district": r.district,
            "upazila": r.upazila,
            "source_file": r.source_file,
            "upload_batch": r.upload_batch,
            "data": r.data,
            "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
            "updated_at": r.updated_at.isoformat() + "Z" if r.updated_at else None,
        })
    return {"records": out}

@app.post("/sync/import")
async def sync_import(
    payload: dict,
    db: Session = Depends(get_db),
    api_user: User = Depends(get_api_key)
):
    """Import ValidRecords via bulk upsert."""
    records = payload.get("records", [])
    if not records:
        return {"imported": 0}

    insert_data = []
    for r in records:
        created_at = datetime.fromisoformat(r["created_at"].rstrip("Z")) if r.get("created_at") else datetime.utcnow()
        updated_at = datetime.fromisoformat(r["updated_at"].rstrip("Z")) if r.get("updated_at") else datetime.utcnow()
        insert_data.append({
            "nid": r["nid"],
            "dob": r.get("dob", ""),
            "name": r.get("name", ""),
            "division": r.get("division", ""),
            "district": r.get("district", ""),
            "upazila": r.get("upazila", ""),
            "source_file": r.get("source_file", ""),
            "upload_batch": r.get("upload_batch", 1),
            "data": r.get("data", {}),
            "created_at": created_at,
            "updated_at": updated_at
        })

    # Chunked upsert
    for i in range(0, len(insert_data), 1000):
        chunk = insert_data[i:i+1000]
        stmt = insert(ValidRecord).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=['nid'],
            set_={
                'dob': stmt.excluded.dob,
                'name': stmt.excluded.name,
                'division': stmt.excluded.division,
                'district': stmt.excluded.district,
                'upazila': stmt.excluded.upazila,
                'source_file': stmt.excluded.source_file,
                'upload_batch': stmt.excluded.upload_batch,
                'data': stmt.excluded.data,
                'updated_at': stmt.excluded.updated_at
            }
        )
        db.execute(stmt)
    db.commit()
    return {"imported": len(records)}

import httpx

@app.post("/admin/instances/{id}/trigger-sync")
async def trigger_remote_sync(
    id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(require_role(["admin"]))
):
    from .models import RemoteInstance
    instance = db.query(RemoteInstance).filter(RemoteInstance.id == id).first()
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")
        
    since_param = f"?since={instance.last_synced_at.isoformat()}" if instance.last_synced_at else ""
    url = f"{instance.url.rstrip('/')}/sync/export{since_param}"
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(url, headers={"X-API-Key": instance.api_key})
            resp.raise_for_status()
            data = resp.json()
            records = data.get("records", [])
            
            # Use our own import function to dry run the code
            if records:
                await sync_import({"records": records}, db=db, api_user=admin)
            
            instance.last_synced_at = datetime.utcnow()
            db.commit()
            return {"synced_count": len(records), "status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
