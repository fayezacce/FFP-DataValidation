from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
import os
import uvicorn
from contextlib import asynccontextmanager
import openpyxl
from openpyxl.styles import PatternFill

from .validator import process_dataframe
from .pdf_generator import generate_pdf_report

@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs("downloads", exist_ok=True)
    yield

app = FastAPI(title="FFP Data Validator API", lifespan=lifespan)

# Allow CORS for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict to frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MAX_UPLOAD_SIZE = int(os.environ.get("MAX_UPLOAD_SIZE", 20 * 1024 * 1024)) # 20MB

@app.post("/validate")
async def validate_file(
    file: UploadFile = File(...),
    dob_column: str = Form(...),
    nid_column: str = Form(...),
    header_row: int = Form(1),
    additional_columns: str = Form(""),
    sheet_name: str = Form(None)
):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are allowed.")
        
    # Read file content
    contents = await file.read()
    if len(contents) > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=400, detail="File too large. Max 20MB allowed.")
        
    try:
        # header_row is 1-indexed from user perspective, pandas is 0-indexed
        if sheet_name and sheet_name.strip():
            df = pd.read_excel(io.BytesIO(contents), sheet_name=sheet_name.strip(), header=header_row - 1)
        else:
            df = pd.read_excel(io.BytesIO(contents), header=header_row - 1)
        # Drop rows where all name/dob/nid columns might be purely empty to avoid blank row issues
        # We will do this carefully or let process_dataframe handle it.
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read Excel file: {str(e)}")
        
    try:
        processed_df, stats = process_dataframe(df, dob_col=dob_column, nid_col=nid_column)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")
        
    # Parse additional columns
    add_cols = [c.strip() for c in additional_columns.split(",")] if additional_columns else []
        
    # Generate PDF
    original_filename_no_ext = os.path.splitext(file.filename)[0]
    pdf_path = generate_pdf_report(processed_df, stats, additional_columns=add_cols, original_filename=original_filename_no_ext)
    filename = os.path.basename(pdf_path)
    
    # Generate Excel export
    # Always output as .xlsx because pandas 2.2 no longer supports writing .xls via xlwt.
    excel_filename = f"{original_filename_no_ext}_validation_tested.xlsx"
    excel_path = os.path.join("downloads", excel_filename)
    
    red_fill = PatternFill(start_color='FFFFCCCC', end_color='FFFFCCCC', fill_type='solid') # light red
    yellow_fill = PatternFill(start_color='FFFFFF99', end_color='FFFFFF99', fill_type='solid') # light yellow
    
    if file.filename.endswith('.xls') and not file.filename.endswith('.xlsx'):
        # Fallback to pandas basic export for strict .xls format because openpyxl can't load .xls for style preserving
        processed_df[dob_column] = processed_df[dob_column].astype(object)
        processed_df[nid_column] = processed_df[nid_column].astype(object)
        
        for idx, row in processed_df.iterrows():
            processed_df.at[idx, dob_column] = row['Cleaned_DOB']
            processed_df.at[idx, nid_column] = row['Cleaned_NID']
            
        cols_to_drop = ['Cleaned_DOB', 'Cleaned_NID', 'Status', 'Message', 'Excel_Row']
        export_df = processed_df.drop(columns=[c for c in cols_to_drop if c in processed_df.columns])
        export_df.to_excel(excel_path, index=False, engine='openpyxl')
        
        # Color the rows in the newly generated .xlsx
        wb = openpyxl.load_workbook(excel_path)
        ws = wb.active
        for idx, row in processed_df.iterrows():
            r = idx + 2 # pandas index + header(1) + 1
            status = row['Status']
            if status == 'error':
                for c in range(1, ws.max_column + 1):
                    ws.cell(row=r, column=c).fill = red_fill
            elif status == 'warning':
                for c in range(1, ws.max_column + 1):
                    ws.cell(row=r, column=c).fill = yellow_fill
        wb.save(excel_path)
    else:
        
        wb = openpyxl.load_workbook(io.BytesIO(contents))
        if sheet_name and sheet_name.strip() in wb.sheetnames:
            ws = wb[sheet_name.strip()]
        else:
            ws = wb.active
            
        dob_col_idx = None
        nid_col_idx = None
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
                ws.cell(row=r, column=dob_col_idx, value=row['Cleaned_DOB'])
            if nid_col_idx:
                ws.cell(row=r, column=nid_col_idx, value=row['Cleaned_NID'])
            
            if status == 'error':
                for c in range(1, ws.max_column + 1):
                    ws.cell(row=r, column=c).fill = red_fill
            elif status == 'warning':
                for c in range(1, ws.max_column + 1):
                    ws.cell(row=r, column=c).fill = yellow_fill
                    
        wb.save(excel_path)
    
    # Prepare preview data (first 50 rows to limit payload size)
    # Convert NaNs to None for JSON serialization
    preview_df = processed_df.head(50).replace({float('nan'): None})
    preview_data = preview_df.to_dict(orient="records")
    
    return {
        "summary": stats,
        "pdf_url": f"/downloads/{filename}",
        "excel_url": f"/downloads/{excel_filename}",
        "preview_data": preview_data
    }

@app.get("/downloads/{filename}")
async def download_file(filename: str):
    file_path = os.path.join("downloads", filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
        
    media_type = "application/pdf" if filename.endswith('.pdf') else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return FileResponse(file_path, media_type=media_type, filename=filename)

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
