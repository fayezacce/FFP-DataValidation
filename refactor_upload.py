import re
import sys

with open('c:/FFP-DataValidation/backend/app/upload_routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# I want to add `from .database import SessionLocal`
content = content.replace('from .database import get_db', 'from .database import get_db, SessionLocal')

# Insert the get status endpoint just before the /validate endpoint
status_endpoint = """
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

"""
content = re.sub(r'(@router\.post\("/validate")', status_endpoint + r'\1', content)

# Now, we extract the large logic block from `validate_excel` and turn it into `process_validation_bg`

# We need to split `validate_excel`
# It starts at `def validate_excel(`
# It ends around `return { "summary": stats, ... }`

# Since regex on 400 lines is hard to get exactly right, we will just construct the new file using parts.

new_bg_function = """

async def process_validation_bg(
    batch_id: int,
    file_path: str,
    original_filename_no_ext: str,
    contents: bytes,
    dob_column: str,
    nid_column: str,
    header_row: int,
    additional_columns: str,
    sheet_name: str,
    geo: dict,
    is_correction: bool,
    current_user_id: int
):
    db: Session = SessionLocal()
    try:
        # Fetch the user to avoid detached instance issues if needed
        current_user = db.query(User).get(current_user_id)
        batch = db.query(UploadBatch).get(batch_id)
        if not batch or not current_user:
            return

        tz_limit_conf = db.query(SystemConfig).filter(SystemConfig.key == "trailing_zero_limit").first()
        tz_limit = int(tz_limit_conf.value) if tz_limit_conf and tz_limit_conf.value.isdigit() else 0

        tz_whitelist_records = db.query(TrailingZeroWhitelist.nid).all()
        tz_whitelist = {r[0] for r in tz_whitelist_records}

        def read_and_process():
            if sheet_name and sheet_name.strip():
                df = pd.read_excel(io.BytesIO(contents), sheet_name=sheet_name.strip(), header=header_row - 1, dtype=str)
            else:
                df = pd.read_excel(io.BytesIO(contents), header=header_row - 1, dtype=str)
            return process_dataframe(df, dob_col=dob_column, nid_col=nid_column, header_row=header_row, tz_limit=tz_limit, tz_whitelist=tz_whitelist)

        try:
            processed_df, stats = await asyncio.to_thread(read_and_process)
        except Exception as e:
            logger.error(f"Error processing dataframe: {e}")
            batch.status = "failed"
            db.commit()
            return

        add_cols = [c.strip() for c in additional_columns.split(",")] if additional_columns else []
        if geo and geo.get("district") and geo.get("upazila") and geo.get("district") != "Unknown" and geo.get("upazila") != "Unknown":
            base_filename = f"{geo['district']}_{geo['upazila']}".replace(" ", "_").replace("/", "_")
        else:
            base_filename = original_filename_no_ext

        pdf_path = generate_pdf_report(processed_df, stats, additional_columns=add_cols, original_filename=base_filename, geo=geo)
        filename = os.path.basename(pdf_path)

        pdf_invalid_path = generate_pdf_report(processed_df, stats, additional_columns=add_cols, original_filename=base_filename, geo=geo, invalid_only=True)
        pdf_invalid_filename = os.path.basename(pdf_invalid_path)

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

        if file_path.endswith(".xls") and not file_path.endswith(".xlsx"):
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

        error_count = int((processed_df["Status"] == "error").sum())
        valid_count = stats["total_rows"] - error_count

        # Set preliminary batch data and commit
        batch.total_rows = stats["total_rows"]
        batch.valid_count = valid_count
        batch.invalid_count = error_count
        db.commit()

        summary = db.query(SummaryStats).filter(
            SummaryStats.district == geo["district"],
            SummaryStats.upazila == geo["upazila"]
        ).first()
        current_version = (summary.version + 1) if summary else 1

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
        insert_data = []

        for _, row in valid_rows.iterrows():
            nid = str(row["Cleaned_NID"]).strip()
            if not nid: continue
            row_dict = row.to_dict()
            row_dict = {k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in row_dict.items()}

            name_val = "Unknown" if pd.isna(row.get("Extracted_Name")) else row.get("Extracted_Name")
            dob_val = "" if pd.isna(row.get("Cleaned_DOB")) else row.get("Cleaned_DOB")

            if nid in existing_map:
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
                "card_no": row.get("Card_No", ""),
                "source_file": os.path.basename(file_path),
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
                        "card_no": stmt.excluded.card_no,
                        "source_file": stmt.excluded.source_file,
                        "batch_id": stmt.excluded.batch_id,
                        "upload_batch": stmt.excluded.upload_batch,
                        "data": stmt.excluded.data,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
                db.execute(stmt)

        invalid_rows = processed_df[processed_df["Status"] == "error"]
        invalid_insert_data = []
        for _, row in invalid_rows.iterrows():
            nid_val = "" if pd.isna(row.get("Cleaned_NID")) else str(row.get("Cleaned_NID")).strip()
            row_dict = row.to_dict()
            row_dict = {k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in row_dict.items()}

            name_val = "Unknown" if pd.isna(row.get("Extracted_Name")) else row.get("Extracted_Name")
            dob_val = "" if pd.isna(row.get("Cleaned_DOB")) else row.get("Cleaned_DOB")

            invalid_insert_data.append({
                "nid": nid_val,
                "dob": dob_val,
                "name": name_val,
                "division": geo["division"],
                "district": geo["district"],
                "upazila": geo["upazila"],
                "card_no": row.get("Card_No", ""),
                "master_serial": row.get("Master_Serial", ""),
                "mobile": row.get("Mobile", ""),
                "source_file": os.path.basename(file_path),
                "batch_id": batch.id,
                "upload_batch": current_version,
                "error_message": str(row.get("Message", "Unknown Error")),
                "data": row_dict,
            })

        if invalid_insert_data:
            for i in range(0, len(invalid_insert_data), 2000):
                chunk = invalid_insert_data[i : i + 2000]
                db.bulk_insert_mappings(InvalidRecord, chunk)

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
            if valid_nids_corr: match_conditions.append(InvalidRecord.nid.in_(valid_nids_corr))
            if valid_cards: match_conditions.append(InvalidRecord.card_no.in_(valid_cards))
            if valid_names: match_conditions.append(InvalidRecord.name.in_(valid_names))
            if valid_mobiles: match_conditions.append(InvalidRecord.mobile.in_(valid_mobiles))

            if match_conditions:
                db.query(InvalidRecord).filter(
                    InvalidRecord.upazila == geo["upazila"],
                    InvalidRecord.district == geo["district"],
                    or_(*match_conditions),
                ).delete(synchronize_session=False)

        batch.new_records = new_count
        batch.updated_records = updated_count
        batch.status = "completed"
        db.commit()

        summary = refresh_summary_stats(
            db,
            geo["division"], geo["district"], geo["upazila"],
            filename=os.path.basename(file_path),
            stats_source={"total_rows": stats["total_rows"], "valid_count": valid_count, "error_count": error_count, "new_count": new_count, "updated_count": updated_count},
            current_version=current_version,
        )

        summary.pdf_url = f"/api/downloads/{urllib.parse.quote(filename)}"
        summary.pdf_invalid_url = f"/api/downloads/{urllib.parse.quote(pdf_invalid_filename)}" if pdf_invalid_filename else ""
        summary.excel_url = f"/api/downloads/{urllib.parse.quote(excel_filename)}" if excel_filename else ""
        summary.excel_valid_url = f"/api/downloads/{urllib.parse.quote(excel_valid_filename)}" if excel_valid_filename else ""
        summary.excel_invalid_url = f"/api/downloads/{urllib.parse.quote(excel_invalid_filename)}" if excel_invalid_filename else ""
        db.commit()

        log_audit(
            db, current_user, "CREATE", "upload_batch", batch.id,
            new_data={
                "action": "file_upload_validation", "filename": os.path.basename(file_path),
                "total_rows": stats["total_rows"], "valid": valid_count, "invalid": error_count,
                "new": new_count, "updated": updated_count, "geo": geo, "summary_id": summary.id,
            },
        )
    except Exception as e:
        logger.error(f"Background processing error: {e}")
        try:
            batch = db.query(UploadBatch).get(batch_id)
            if batch:
                batch.status = "failed"
                db.commit()
        except:
            pass
    finally:
        db.close()

"""
new_validate_func = """
@router.post("/validate", dependencies=[Depends(PermissionChecker("upload_data"))])
async def validate_excel(
    request: Request,
    background_tasks: BackgroundTasks,
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
    if getattr(request.app.state, "security_lockout", False):
        raise HTTPException(status_code=503, detail="Security lockout active.")

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

    # Create Batch immediately
    batch = UploadBatch(
        filename=safe_filename,
        original_name=file.filename,
        uploader_id=current_user.id,
        username=current_user.username,
        division=geo["division"],
        district=geo["district"],
        upazila=geo["upazila"],
        total_rows=0,
        valid_count=0,
        invalid_count=0,
        status="processing",
    )
    db.add(batch)
    db.commit()
    db.refresh(batch)

    # Convert contents to pass by value if needed, or pass bytes
    original_filename_no_ext = os.path.splitext(file.filename)[0]

    background_tasks.add_task(
        process_validation_bg,
        batch.id,
        upload_path,
        original_filename_no_ext,
        contents,
        dob_column,
        nid_column,
        header_row,
        additional_columns,
        sheet_name,
        geo,
        is_correction,
        current_user.id
    )

    return {"status": "queued", "task_id": batch.id, "message": "File queued for validation successfully."}

@router.post("/preview", dependencies=[Depends(PermissionChecker("upload_data"))])
"""

content = re.sub(r'@router\.post\("/validate"\).*?@router\.post\("/preview"', new_bg_function + new_validate_func, content, flags=re.DOTALL)

with open('c:/FFP-DataValidation/backend/app/upload_routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("upload_routes.py successfully refactored!")

