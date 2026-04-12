from sqlalchemy.orm import Session
from sqlalchemy import func
from .models import SummaryStats, ValidRecord, InvalidRecord
import logging

logger = logging.getLogger("ffp")

def refresh_summary_stats(db: Session, division: str, district: str, upazila: str, filename: str = None, stats_source: dict = None, current_version: int = None, column_headers: list = None):
    """
    Recalculates Valid/Invalid counts for a specific Upazila from absolute truth (ValidRecord/InvalidRecord tables)
    and updates the SummaryStats table. Highly robust against data drift.

    column_headers: ordered list of original Excel column names. Stored per-upazila so
                    the export can reconstruct exact original headers at download time.
    """
    from sqlalchemy import func
    
    # We use LOWER(TRIM()) to handle all the common data entry issues
    total_valid = db.query(func.count(ValidRecord.id)).filter(
        func.lower(func.trim(ValidRecord.upazila)) == func.lower(func.trim(upazila)),
        func.lower(func.trim(ValidRecord.district)) == func.lower(func.trim(district))
    ).scalar() or 0
    
    total_invalid = db.query(func.count(InvalidRecord.id)).filter(
        func.lower(func.trim(InvalidRecord.upazila)) == func.lower(func.trim(upazila)),
        func.lower(func.trim(InvalidRecord.district)) == func.lower(func.trim(district))
    ).scalar() or 0

    summary = db.query(SummaryStats).filter(
        func.lower(func.trim(SummaryStats.district)) == func.lower(func.trim(district)),
        func.lower(func.trim(SummaryStats.upazila)) == func.lower(func.trim(upazila))
    ).first()

    # Get geo IDs
    from .models import Division, District, Upazila
    
    div_obj = db.query(Division).filter(func.lower(Division.name) == func.lower(func.trim(division))).first()
    dist_obj = db.query(District).filter(func.lower(District.name) == func.lower(func.trim(district))).first()
    upz_obj = db.query(Upazila).filter(
        func.lower(Upazila.name) == func.lower(func.trim(upazila)),
        func.lower(Upazila.district_name) == func.lower(func.trim(district))
    ).first()

    div_id = div_obj.id if div_obj else None
    dist_id = dist_obj.id if dist_obj else None
    upazila_id = upz_obj.id if upz_obj else None

    if summary:
        summary.valid = total_valid
        summary.invalid = total_invalid
        summary.total = total_valid + total_invalid
        summary.division_id = div_id
        summary.district_id = dist_id
        summary.upazila_id = upazila_id
        
        # If we have immediate upload results, update those fields too
        if stats_source:
            summary.last_upload_total = stats_source.get('total_rows', summary.last_upload_total)
            summary.last_upload_valid = stats_source.get('valid_count', summary.last_upload_valid)
            summary.last_upload_invalid = stats_source.get('error_count', summary.last_upload_invalid)
            summary.last_upload_new = stats_source.get('new_count', summary.last_upload_new)
            summary.last_upload_updated = stats_source.get('updated_count', summary.last_upload_updated)
        
        if current_version:
            summary.version = current_version
        if filename:
            summary.filename = filename
        # Always update headers if provided (latest upload wins)
        if column_headers:
            summary.column_headers = column_headers
    else:
        # Create new summary row if it doesn't exist
        summary = SummaryStats(
            division=division,
            district=district,
            upazila=upazila,
            total=total_valid + total_invalid,
            valid=total_valid,
            invalid=total_invalid,
            version=current_version or 1,
            filename=filename or "System Refresh",
            division_id=div_id,
            district_id=dist_id,
            upazila_id=upazila_id,
            column_headers=column_headers,
        )
        db.add(summary)
    
    db.commit()
    db.refresh(summary)
    return summary
