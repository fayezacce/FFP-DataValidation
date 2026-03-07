from sqlalchemy import Column, Integer, String, DateTime, Float, Index, JSON, UniqueConstraint
from .database import Base
from datetime import datetime

class SummaryStats(Base):
    __tablename__ = "summary_stats"

    id = Column(Integer, primary_key=True, index=True)
    division = Column(String, index=True)
    district = Column(String, index=True)
    upazila = Column(String, index=True)
    total = Column(Integer, default=0)        # Cumulative unique valid + invalid
    valid = Column(Integer, default=0)         # Cumulative unique valid NIDs
    invalid = Column(Integer, default=0)       # Cumulative invalid rows
    # Per-upload snapshot (latest upload stats)
    last_upload_total = Column(Integer, default=0)
    last_upload_valid = Column(Integer, default=0)
    last_upload_invalid = Column(Integer, default=0)
    last_upload_new = Column(Integer, default=0)      # New NIDs added this upload
    last_upload_updated = Column(Integer, default=0)   # Existing NIDs updated this upload
    last_upload_duplicate = Column(Integer, default=0)  # NIDs found in other upazilas
    version = Column(Integer, default=1)
    filename = Column(String)
    pdf_url = Column(String)
    excel_url = Column(String)
    excel_valid_url = Column(String)
    excel_invalid_url = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Ensure unique constraint on district/upazila combo
    __table_args__ = (
        Index('ix_district_upazila', 'district', 'upazila', unique=True),
    )

class ValidRecord(Base):
    __tablename__ = "valid_records"

    id = Column(Integer, primary_key=True, index=True)
    nid = Column(String, unique=True, index=True)  # NID is unique across all of Bangladesh
    dob = Column(String, index=True)
    name = Column(String)
    division = Column(String)
    district = Column(String)
    upazila = Column(String)
    source_file = Column(String)
    upload_batch = Column(Integer, default=1)  # Which upload round added/updated this
    data = Column(JSON)  # Stores all original Excel fields
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('ix_nid_dob', 'nid', 'dob'),
    )

class UploadedFile(Base):
    __tablename__ = "uploaded_files"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String)
    original_name = Column(String)
    filepath = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
