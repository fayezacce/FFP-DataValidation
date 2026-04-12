from sqlalchemy import Column, Integer, String, DateTime, Float, Index, JSON, UniqueConstraint, Boolean
from .database import Base
from datetime import datetime, timezone


def _utcnow():
    return datetime.now(timezone.utc)

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    role = Column(String, default="viewer")  # admin | uploader | viewer
    is_active = Column(Boolean, default=True)
    api_key = Column(String, unique=True, index=True, nullable=True)
    api_key_last_used = Column(DateTime, nullable=True)
    api_rate_limit = Column(Integer, default=60) # Requests per minute
    api_total_limit = Column(Integer, nullable=True) # Overall request limit
    api_usage_count = Column(Integer, default=0)
    api_ip_whitelist = Column(String, nullable=True) # Comma-separated IPs
    division_access = Column(String, nullable=True)
    district_access = Column(String, nullable=True)
    upazila_access = Column(String, nullable=True)
    created_at = Column(DateTime, default=_utcnow)

class SystemConfig(Base):
    __tablename__ = "system_configs"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String, unique=True, index=True, nullable=False)
    value = Column(String, nullable=False)
    description = Column(String, nullable=True)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

class RemoteInstance(Base):
    __tablename__ = "remote_instances"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    api_key = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    last_synced_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=_utcnow)

class Division(Base):
    __tablename__ = "divisions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    is_active = Column(Boolean, default=True)


class District(Base):
    __tablename__ = "districts"

    id = Column(Integer, primary_key=True, index=True)
    division_name = Column(String, index=True, nullable=False)
    name = Column(String, unique=True, index=True, nullable=False)
    is_active = Column(Boolean, default=True)


class Upazila(Base):
    __tablename__ = "upazilas"

    id = Column(Integer, primary_key=True, index=True)
    division_name = Column(String, index=True, nullable=False)
    district_name = Column(String, index=True, nullable=False)
    name = Column(String, index=True, nullable=False)
    quota = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)

    __table_args__ = (
        Index('ix_upazila_district_name', 'district_name', 'name', unique=True),
    )

class SummaryStats(Base):
    __tablename__ = "summary_stats"

    id = Column(Integer, primary_key=True, index=True)
    division = Column(String, index=True)
    district = Column(String, index=True)
    upazila = Column(String, index=True)
    total = Column(Integer, default=0)        # Cumulative unique valid + invalid
    valid = Column(Integer, default=0)         # Cumulative unique valid NIDs
    invalid = Column(Integer, default=0)       # Cumulative invalid rows
    division_id = Column(Integer, index=True, nullable=True)
    district_id = Column(Integer, index=True, nullable=True)
    upazila_id = Column(Integer, index=True, nullable=True)
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
    pdf_invalid_url = Column(String)
    # Original excel column headers preserved per-upazila (ordered list of original names)
    # Updated on every upload so it always reflects the most recent file's headers.
    column_headers = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    # Ensure unique constraint on district/upazila combo
    __table_args__ = (
        Index('ix_district_upazila', 'district', 'upazila', unique=True),
        Index('ix_summary_stats_sorting', 'division', 'district', 'upazila'), # For Statistics Dashboard
    )

class UploadBatch(Base):
    __tablename__ = "upload_batches"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String)
    original_name = Column(String)
    uploader_id = Column(Integer, index=True)
    username = Column(String)
    division = Column(String)
    district = Column(String)
    upazila = Column(String)
    total_rows = Column(Integer)
    valid_count = Column(Integer)
    invalid_count = Column(Integer)
    division_id = Column(Integer, index=True, nullable=True)
    district_id = Column(Integer, index=True, nullable=True)
    upazila_id = Column(Integer, index=True, nullable=True)
    new_records = Column(Integer)
    updated_records = Column(Integer)
    valid_url = Column(String)
    invalid_url = Column(String)
    pdf_url = Column(String)
    pdf_invalid_url = Column(String)
    status = Column(String, default="completed") # completed | deleted
    # Original excel column headers at time of this upload (ordered list)
    column_headers = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=_utcnow)

class ValidRecord(Base):
    __tablename__ = "valid_records"

    id = Column(Integer, primary_key=True, index=True)
    nid = Column(String, unique=True, index=True)  # NID is unique across all of Bangladesh
    dob = Column(String, index=True)
    name = Column(String)
    division = Column(String)
    district = Column(String)
    upazila = Column(String)
    division_id = Column(Integer, index=True, nullable=True)
    district_id = Column(Integer, index=True, nullable=True)
    upazila_id = Column(Integer, index=True, nullable=True)
    source_file = Column(String)
    batch_id = Column(Integer, index=True) # Linked to upload_batches.id
    upload_batch = Column(Integer, default=1)  # Keeping this for legacy compatibility (as version)
    card_no = Column(String, index=True) # Unique card number for upazila
    mobile = Column(String, index=True) # Secondary identifier
    data = Column(JSON)  # Stores all original Excel fields
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    __table_args__ = (
        Index('ix_nid_dob', 'nid', 'dob'),
        Index('ix_valid_record_district_upazila', 'district', 'upazila'),
        Index('ix_valid_record_name', 'name'),
        Index('ix_valid_record_batch', 'upload_batch'),
        Index('ix_valid_record_batch_id', 'batch_id'),
        Index('ix_valid_upazila_nid', 'upazila_id', 'nid'),
        # Trigram indexes for fast global search (requires pg_trgm)
        Index('ix_valid_record_nid_trgm', 'nid', postgresql_using='gist', postgresql_ops={'nid': 'gist_trgm_ops'}),
        Index('ix_valid_record_name_trgm', 'name', postgresql_using='gist', postgresql_ops={'name': 'gist_trgm_ops'}),
    )

class InvalidRecord(Base):
    __tablename__ = "invalid_records"

    id = Column(Integer, primary_key=True, index=True)
    nid = Column(String, index=True)  # Using index instead of unique because error records could be re-uploaded
    dob = Column(String, index=True)
    name = Column(String)
    division = Column(String)
    district = Column(String)
    upazila = Column(String)
    division_id = Column(Integer, index=True, nullable=True)
    district_id = Column(Integer, index=True, nullable=True)
    upazila_id = Column(Integer, index=True, nullable=True)
    source_file = Column(String)
    batch_id = Column(Integer, index=True) # Linked to upload_batches.id
    upload_batch = Column(Integer, default=1)
    master_serial = Column(String, index=True) # Original ID from the excel file for tracking
    card_no = Column(String, index=True) # Unique card number for upazila
    mobile = Column(String, index=True) # Secondary identifier
    error_message = Column(String)  # The validation failure reason
    data = Column(JSON)  # Stores all original Excel fields
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    __table_args__ = (
        Index('ix_invalid_record_batch', 'upload_batch'),
        Index('ix_invalid_record_batch_id', 'batch_id'),
        Index('ix_invalid_upazila_nid', 'upazila_id', 'nid'),
    )

class UploadedFile(Base):

    __tablename__ = "uploaded_files"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String)
    original_name = Column(String)
    filepath = Column(String)
    created_at = Column(DateTime, default=_utcnow)

class Permission(Base):
    __tablename__ = "permissions"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False) # e.g. "upload_data", "view_admin"
    description = Column(String)

class RolePermission(Base):
    __tablename__ = "role_permissions"
    id = Column(Integer, primary_key=True, index=True)
    role = Column(String, index=True, nullable=False) # admin, uploader, viewer
    permission_name = Column(String, index=True, nullable=False)

class AuditLog(Base):
    __tablename__ = "audit_logs"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    username = Column(String, index=True)
    action = Column(String) # CREATE, UPDATE, DELETE
    target_table = Column(String, index=True)
    target_id = Column(String)
    details = Column(JSON) # e.g. {"old": {...}, "new": {...}}
    created_at = Column(DateTime, default=_utcnow)

class ApiUsageLog(Base):
    __tablename__ = "api_usage_logs"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True, nullable=True)
    username = Column(String, index=True, nullable=True)
    method = Column(String)
    path = Column(String, index=True)
    status_code = Column(Integer)
    ip_address = Column(String)
    latency_ms = Column(Float)
    created_at = Column(DateTime, default=_utcnow)

class TrailingZeroWhitelist(Base):
    __tablename__ = "trailing_zero_whitelist"
    id = Column(Integer, primary_key=True, index=True)
    nid = Column(String, unique=True, index=True, nullable=False)
    added_by = Column(String, nullable=True)
    created_at = Column(DateTime, default=_utcnow)

class BackgroundTask(Base):
    __tablename__ = "background_tasks"
    id = Column(String, primary_key=True, index=True) # UUID string
    task_name = Column(String, index=True, nullable=False)
    user_id = Column(Integer, index=True, nullable=True) # ID of user who started it
    status = Column(String, default="pending", index=True) # pending, running, completed, error
    progress = Column(Integer, default=0) # 0-100
    message = Column(String, nullable=True)
    result_url = Column(String, nullable=True) # Link to download file
    error_details = Column(String, nullable=True)
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)
class ExportHistory(Base):
    __tablename__ = "export_history"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    username = Column(String, index=True)
    scope = Column(String)  # all | division | district | upazila
    scope_value = Column(String, nullable=True) # Name of div/dist/upz
    export_type = Column(String) # all | new
    record_count = Column(Integer)
    filename = Column(String)
    created_at = Column(DateTime, default=_utcnow)

class ExportTracking(Base):
    __tablename__ = "export_tracking"
    id = Column(Integer, primary_key=True, index=True)
    scope = Column(String, index=True)  # all | division | district | upazila
    scope_value = Column(String, index=True, nullable=True)
    last_exported_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    __table_args__ = (
        UniqueConstraint('scope', 'scope_value', name='uix_scope_value'),
    )
