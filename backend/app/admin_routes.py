from fastapi import APIRouter, Depends, HTTPException, status, Form
from sqlalchemy import text
from sqlalchemy.orm import Session
from .database import get_db
from .models import User, SystemConfig, RemoteInstance, Upazila, TrailingZeroWhitelist, AuditLog, ApiUsageLog
from .auth import get_current_user
from .rbac import PermissionChecker
from .audit import log_audit
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import subprocess
import os
from fastapi.responses import StreamingResponse, FileResponse
from fastapi import UploadFile, File, BackgroundTasks
import tempfile
import shutil
import time
import threading

# Background task status now tracked via BackgroundTask DB model
BACKUP_DIR = "/app/backups"
RETENTION_DAYS = 30


import logging
logger = logging.getLogger("ffp")

router = APIRouter(tags=["admin"])

# --- System Configs ---

class ConfigOut(BaseModel):
    id: int
    key: str
    value: str
    description: Optional[str] = None
    
    class Config:
        from_attributes = True

class ConfigUpdate(BaseModel):
    value: str

@router.get("/config", response_model=List[ConfigOut], dependencies=[Depends(PermissionChecker("view_admin"))])
async def get_configs(db: Session = Depends(get_db)):
    return db.query(SystemConfig).all()

@router.put("/config/{key}", response_model=ConfigOut, dependencies=[Depends(PermissionChecker("view_admin"))])
async def update_config(key: str, update: ConfigUpdate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    config = db.query(SystemConfig).filter(SystemConfig.key == key).first()
    if not config:
        # Create if doesn't exist
        config = SystemConfig(key=key, value=update.value)
        db.add(config)
    else:
        config.value = update.value
    db.commit()
    db.refresh(config)
    
    log_audit(db, current_user, "UPDATE", "system_configs", config.id, new_data={"key": key, "value": update.value})
    
    return config

# --- Trailing Zero Whitelist ---

class TzWhitelistCreate(BaseModel):
    nid: str

class TzWhitelistOut(BaseModel):
    nid: str
    added_by: Optional[str] = None
    created_at: datetime
    
    class Config:
        from_attributes = True

@router.get("/trailing-zero-whitelist", response_model=List[TzWhitelistOut], dependencies=[Depends(PermissionChecker("view_admin"))])
async def get_tz_whitelist(db: Session = Depends(get_db)):
    return db.query(TrailingZeroWhitelist).all()

@router.post("/trailing-zero-whitelist", response_model=TzWhitelistOut, dependencies=[Depends(PermissionChecker("manage_users"))])
async def add_tz_whitelist(data: TzWhitelistCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    existing = db.query(TrailingZeroWhitelist).filter(TrailingZeroWhitelist.nid == data.nid).first()
    if existing:
        raise HTTPException(status_code=400, detail="NID already in whitelist")
        
    entry = TrailingZeroWhitelist(nid=data.nid, added_by=current_user.username)
    db.add(entry)
    db.commit()
    db.refresh(entry)
    
    log_audit(db, current_user, "CREATE", "tz_whitelist", 0, new_data={"nid": data.nid})
    
    return entry

@router.delete("/trailing-zero-whitelist/{nid}", dependencies=[Depends(PermissionChecker("manage_users"))])
async def remove_tz_whitelist(nid: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    entry = db.query(TrailingZeroWhitelist).filter(TrailingZeroWhitelist.nid == nid).first()
    if not entry:
        raise HTTPException(status_code=404, detail="NID not found in whitelist")
        
    db.delete(entry)
    db.commit()
    
    log_audit(db, current_user, "DELETE", "tz_whitelist", 0, old_data={"nid": nid})
    
    return {"detail": "Removed"}

# --- Remote Instances ---

class InstanceCreate(BaseModel):
    name: str
    url: str
    api_key: str

class InstanceOut(BaseModel):
    id: int
    name: str
    url: str
    is_active: bool
    last_synced_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

@router.get("/instances", response_model=List[InstanceOut], dependencies=[Depends(PermissionChecker("view_admin"))])
async def get_instances(db: Session = Depends(get_db)):
    return db.query(RemoteInstance).all()

@router.post("/instances", response_model=InstanceOut, dependencies=[Depends(PermissionChecker("view_admin"))])
async def create_instance(data: InstanceCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    instance = RemoteInstance(**data.dict())
    db.add(instance)
    db.commit()
    db.refresh(instance)
    
    log_audit(db, current_user, "CREATE", "remote_instances", instance.id, new_data={"name": instance.name, "url": instance.url})
    
    return instance

@router.delete("/instances/{id}", dependencies=[Depends(PermissionChecker("view_admin"))])
async def delete_instance(id: int, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    instance = db.query(RemoteInstance).filter(RemoteInstance.id == id).first()
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")
    instance_name = instance.name
    db.delete(instance)
    db.commit()
    
    log_audit(db, current_user, "DELETE", "remote_instances", id, old_data={"name": instance_name})
    
    return {"detail": "Deleted"}
    
@router.post("/instances/{id}/sync", dependencies=[Depends(PermissionChecker("view_admin"))])
async def sync_instance_data(id: int, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """
    Triggers a data sync from a remote instance.
    This pulls data FROM the remote instance TO this instance.
    """
    from .main import trigger_remote_sync
    return await trigger_remote_sync(id, db, current_user)
    


@router.post("/instances/{id}/test", dependencies=[Depends(PermissionChecker("view_admin"))])
async def test_instance(id: int, db: Session = Depends(get_db)):
    """
    Tests connection to a remote instance by calling its /health or / endpoint.
    """
    instance = db.query(RemoteInstance).filter(RemoteInstance.id == id).first()
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")
    
    import httpx
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # We try to hit the root or a health endpoint
            response = await client.get(instance.url.rstrip("/") + "/auth/users", headers={"X-API-KEY": instance.api_key})
            if response.status_code == 200:
                return {"status": "online", "message": "Connection successful"}
            else:
                return {"status": "error", "message": f"Failed with status: {response.status_code}"}
    except Exception as e:
        return {"status": "offline", "message": str(e)}

# --- Upazilas ---

class UpazilaCreate(BaseModel):
    division_name: str
    district_name: str
    name: str

class UpazilaOut(BaseModel):
    id: int
    division_name: str
    district_name: str
    name: str
    quota: int = 0
    is_active: bool
    
    class Config:
        from_attributes = True

@router.get("/upazilas", response_model=List[UpazilaOut], dependencies=[Depends(PermissionChecker("view_geo"))])
async def get_upazilas(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    query = db.query(Upazila)
    if current_user.role != "admin":
        if getattr(current_user, "division_access", None):
            query = query.filter(Upazila.division_name == current_user.division_access)
        if getattr(current_user, "district_access", None):
            query = query.filter(Upazila.district_name == current_user.district_access)
        if getattr(current_user, "upazila_access", None):
            query = query.filter(Upazila.name == current_user.upazila_access)
    return query.all()

@router.post("/upazilas", response_model=UpazilaOut, dependencies=[Depends(PermissionChecker("manage_geo"))])
async def create_upazila(data: UpazilaCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    # check exists
    existing = db.query(Upazila).filter(Upazila.district_name == data.district_name, Upazila.name == data.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Upazila already exists in this district")
    
    upz = Upazila(**data.dict())
    db.add(upz)
    db.commit()
    db.refresh(upz)
    
    log_audit(db, current_user, "CREATE", "upazilas", upz.id, new_data={"district": upz.district_name, "name": upz.name})
    
    return upz

@router.delete("/upazilas/{id}", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def delete_upazila(id: int, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    upz = db.query(Upazila).filter(Upazila.id == id).first()
    if not upz:
        raise HTTPException(status_code=404, detail="Upazila not found")
    upz_name = upz.name
    db.delete(upz)
    db.commit()
    
    log_audit(db, current_user, "DELETE", "upazilas", id, old_data={"name": upz_name})
    
    return {"detail": "Deleted"}

class UpazilaQuotaUpdate(BaseModel):
    quota: int

@router.put("/upazilas/{id}/quota", response_model=UpazilaOut, dependencies=[Depends(PermissionChecker("manage_geo"))])
async def update_upazila_quota(id: int, data: UpazilaQuotaUpdate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    upz = db.query(Upazila).filter(Upazila.id == id).first()
    if not upz:
        raise HTTPException(status_code=404, detail="Upazila not found")
    
    old_quota = upz.quota
    upz.quota = data.quota
    db.commit()
    db.refresh(upz)
    
    log_audit(db, current_user, "UPDATE", "upazilas", id, old_data={"quota": old_quota}, new_data={"quota": data.quota})
    
    return upz

# --- Database Management ---

# --- Database Management (Background Jobs) ---

def _purge_old_backups():
    """Deletes backups older than RETENTION_DAYS."""
    try:
        if not os.path.exists(BACKUP_DIR):
            return 
        now = time.time()
        for f in os.listdir(BACKUP_DIR):
            if not f.endswith(".sql.gz"):
                continue
            f_path = os.path.join(BACKUP_DIR, f)
            if os.path.isfile(f_path):
                if os.stat(f_path).st_mtime < now - (RETENTION_DAYS * 86400):
                    os.remove(f_path)
    except Exception as e:
        print(f"Error purging old backups: {e}")

def _run_db_backup(db_session_factory, task_id: str, user_id: int):
    db = db_session_factory()
    from .models import BackgroundTask
    task = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
    if not task:
        db.close()
        return

    task.status = "running"
    db.commit()

    try:
        db_name = os.environ.get("POSTGRES_DB", "ffp_validator")
        db_user = os.environ.get("POSTGRES_USER", "fayez")
        db_host = os.environ.get("POSTGRES_HOST", "db")
        db_pass = os.environ.get("POSTGRES_PASSWORD", "")
        # Default port to 6432 if using pgbouncer, otherwise 5432
        default_port = "6432" if "pgbouncer" in db_host.lower() else "5432"
        db_port = os.environ.get("POSTGRES_PORT", default_port)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ffp_manual_backup_{timestamp}.sql.gz"
        os.makedirs(BACKUP_DIR, exist_ok=True)
        filepath = os.path.join(BACKUP_DIR, filename)
        
        env = os.environ.copy()
        env["PGPASSWORD"] = db_pass
        
        task.message = f"Creating compressed backup: {filename}"
        db.commit()

        # pg_dump piped to gzip
        with open(filepath, "wb") as f_out:
            dump_proc = subprocess.Popen(
                ["pg_dump", "-h", db_host, "-p", db_port, "-U", db_user, db_name],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            gzip_proc = subprocess.Popen(
                ["gzip"], stdin=dump_proc.stdout, stdout=f_out
            )
            dump_proc.stdout.close()
            _, stderr = dump_proc.communicate()
            gzip_proc.communicate()

        if dump_proc.returncode != 0:
            raise Exception(stderr.decode() if stderr else "pg_dump failed")

        _purge_old_backups()

        task.status = "completed"
        task.progress = 100
        task.message = f"Backup completed: {filename}"
        task.result_url = f"/api/admin/db/backups/{filename}/download"
        db.commit()

    except Exception as e:
        db.rollback()
        task.status = "error"
        task.error_details = str(e)
        task.message = f"Backup failed: {str(e)}"
        db.commit()
    finally:
        db.close()

def _run_db_restore(db_session_factory, task_id: str, filepath: str):
    db = db_session_factory()
    from .models import BackgroundTask
    task = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
    if not task:
        db.close()
        return

    task.status = "running"
    db.commit()

    try:
        db_name = os.environ.get("POSTGRES_DB", "ffp_validator")
        db_user = os.environ.get("POSTGRES_USER", "fayez")
        db_host = os.environ.get("POSTGRES_HOST", "db")
        db_pass = os.environ.get("POSTGRES_PASSWORD", "")
        # Default port to 6432 if using pgbouncer, otherwise 5432
        default_port = "6432" if "pgbouncer" in db_host.lower() else "5432"
        db_port = os.environ.get("POSTGRES_PORT", default_port)
        
        env = os.environ.copy()
        env["PGPASSWORD"] = db_pass
        
        # Determine if it's gzipped
        is_gz = filepath.endswith(".gz")
        task.message = f"Restoring database from {'compressed ' if is_gz else ''}file..."
        db.commit()

        if is_gz:
            # zcat | psql
            zcat_cmd = "zcat" if os.name != "nt" else "gunzip -c"
            # Note: subprocess.run with shell=True or pipes
            restore_proc = subprocess.Popen(
                ["psql", "-h", db_host, "-p", db_port, "-U", db_user, "-d", db_name],
                env=env, stdin=subprocess.PIPE, stderr=subprocess.PIPE
            )
            with subprocess.Popen([zcat_cmd, filepath], stdout=restore_proc.stdin) as zcat_proc:
                restore_proc.stdin.close()
                _, stderr = restore_proc.communicate()
        else:
            # psql -f
            restore_proc = subprocess.run(
                ["psql", "-h", db_host, "-p", db_port, "-U", db_user, "-d", db_name, "-f", filepath],
                env=env, capture_output=True
            )
            stderr = restore_proc.stderr

        if restore_proc.returncode != 0:
            raise Exception(stderr.decode() if stderr else "psql restore failed")

        task.status = "completed"
        task.progress = 100
        task.message = "Database restore successful"
        db.commit()

    except Exception as e:
        db.rollback()
        task.status = "error"
        task.error_details = str(e)
        task.message = f"Restore failed: {str(e)}"
        db.commit()
    finally:
        if "/tmp/" in filepath and os.path.exists(filepath):
            os.remove(filepath)
        db.close()

@router.get("/db/backups", dependencies=[Depends(PermissionChecker("view_admin"))])
async def list_backups():
    """Lists all files in the persistent backup directory."""
    if not os.path.exists(BACKUP_DIR):
        return []
    
    files = []
    for f in os.listdir(BACKUP_DIR):
        if not (f.endswith(".sql") or f.endswith(".sql.gz")):
            continue
        f_path = os.path.join(BACKUP_DIR, f)
        stats = os.stat(f_path)
        files.append({
            "filename": f,
            "size": stats.st_size,
            "created_at": datetime.fromtimestamp(stats.st_mtime).isoformat()
        })
    return sorted(files, key=lambda x: x["created_at"], reverse=True)

@router.post("/db/backups/run", dependencies=[Depends(PermissionChecker("view_admin"))])
async def trigger_db_backup(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Triggers a background database backup."""
    from .models import BackgroundTask
    import uuid
    from .database import SessionLocal
    
    task_id = str(uuid.uuid4())
    task = BackgroundTask(
        id=task_id,
        task_name="db_backup",
        user_id=current_user.id,
        status="pending",
        message="Initializing backup process..."
    )
    db.add(task)
    db.commit()
    
    background_tasks.add_task(_run_db_backup, SessionLocal, task_id, current_user.id)
    return {"message": "Backup task started", "task_id": task_id}

@router.post("/db/backups/upload", dependencies=[Depends(PermissionChecker("view_admin"))])
async def upload_backup_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user)
):
    """Uploads a backup file to the persistent directory."""
    if not (file.filename.endswith(".sql") or file.filename.endswith(".sql.gz")):
        raise HTTPException(status_code=400, detail="Only .sql or .sql.gz files allowed")
    
    os.makedirs(BACKUP_DIR, exist_ok=True)
    filepath = os.path.join(BACKUP_DIR, file.filename)
    with open(filepath, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    return {"message": "File uploaded", "filename": file.filename}

@router.get("/db/backups/{filename}/download", dependencies=[Depends(PermissionChecker("view_admin"))])
async def download_backup(filename: str):
    """Downloads a specific backup file."""
    filepath = os.path.join(BACKUP_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(filepath, filename=filename)

@router.post("/db/backups/{filename}/restore", dependencies=[Depends(PermissionChecker("view_admin"))])
async def trigger_db_restore(
    filename: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Triggers a background database restore from an existing file."""
    filepath = os.path.join(BACKUP_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")
    
    # Check if a restore/backup is already running
    from .models import BackgroundTask
    import uuid
    from .database import SessionLocal
    
    running = db.query(BackgroundTask).filter(
        BackgroundTask.task_name.in_(["db_backup", "db_restore"]),
        BackgroundTask.status == "running"
    ).first()
    if running:
        raise HTTPException(status_code=400, detail="Another database operation is currently in progress")
    
    task_id = str(uuid.uuid4())
    task = BackgroundTask(
        id=task_id,
        task_name="db_restore",
        user_id=current_user.id,
        status="pending",
        message="Initializing restore process..."
    )
    db.add(task)
    db.commit()
    
    # Auto-backup before restore for safety
    background_tasks.add_task(_run_db_backup, SessionLocal, str(uuid.uuid4()), current_user.id)
    background_tasks.add_task(_run_db_restore, SessionLocal, task_id, filepath)
    
    return {"message": "Restore task scheduled (Safety backup prioritized)", "task_id": task_id}

@router.delete("/db/backups/{filename}", dependencies=[Depends(PermissionChecker("view_admin"))])
async def delete_backup(filename: str):
    """Deletes a backup file from the persistent directory."""
    filepath = os.path.join(BACKUP_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")
    os.remove(filepath)
    return {"message": "File deleted"}

# --- Location Renaming ---

class LocationRename(BaseModel):
    id: int
    level: str # 'division', 'district', 'upazila'
    new_name: str

@router.put("/location/rename", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def rename_location(data: LocationRename, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """
    Renames a location and cascades the change to all referencing tables using DB IDs.
    """
    from .models import Division, District, Upazila, SummaryStats, ValidRecord, InvalidRecord, UploadBatch
    
    level = data.level.lower()
    target_id = data.id
    new_name = data.new_name.strip()
    
    if not new_name:
        raise HTTPException(status_code=400, detail="New name cannot be empty")

    old_name = ""
    parent_name = None 

    if level == 'division':
        division = db.query(Division).filter(Division.id == target_id).first()
        if not division:
            raise HTTPException(status_code=404, detail="Division not found")
        old_name = division.name
        division.name = new_name
        
        # Update District Table (division_name)
        db.query(District).filter(District.division_name == old_name).update({"division_name": new_name})
        # Update Upazila Table (division_name)
        db.query(Upazila).filter(Upazila.division_name == old_name).update({"division_name": new_name})
        
        # Update Data Tables
        db.query(SummaryStats).filter(SummaryStats.division == old_name).update({"division": new_name})
        db.query(ValidRecord).filter(ValidRecord.division == old_name).update({"division": new_name})
        db.query(InvalidRecord).filter(InvalidRecord.division == old_name).update({"division": new_name})
        db.query(UploadBatch).filter(UploadBatch.division == old_name).update({"division": new_name})
        
    elif level == 'district':
        district = db.query(District).filter(District.id == target_id).first()
        if not district:
            raise HTTPException(status_code=404, detail="District not found")
        old_name = district.name
        district.name = new_name
            
        # Update Upazila Table (district_name)
        db.query(Upazila).filter(Upazila.district_name == old_name).update({"district_name": new_name})
        
        # Update Data Tables
        db.query(SummaryStats).filter(SummaryStats.district == old_name).update({"district": new_name})
        db.query(ValidRecord).filter(ValidRecord.district == old_name).update({"district": new_name})
        db.query(InvalidRecord).filter(InvalidRecord.district == old_name).update({"district": new_name})
        db.query(UploadBatch).filter(UploadBatch.district == old_name).update({"district": new_name})
        
    elif level == 'upazila':
        upz = db.query(Upazila).filter(Upazila.id == target_id).first()
        if not upz:
            raise HTTPException(status_code=404, detail="Upazila not found")
        old_name = upz.name
        parent_name = upz.district_name
        upz.name = new_name
            
        # Update Data Tables
        db.query(SummaryStats).filter(SummaryStats.upazila == old_name, SummaryStats.district == parent_name).update({"upazila": new_name})
        db.query(ValidRecord).filter(ValidRecord.upazila == old_name, ValidRecord.district == parent_name).update({"upazila": new_name})
        db.query(InvalidRecord).filter(InvalidRecord.upazila == old_name, InvalidRecord.district == parent_name).update({"upazila": new_name})
        db.query(UploadBatch).filter(UploadBatch.upazila == old_name, UploadBatch.district == parent_name).update({"upazila": new_name})
    
    else:
        raise HTTPException(status_code=400, detail="Invalid level")
        
    db.commit()
    
    # Audit Log
    log_audit(db, current_user, "RENAME", level, target_id, old_data={"name": old_name}, new_data={"name": new_name})

    from .stats_utils import refresh_summary_stats
    if level == 'upazila':
        refresh_summary_stats(db, "", parent_name or "", new_name)
    elif level == 'district':
        upz_list = db.query(Upazila).filter(Upazila.district_name == new_name).all()
        for u in upz_list:
             refresh_summary_stats(db, "", new_name, u.name)

    
    else:
        raise HTTPException(status_code=400, detail="Invalid level")
        
    db.commit()
    
    # Refresh SummaryStats after rename
    from .stats_utils import refresh_summary_stats
    if level == 'upazila':
        # Refresh for the new name
        refresh_summary_stats(db, "", parent_name or "", new_name)
    elif level == 'district':
        # Refresh all Upazilas in this district
        upazilas = db.query(Upazila).filter(Upazila.district_name == new_name).all()
        for upz in upazilas:
            refresh_summary_stats(db, upz.division_name, new_name, upz.name)
    
    log_audit(db, current_user, "RENAME", level, 0, old_data={"name": old_name, "parent": parent_name}, new_data={"name": new_name})
    
    return {"detail": f"Renamed {level} from {old_name} to {new_name}"}


# --- Upazila Management ---

class UpazilaCreate(BaseModel):
    division_name: str
    district_name: str
    name: str
    quota: int = 0

@router.get("/geo/upazilas", dependencies=[Depends(PermissionChecker("view_admin"))])
async def list_upazilas(db: Session = Depends(get_db)):
    from .models import Upazila
    upz_list = db.query(Upazila).filter(Upazila.is_active == True).order_by(Upazila.division_name, Upazila.district_name, Upazila.name).all()
    return [{
        "id": u.id,
        "division_name": u.division_name,
        "district_name": u.district_name,
        "name": u.name,
        "quota": u.quota
    } for u in upz_list]

@router.get("/geo/tree", dependencies=[Depends(PermissionChecker("view_admin"))])
async def get_geo_tree(db: Session = Depends(get_db)):
    """ Returns nested hierarchy: Division -> District -> Upazila with aliases. """
    from .models import Division, District, Upazila, GeoAlias
    
    divisions = db.query(Division).order_by(Division.name).all()
    districts = db.query(District).order_by(District.division_name, District.name).all()
    upazilas = db.query(Upazila).order_by(Upazila.district_name, Upazila.name).all()
    aliases = db.query(GeoAlias).all()
    
    # Map aliases by (target_type, target_id)
    alias_map = {}
    for a in aliases:
        key = (a.target_type, a.target_id)
        if key not in alias_map:
            alias_map[key] = []
        alias_map[key].append({"id": a.id, "alias_name": a.alias_name})
        
    # Build Tree
    tree = []
    
    # Normalize data structures for nested grouping
    dist_by_div = {}
    for d in districts:
        if d.division_name not in dist_by_div:
            dist_by_div[d.division_name] = []
        dist_by_div[d.division_name].append({
            "id": d.id,
            "name": d.name,
            "type": "district",
            "aliases": alias_map.get(("district", d.id), []),
            "upazilas": []
        })
        
    upz_by_dist = {}
    for u in upazilas:
        if u.district_name not in upz_by_dist:
            upz_by_dist[u.district_name] = []
        upz_by_dist[u.district_name].append({
            "id": u.id,
            "name": u.name,
            "type": "upazila",
            "aliases": alias_map.get(("upazila", u.id), []),
            "quota": u.quota
        })
        
    for div in divisions:
        div_data = {
            "id": div.id,
            "name": div.name,
            "type": "division",
            "aliases": alias_map.get(("division", div.id), []),
            "districts": dist_by_div.get(div.name, [])
        }
        # Nested upazilas into districts
        for dist in div_data["districts"]:
            dist["upazilas"] = upz_by_dist.get(dist["name"], [])
            
        tree.append(div_data)
        
    return tree

@router.post("/geo/upazilas", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def create_upazila(data: UpazilaCreate, db: Session = Depends(get_db)):
    from .models import Upazila, Division, District
    
    # Ensure parents exist (optional, but good for data integrity)
    if not db.query(Division).filter(Division.name == data.division_name).first():
        db.add(Division(name=data.division_name, is_active=True))
    if not db.query(District).filter(District.name == data.district_name).first():
        db.add(District(division_name=data.division_name, name=data.district_name, is_active=True))
    
    # Check for duplicate
    existing = db.query(Upazila).filter(
        Upazila.district_name == data.district_name,
        Upazila.name == data.name
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Upazila already exists in this district")
    
    new_upz = Upazila(
        division_name=data.division_name,
        district_name=data.district_name,
        name=data.name,
        quota=data.quota,
        is_active=True
    )
    db.add(new_upz)
    db.commit()
    return {"message": "Upazila created successfully", "id": new_upz.id}


# --- Geo Aliases CRUD ---

class GeoAliasCreate(BaseModel):
    alias_name: str
    target_type: str
    target_id: int

@router.get("/geo/aliases", dependencies=[Depends(PermissionChecker("view_admin"))])
async def list_geo_aliases(db: Session = Depends(get_db)):
    from .models import GeoAlias
    aliases = db.query(GeoAlias).all()
    return [{"id": a.id, "alias_name": a.alias_name, "target_type": a.target_type, "target_id": a.target_id} for a in aliases]

@router.post("/geo/aliases", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def create_geo_alias(data: GeoAliasCreate, db: Session = Depends(get_db)):
    from .models import GeoAlias
    alias_norm = data.alias_name.strip().lower()
    if db.query(GeoAlias).filter(GeoAlias.alias_name == alias_norm).first():
        raise HTTPException(status_code=400, detail="Alias exactly matching this spelling already exists")
    
    new_alias = GeoAlias(
        alias_name=alias_norm,
        target_type=data.target_type.lower(),
        target_id=data.target_id
    )
    db.add(new_alias)
    db.commit()
    return {"message": "Alias added successfully", "id": new_alias.id}

@router.delete("/geo/aliases/{alias_id}", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def delete_geo_alias(alias_id: int, db: Session = Depends(get_db)):
    from .models import GeoAlias
    alias = db.query(GeoAlias).filter(GeoAlias.id == alias_id).first()
    if not alias:
        raise HTTPException(status_code=404, detail="Alias not found")
    db.delete(alias)
    db.commit()
    return {"message": "Alias deleted successfully"}

# --- Data Maintenance ---

@router.get("/maintenance/preview-orphans", dependencies=[Depends(PermissionChecker("view_admin"))])
async def preview_orphans(db: Session = Depends(get_db)):
    """
    Dry-run: shows records whose geo names don't match any canonical Upazila.
    ZERO data changes. Safe to call at any time.
    """
    from .models import ValidRecord, InvalidRecord, SummaryStats, Division, District

    # Build canonical sets
    upazila_keys = {
        (u.district_name.lower().strip(), u.name.lower().strip())
        for u in db.query(Upazila).all()
    }
    divisions_map = {d.name.lower().strip(): d.id for d in db.query(Division).all()}
    districts_map = {d.name.lower().strip(): d.id for d in db.query(District).all()}

    results = {}
    for model, label in [(ValidRecord, "valid_records"), (InvalidRecord, "invalid_records"), (SummaryStats, "summary_stats")]:
        geo_groups = (
            db.query(model.division, model.district, model.upazila)
            .group_by(model.division, model.district, model.upazila)
            .all()
        )
        orphans = []
        for div_n, dist_n, upz_n in geo_groups:
            d_key = (dist_n or "").lower().strip()
            u_key = (upz_n or "").lower().strip()
            if (d_key, u_key) not in upazila_keys:
                # Count rows in this orphan group
                count = db.query(model).filter(
                    model.division == div_n,
                    model.district == dist_n,
                    model.upazila == upz_n,
                ).count()
                orphans.append({
                    "division": div_n,
                    "district": dist_n,
                    "upazila": upz_n,
                    "row_count": count,
                    "issue": "no_canonical_match",
                })
        results[label] = {"orphan_groups": len(orphans), "orphans": orphans}

    # Also count records where geo IDs are still NULL (not yet backfilled)
    from sqlalchemy import text as _text
    null_counts = {}
    for tbl in ["valid_records", "invalid_records", "summary_stats", "upload_batches"]:
        row = db.execute(_text(f"SELECT COUNT(*) FROM {tbl} WHERE upazila_id IS NULL")).fetchone()
        null_counts[tbl] = row[0] if row else 0

    return {"orphans_by_table": results, "null_geo_id_counts": null_counts}


@router.get("/maintenance/status", dependencies=[Depends(PermissionChecker("view_admin"))])
async def get_maintenance_status(db: Session = Depends(get_db)):
    """Poll the status of background maintenance tasks to support legacy UI."""
    from .models import BackgroundTask
    cleanup_task = db.query(BackgroundTask).filter(BackgroundTask.task_name == "geo_cleanup").order_by(BackgroundTask.created_at.desc()).first()
    delete_task = db.query(BackgroundTask).filter(BackgroundTask.task_name == "geo_delete").order_by(BackgroundTask.created_at.desc()).first()
    repair_geo_task = db.query(BackgroundTask).filter(BackgroundTask.task_name == "repair_geo").order_by(BackgroundTask.created_at.desc()).first()
    
    return {
        "cleanup": {
            "status": cleanup_task.status if cleanup_task else "idle",
            "progress": cleanup_task.progress if cleanup_task else 0,
            "message": cleanup_task.message if cleanup_task else "",
            "error": cleanup_task.error_details if cleanup_task else None,
            "last_run": cleanup_task.created_at.isoformat() if cleanup_task else None
        },
        "delete": {
            "status": delete_task.status if delete_task else "idle",
            "message": delete_task.message if delete_task else "",
            "error": delete_task.error_details if delete_task else None,
            "last_run": delete_task.created_at.isoformat() if delete_task else None
        },
        "repair_geo": {
            "status": repair_geo_task.status if repair_geo_task else "idle",
            "progress": repair_geo_task.progress if repair_geo_task else 0,
            "message": repair_geo_task.message if repair_geo_task else "",
            "error": repair_geo_task.error_details if repair_geo_task else None,
            "last_run": repair_geo_task.created_at.isoformat() if repair_geo_task else None
        }
    }


def run_cleanup_background(db_session_factory, task_id: str, user_id: int, username: str):
    """Heavy-lifting background job for geo-cleanup."""
    db = db_session_factory()
    from .models import BackgroundTask
    task = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
    if not task:
        db.close()
        return

    task.status = "running"
    db.commit()
    task.message = "Trimming whitespace from names..."
    try:
        from .models import ValidRecord, InvalidRecord, SummaryStats, UploadBatch, Division, District, Upazila
        from sqlalchemy import text as _text
        
        # 1. Trim names (Fast bulk update)
        tables_and_cols = {
            "valid_records":   ["division", "district", "upazila"],
            "invalid_records": ["division", "district", "upazila"],
            "summary_stats":   ["division", "district", "upazila"],
            "upload_batches":  ["division", "district", "upazila"],
        }
        trimmed = 0
        for idx, (tbl, cols) in enumerate(tables_and_cols.items()):
            task.progress = int((idx / 4) * 50) # first half of progress
            db.commit()
            for col in cols:
                res = db.execute(_text(f"UPDATE {tbl} SET {col} = trim({col}) WHERE {col} != trim({col})"))
                trimmed += res.rowcount
                db.commit()
        
        # 2. Bulk Backfill IDs using JOIN (High Performance)
        backfilled = 0
        tables = ["valid_records", "invalid_records", "summary_stats", "upload_batches"]
        
        # We have 4 tables and each has 6 backfill steps (Upz, Upz-Alias, Dist, Dist-Alias, Div, Div-Alias)
        # Total steps = 24
        total_backfill_steps = len(tables) * 6
        current_step = 0

        for tbl in tables:
            # 1. Match by Upazila + District (most precise)
            current_step += 1
            task.progress = 50 + int((current_step / total_backfill_steps) * 50)
            task.message = f"Matching Upazila IDs for {tbl} (Direct Match)..."
            db.commit()

            sql_upz = f"""
                UPDATE {tbl} t
                SET upazila_id = u.id
                FROM upazilas u
                WHERE lower(t.upazila) = lower(u.name)
                  AND lower(t.district) = lower(u.district_name)
                  AND t.upazila_id IS NULL
            """
            res = db.execute(_text(sql_upz))
            backfilled += res.rowcount
            db.commit()

            # 1.5 Match Upazila via Aliases
            current_step += 1
            task.progress = 50 + int((current_step / total_backfill_steps) * 50)
            task.message = f"Matching Upazila IDs for {tbl} (Alias Match)..."
            db.commit()

            sql_upz_alias = f"""
                UPDATE {tbl} t
                SET upazila_id = a.target_id
                FROM geo_aliases a
                WHERE lower(t.upazila) = lower(a.alias_name)
                  AND a.target_type = 'upazila'
                  AND t.upazila_id IS NULL
            """
            res_alias = db.execute(_text(sql_upz_alias))
            backfilled += res_alias.rowcount
            db.commit()
            
            # 2. Match by District
            current_step += 1
            task.progress = 50 + int((current_step / total_backfill_steps) * 50)
            task.message = f"Matching District IDs for {tbl} (Direct Match)..."
            db.commit()

            sql_dist = f"""
                UPDATE {tbl} t
                SET district_id = d.id
                FROM districts d
                WHERE lower(t.district) = lower(d.name)
                  AND t.district_id IS NULL
            """
            res = db.execute(_text(sql_dist))
            backfilled += res.rowcount
            db.commit()

            # 2.5 Match District via Aliases
            current_step += 1
            task.progress = 50 + int((current_step / total_backfill_steps) * 50)
            task.message = f"Matching District IDs for {tbl} (Alias Match)..."
            db.commit()

            sql_dist_alias = f"""
                UPDATE {tbl} t
                SET district_id = a.target_id
                FROM geo_aliases a
                WHERE lower(t.district) = lower(a.alias_name)
                  AND a.target_type = 'district'
                  AND t.district_id IS NULL
            """
            res_alias = db.execute(_text(sql_dist_alias))
            backfilled += res_alias.rowcount
            db.commit()
            
            # 3. Match by Division
            current_step += 1
            task.progress = 50 + int((current_step / total_backfill_steps) * 50)
            task.message = f"Matching Division IDs for {tbl} (Direct Match)..."
            db.commit()

            sql_div = f"""
                UPDATE {tbl} t
                SET division_id = dv.id
                FROM divisions dv
                WHERE lower(t.division) = lower(dv.name)
                  AND t.division_id IS NULL
            """
            res = db.execute(_text(sql_div))
            backfilled += res.rowcount
            db.commit()

            # 3.5 Match Division via Aliases
            current_step += 1
            task.progress = 50 + int((current_step / total_backfill_steps) * 50)
            task.message = f"Matching Division IDs for {tbl} (Alias Match)..."
            db.commit()

            sql_div_alias = f"""
                UPDATE {tbl} t
                SET division_id = a.target_id
                FROM geo_aliases a
                WHERE lower(t.division) = lower(a.alias_name)
                  AND a.target_type = 'division'
                  AND t.division_id IS NULL
            """
            res_alias = db.execute(_text(sql_div_alias))
            backfilled += res_alias.rowcount
            db.commit()

        task.status = "completed"
        task.progress = 100
        task.message = f"Cleanup successful. Trimmed: {trimmed}, Backfilled: {backfilled}"
        db.commit()
        
        # Log Audit
        from .models import User
        user = db.query(User).filter(User.id == user_id).first()
        log_audit(db, user, "MAINTENANCE", "system", 0, 
                  new_data={"action": "geo_cleanup", "trimmed": trimmed, "backfilled": backfilled})
        
    except Exception as e:
        db.rollback()
        task.status = "error"
        task.error_details = str(e)
        task.message = f"Cleanup failed: {str(e)}"
        db.commit()
    finally:
        db.close()

import uuid

@router.post("/maintenance/run-cleanup", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def trigger_cleanup(
    background_tasks: BackgroundTasks, 
    db: Session = Depends(get_db), 
    current_user: User = Depends(get_current_user)
):
    """Triggers the high-performance geo-cleanup in the background."""
    from .models import BackgroundTask
    running_task = db.query(BackgroundTask).filter(
        BackgroundTask.task_name == "geo_cleanup", 
        BackgroundTask.status == "running"
    ).first()
    if running_task:
        return {"message": "Cleanup is already running", "status": {"status": "running"}}
    
    task_id = str(uuid.uuid4())
    task = BackgroundTask(
        id=task_id,
        task_name="geo_cleanup",
        user_id=current_user.id,
        status="pending",
        message="Trimming whitespace from names..."
    )
    db.add(task)
    db.commit()

    from .database import SessionLocal
    background_tasks.add_task(run_cleanup_background, SessionLocal, task_id, current_user.id, current_user.username)
    
    return {"message": "Cleanup started in background", "task_id": task_id}


def run_repair_geo_ids_background(db_session_factory, task_id: str, user_id: int, username: str):
    """Heavy-lifting background job for repairing geo IDs."""
    db = db_session_factory()
    from .models import BackgroundTask, User
    task = db.query(BackgroundTask).filter(BackgroundTask.id == task_id).first()
    if not task:
        db.close()
        return

    task.status = "running"
    db.commit()
    report = {}
    try:
        tables = ["valid_records", "invalid_records", "summary_stats", "upload_batches"]
        for idx, tbl in enumerate(tables):
            task.progress = int((idx / len(tables)) * 90)
            task.message = f"Repairing Geographic IDs for {tbl} (High-performance set matching)..."
            db.commit()
            
            # Re-assign upazila_id, district_id, division_id for every row by name match.
            # Uses a single SET-based UPDATE with a lateral join — no Python loops.
            fix_sql = text(f"""
                UPDATE {tbl} t
                SET
                    upazila_id  = u.id,
                    district_id = d.id,
                    division_id = dv.id
                FROM upazilas u
                JOIN districts d  ON d.name  = u.district_name
                JOIN divisions dv ON dv.name  = d.division_name
                WHERE LOWER(TRIM(u.name))          = LOWER(TRIM(t.upazila))
                  AND LOWER(TRIM(u.district_name)) = LOWER(TRIM(t.district))
                  AND (
                        t.upazila_id  IS DISTINCT FROM u.id
                     OR t.district_id IS DISTINCT FROM d.id
                     OR t.division_id IS DISTINCT FROM dv.id
                  )
            """)
            result = db.execute(fix_sql)
            report[tbl] = result.rowcount
            db.commit()

        task.status = "completed"
        task.progress = 100
        total = sum(report.values())
        task.message = f"Repaired {total} rows. Refreshing stats..."
        db.commit()

        # Auto-refresh SummaryStats from truth tables after repair
        try:
            recalc_sql = text("""
                WITH counts AS (
                    SELECT 
                        district, upazila,
                        SUM(valid_cnt) as live_valid,
                        SUM(invalid_cnt) as live_invalid
                    FROM (
                        SELECT district, upazila, COUNT(*) as valid_cnt, 0 as invalid_cnt 
                        FROM valid_records GROUP BY district, upazila
                        UNION ALL
                        SELECT district, upazila, 0 as valid_cnt, COUNT(*) as invalid_cnt 
                        FROM invalid_records GROUP BY district, upazila
                    ) combined
                    GROUP BY district, upazila
                )
                UPDATE summary_stats s
                SET valid = COALESCE(c.live_valid, 0),
                    invalid = COALESCE(c.live_invalid, 0),
                    total = COALESCE(c.live_valid, 0) + COALESCE(c.live_invalid, 0),
                    updated_at = NOW()
                FROM counts c
                WHERE LOWER(TRIM(s.district)) = LOWER(TRIM(c.district))
                  AND LOWER(TRIM(s.upazila))  = LOWER(TRIM(c.upazila))
            """)
            stats_result = db.execute(recalc_sql)
            stats_updated = stats_result.rowcount

            # Zero out ghost entries (SummaryStats with no matching records)
            zero_sql = text("""
                UPDATE summary_stats s
                SET valid = 0, invalid = 0, total = 0, updated_at = NOW()
                WHERE NOT EXISTS (
                    SELECT 1 FROM valid_records v
                    WHERE LOWER(TRIM(v.district)) = LOWER(TRIM(s.district))
                      AND LOWER(TRIM(v.upazila))  = LOWER(TRIM(s.upazila))
                )
                AND NOT EXISTS (
                    SELECT 1 FROM invalid_records i
                    WHERE LOWER(TRIM(i.district)) = LOWER(TRIM(s.district))
                      AND LOWER(TRIM(i.upazila))  = LOWER(TRIM(s.upazila))
                )
                AND (s.valid > 0 OR s.invalid > 0)
            """)
            ghost_result = db.execute(zero_sql)
            ghosts_zeroed = ghost_result.rowcount
            db.commit()

            task.message = f"Repaired {total} rows. Stats refreshed ({stats_updated} synced, {ghosts_zeroed} ghost entries zeroed)."
            db.commit()
            logger.info("repair-geo-ids: auto-refreshed %d stats, zeroed %d ghosts", stats_updated, ghosts_zeroed)
        except Exception as stats_err:
            db.rollback()
            logger.warning("repair-geo-ids: stats auto-refresh failed: %s", str(stats_err))
            task.message = f"Repaired {total} rows. Stats refresh failed — run Refresh Stats manually."
            db.commit()

        # Log Audit
        user = db.query(User).filter(User.id == user_id).first()
        log_audit(db, user, "MAINTENANCE", "system", 0, 
                  new_data={"action": "repair_geo_ids", "rows_updated": report})

    except Exception as e:
        db.rollback()
        task.status = "error"
        task.error_details = str(e)
        logger.error("repair-geo-ids background failed: %s", str(e))
        task.message = f"Repair failed: {str(e)}"
        db.commit()
    finally:
        db.close()

@router.post("/maintenance/repair-geo-ids", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def trigger_repair_geo_ids(
    background_tasks: BackgroundTasks, 
    db: Session = Depends(get_db), 
    current_user: User = Depends(get_current_user)
):
    """
    Force-repairs upazila_id, district_id, and division_id on ALL records by re-matching
    against the canonical upazilas/districts/divisions master tables using text names.

    Triggers the high-performance geo-repair in the background to prevent timeouts.
    """
    from .models import BackgroundTask
    running_task = db.query(BackgroundTask).filter(
        BackgroundTask.task_name == "repair_geo", 
        BackgroundTask.status == "running"
    ).first()
    if running_task:
        return {"message": "Repair is already running", "status": {"status": "running"}}
    
    task_id = str(uuid.uuid4())
    task = BackgroundTask(
        id=task_id,
        task_name="repair_geo",
        user_id=current_user.id,
        status="pending",
        message="Starting geo ID repair..."
    )
    db.add(task)
    db.commit()

    from .database import SessionLocal
    background_tasks.add_task(run_repair_geo_ids_background, SessionLocal, task_id, current_user.id, current_user.username)
    
    return {"message": "Repair started in background", "task_id": task_id}




@router.delete("/maintenance/delete-unresolved", dependencies=[Depends(PermissionChecker("manage_geo"))])
def delete_unresolved(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """
    Permanently deletes all records (valid, invalid, summary_stats) whose upazila
    cannot be resolved to a canonical entry in the upazilas master table.
    After deletion, recalculates SummaryStats from live record counts so that
    all totals (stats page, grand total, Excel, PDF) are consistent.

    This is irreversible. Admin only.
    """
    from .models import ValidRecord, InvalidRecord, SummaryStats, UploadBatch, Division, District
    from sqlalchemy import func

    # 1. Build canonical (district, upazila) key set
    canonical_keys = {
        (u.district_name.lower().strip(), u.name.lower().strip())
        for u in db.query(Upazila).all()
    }

    report = {"deleted": {}, "errors": []}
    total_deleted = 0

    # 1. Set-based deletion for data tables
    try:
        targets = ["valid_records", "invalid_records", "summary_stats"]
        report["deleted"] = {}

        for table in targets:
            delete_sql = f"""
                DELETE FROM {table} t
                WHERE NOT EXISTS (
                    SELECT 1 FROM upazilas u
                    WHERE LOWER(TRIM(u.district_name)) = LOWER(TRIM(t.district))
                      AND LOWER(TRIM(u.name)) = LOWER(TRIM(t.upazila))
                )
            """
            result = db.execute(text(delete_sql))
            report["deleted"][table] = result.rowcount
            total_deleted += result.rowcount

        # 2. Set-based status update for upload_batches
        batch_update_sql = """
            UPDATE upload_batches b
            SET status = 'deleted'
            WHERE status != 'deleted'
              AND NOT EXISTS (
                SELECT 1 FROM upazilas u
                WHERE LOWER(TRIM(u.district_name)) = LOWER(TRIM(b.district))
                  AND LOWER(TRIM(u.name)) = LOWER(TRIM(b.upazila))
            )
        """
        batch_result = db.execute(text(batch_update_sql))
        report["deleted"]["upload_batches_marked"] = batch_result.rowcount

        db.commit()
    except Exception as e:
        db.rollback()
        report["errors"].append(f"Set-based cleanup failure: {str(e)}")
        return report

    # 3. Recalculate SummaryStats for consistency using Set-Based SQL
    try:
        recalculate_sql = """
            WITH counts AS (
                SELECT 
                    division, district, upazila,
                    SUM(valid_cnt) as live_valid,
                    SUM(invalid_cnt) as live_invalid
                FROM (
                    SELECT division, district, upazila, COUNT(*) as valid_cnt, 0 as invalid_cnt 
                    FROM valid_records GROUP BY division, district, upazila
                    UNION ALL
                    SELECT division, district, upazila, 0 as valid_cnt, COUNT(*) as invalid_cnt 
                    FROM invalid_records GROUP BY division, district, upazila
                ) combined
                GROUP BY division, district, upazila
            )
            UPDATE summary_stats s
            SET valid = c.live_valid,
                invalid = c.live_invalid,
                total = c.live_valid + c.live_invalid,
                updated_at = NOW()
            FROM counts c
            WHERE s.division = c.division 
              AND s.district = c.district 
              AND s.upazila = c.upazila
        """
        db.execute(text(recalculate_sql))
        db.commit()
    except Exception as e:
        db.rollback()
        report["errors"].append(f"Set-based recalculation failure: {str(e)}")

    log_audit(db, current_user, "DELETE", "system", 0, new_data={
        "action": "delete_unresolved_geo",
        "total_deleted": total_deleted,
        "report": report
    })

    return {
        "success": len(report["errors"]) == 0,
        "total_records_deleted": total_deleted,
        "valid_records_deleted": report["deleted"].get("valid_records", 0),
        "invalid_records_deleted": report["deleted"].get("invalid_records", 0),
        "summary_stats_rows_deleted": report["deleted"].get("summary_stats", 0),
        "upload_batches_archived": report["deleted"].get("upload_batches_marked", 0),
        "stats_recalculated": report["deleted"].get("summary_stats", 0), # Reclac happened for all remaining matching stats
        "report": report,
    }

@router.post("/maintenance/refresh-stats", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def manual_refresh_stats(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """
    Manually triggers a full re-calculation of all SummaryStats rows.
    Ensures that the dashboard perfectly aligns with Detail records.
    """
    from .models import SummaryStats
    from .stats_utils import refresh_summary_stats
    
    all_stats = db.query(SummaryStats).all()
    count = 0
    for stat in all_stats:
        refresh_summary_stats(db, stat.division, stat.district, stat.upazila)
        count += 1
    
    log_audit(db, current_user, "MAINTENANCE", "summary_stats", 0, new_data={"action": "full_stats_recalc", "rows": count})
    
    return {"detail": f"Successfully refreshed {count} statistics rows."}


@router.get("/audit-logs", dependencies=[Depends(PermissionChecker("view_admin"))])
async def get_audit_logs(limit: int = 100, db: Session = Depends(get_db)):
    """Admin-only: Retrieve system audit logs."""
    return db.query(AuditLog).order_by(AuditLog.created_at.desc()).limit(limit).all()


@router.get("/api-usage", dependencies=[Depends(PermissionChecker("view_admin"))])
async def get_api_usage(limit: int = 100, db: Session = Depends(get_db)):
    """Admin-only: Retrieve API usage logs."""
    return db.query(ApiUsageLog).order_by(ApiUsageLog.created_at.desc()).limit(limit).all()

@router.post("/maintenance/normalize-json-keys", dependencies=[Depends(PermissionChecker("view_admin"))])
async def trigger_normalize_json_keys(background_tasks: BackgroundTasks, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    from .models import BackgroundTask
    import uuid

    task_id = str(uuid.uuid4())
    task = BackgroundTask(
        id=task_id,
        user_id=current_user.id,
        task_name="JSON Key Normalization",
        status="pending",
        message="Initializing JSON key normalization..."
    )
    db.add(task)
    db.commit()

    def _normalize_task_wrapper(tid: str):
        from .database import SessionLocal
        from .main import _normalize_json_keys
        with SessionLocal() as session:
            _normalize_json_keys(session, tid)

    background_tasks.add_task(_normalize_task_wrapper, task_id)
    log_audit(db, current_user, "MAINTENANCE", "json_normalization", 0, new_data={"task_id": task_id})

    return {"message": "Background normalization task initiated.", "task_id": task_id}

@router.post("/maintenance/migrate-dealers", dependencies=[Depends(PermissionChecker("view_admin"))])
async def trigger_dealer_migration(background_tasks: BackgroundTasks, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    from .models import BackgroundTask
    import uuid
    
    task_id = str(uuid.uuid4())
    task = BackgroundTask(
        id=task_id,
        user_id=current_user.id,
        task_name="Dealer & Configuration Backfill Migration",
        status="pending",
        message="Initializing global database backfill..."
    )
    db.add(task)
    db.commit()
    
    def _migrate_task_wrapper(tid: str):
        from .database import SessionLocal
        from .main import _migrate_dealers_from_json
        with SessionLocal() as session:
            _migrate_dealers_from_json(session, tid)
            
    background_tasks.add_task(_migrate_task_wrapper, task_id)
    log_audit(db, current_user, "MAINTENANCE", "dealers_migration", 0, new_data={"task_id": task_id})
    
    return {"message": "Background migration task initiated.", "task_id": task_id}



@router.post("/maintenance/backfill-canonical", dependencies=[Depends(PermissionChecker("view_admin"))])
async def trigger_backfill_canonical(background_tasks: BackgroundTasks, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Task 6: Backfill 7 new canonical columns from data JSONB into dedicated DB columns. Requires Task 4 to have run first."""
    from .models import BackgroundTask
    import uuid

    task_id = str(uuid.uuid4())
    task = BackgroundTask(
        id=task_id,
        user_id=current_user.id,
        task_name="Backfill Canonical Columns",
        status="pending",
        message="Initializing canonical column backfill..."
    )
    db.add(task)
    db.commit()

    def _backfill_wrapper(tid: str):
        from .database import SessionLocal
        from .main import _backfill_canonical_columns
        with SessionLocal() as session:
            _backfill_canonical_columns(session, tid)

    background_tasks.add_task(_backfill_wrapper, task_id)
    log_audit(db, current_user, "MAINTENANCE", "canonical_backfill", 0, new_data={"task_id": task_id})

    return {"message": "Canonical column backfill task initiated.", "task_id": task_id}
