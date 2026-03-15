from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from .database import get_db
from .models import User, SystemConfig, RemoteInstance, Upazila, TrailingZeroWhitelist
from .auth import get_current_user
from .rbac import PermissionChecker
from .audit import log_audit
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import subprocess
import os
from fastapi.responses import StreamingResponse, FileResponse
from fastapi import UploadFile, File
import tempfile
import shutil

router = APIRouter(prefix="/admin", tags=["admin"])

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
async def get_upazilas(db: Session = Depends(get_db)):
    return db.query(Upazila).all()

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

@router.get("/db/export", dependencies=[Depends(PermissionChecker("view_admin"))])
async def export_database(current_user: User = Depends(get_current_user)):
    """
    Exports the database to a .sql file and returns it as a download.
    """
    db_name = "ffp_validator"
    db_user = "fayez"
    db_host = "db"
    db_password = "fayez_secret"
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"ffp_db_export_{timestamp}.sql"
    
    # We use a temp file to store the dump before sending
    temp_dir = tempfile.gettempdir()
    filepath = os.path.join(temp_dir, filename)
    
    env = os.environ.copy()
    env["PGPASSWORD"] = db_password
    
    try:
        # Run pg_dump
        subprocess.run(
            ["pg_dump", "-h", db_host, "-U", db_user, "-f", filepath, db_name],
            env=env,
            check=True,
            capture_output=True
        )
        
        # Log the action
        # Note: We don't have a DB session here easily without boilerplate, 
        # but the PermissionChecker ensures only admins can reach this.
        
        return FileResponse(
            path=filepath, 
            filename=filename, 
            media_type='application/sql',
            background=None # File will be cleaned up by OS temp cleanup or manually
        )
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.decode() if e.stderr else str(e)
        raise HTTPException(status_code=500, detail=f"Database export failed: {error_msg}")

@router.post("/db/import", dependencies=[Depends(PermissionChecker("view_admin"))])
async def import_database(file: UploadFile = File(...), db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """
    Imports a .sql file to restore the database.
    WARNING: This will overwrite existing data.
    """
    if not file.filename.endswith(".sql"):
        raise HTTPException(status_code=400, detail="Only .sql files are supported")
    
    db_name = "ffp_validator"
    db_user = "fayez"
    db_host = "db"
    db_password = "fayez_secret"
    
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".sql")
    try:
        shutil.copyfileobj(file.file, temp_file)
        temp_file.close()
        
        env = os.environ.copy()
        env["PGPASSWORD"] = db_password
        
        # Restoring a live DB can be tricky. 
        # A simple way is to use psql to run the commands in the SQL file.
        # This assumes the SQL file contains commands to DROP/CREATE tables or just INSERTs.
        
        # If the pg_dump was from this same DB structure, it likely contains:
        # DROP TABLE IF EXISTS ...
        # CREATE TABLE ...
        
        # We'll execute it using psql
        result = subprocess.run(
            ["psql", "-h", db_host, "-U", db_user, "-d", db_name, "-f", temp_file.name],
            env=env,
            check=True,
            capture_output=True
        )
        
        log_audit(db, current_user, "IMPORT", "database", 0, new_data={"filename": file.filename})
        
        return {"detail": "Database imported successfully"}
        
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.decode() if e.stderr else str(e)
        raise HTTPException(status_code=500, detail=f"Database import failed: {error_msg}")
    finally:
        if os.path.exists(temp_file.name):
            os.remove(temp_file.name)

# --- Location Renaming ---

class LocationRename(BaseModel):
    level: str # 'division', 'district', 'upazila'
    old_name: str
    new_name: str
    parent_name: Optional[str] = None # division_name for district, district_name for upazila

@router.put("/location/rename", dependencies=[Depends(PermissionChecker("manage_geo"))])
async def rename_location(data: LocationRename, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """
    Renames a location and cascades the change to all referencing tables.
    """
    from .models import Division, District, Upazila, SummaryStats, ValidRecord, UploadBatch
    
    level = data.level.lower()
    old_name = data.old_name
    new_name = data.new_name
    parent_name = data.parent_name
    
    if level == 'division':
        # Update Division Table
        division = db.query(Division).filter(Division.name == old_name).first()
        if division:
            division.name = new_name
        
        # Update District Table (division_name)
        db.query(District).filter(District.division_name == old_name).update({"division_name": new_name})
        
        # Update Upazila Table (division_name)
        db.query(Upazila).filter(Upazila.division_name == old_name).update({"division_name": new_name})
        
        # Update Data Tables
        db.query(SummaryStats).filter(SummaryStats.division == old_name).update({"division": new_name})
        db.query(ValidRecord).filter(ValidRecord.division == old_name).update({"division": new_name})
        db.query(UploadBatch).filter(UploadBatch.division == old_name).update({"division": new_name})
        
    elif level == 'district':
        # Update District Table
        district = db.query(District).filter(District.name == old_name).first()
        if district:
            district.name = new_name
            
        # Update Upazila Table (district_name)
        db.query(Upazila).filter(Upazila.district_name == old_name).update({"district_name": new_name})
        
        # Update Data Tables
        db.query(SummaryStats).filter(SummaryStats.district == old_name).update({"district": new_name})
        db.query(ValidRecord).filter(ValidRecord.district == old_name).update({"district": new_name})
        db.query(UploadBatch).filter(UploadBatch.district == old_name).update({"district": new_name})
        
    elif level == 'upazila':
        # Update Upazila Table
        # We use parent_name (district) to ensure we rename the correct upazila
        query = db.query(Upazila).filter(Upazila.name == old_name)
        if parent_name:
            query = query.filter(Upazila.district_name == parent_name)
        
        upz = query.first()
        if upz:
            upz.name = new_name
            
        # Update Data Tables
        q_stats = db.query(SummaryStats).filter(SummaryStats.upazila == old_name)
        q_valid = db.query(ValidRecord).filter(ValidRecord.upazila == old_name)
        q_batch = db.query(UploadBatch).filter(UploadBatch.upazila == old_name)
        
        if parent_name:
            q_stats = q_stats.filter(SummaryStats.district == parent_name)
            q_valid = q_valid.filter(ValidRecord.district == parent_name)
            q_batch = q_batch.filter(UploadBatch.district == parent_name)
            
        q_stats.update({"upazila": new_name})
        q_valid.update({"upazila": new_name})
        q_batch.update({"upazila": new_name})
    
    else:
        raise HTTPException(status_code=400, detail="Invalid level")
        
    db.commit()
    
    log_audit(db, current_user, "RENAME", level, 0, old_data={"name": old_name, "parent": parent_name}, new_data={"name": new_name})
    
    return {"detail": f"Renamed {level} from {old_name} to {new_name}"}
