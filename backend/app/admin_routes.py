from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from .database import get_db
from .models import User, SystemConfig, RemoteInstance, Upazila
from .auth import get_current_user
from .rbac import PermissionChecker
from .audit import log_audit
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

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
