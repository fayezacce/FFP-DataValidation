from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from .database import get_db
from .models import User, SystemConfig, RemoteInstance, Upazila
from .auth import require_role
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

@router.get("/config", response_model=List[ConfigOut])
async def get_configs(db: Session = Depends(get_db), admin: User = Depends(require_role(["admin"]))):
    return db.query(SystemConfig).all()

@router.put("/config/{key}", response_model=ConfigOut)
async def update_config(key: str, update: ConfigUpdate, db: Session = Depends(get_db), admin: User = Depends(require_role(["admin"]))):
    config = db.query(SystemConfig).filter(SystemConfig.key == key).first()
    if not config:
        # Create if doesn't exist
        config = SystemConfig(key=key, value=update.value)
        db.add(config)
    else:
        config.value = update.value
    db.commit()
    db.refresh(config)
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

@router.get("/instances", response_model=List[InstanceOut])
async def get_instances(db: Session = Depends(get_db), admin: User = Depends(require_role(["admin"]))):
    return db.query(RemoteInstance).all()

@router.post("/instances", response_model=InstanceOut)
async def create_instance(data: InstanceCreate, db: Session = Depends(get_db), admin: User = Depends(require_role(["admin"]))):
    instance = RemoteInstance(**data.dict())
    db.add(instance)
    db.commit()
    db.refresh(instance)
    return instance

@router.delete("/instances/{id}")
async def delete_instance(id: int, db: Session = Depends(get_db), admin: User = Depends(require_role(["admin"]))):
    instance = db.query(RemoteInstance).filter(RemoteInstance.id == id).first()
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")
    db.delete(instance)
    db.commit()
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

@router.get("/upazilas", response_model=List[UpazilaOut])
async def get_upazilas(db: Session = Depends(get_db), admin: User = Depends(require_role(["admin"]))):
    return db.query(Upazila).all()

@router.post("/upazilas", response_model=UpazilaOut)
async def create_upazila(data: UpazilaCreate, db: Session = Depends(get_db), admin: User = Depends(require_role(["admin"]))):
    # check exists
    existing = db.query(Upazila).filter(Upazila.district_name == data.district_name, Upazila.name == data.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Upazila already exists in this district")
    
    upz = Upazila(**data.dict())
    db.add(upz)
    db.commit()
    db.refresh(upz)
    return upz

@router.delete("/upazilas/{id}")
async def delete_upazila(id: int, db: Session = Depends(get_db), admin: User = Depends(require_role(["admin"]))):
    upz = db.query(Upazila).filter(Upazila.id == id).first()
    if not upz:
        raise HTTPException(status_code=404, detail="Upazila not found")
    db.delete(upz)
    db.commit()
    return {"detail": "Deleted"}
