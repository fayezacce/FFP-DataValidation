from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional

from .database import get_db
from .models import HeaderAlias, User
from .auth import get_current_user
from .rbac import PermissionChecker
from .audit import log_audit

router = APIRouter(prefix="/admin/header-aliases", tags=["header-aliases"])


class HeaderAliasOut(BaseModel):
    id: int
    original_header: str
    canonical_key: str

    class Config:
        from_attributes = True


class HeaderAliasCreate(BaseModel):
    original_header: str
    canonical_key: str


class HeaderAliasUpdate(BaseModel):
    original_header: Optional[str] = None
    canonical_key: Optional[str] = None


@router.get("/", response_model=List[HeaderAliasOut], dependencies=[Depends(PermissionChecker("view_admin"))])
async def get_header_aliases(db: Session = Depends(get_db)):
    return db.query(HeaderAlias).all()


@router.post("/", response_model=HeaderAliasOut, dependencies=[Depends(PermissionChecker("view_admin"))])
async def create_header_alias(
    data: HeaderAliasCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    existing = db.query(HeaderAlias).filter(HeaderAlias.original_header == data.original_header).first()
    if existing:
        raise HTTPException(status_code=400, detail="This header alias already exists.")

    new_alias = HeaderAlias(
        original_header=data.original_header,
        canonical_key=data.canonical_key
    )
    db.add(new_alias)
    db.commit()
    db.refresh(new_alias)

    log_audit(db, current_user, "CREATE", "header_aliases", new_alias.id, new_data={"original_header": data.original_header, "canonical_key": data.canonical_key})

    return new_alias


@router.put("/{alias_id}", response_model=HeaderAliasOut, dependencies=[Depends(PermissionChecker("view_admin"))])
async def update_header_alias(
    alias_id: int,
    data: HeaderAliasUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    alias = db.query(HeaderAlias).filter(HeaderAlias.id == alias_id).first()
    if not alias:
        raise HTTPException(status_code=404, detail="Alias not found.")

    old_data = {"original_header": alias.original_header, "canonical_key": alias.canonical_key}

    if data.original_header is not None:
        alias.original_header = data.original_header
    if data.canonical_key is not None:
        alias.canonical_key = data.canonical_key

    db.commit()
    db.refresh(alias)

    new_data = {"original_header": alias.original_header, "canonical_key": alias.canonical_key}
    log_audit(db, current_user, "UPDATE", "header_aliases", alias_id, old_data=old_data, new_data=new_data)

    return alias


@router.delete("/{alias_id}", dependencies=[Depends(PermissionChecker("view_admin"))])
async def delete_header_alias(
    alias_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    alias = db.query(HeaderAlias).filter(HeaderAlias.id == alias_id).first()
    if not alias:
        raise HTTPException(status_code=404, detail="Alias not found.")

    old_data = {"original_header": alias.original_header, "canonical_key": alias.canonical_key}
    
    db.delete(alias)
    db.commit()

    log_audit(db, current_user, "DELETE", "header_aliases", alias_id, old_data=old_data)

    return {"detail": "Deleted"}
