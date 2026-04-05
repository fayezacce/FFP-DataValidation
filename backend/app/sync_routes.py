"""
FFP Data Validator — Sync Routes
Handles multi-instance sync (export/import), IBAS NID verification, and remote triggers.
"""
from fastapi import APIRouter, HTTPException, Depends
from fastapi.requests import Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timezone
import json as json_mod
import logging

from .database import get_db
from .models import User, ValidRecord
from .auth import get_current_user, get_api_key, limiter
from .rbac import PermissionChecker

logger = logging.getLogger("ffp")
router = APIRouter(tags=["sync"])


def _get_db_rate_limit(request: Request):
    from .models import SystemConfig
    db: Session = next(get_db())
    config = db.query(SystemConfig).filter(SystemConfig.key == "rate_limit_value").first()
    return config.value if config else "60/minute"


@router.get("/ibas/nid-verify")
@limiter.limit(_get_db_rate_limit)
async def ibas_verify_nid(
    request: Request,
    id: str,
    db: Session = Depends(get_db),
    api_user: User = Depends(get_api_key),
):
    """Secure, rate-limited endpoint for IBAS to verify NID."""
    if not id or not id.strip():
        raise HTTPException(status_code=400, detail="Missing NID")
    exists = db.query(ValidRecord).filter(ValidRecord.nid == id.strip()).first() is not None
    return {"found": exists}


@router.get("/export")
async def sync_export(
    since: datetime = None,
    db: Session = Depends(get_db),
    api_user: User = Depends(get_api_key),
):
    """Export ValidRecords modified after the provided timestamp. Streams in chunks for 100M+ scale."""
    query = db.query(ValidRecord)
    if since:
        query = query.filter(ValidRecord.updated_at > since)

    total_count = query.count()

    def record_to_dict(r):
        return {
            "nid": r.nid,
            "dob": r.dob,
            "name": r.name,
            "division": r.division,
            "district": r.district,
            "upazila": r.upazila,
            "source_file": r.source_file,
            "upload_batch": r.upload_batch,
            "data": r.data,
            "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
            "updated_at": r.updated_at.isoformat() + "Z" if r.updated_at else None,
        }

    CHUNK_SIZE = 5000
    if total_count <= CHUNK_SIZE:
        records = query.all()
        return {"records": [record_to_dict(r) for r in records]}

    def stream_records():
        offset = 0
        yield '{"total_count": ' + str(total_count) + ', "records": ['
        first = True
        while True:
            batch = query.order_by(ValidRecord.id).offset(offset).limit(CHUNK_SIZE).all()
            if not batch:
                break
            for r in batch:
                prefix = "" if first else ","
                first = False
                yield prefix + json_mod.dumps(record_to_dict(r))
            offset += CHUNK_SIZE
        yield "]}"

    return StreamingResponse(stream_records(), media_type="application/json")


class SyncImportPayload(BaseModel):
    records: list = []


@router.post("/import")
async def sync_import(
    payload: SyncImportPayload,
    db: Session = Depends(get_db),
    api_user: User = Depends(get_api_key),
):
    """Import ValidRecords via bulk upsert. Limited to 10,000 records per call."""
    records = payload.records
    if not records:
        return {"imported": 0}
    if len(records) > 10000:
        raise HTTPException(status_code=400, detail="Import limited to 10,000 records per request. Split your payload.")

    insert_data = []
    for r in records:
        created_at = datetime.fromisoformat(r["created_at"].rstrip("Z")) if r.get("created_at") else datetime.now(timezone.utc)
        updated_at = datetime.fromisoformat(r["updated_at"].rstrip("Z")) if r.get("updated_at") else datetime.now(timezone.utc)
        insert_data.append({
            "nid": r["nid"],
            "dob": r.get("dob", ""),
            "name": r.get("name", ""),
            "division": r.get("division", ""),
            "district": r.get("district", ""),
            "upazila": r.get("upazila", ""),
            "source_file": r.get("source_file", ""),
            "upload_batch": r.get("upload_batch", 1),
            "data": r.get("data", {}),
            "created_at": created_at,
            "updated_at": updated_at,
        })

    for i in range(0, len(insert_data), 1000):
        chunk = insert_data[i : i + 1000]
        stmt = insert(ValidRecord).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=["nid"],
            set_={
                "dob": stmt.excluded.dob,
                "name": stmt.excluded.name,
                "division": stmt.excluded.division,
                "district": stmt.excluded.district,
                "upazila": stmt.excluded.upazila,
                "source_file": stmt.excluded.source_file,
                "upload_batch": stmt.excluded.upload_batch,
                "data": stmt.excluded.data,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        db.execute(stmt)
    db.commit()
    return {"imported": len(records)}


import httpx


@router.post("/admin/instances/{id}/trigger-sync", dependencies=[Depends(PermissionChecker("view_admin"))])
async def trigger_remote_sync(
    id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    from .models import RemoteInstance

    instance = db.query(RemoteInstance).filter(RemoteInstance.id == id).first()
    if not instance:
        raise HTTPException(status_code=404, detail="Instance not found")

    since_param = f"?since={instance.last_synced_at.isoformat()}" if instance.last_synced_at else ""
    url = f"{instance.url.rstrip('/')}/sync/export{since_param}"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(url, headers={"X-API-Key": instance.api_key})
            resp.raise_for_status()
            data = resp.json()
            records = data.get("records", [])

            if records:
                await sync_import(SyncImportPayload(records=records), db=db, api_user=current_user)

            instance.last_synced_at = datetime.now(timezone.utc)
            db.commit()
            return {"synced_count": len(records), "status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")
