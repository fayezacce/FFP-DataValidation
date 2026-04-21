"""
FFP Data Validator — Records Routes
Beneficiary and Dealer management: list, detail, edit, delete, verify, add.
All endpoints enforce geo-scoped access control via RBAC.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from datetime import datetime, timezone
from typing import Optional, List
from pydantic import BaseModel
import logging

from .database import get_db
from .models import User, ValidRecord, InvalidRecord, SummaryStats, Dealer
from .auth import get_current_user
from .rbac import PermissionChecker
from .audit import log_audit
from .stats_utils import refresh_summary_stats

logger = logging.getLogger("ffp")
router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _apply_geo_scope(query, model, user: User):
    """Restrict query to the user's permitted geo scope."""
    if user.role == "admin":
        return query
    if user.upazila_access:
        return query.filter(model.upazila == user.upazila_access)
    if user.district_access:
        return query.filter(model.district == user.district_access)
    if user.division_access:
        return query.filter(model.division == user.division_access)
    return query


def _check_geo_permission(user: User, division: str, district: str, upazila: str):
    """Raise 403 if the requested location is outside the user's access scope."""
    if user.role == "admin":
        return
    if user.upazila_access and upazila and user.upazila_access != upazila:
        raise HTTPException(403, f"Access restricted to {user.upazila_access}")
    if user.district_access and district and user.district_access != district:
        raise HTTPException(403, f"Access restricted to {user.district_access}")
    if user.division_access and division and user.division_access != division:
        raise HTTPException(403, f"Access restricted to {user.division_access}")


def _extract_extended_fields(record) -> dict:
    """Build extended_fields dict, preferring root canonical columns over JSON lookup."""
    # First extract from data JSON as fallback
    data = {}
    if hasattr(record, 'data') and isinstance(record.data, dict):
        data = record.data
    elif isinstance(record, dict):
        data = record.get('data') or {}

    _pick = lambda keys: next((str(data[k]).strip() for k in keys if data.get(k)), None)

    # For each field: prefer root column (may be None), fallback to JSON _pick
    def _col_or_json(col_val, *json_keys):
        if col_val is not None and str(col_val).strip():
            return str(col_val).strip()
        return _pick(set(json_keys))

    # Get root column values if record is ORM object
    def _rc(attr):
        if hasattr(record, attr):
            v = getattr(record, attr)
            return v if v is not None else None
        return None

    return {
        "father_husband_name": _col_or_json(_rc('father_husband_name'), "father_husband_name", "পিতা / স্বামীর নাম", "পিতার নাম"),
        "mother_name":         _pick({"mother_name", "মায়ের নাম"}),
        "spouse_name":         _col_or_json(_rc('spouse_name'), "spouse_name", "স্বামী/স্ত্রীর নাম (NID সাথে মিল থাকতে হবে)", "স্বামী/স্ত্রীর নাম"),
        "spouse_nid":          _col_or_json(_rc('spouse_nid'), "spouse_nid", "স্বামী/স্ত্রী জাতীয় পরিচয় নম্বর"),
        "spouse_dob":          _col_or_json(_rc('spouse_dob'), "spouse_dob", "স্বামী/স্ত্রী জন্ম তারিখ (NID সাথে মিল থাকতে হবে)"),
        "name_bn":             _col_or_json(_rc('name_bn'), "name_bn", "নাম (বাংলা)", "বাংলা নাম", "উপকার ভোগীর নাম (বাংলা) (NID সাথে মিল থাকতে হবে)"),
        "name_en":             _col_or_json(_rc('name_en'), "name_en", "Name_EN", "উপকার ভোগীর নাম (ইংরেজি) (NID সাথে মিল থাকতে হবে)"),
        "gender":              _col_or_json(_rc('gender'), "gender", "লিঙ্গ"),
        "religion":            _col_or_json(_rc('religion'), "religion", "ধর্ম"),
        "address":             _col_or_json(_rc('address'), "address", "ঠিকানা", "গ্রাম", "গ্রামের নাম"),
        "occupation":          _col_or_json(_rc('occupation'), "occupation", "পেশা"),
        "ward":                _col_or_json(_rc('ward'), "ward", "ওয়ার্ড নং"),
        "union_name":          _col_or_json(_rc('union_name'), "union_name", "ইউনিয়ন", "ইউনিয়নের নাম"),
        "remarks":             _pick({"remarks", "মন্তব্য"}),
        "dealer_name":         _pick({"dealer_name", "Dealer_Name", "ডিলারের নাম", "রেজিস্টার্ড ডিলারের নাম (NID সাথে মিল থাকতে হবে)"}),
        "dealer_nid":          _pick({"dealer_nid", "Dealer_NID", "রেজিস্টার্ড ডিলারের এনআইডি নম্বর"}),
        "dealer_mobile":       _pick({"dealer_mobile", "Dealer_Mobile", "রেজিস্টার্ড ডিলারের মোবাইল নং"}),
    }


# ─────────────────────────────────────────────────────────────────────────────
# PYDANTIC SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────

class BeneficiaryUpdate(BaseModel):
    dob:                 Optional[str] = None
    name:                Optional[str] = None
    card_no:             Optional[str] = None
    mobile:              Optional[str] = None
    father_husband_name: Optional[str] = None
    name_bn:             Optional[str] = None
    name_en:             Optional[str] = None
    ward:                Optional[str] = None
    union_name:          Optional[str] = None
    # Standard canonical fields — now first-class columns
    occupation:          Optional[str] = None
    gender:              Optional[str] = None
    religion:            Optional[str] = None
    address:             Optional[str] = None
    spouse_name:         Optional[str] = None
    spouse_nid:          Optional[str] = None
    spouse_dob:          Optional[str] = None
    # Additional JSON fields passed through as-is
    extra_fields:        Optional[dict] = None


class BeneficiaryCreate(BaseModel):
    nid:                 str
    dob:                 str
    name:                str
    division:            str
    district:            str
    upazila:             str
    card_no:             Optional[str] = None
    mobile:              Optional[str] = None
    father_husband_name: Optional[str] = None
    name_bn:             Optional[str] = None
    name_en:             Optional[str] = None
    ward:                Optional[str] = None
    union_name:          Optional[str] = None
    # Standard canonical fields
    occupation:          Optional[str] = None
    gender:              Optional[str] = None
    religion:            Optional[str] = None
    address:             Optional[str] = None
    spouse_name:         Optional[str] = None
    spouse_nid:          Optional[str] = None
    spouse_dob:          Optional[str] = None
    extra_fields:        Optional[dict] = None


class BulkVerifyRequest(BaseModel):
    record_ids: List[int]


class DealerUpdate(BaseModel):
    name:   Optional[str] = None
    mobile: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# BENEFICIARY ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/beneficiaries", dependencies=[Depends(PermissionChecker("view_stats"))])
def list_beneficiaries(
    division:            Optional[str] = Query(None),
    district:            Optional[str] = Query(None),
    upazila:             Optional[str] = Query(None),
    filter:              Optional[str] = Query("all"),   # all | valid (valid_records) — kept for URL compat
    search:              Optional[str] = Query(None),
    verification_status: Optional[str] = Query(None),   # verified | unverified
    page:                int           = Query(1, ge=1),
    page_size:           int           = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User  = Depends(get_current_user),
):
    """Paginated list of valid beneficiary records. Fast columns only — no data JSON."""
    _check_geo_permission(current_user, division, district, upazila)

    q = db.query(
        ValidRecord.id,
        ValidRecord.nid,
        ValidRecord.dob,
        ValidRecord.name,
        ValidRecord.card_no,
        ValidRecord.mobile,
        ValidRecord.division,
        ValidRecord.district,
        ValidRecord.upazila,
        ValidRecord.father_husband_name,
        ValidRecord.name_bn,
        ValidRecord.name_en,
        ValidRecord.ward,
        ValidRecord.union_name,
        ValidRecord.dealer_id,
        ValidRecord.verification_status,
        ValidRecord.verified_by,
        ValidRecord.verified_at,
        ValidRecord.created_at,
        ValidRecord.updated_at,
    )

    q = _apply_geo_scope(q, ValidRecord, current_user)

    if division:
        q = q.filter(ValidRecord.division == division)
    if district:
        q = q.filter(ValidRecord.district == district)
    if upazila:
        q = q.filter(ValidRecord.upazila == upazila)
    if verification_status in ("verified", "unverified"):
        q = q.filter(ValidRecord.verification_status == verification_status)
    if search:
        s = f"%{search}%"
        from sqlalchemy import or_
        q = q.filter(or_(
            ValidRecord.name.ilike(s),
            ValidRecord.nid.ilike(s),
            ValidRecord.mobile.ilike(s),
            ValidRecord.card_no.ilike(s),
        ))

    total = q.count()
    records_raw = q.order_by(ValidRecord.id.asc()) \
                   .offset((page - 1) * page_size) \
                   .limit(page_size) \
                   .all()

    # Fetch dealer names for the returned records in one query
    dealer_ids = [r.dealer_id for r in records_raw if r.dealer_id]
    dealer_map: dict[int, str] = {}
    if dealer_ids:
        dealers = db.query(Dealer.id, Dealer.name, Dealer.nid).filter(Dealer.id.in_(dealer_ids)).all()
        dealer_map = {d.id: {"name": d.name, "nid": d.nid} for d in dealers}

    records = []
    for r in records_raw:
        d_info = dealer_map.get(r.dealer_id, {}) if r.dealer_id else {}
        records.append({
            "id":                  r.id,
            "nid":                 r.nid,
            "dob":                 r.dob,
            "name":                r.name,
            "card_no":             r.card_no,
            "mobile":              r.mobile,
            "division":            r.division,
            "district":            r.district,
            "upazila":             r.upazila,
            "father_husband_name": r.father_husband_name,
            "name_bn":             r.name_bn,
            "name_en":             r.name_en,
            "ward":                r.ward,
            "union_name":          r.union_name,
            "dealer_id":           r.dealer_id,
            "dealer_name":         d_info.get("name"),
            "dealer_nid":          d_info.get("nid"),
            "verification_status": r.verification_status or "unverified",
            "verified_by":         r.verified_by,
            "verified_at":         r.verified_at.isoformat() if r.verified_at else None,
            "created_at":          r.created_at.isoformat() if r.created_at else None,
            "updated_at":          r.updated_at.isoformat() if r.updated_at else None,
        })

    return {"records": records, "total": total, "page": page, "page_size": page_size}


@router.get("/beneficiaries/{record_id}", dependencies=[Depends(PermissionChecker("view_stats"))])
def get_beneficiary(
    record_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Full beneficiary record detail including raw data JSON."""
    record = db.query(ValidRecord).filter(ValidRecord.id == record_id).first()
    if not record:
        raise HTTPException(404, "Record not found")
    _check_geo_permission(current_user, record.division, record.district, record.upazila)

    dealer_info = {}
    if record.dealer_id:
        dealer = db.query(Dealer).filter(Dealer.id == record.dealer_id).first()
        if dealer:
            dealer_info = {"id": dealer.id, "name": dealer.name, "nid": dealer.nid, "mobile": dealer.mobile}

    extended = _extract_extended_fields(record)
    return {
        "id":                  record.id,
        "nid":                 record.nid,
        "dob":                 record.dob,
        "name":                record.name,
        "card_no":             record.card_no,
        "mobile":              record.mobile,
        "division":            record.division,
        "district":            record.district,
        "upazila":             record.upazila,
        "father_husband_name": record.father_husband_name,
        "name_bn":             record.name_bn,
        "name_en":             record.name_en,
        "ward":                record.ward,
        "union_name":          record.union_name,
        # ── New canonical columns ──
        "occupation":          record.occupation,
        "gender":              record.gender,
        "religion":            record.religion,
        "address":             record.address,
        "spouse_name":         record.spouse_name,
        "spouse_nid":          record.spouse_nid,
        "spouse_dob":          record.spouse_dob,
        "dealer":              dealer_info,
        "dealer_name":         dealer_info.get("name"),
        "dealer_nid":          dealer_info.get("nid"),
        "dealer_mobile":       dealer_info.get("mobile"),
        "verification_status": record.verification_status or "unverified",
        "verified_by":         record.verified_by,
        "verified_by_id":      record.verified_by_id,
        "verified_at":         record.verified_at.isoformat() if record.verified_at else None,
        "created_at":          record.created_at.isoformat() if record.created_at else None,
        "updated_at":          record.updated_at.isoformat() if record.updated_at else None,
        "extended_fields":     extended,
        "raw_data":            record.data,
    }


@router.put("/beneficiaries/{record_id}", dependencies=[Depends(PermissionChecker("manage_records"))])
def update_beneficiary(
    record_id: int,
    payload:   BeneficiaryUpdate,
    db:        Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Edit a valid beneficiary record.
    Every edit resets verification_status to 'unverified'.
    NID is immutable — use delete + add for NID correction.
    """
    record = db.query(ValidRecord).filter(ValidRecord.id == record_id).first()
    if not record:
        raise HTTPException(404, "Record not found")
    _check_geo_permission(current_user, record.division, record.district, record.upazila)

    old_data = {
        "dob": record.dob, "name": record.name, "mobile": record.mobile,
        "father_husband_name": record.father_husband_name,
    }

    # Update dedicated columns
    if payload.dob            is not None: record.dob                = payload.dob
    if payload.name           is not None: record.name               = payload.name
    if payload.card_no        is not None: record.card_no            = payload.card_no
    if payload.mobile         is not None: record.mobile             = payload.mobile
    if payload.father_husband_name is not None: record.father_husband_name = payload.father_husband_name
    if payload.name_bn        is not None: record.name_bn            = payload.name_bn
    if payload.name_en        is not None: record.name_en            = payload.name_en
    if payload.ward           is not None: record.ward               = payload.ward
    if payload.union_name     is not None: record.union_name         = payload.union_name

    # Canonical fields
    if payload.occupation     is not None: record.occupation         = payload.occupation
    if payload.gender         is not None: record.gender             = payload.gender
    if payload.religion       is not None: record.religion           = payload.religion
    if payload.address        is not None: record.address            = payload.address
    if payload.spouse_name    is not None: record.spouse_name        = payload.spouse_name
    if payload.spouse_nid     is not None: record.spouse_nid         = payload.spouse_nid
    if payload.spouse_dob     is not None: record.spouse_dob         = payload.spouse_dob

    # Merge extra_fields into the data JSON (keeps original Bangla keys intact for exports)
    if payload.extra_fields:
        updated_data = dict(record.data or {})
        updated_data.update(payload.extra_fields)
        record.data = updated_data

    # Sync promoted and standard columns back into data JSON for export compatibility
    # This ensures that Excel/CSV exports using original Bengali headers remain accurate.
    data = dict(record.data or {})
    
    # Root columns
    if payload.dob:                data["Cleaned_DOB"] = payload.dob; data["জন্ম তারিখ (NID সাথে মিল থাকতে হবে)"] = payload.dob
    if payload.name:               data["Extracted_Name"] = payload.name
    if payload.father_husband_name: 
        data["father_husband_name"] = payload.father_husband_name
        data["পিতার নাম"] = payload.father_husband_name
    if payload.name_bn:            
        data["name_bn"] = payload.name_bn
        data["উপকার ভোগীর নাম (বাংলা) (NID সাথে মিল থাকতে হবে)"] = payload.name_bn
    if payload.name_en:            
        data["name_en"] = payload.name_en
        data["উপকার ভোগীর নাম (ইংরেজি) (NID সাথে মিল থাকতে হবে)"] = payload.name_en
    if payload.ward:               
        data["ward"] = payload.ward
        data["ওয়ার্ড নং"] = payload.ward
    if payload.union_name:         
        data["union_name"] = payload.union_name
        data["ইউনিয়নের নাম"] = payload.union_name
    if payload.card_no:
        data["কার্ড নং"] = payload.card_no
    if payload.mobile:
        data["মোবাইল নং (নিজ নামে)"] = payload.mobile

    # Set standard Bengali headers for canonical fields into JSON
    if payload.occupation     is not None: data["পেশা"] = payload.occupation
    if payload.gender         is not None: data["লিঙ্গ"] = payload.gender
    if payload.religion       is not None: data["ধর্ম"] = payload.religion
    if payload.address        is not None: data["গ্রামের নাম"] = payload.address; data["address"] = payload.address
    if payload.spouse_name    is not None: data["স্বামী/স্ত্রীর নাম (NID সাথে মিল থাকতে হবে)"] = payload.spouse_name
    if payload.spouse_nid     is not None: data["স্বামী/স্ত্রী জাতীয় পরিচয় নম্বর"] = payload.spouse_nid
    if payload.spouse_dob     is not None: data["স্বামী/স্ত্রী জন্ম তারিখ (NID সাথে মিল থাকতে হবে)"] = payload.spouse_dob

    # Standard JSON fields (Extra fields)
    if payload.extra_fields:
        ef = payload.extra_fields
        if "spouse_name" in ef: data["স্বামী/স্ত্রীর নাম (NID সাথে মিল থাকতে হবে)"] = ef["spouse_name"]
        if "spouse_nid"  in ef: data["স্বামী/স্ত্রী জাতীয় পরিচয় নম্বর"] = ef["spouse_nid"]
        if "spouse_dob"  in ef: data["স্বামী/স্ত্রী জন্ম তারিখ (NID সাথে মিল থাকতে হবে)"] = ef["spouse_dob"]
        if "occupation"  in ef: data["পেশা"] = ef["occupation"]
        if "village_name" in ef: data["গ্রামের নাম"] = ef["village_name"]; data["address"] = ef["village_name"]
        if "gender"      in ef: data["লিঙ্গ"] = ef["gender"]
        if "religion"    in ef: data["ধর্ম"] = ef["religion"]

    record.data = data

    # Reset verification — edit always requires re-verification
    record.verification_status = "unverified"
    record.verified_by         = None
    record.verified_by_id      = None
    record.verified_at         = None
    record.updated_at          = datetime.now(timezone.utc)

    db.commit()
    log_audit(db, current_user, "UPDATE", "valid_records", record_id,
              old_data=old_data,
              new_data={"dob": record.dob, "name": record.name, "mobile": record.mobile})
    return {"detail": "Record updated. Status reset to unverified."}


@router.post("/beneficiaries", dependencies=[Depends(PermissionChecker("manage_records"))])
def add_beneficiary(
    payload:  BeneficiaryCreate,
    db:       Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Manually add a new valid beneficiary record. Marked unverified until reviewed."""
    _check_geo_permission(current_user, payload.division, payload.district, payload.upazila)

    # Check NID uniqueness
    existing = db.query(ValidRecord.id).filter(ValidRecord.nid == payload.nid).first()
    if existing:
        raise HTTPException(400, f"NID {payload.nid} already exists in the database")

    # Resolve geo IDs
    from .models import Division, District, Upazila
    div_obj  = db.query(Division).filter(func.lower(Division.name) == payload.division.lower()).first()
    dist_obj = db.query(District).filter(func.lower(District.name) == payload.district.lower()).first()
    upz_obj  = db.query(Upazila).filter(
        func.lower(Upazila.name)          == payload.upazila.lower(),
        func.lower(Upazila.district_name) == payload.district.lower()
    ).first()

    data_dict = {
        "Cleaned_NID":       payload.nid,
        "Cleaned_DOB":       payload.dob,
        "Extracted_Name":    payload.name,
        "Status":            "success",
        "father_husband_name": payload.father_husband_name or "",
        "name_bn":           payload.name_bn or "",
        "name_en":           payload.name_en or "",
        "ward":              payload.ward or "",
        "union_name":        payload.union_name or "",
        "পেশা":              payload.occupation or "",
        "লিঙ্গ":               payload.gender or "",
        "ধর্ম":                payload.religion or "",
        "গ্রামের নাম":         payload.address or "",
        "address":           payload.address or "",
        "স্বামী/স্ত্রীর নাম (NID সাথে মিল থাকতে হবে)": payload.spouse_name or "",
        "স্বামী/স্ত্রী জাতীয় পরিচয় নম্বর": payload.spouse_nid or "",
        "স্বামী/স্ত্রী জন্ম তারিখ (NID সাথে মিল থাকতে হবে)": payload.spouse_dob or "",
    }
    if payload.extra_fields:
        data_dict.update(payload.extra_fields)

    record = ValidRecord(
        nid=payload.nid,
        dob=payload.dob,
        name=payload.name,
        division=payload.division,
        district=payload.district,
        upazila=payload.upazila,
        division_id=div_obj.id  if div_obj  else None,
        district_id=dist_obj.id if dist_obj else None,
        upazila_id=upz_obj.id   if upz_obj  else None,
        card_no=payload.card_no,
        mobile=payload.mobile,
        father_husband_name=payload.father_husband_name,
        name_bn=payload.name_bn,
        name_en=payload.name_en,
        ward=payload.ward,
        union_name=payload.union_name,
        # ── New canonical columns ──
        occupation=payload.occupation,
        gender=payload.gender,
        religion=payload.religion,
        address=payload.address,
        spouse_name=payload.spouse_name,
        spouse_nid=payload.spouse_nid,
        spouse_dob=payload.spouse_dob,
        verification_status="unverified",
        data=data_dict,
        source_file="manual_entry",
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    # Update summary stats
    refresh_summary_stats(db, payload.division, payload.district, payload.upazila)

    log_audit(db, current_user, "CREATE", "valid_records", record.id,
              new_data={"nid": payload.nid, "name": payload.name, "upazila": payload.upazila})
    return {"detail": "Record added successfully.", "id": record.id}


@router.delete("/beneficiaries/{record_id}", dependencies=[Depends(PermissionChecker("manage_records"))])
def delete_beneficiary(
    record_id: int,
    db:        Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete a valid beneficiary record and decrement SummaryStats counts."""
    record = db.query(ValidRecord).filter(ValidRecord.id == record_id).first()
    if not record:
        raise HTTPException(404, "Record not found")
    _check_geo_permission(current_user, record.division, record.district, record.upazila)

    div, dist, upz = record.division, record.district, record.upazila
    snapshot = {"nid": record.nid, "name": record.name, "upazila": upz}

    db.delete(record)
    db.commit()

    # Decrement SummaryStats
    refresh_summary_stats(db, div, dist, upz)

    log_audit(db, current_user, "DELETE", "valid_records", record_id, old_data=snapshot)
    return {"detail": "Record deleted."}


@router.post("/beneficiaries/bulk-verify", dependencies=[Depends(PermissionChecker("manage_records"))])
def bulk_verify_beneficiaries(
    payload:  BulkVerifyRequest,
    db:       Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Mark a list of beneficiary records as verified."""
    if not payload.record_ids:
        raise HTTPException(400, "No record IDs provided")

    now = datetime.now(timezone.utc)
    updated = db.query(ValidRecord) \
                .filter(ValidRecord.id.in_(payload.record_ids)) \
                .all()

    # Enforce geo scope — block any records outside user's access
    for r in updated:
        _check_geo_permission(current_user, r.division, r.district, r.upazila)

    for r in updated:
        r.verification_status = "verified"
        r.verified_by_id      = current_user.id
        r.verified_by         = current_user.username
        r.verified_at         = now

    db.commit()
    log_audit(db, current_user, "VERIFY", "valid_records", None,
              new_data={"count": len(updated), "ids": payload.record_ids})
    return {"detail": f"{len(updated)} records verified.", "count": len(updated)}


# ─────────────────────────────────────────────────────────────────────────────
# INVALID RECORD ENDPOINTS  (delete only — no edit)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/invalid", dependencies=[Depends(PermissionChecker("view_stats"))])
def list_invalid_records(
    division:  Optional[str] = Query(None),
    district:  Optional[str] = Query(None),
    upazila:   Optional[str] = Query(None),
    search:    Optional[str] = Query(None),
    page:      int           = Query(1, ge=1),
    page_size: int           = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Paginated list of invalid records."""
    _check_geo_permission(current_user, division, district, upazila)

    q = db.query(InvalidRecord)
    q = _apply_geo_scope(q, InvalidRecord, current_user)

    if division: q = q.filter(InvalidRecord.division == division)
    if district: q = q.filter(InvalidRecord.district == district)
    if upazila:  q = q.filter(InvalidRecord.upazila  == upazila)
    if search:
        from sqlalchemy import or_
        s = f"%{search}%"
        q = q.filter(or_(
            InvalidRecord.name.ilike(s),
            InvalidRecord.nid.ilike(s),
        ))

    total = q.count()
    records = q.order_by(InvalidRecord.id.desc()) \
               .offset((page - 1) * page_size) \
               .limit(page_size) \
               .all()

    return {
        "records": [
            {
                "id":            r.id,
                "nid":           r.nid,
                "dob":           r.dob,
                "name":          r.name,
                "card_no":       r.card_no,
                "mobile":        r.mobile,
                "division":      r.division,
                "district":      r.district,
                "upazila":       r.upazila,
                "father_husband_name": r.father_husband_name,
                "name_bn":       r.name_bn,
                "name_en":       r.name_en,
                "ward":          r.ward,
                "union_name":    r.union_name,
                "dealer_id":     r.dealer_id,
                "error_message": r.error_message,
                "created_at":    r.created_at.isoformat() if r.created_at else None,
            }
            for r in records
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/invalid/{record_id}", dependencies=[Depends(PermissionChecker("view_stats"))])
def get_invalid_record(
    record_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Full detail of an invalid record."""
    record = db.query(InvalidRecord).filter(InvalidRecord.id == record_id).first()
    if not record: raise HTTPException(404, "Invalid record not found")
    _check_geo_permission(current_user, record.division, record.district, record.upazila)
    
    extended = _extract_extended_fields(record.data or {})
    return {
        "id":            record.id,
        "nid":           record.nid,
        "dob":           record.dob,
        "name":          record.name,
        "card_no":       record.card_no,
        "mobile":        record.mobile,
        "division":      record.division,
        "district":      record.district,
        "upazila":       record.upazila,
        "father_husband_name": record.father_husband_name,
        "name_bn":       record.name_bn,
        "name_en":       record.name_en,
        "ward":          record.ward,
        "union_name":    record.union_name,
        "error_message": record.error_message,
        "extended_fields": extended,
        "raw_data":      record.data,
        "created_at":    record.created_at.isoformat() if record.created_at else None,
    }


@router.put("/invalid/{record_id}", dependencies=[Depends(PermissionChecker("manage_records"))])
def update_invalid_record(
    record_id: int,
    payload:   BeneficiaryUpdate,
    db:        Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Edit an invalid record. 
    Note: This does NOT auto-promote to ValidRecord; 
    it just allows fixing the data in place for reference or later processing.
    """
    record = db.query(InvalidRecord).filter(InvalidRecord.id == record_id).first()
    if not record: raise HTTPException(404, "Invalid record not found")
    _check_geo_permission(current_user, record.division, record.district, record.upazila)

    if payload.dob            is not None: record.dob                = payload.dob
    if payload.name           is not None: record.name               = payload.name
    if payload.card_no        is not None: record.card_no            = payload.card_no
    if payload.mobile         is not None: record.mobile             = payload.mobile
    if payload.father_husband_name is not None: record.father_husband_name = payload.father_husband_name
    if payload.name_bn        is not None: record.name_bn            = payload.name_bn
    if payload.name_en        is not None: record.name_en            = payload.name_en
    if payload.ward           is not None: record.ward               = payload.ward
    if payload.union_name     is not None: record.union_name         = payload.union_name

    if payload.extra_fields:
        updated_data = dict(record.data or {})
        updated_data.update(payload.extra_fields)
        record.data = updated_data

    record.updated_at = datetime.now(timezone.utc)
    db.commit()
    return {"detail": "Invalid record updated."}


@router.delete("/invalid/{record_id}", dependencies=[Depends(PermissionChecker("manage_records"))])
def delete_invalid_record(
    record_id: int,
    db:        Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete an invalid record and decrement SummaryStats.invalid count."""
    record = db.query(InvalidRecord).filter(InvalidRecord.id == record_id).first()
    if not record:
        raise HTTPException(404, "Record not found")
    _check_geo_permission(current_user, record.division, record.district, record.upazila)

    div, dist, upz = record.division, record.district, record.upazila
    snapshot = {"nid": record.nid, "error": record.error_message}

    db.delete(record)
    db.commit()
    refresh_summary_stats(db, div, dist, upz)

    log_audit(db, current_user, "DELETE", "invalid_records", record_id, old_data=snapshot)
    return {"detail": "Invalid record deleted."}


# ─────────────────────────────────────────────────────────────────────────────
# DEALER ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/dealers", dependencies=[Depends(PermissionChecker("view_stats"))])
def list_dealers(
    division:  Optional[str] = Query(None),
    district:  Optional[str] = Query(None),
    upazila:   Optional[str] = Query(None),
    search:    Optional[str] = Query(None),
    page:      int           = Query(1, ge=1),
    page_size: int           = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Paginated list of dealers with beneficiary counts and cross-upazila warning.
    Cross-upazila warning = same dealer NID appears in > 1 upazila.
    """
    _check_geo_permission(current_user, division, district, upazila)

    # Subquery: which dealer NIDs appear in more than 1 upazila?
    cross_nids_sq = db.query(Dealer.nid) \
                      .group_by(Dealer.nid) \
                      .having(func.count(Dealer.upazila_id.distinct()) > 1) \
                      .subquery()

    q = db.query(Dealer)

    if current_user.role != "admin":
        if current_user.upazila_access:
            q = q.filter(Dealer.upazila == current_user.upazila_access)
        elif current_user.district_access:
            q = q.filter(Dealer.district == current_user.district_access)
        elif current_user.division_access:
            q = q.filter(Dealer.division == current_user.division_access)

    if division: q = q.filter(Dealer.division == division)
    if district: q = q.filter(Dealer.district == district)
    if upazila:  q = q.filter(Dealer.upazila  == upazila)
    if search:
        from sqlalchemy import or_
        s = f"%{search}%"
        q = q.filter(or_(Dealer.name.ilike(s), Dealer.nid.ilike(s)))

    total = q.count()
    dealers = q.order_by(Dealer.district, Dealer.upazila, Dealer.name) \
               .offset((page - 1) * page_size) \
               .limit(page_size) \
               .all()

    cross_nids_set: set[str] = set()
    if dealers:
        dealer_nids = [d.nid for d in dealers]
        cross_rows = db.execute(
            text("SELECT nid FROM dealers GROUP BY nid HAVING COUNT(DISTINCT upazila_id) > 1")
        ).fetchall()
        cross_nids_set = {r[0] for r in cross_rows}

    # Count beneficiaries per dealer
    dealer_ids = [d.id for d in dealers]
    bene_counts: dict[int, int] = {}
    if dealer_ids:
        rows = db.execute(
            text("SELECT dealer_id, COUNT(*) FROM valid_records WHERE dealer_id = ANY(:ids) GROUP BY dealer_id"),
            {"ids": dealer_ids}
        ).fetchall()
        bene_counts = {r[0]: r[1] for r in rows}

    return {
        "dealers": [
            {
                "id":                    d.id,
                "nid":                   d.nid,
                "name":                  d.name,
                "mobile":                d.mobile,
                "division":              d.division,
                "district":              d.district,
                "upazila":               d.upazila,
                "upazila_id":            d.upazila_id,
                "beneficiary_count":     bene_counts.get(d.id, 0),
                "cross_upazila_warning": d.nid in cross_nids_set,
                "is_active":             d.is_active,
            }
            for d in dealers
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/dealers/{dealer_id}", dependencies=[Depends(PermissionChecker("view_stats"))])
def get_dealer(
    dealer_id: int,
    page:      int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Dealer detail with paginated list of linked beneficiaries."""
    dealer = db.query(Dealer).filter(Dealer.id == dealer_id).first()
    if not dealer:
        raise HTTPException(404, "Dealer not found")
    _check_geo_permission(current_user, dealer.division, dealer.district, dealer.upazila)

    # Cross-upazila check
    other_upazilas = db.query(Dealer.upazila).filter(
        Dealer.nid == dealer.nid,
        Dealer.id  != dealer.id
    ).all()

    bq = db.query(ValidRecord.id, ValidRecord.nid, ValidRecord.name, ValidRecord.dob,
                  ValidRecord.card_no, ValidRecord.mobile, ValidRecord.verification_status) \
           .filter(ValidRecord.dealer_id == dealer_id)
    total = bq.count()
    beneficiaries = bq.order_by(ValidRecord.id).offset((page - 1) * page_size).limit(page_size).all()

    return {
        "dealer": {
            "id":           dealer.id,
            "nid":          dealer.nid,
            "name":         dealer.name,
            "mobile":       dealer.mobile,
            "division":     dealer.division,
            "district":     dealer.district,
            "upazila":      dealer.upazila,
            "is_active":    dealer.is_active,
            "cross_upazila_warning": len(other_upazilas) > 0,
            "other_upazilas": [r[0] for r in other_upazilas],
        },
        "beneficiaries": {
            "records": [
                {
                    "id":                  b.id,
                    "nid":                 b.nid,
                    "name":                b.name,
                    "dob":                 b.dob,
                    "card_no":             b.card_no,
                    "mobile":              b.mobile,
                    "verification_status": b.verification_status or "unverified",
                }
                for b in beneficiaries
            ],
            "total":     total,
            "page":      page,
            "page_size": page_size,
        },
    }


@router.put("/dealers/{dealer_id}", dependencies=[Depends(PermissionChecker("manage_records"))])
def update_dealer(
    dealer_id: int,
    payload:   DealerUpdate,
    db:        Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Edit dealer name/mobile.
    Propagates changes atomically to all linked valid_records.data JSON fields
    so that exports remain accurate.
    """
    dealer = db.query(Dealer).filter(Dealer.id == dealer_id).first()
    if not dealer:
        raise HTTPException(404, "Dealer not found")
    _check_geo_permission(current_user, dealer.division, dealer.district, dealer.upazila)

    old = {"name": dealer.name, "mobile": dealer.mobile}

    if payload.name   is not None: dealer.name   = payload.name
    if payload.mobile is not None: dealer.mobile = payload.mobile
    dealer.updated_at = datetime.now(timezone.utc)
    db.commit()

    # Propagate to valid_records.data JSON for export accuracy
    # We use a PostgreSQL jsonb merge to update dealer_name / dealer_mobile keys
    if payload.name is not None or payload.mobile is not None:
        update_parts = []
        params: dict = {"did": dealer_id}
        if payload.name:
            update_parts.append("data = data || jsonb_build_object('dealer_name', :dname)")
            params["dname"] = payload.name
        if payload.mobile:
            update_parts.append("data = data || jsonb_build_object('dealer_mobile', :dmob)")
            params["dmob"] = payload.mobile
        if update_parts:
            try:
                db.execute(
                    text(f"UPDATE valid_records SET {', '.join(update_parts)} WHERE dealer_id = :did"),
                    params
                )
                db.commit()
            except Exception as e:
                db.rollback()
                logger.warning(f"Could not propagate dealer changes to valid_records.data: {e}")

    log_audit(db, current_user, "UPDATE", "dealers", dealer_id,
              old_data=old, new_data={"name": dealer.name, "mobile": dealer.mobile})
    return {"detail": "Dealer updated.", "id": dealer_id}
