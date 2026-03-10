"""
Bangladesh Administrative Geo Data Module
Author: Fayez Ahmed
Data source: github.com/nuhil/bangladesh-geocode

Provides Division → District → Upazila hierarchy and fuzzy filename matching.
"""

import os
import re
from difflib import get_close_matches
from sqlalchemy.orm import Session
from .models import Division, District, Upazila

# ─── Memory Indexes ────────────────────────────────────────────────────────────

# Division name → db record reference
_division_lookup: dict[str, str] = {}
# District name → db record reference (including aliases if provided later)
_district_lookup: dict[str, dict] = {}
_district_names: list[str] = []
# Upazila name → list of db record references
_upazila_lookup: dict[str, list[dict]] = {}
_upazila_names: list[str] = []


def _normalize(s: str) -> str:
    """Lowercase, strip, remove hyphens/extra spaces."""
    return re.sub(r'[\s\-]+', ' ', s.lower().strip())


def load_geo_data_from_db(db: Session):
    """
    Called exactly once during application startup (in main.py lifespan) to pull
    the entire geo tree from Postgres into memory for fast fuzzy matching.
    """
    global _division_lookup, _district_lookup, _district_names, _upazila_lookup, _upazila_names
    
    # Reset
    _division_lookup.clear()
    _district_lookup.clear()
    _district_names.clear()
    _upazila_lookup.clear()
    _upazila_names.clear()
    
    # Load Divisions
    for div in db.query(Division).filter(Division.is_active == True).all():
        norm = _normalize(div.name)
        _division_lookup[norm] = div.name
        
    # Load Districts
    for dist in db.query(District).filter(District.is_active == True).all():
        norm = _normalize(dist.name)
        rec = {"division_name": dist.division_name, "name": dist.name}
        _district_lookup[norm] = rec
        _district_names.append(norm)
        
        # We can add a few hardcoded aliases for common misspellings here if needed
        aliases = []
        if norm == "comilla": aliases = ["cumilla", "kumilla"]
        elif norm == "chattogram": aliases = ["chittagong", "ctg"]
        elif norm == "coxsbazar": aliases = ["cox's bazar", "coxs bazar"]
        elif norm == "khagrachhari": aliases = ["khagrachari"]
        elif norm == "bogura": aliases = ["bogra"]
        elif norm == "chapainawabganj": aliases = ["chapai nawabganj", "chapai"]
        elif norm == "jashore": aliases = ["jessore"]
        elif norm == "jhenaidah": aliases = ["jhenaidaha"]
        elif norm == "jhalakathi": aliases = ["jhalokati", "jhalokathi"]
        elif norm == "barishal": aliases = ["barisal"]
        elif norm == "moulvibazar": aliases = ["maulvibazar"]
        elif norm == "shariatpur": aliases = ["shariatpur"]
        elif norm == "netrokona": aliases = ["netrakona"]

        for alias in aliases:
            norm_alias = _normalize(alias)
            _district_lookup[norm_alias] = rec
            _district_names.append(norm_alias)
            
    # Load Upazilas
    for upz in db.query(Upazila).filter(Upazila.is_active == True).all():
        norm = _normalize(upz.name)
        if norm not in _upazila_lookup:
            _upazila_lookup[norm] = []
            _upazila_names.append(norm)
        
        _upazila_lookup[norm].append({
            "division_name": upz.division_name,
            "district_name": upz.district_name,
            "name": upz.name
        })


# ─── Public API ────────────────────────────────────────────────────────────────

def fuzzy_match_location(filename: str) -> dict:
    """
    Parse a filename like 'Cumilla_Brammanpara - DC Food Comilla (1).xlsx'
    and fuzzy-match to Division/District/Upazila.

    Returns: {"division": str, "district": str, "upazila": str}
    All fields may be "Unknown" if no match found.
    """
    result = {"division": "Unknown", "district": "Unknown", "upazila": "Unknown"}

    # Strip extension
    name_no_ext = os.path.splitext(filename)[0]

    # Split on underscore to get [district_part, upazila_part, ...]
    parts = name_no_ext.split("_")
    if len(parts) < 2:
        # Try to match the whole name as district at least
        parts = [name_no_ext, ""]

    # Clean each part: remove extra suffixes like " - DC Food Comilla (1)"
    district_raw = re.split(r'\s*[-–]\s*', parts[0])[0].strip()
    upazila_raw = re.split(r'\s*[-–]\s*', parts[1])[0].strip() if len(parts) > 1 else ""

    # ── Match District ──
    district_norm = _normalize(district_raw)
    matched_district = None

    # Exact match first
    if district_norm in _district_lookup:
        matched_district = _district_lookup[district_norm]
    else:
        # Fuzzy match
        close = get_close_matches(district_norm, _district_names, n=1, cutoff=0.6)
        if close:
            matched_district = _district_lookup[close[0]]

    if matched_district:
        result["district"] = matched_district["name"]
        result["division"] = matched_district["division_name"]

    # ── Match Upazila ──
    if upazila_raw:
        upazila_norm = _normalize(upazila_raw)

        # Try exact match
        if upazila_norm in _upazila_lookup:
            candidates = _upazila_lookup[upazila_norm]
            # If we matched a district, prefer upazila from that district
            if matched_district:
                for c in candidates:
                    if c["district_name"] == matched_district["name"]:
                        result["upazila"] = c["name"]
                        break
                else:
                    result["upazila"] = candidates[0]["name"]
            else:
                result["upazila"] = candidates[0]["name"]
        else:
            # Fuzzy match
            close = get_close_matches(upazila_norm, _upazila_names, n=3, cutoff=0.6)
            if close:
                # Prefer match within the same district
                best = None
                for match_name in close:
                    for c in _upazila_lookup[match_name]:
                        if matched_district and c["district_name"] == matched_district["name"]:
                            best = c
                            break
                    if best:
                        break
                if not best:
                    best = _upazila_lookup[close[0]][0]
                result["upazila"] = best["name"]

    return result


def get_division_for_district(district_name: str) -> str:
    """Get division name for a given district name."""
    norm = _normalize(district_name)
    if norm in _district_lookup:
        return _district_lookup[norm]["division_name"]
    return "Unknown"


def get_dynamic_upazilas(db: Session = None):
    """Get upazilas from DB if possible, else fallback to memory lookup."""
    from .models import Upazila
    
    # Try fully dynamic first
    if db:
        try:
            db_upazilas = db.query(Upazila).filter(Upazila.is_active == True).all()
            if db_upazilas:
                 upazilas = {}
                 for u in db_upazilas:
                     dist_name = u.district_name
                     if dist_name not in upazilas:
                         upazilas[dist_name] = []
                     upazilas[dist_name].append(u.name)
                 for dist in upazilas:
                     upazilas[dist].sort()
                 return upazilas
        except Exception:
            pass
            
    # Fallback to in-core memory indices 
    upazilas = {}
    for upz_list in _upazila_lookup.values():
        for u in upz_list:
            dist_name = u.get("district_name", "Unknown")
            if dist_name not in upazilas:
                upazilas[dist_name] = []
            if u["name"] not in upazilas[dist_name]:
                upazilas[dist_name].append(u["name"])
    
    for dist in upazilas:
        upazilas[dist].sort()
    return upazilas
