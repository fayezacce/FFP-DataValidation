"""
Statistics Store — JSON file-based persistence for validation results.
Author: Fayez Ahmed

Stores validation stats keyed by district+upazila with version tracking.
Each entry records: division, district, upazila, total, valid, invalid,
version number, and timestamps (created_at, updated_at).
"""

import json
import os
import threading
from datetime import datetime

STATS_FILE = os.path.join("downloads", "validation_stats.json")
_lock = threading.Lock()


def _load() -> dict:
    """Load stats from JSON file."""
    if not os.path.exists(STATS_FILE):
        return {"entries": {}}
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {"entries": {}}


def _save(data: dict):
    """Save stats to JSON file."""
    os.makedirs(os.path.dirname(STATS_FILE), exist_ok=True)
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _make_key(district: str, upazila: str) -> str:
    """Create a unique key from district+upazila."""
    return f"{district.lower().strip()}|{upazila.lower().strip()}"


def upsert_stats(
    division: str,
    district: str,
    upazila: str,
    total: int,
    valid: int,
    invalid: int,
    filename: str,
    pdf_url: str = "",
    excel_url: str = "",
    excel_valid_url: str = "",
    excel_invalid_url: str = "",
) -> dict:
    """
    Insert or update stats for a district+upazila.
    If an entry already exists, it increments the version and updates timestamps.
    Returns the updated entry.
    """
    with _lock:
        data = _load()
        key = _make_key(district, upazila)
        now = datetime.utcnow().isoformat() + "Z"

        existing = data["entries"].get(key)
        if existing:
            version = existing.get("version", 1) + 1
            created_at = existing.get("created_at", now)
        else:
            version = 1
            created_at = now

        entry = {
            "division": division,
            "district": district,
            "upazila": upazila,
            "total": total,
            "valid": valid,
            "invalid": invalid,
            "filename": filename,
            "version": version,
            "created_at": created_at,
            "updated_at": now,
            "pdf_url": pdf_url,
            "excel_url": excel_url,
            "excel_valid_url": excel_valid_url,
            "excel_invalid_url": excel_invalid_url,
        }
        data["entries"][key] = entry
        _save(data)
        return entry

from .bd_geo import get_division_for_district

def update_stats_manual(old_district: str, old_upazila: str, new_district: str, new_upazila: str, total: int, valid: int, invalid: int) -> dict:
    """Manually update the counts and location for an existing entry."""
    with _lock:
        data = _load()
        old_key = _make_key(old_district, old_upazila)
        if old_key not in data.get("entries", {}):
            raise ValueError(f"No statistics found for {old_district} - {old_upazila}")
            
        now = datetime.utcnow().isoformat() + "Z"
        entry = data["entries"].pop(old_key)
        
        entry["district"] = new_district
        entry["upazila"] = new_upazila
        entry["division"] = get_division_for_district(new_district)
        
        entry["total"] = total
        entry["valid"] = valid
        entry["invalid"] = invalid
        entry["version"] = entry.get("version", 1) + 1
        entry["updated_at"] = now
        
        new_key = _make_key(new_district, new_upazila)
        data["entries"][new_key] = entry
        
        _save(data)
        return entry

def get_all_stats() -> list[dict]:
    """Return all stats entries as a sorted list (by division, district, upazila)."""
    data = _load()
    entries = list(data.get("entries", {}).values())
    entries.sort(key=lambda e: (e.get("division", ""), e.get("district", ""), e.get("upazila", "")))
    return entries


def get_grand_total(entries: list[dict] = None) -> dict:
    """Calculate grand totals across all entries."""
    if entries is None:
        entries = get_all_stats()
    return {
        "total": sum(e.get("total", 0) for e in entries),
        "valid": sum(e.get("valid", 0) for e in entries),
        "invalid": sum(e.get("invalid", 0) for e in entries),
    }
