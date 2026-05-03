"use client";
import React, { useState, useEffect } from "react";
import { MapPin, ChevronDown } from "lucide-react";
import { fetchWithAuth } from "@/lib/auth";
import type { User } from "@/types/ffp";

interface GeoInfo {
  divisions: string[];
  districts: Record<string, string[]>;   // division → districts
  upazilas: Record<string, string[]>;    // district → upazilas
}

interface GeoFilterBarProps {
  user: User | null;
  onChange: (division: string, district: string, upazila: string) => void;
  initialValues?: { division?: string; district?: string; upazila?: string };
  className?: string;
}

/**
 * Permission-aware three-level geo filter.
 * - Admin: all three dropdowns free selection
 * - Division user: division locked; district + upazila selectable (within division)
 * - District user: division + district locked; upazila selectable (within district)
 * - Upazila user: all three locked (pre-filled + disabled)
 */
export default function GeoFilterBar({ user, onChange, initialValues, className }: GeoFilterBarProps) {
  const [geoInfo, setGeoInfo] = useState<GeoInfo>({ divisions: [], districts: {}, upazilas: {} });
  const [division, setDivision] = useState(initialValues?.division || user?.division_access || "");
  const [district, setDistrict] = useState(initialValues?.district || user?.district_access || "");
  const [upazila, setUpazila]   = useState(initialValues?.upazila || user?.upazila_access || "");
  const [loading, setLoading]   = useState(true);

  // Compute lock state
  const divLocked  = !!user?.upazila_access || !!user?.district_access || !!user?.division_access;
  const distLocked = !!user?.upazila_access || !!user?.district_access;
  const upzLocked  = !!user?.upazila_access;

  useEffect(() => {
    const load = async () => {
      try {
        const res = await fetchWithAuth("/api/geo/info");
        if (!res.ok) return;
        const raw = await res.json();

        // Build lookup maps from the API response
        const divs: string[] = [];
        const dists: Record<string, string[]> = {};
        const upzs: Record<string, string[]>  = {};

        (raw.divisions || []).forEach((divStr: string) => {
          // Filter by user access
          if (user?.division_access && divStr !== user.division_access) return;
          divs.push(divStr);
          dists[divStr] = [];
          (raw.districts[divStr] || []).forEach((distStr: string) => {
            if (user?.district_access && distStr !== user.district_access) return;
            dists[divStr].push(distStr);
            upzs[distStr] = [];
            (raw.upazilas[distStr] || []).forEach((upzStr: string) => {
              if (user?.upazila_access && upzStr !== user.upazila_access) return;
              upzs[distStr].push(upzStr);
            });
          });
        });

        setGeoInfo({ divisions: divs, districts: dists, upazilas: upzs });
      } catch (e) { /* silent */ }
      finally { setLoading(false); }
    };
    load();
  }, [user]);

  // Auto-select when locked
  useEffect(() => {
    if (geoInfo.divisions.length === 1 && !division) {
      setDivision(geoInfo.divisions[0]);
    }
  }, [geoInfo.divisions, division]);

  useEffect(() => {
    if (division) {
      const dists = geoInfo.districts[division] || [];
      if (dists.length === 1 && !district) setDistrict(dists[0]);
    }
  }, [division, geoInfo, district]);

  useEffect(() => {
    if (district) {
      const upzs = geoInfo.upazilas[district] || [];
      if (upzs.length === 1 && !upazila) setUpazila(upzs[0]);
    }
  }, [district, geoInfo, upazila]);

  const handleDivChange = (val: string) => {
    setDivision(val);
    setDistrict("");
    setUpazila("");
    onChange(val, "", "");
  };

  const handleDistChange = (val: string) => {
    setDistrict(val);
    setUpazila("");
    onChange(division, val, "");
  };

  const handleUpzChange = (val: string) => {
    setUpazila(val);
    onChange(division, district, val);
  };

  const selectClass = (locked: boolean) =>
    `flex-1 appearance-none px-3 py-2 rounded-lg text-sm border transition-all outline-none [&>option]:bg-slate-900 [&>option]:text-slate-200
     ${locked
       ? "bg-slate-800/80 border-slate-700 text-slate-400 cursor-not-allowed opacity-70"
       : "bg-slate-900/60 border-slate-600 text-slate-200 hover:border-indigo-500/60 focus:border-indigo-500"}`;

  const availableDistricts = division ? (geoInfo.districts[division] || []) : [];
  const availableUpazilas  = district ? (geoInfo.upazilas[district]  || []) : [];

  return (
    <div className={`flex flex-wrap items-center gap-3 ${className || ""}`}>
      <div className="flex items-center gap-2 text-slate-400 shrink-0">
        <MapPin className="w-4 h-4 text-indigo-400" />
        <span className="text-sm font-medium text-slate-300">Filter by Location</span>
      </div>

      {/* Division */}
      <div className="relative flex-1 min-w-[160px]">
        <select
          value={division}
          onChange={e => handleDivChange(e.target.value)}
          disabled={divLocked || loading}
          className={selectClass(divLocked)}
          id="geo-filter-division"
        >
          <option value="">All Divisions</option>
          {geoInfo.divisions.map(d => <option key={d} value={d}>{d}</option>)}
        </select>
        {!divLocked && <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500 pointer-events-none" />}
      </div>

      {/* District */}
      <div className="relative flex-1 min-w-[160px]">
        <select
          value={district}
          onChange={e => handleDistChange(e.target.value)}
          disabled={distLocked || !division || loading}
          className={selectClass(distLocked)}
          id="geo-filter-district"
        >
          <option value="">All Districts</option>
          {availableDistricts.map(d => <option key={d} value={d}>{d}</option>)}
        </select>
        {!distLocked && <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500 pointer-events-none" />}
      </div>

      {/* Upazila */}
      <div className="relative flex-1 min-w-[160px]">
        <select
          value={upazila}
          onChange={e => handleUpzChange(e.target.value)}
          disabled={upzLocked || !district || loading}
          className={selectClass(upzLocked)}
          id="geo-filter-upazila"
        >
          <option value="">All Upazilas</option>
          {availableUpazilas.map(u => <option key={u} value={u}>{u}</option>)}
        </select>
        {!upzLocked && <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500 pointer-events-none" />}
      </div>
    </div>
  );
}
