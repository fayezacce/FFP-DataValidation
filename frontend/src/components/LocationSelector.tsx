import React, { useState, useMemo, useEffect, useRef } from 'react';

interface Upazila {
  id: number;
  division_name: string;
  district_name: string;
  name: string;
}

interface LocationSelectorProps {
  upazilas: Upazila[];
  selectedDivision: string | null;
  selectedDistrict: string | null;
  selectedUpazila: string | null;
  onSelect: (selection: { division: string | null; district: string | null; upazila: string | null }) => void;
  disabled?: boolean;
}

const LocationSelector: React.FC<LocationSelectorProps> = ({
  upazilas,
  selectedDivision,
  selectedDistrict,
  selectedUpazila,
  onSelect,
  disabled = false
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const containerRef = useRef<HTMLDivElement>(null);

  // Build the hierarchical options
  const options = useMemo(() => {
    const list: { label: string; division: string | null; district: string | null; upazila: string | null; path: string; level: 'global' | 'division' | 'district' | 'upazila' }[] = [];
    
    // 1. Root: Nationwide
    list.push({
      label: 'Nationwide (Full Access)',
      division: null,
      district: null,
      upazila: null,
      path: 'Nationwide',
      level: 'global'
    });

    // Group by Division
    const byDiv: Record<string, Record<string, string[]>> = {};
    upazilas.forEach(u => {
      if (!byDiv[u.division_name]) byDiv[u.division_name] = {};
      if (!byDiv[u.division_name][u.district_name]) byDiv[u.division_name][u.district_name] = [];
      byDiv[u.division_name][u.district_name].push(u.name);
    });

    // 2. Add Divisions, Districts, Upazilas
    Object.keys(byDiv).sort().forEach(div => {
      list.push({
        label: `RCF: ${div}`,
        division: div,
        district: null,
        upazila: null,
        path: div,
        level: 'division'
      });

      Object.keys(byDiv[div]).sort().forEach(dist => {
        list.push({
          label: `DCF: ${dist}`,
          division: div,
          district: dist,
          upazila: null,
          path: `${div} > ${dist}`,
          level: 'district'
        });

        byDiv[div][dist].sort().forEach(upz => {
          list.push({
            label: `UCF: ${upz}`,
            division: div,
            district: dist,
            upazila: upz,
            path: `${div} > ${dist} > ${upz}`,
            level: 'upazila'
          });
        });
      });
    });

    return list;
  }, [upazilas]);

  const filteredOptions = useMemo(() => {
    if (!searchTerm) return options;
    const lower = searchTerm.toLowerCase();
    return options.filter(opt => 
      opt.label.toLowerCase().includes(lower) || 
      opt.path.toLowerCase().includes(lower)
    );
  }, [options, searchTerm]);

  const currentSelection = useMemo(() => {
    if (!selectedDivision && !selectedDistrict && !selectedUpazila) return options[0];
    return options.find(opt => 
      opt.division === selectedDivision && 
      opt.district === selectedDistrict && 
      opt.upazila === selectedUpazila
    ) || { label: 'Unknown Location', path: 'Selection Mismatch' };
  }, [selectedDivision, selectedDistrict, selectedUpazila, options]);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  return (
    <div className="relative w-full" ref={containerRef}>
      <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Hierarchical Location Access</label>
      
      <div 
        onClick={() => !disabled && setIsOpen(!isOpen)}
        className={`w-full px-4 py-3 rounded-xl bg-[#1a1a1c] border transition-all cursor-pointer flex items-center justify-between ${
          isOpen ? 'border-emerald-500 ring-1 ring-emerald-500/20' : 'border-[#2a2a2e] hover:border-gray-600'
        } ${disabled ? 'opacity-50 cursor-not-allowed' : ''}`}
      >
        <div className="flex flex-col">
          <span className="text-sm font-medium text-white">{currentSelection.label}</span>
          <span className="text-[10px] text-gray-500 uppercase tracking-wider">{currentSelection.path}</span>
        </div>
        <svg className={`w-4 h-4 text-gray-500 transition-transform ${isOpen ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </div>

      {isOpen && (
        <div className="absolute z-50 w-full mt-2 bg-[#1a1a1c] border border-[#2a2a2e] rounded-2xl shadow-2xl overflow-hidden animate-in fade-in slide-in-from-top-2 duration-200">
          <div className="p-3 border-b border-[#2a2a2e]">
            <input
              autoFocus
              type="text"
              placeholder="Search Division, District or Upazila..."
              className="w-full bg-[#121214] border border-[#2a2a2e] rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-emerald-500"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              onClick={(e) => e.stopPropagation()}
            />
          </div>
          
          <div className="max-h-64 overflow-y-auto custom-scrollbar">
            {filteredOptions.length > 0 ? (
              filteredOptions.map((opt, i) => (
                <div
                  key={i}
                  className={`px-4 py-3 cursor-pointer hover:bg-emerald-500/5 transition-colors border-l-2 ${
                    opt.division === selectedDivision && opt.district === selectedDistrict && opt.upazila === selectedUpazila
                    ? 'bg-emerald-500/10 border-emerald-500'
                    : 'border-transparent hover:border-emerald-500/50'
                  }`}
                  onClick={() => {
                    onSelect({ division: opt.division, district: opt.district, upazila: opt.upazila });
                    setIsOpen(false);
                    setSearchTerm('');
                  }}
                >
                  <div className="flex flex-col">
                    <span className={`text-sm ${opt.level === 'global' ? 'text-emerald-400 font-bold' : 'text-gray-200'}`}>
                      {opt.label}
                    </span>
                    <span className="text-[10px] text-gray-500 uppercase">{opt.path}</span>
                  </div>
                </div>
              ))
            ) : (
              <div className="p-4 text-center text-gray-500 text-sm">No locations found.</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default LocationSelector;
