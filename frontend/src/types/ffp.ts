/**
 * FFP Data Validation Platform - Shared Type Definitions
 */

export interface StatsEntry {
  id: number;
  division: string;
  district: string;
  upazila: string;
  total: number;
  valid: number;
  invalid: number;
  quota: number;
  filename: string;
  version: number;
  created_at: string;
  updated_at: string;
  pdf_url?: string;
  pdf_invalid_url?: string;
  excel_url?: string;
  excel_valid_url?: string;
  excel_invalid_url?: string;
}

export interface StatsResponse {
  entries: StatsEntry[];
  grand_total: {
    total: number;
    valid: number;
    invalid: number;
  };
  master_counts: {
    divisions: Record<string, number>;
    districts: Record<string, number>;
  };
}

export interface Batch {
  id: number;
  filename: string;
  uploader_id: number;
  username: string;
  total_rows: number;
  valid_count: number;
  invalid_count: number;
  new_records: number;
  updated_records: number;
  created_at: string;
  status: string;
  valid_url?: string;
  invalid_url?: string;
  pdf_url?: string;
  pdf_invalid_url?: string;
}

export interface User {
  id: number;
  username: string;
  role: 'admin' | 'uploader' | 'viewer';
  is_active: boolean;
  api_key?: string;
  api_rate_limit?: number;
  api_total_limit?: number;
  api_ip_whitelist?: string;
  api_usage_count?: number;
  api_key_last_used?: string;
  division_access?: string;
  district_access?: string;
  upazila_access?: string;
  created_at: string;
}

export interface BeneficiaryRecord {
  id: number;
  nid: string;
  dob: string;
  name: string;
  card_no?: string;
  mobile?: string;
  division: string;
  district: string;
  upazila: string;
  father_husband_name?: string;
  name_bn?: string;
  name_en?: string;
  ward?: string;
  union_name?: string;
  dealer_id?: number;
  dealer_name?: string;
  dealer_nid?: string;
  verification_status: 'verified' | 'unverified';
  verified_by?: string;
  verified_by_id?: number;
  verified_at?: string;
  created_at: string;
  updated_at: string;
  // Detail view only:
  extended_fields?: Record<string, string | null>;
  raw_data?: Record<string, unknown>;
}

export interface InvalidBeneficiaryRecord {
  id: number;
  nid: string;
  dob?: string;
  name?: string;
  card_no?: string;
  mobile?: string;
  division: string;
  district: string;
  upazila: string;
  error_message: string;
  created_at: string;
}

export interface DealerRecord {
  id: number;
  nid: string;
  name: string;
  mobile?: string;
  division: string;
  district: string;
  upazila: string;
  upazila_id?: number;
  beneficiary_count: number;
  cross_upazila_warning: boolean;
  is_active: boolean;
}

export interface BeneficiaryListResponse {
  records: BeneficiaryRecord[];
  total: number;
  page: number;
  page_size: number;
}

export interface DealerListResponse {
  dealers: DealerRecord[];
  total: number;
  page: number;
  page_size: number;
}
