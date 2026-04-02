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
  created_at: string;
}
