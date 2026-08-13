import { API_BASE } from '../config';

export type Scenario = '2a' | '2b' | '2c' | '2d';

export type FlowKind = 'offchain' | 'onchain' | 'mock';

export interface FlowEvent {
  step: number;
  label: string;
  kind: FlowKind;
  detail: string;
  txDigest?: string;
  explorerUrl?: string;
  participant?: string;
  wallet?: string;
}

export interface TxnCard {
  participant: string;
  kind: 'probe' | 'probe-dependent' | 'mint-guardian' | 'mint-dependent';
  recipient: string;
  tx_digest: string;
  explorer_url: string;
  status: string;
  nfts_created?: number;
}

export interface RunTotals {
  transactions: number;
  succeeded: number;
  failed: number;
  nfts_created: number;
}

export interface RunResult {
  scenario: Scenario;
  date: string;
  steps: FlowEvent[];
  transactions: TxnCard[];
  totals: RunTotals;
}

export interface HealthService {
  healthy: boolean;
  error?: string;
}

export interface HealthResponse {
  healthy: boolean;
  services: Record<string, HealthService>;
  ts: string;
}

export interface SeedGuardian {
  email: string;
  wallet: string;
}

export interface SeedDependent {
  name: string;
  guardian_email: string;
  guardian_wallet: string;
}

export interface SeedResult {
  scenario: Scenario;
  date: string;
  guardians: SeedGuardian[];
  dependents: SeedDependent[];
}

export interface WalletNFTObject {
  object_id: string;
  type: string;
  owner: string;
  version: string;
}

export interface WalletAttribution {
  section: string;
  participant: string;
  mint_date: string;
  ride_ids: string[];
}

export interface WalletView {
  address: string;
  balance_mist: string;
  has_dependents: boolean;
  guardian_name?: string;
  nft_objects: WalletNFTObject[];
  attribution: WalletAttribution[];
}

export interface ResetResult {
  truncated_tables: string[];
  redis_flushed: boolean;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((body as { error?: string }).error ?? res.statusText);
  }
  return res.json() as Promise<T>;
}

export const api = {
  health: () => request<HealthResponse>('/api/demo/health'),
  seed: (scenario: Scenario) =>
    request<SeedResult>('/api/demo/seed', {
      method: 'POST',
      body: JSON.stringify({ scenario }),
    }),
  run: (scenario: Scenario) =>
    request<RunResult>('/api/demo/run', {
      method: 'POST',
      body: JSON.stringify({ scenario }),
    }),
  reset: () => request<ResetResult>('/api/demo/reset', { method: 'POST' }),
  wallet: (address: string) =>
    request<WalletView>(`/api/demo/wallet?address=${encodeURIComponent(address)}`),
};