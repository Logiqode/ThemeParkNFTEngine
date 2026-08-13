// Frontend configuration. The API base defaults to the Vite dev-proxy
// (same-origin `/api/demo`), which forwards to the demo orchestrator on :8090.
// Override with VITE_API_BASE for a direct URL (e.g. behind Docker).
export const API_BASE: string =
  import.meta.env.VITE_API_BASE ?? '';

export const SUI_TESTNET_EXPLORER = 'https://suiscan.xyz/testnet';

export const txUrl = (digest: string): string =>
  `${SUI_TESTNET_EXPLORER}/tx/${digest}`;

export const objectUrl = (objectId: string): string =>
  `${SUI_TESTNET_EXPLORER}/object/${objectId}`;