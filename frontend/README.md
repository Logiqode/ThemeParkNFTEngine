# Frontend Test Runner

React 18 + Vite + TypeScript UI for the ThemeParkNFT demo orchestrator. It lets a
non-Web3 visitor watch the park → on-chain flow: run the four scenarios (2a–2d),
see the ordered flow events colour-coded by off-chain / mock / on-chain, follow
tx digests to Suiscan, view wallet objects + attribution, and reset the stack.

## Prerequisites

- Node 18+ and npm
- Backend stack up: `make up && make healthy` from the repo root
- Demo orchestrator running: `make demo` (from repo root, port `:8090`)

## Run

```bash
npm install
npm run dev
```

Open http://localhost:5173. The Vite dev server proxies `/api/demo` to
`http://localhost:8090`.

## Production build

```bash
npm run build   # type-check (tsc) + vite build → dist/
npm run preview # serve the built bundle
```

## Configuration

Environment variables (Vite `import.meta.env`):

- `VITE_API_BASE` — override the demo API base URL (default: same-origin
  `/api/demo` through the dev proxy).

Suiscan testnet explorer URLs are defined in `src/config.ts`.