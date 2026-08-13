import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, WalletView } from '../api/demo';
import WalletObjects from '../components/WalletObjects';

export default function WalletViewer() {
  const [address, setAddress] = useState('');
  const [query, setQuery] = useState('');
  const { data, isFetching, isError, error } = useQuery<WalletView>({
    queryKey: ['wallet', query],
    queryFn: () => api.wallet(query),
    enabled: query.length > 0,
  });

  return (
    <div className="page">
      <h1>Wallet Viewer</h1>
      <p className="muted">
        Paste a Sui address to see its owned AttendanceNFT objects and off-chain
        attribution (guardian own vs each dependent).
      </p>

      <form
        className="wallet-form"
        onSubmit={(e) => {
          e.preventDefault();
          setQuery(address.trim());
        }}
      >
        <input
          value={address}
          onChange={(e) => setAddress(e.target.value)}
          placeholder="0x…"
          spellCheck={false}
        />
        <button type="submit" disabled={isFetching}>
          {isFetching ? 'Loading…' : 'Look up'}
        </button>
      </form>

      {isError && (
        <div className="panel danger">
          <p>Wallet lookup failed: {String(error)}</p>
        </div>
      )}

      {data && <WalletObjects view={data} />}
    </div>
  );
}