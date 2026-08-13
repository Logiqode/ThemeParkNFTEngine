import { WalletView } from '../api/demo';
import { objectUrl } from '../config';

export default function WalletObjects({ view }: { view: WalletView }) {
  const mistToSui = (mist: string): string => {
    const n = Number(mist);
    if (!mist || Number.isNaN(n)) return '—';
    return (n / 1_000_000_000).toFixed(4);
  };

  return (
    <div className="wallet">
      <div className="wallet-summary">
        <div>
          <label className="muted">Address</label>
          <code className="truncate">{view.address}</code>
        </div>
        <div>
          <label className="muted">Balance</label>
          <span>{mistToSui(view.balance_mist)} SUI</span>
        </div>
        <div>
          <label className="muted">Guardian</label>
          <span>{view.guardian_name ?? '—'}</span>
        </div>
      </div>

      {view.has_dependents && (
        <p className="callout">
          This wallet hosts dependents — their NFTs share the guardian address and
          are attributed off-chain (grouped below).
        </p>
      )}

      <h3>Attribution</h3>
      {view.attribution.length === 0 && <p className="muted">No mint attribution yet.</p>}
      {view.attribution.map((a, i) => (
        <div key={`${a.participant}-${a.mint_date}-${i}`} className="attribution-row">
          <span className="badge">{a.section === 'guardian' ? 'guardian' : 'dependent'}</span>
          <strong>{a.participant}</strong>
          <span className="muted">{a.mint_date}</span>
          <span className="rides">rides: {a.ride_ids.join(', ')}</span>
        </div>
      ))}

      <h3>On-chain NFT objects ({view.nft_objects.length})</h3>
      {view.nft_objects.length === 0 && (
        <p className="muted">No AttendanceNFT objects owned yet.</p>
      )}
      <ul className="objects">
        {view.nft_objects.map((o) => (
          <li key={o.object_id}>
            <a href={objectUrl(o.object_id)} target="_blank" rel="noreferrer" className="txlink">
              {o.object_id}
            </a>
            <span className="muted">v{o.version}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}