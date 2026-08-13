import { TxnCard } from '../api/demo';
import { txUrl } from '../config';

export default function TransactionTable({ cards }: { cards: TxnCard[] }) {
  if (!cards.length) return null;
  return (
    <table className="tx-table">
      <thead>
        <tr>
          <th>Participant</th>
          <th>Kind</th>
          <th>Recipient</th>
          <th>Tx digest</th>
          <th>Status</th>
          <th>NFTs</th>
        </tr>
      </thead>
      <tbody>
        {cards.map((c, i) => (
          <tr key={`${c.tx_digest || 'pending'}-${i}`}>
            <td>{c.participant}</td>
            <td>{c.kind}</td>
            <td>
              <code className="truncate-cell">{c.recipient}</code>
            </td>
            <td>
              {c.tx_digest ? (
                <a href={txUrl(c.tx_digest)} target="_blank" rel="noreferrer" className="txlink">
                  {c.tx_digest}
                </a>
              ) : (
                '—'
              )}
            </td>
            <td>
              <span className={c.status === 'success' ? 'dot ok' : 'dot bad'} />
              {c.status}
            </td>
            <td>{c.nfts_created ?? ''}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}