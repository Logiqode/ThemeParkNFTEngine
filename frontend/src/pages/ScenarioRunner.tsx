import { useState } from 'react';
import FlowVisualizer from '../components/FlowVisualizer';
import TransactionTable from '../components/TransactionTable';
import { useRunScenario } from '../components/ScenarioCard';
import { Scenario } from '../api/demo';

export default function ScenarioRunner() {
  const run = useRunScenario();
  const [selected, setSelected] = useState<Scenario>('2a');

  return (
    <div className="page">
      <h1>Scenario Runner</h1>
      <p className="muted">
        Select a scenario and run it. Results show the ordered flow (off-chain ·
        mock · on-chain) plus the on-chain transaction table with Suiscan links.
      </p>

      <div className="selector">
        {(['2a', '2b', '2c', '2d'] as Scenario[]).map((s) => (
          <button
            key={s}
            onClick={() => setSelected(s)}
            className={selected === s ? 'pill active' : 'pill'}
          >
            {s}
          </button>
        ))}
        <button
          className="run"
          onClick={() => run.mutate(selected)}
          disabled={run.isPending}
        >
          {run.isPending ? 'Running…' : `Run ${selected}`}
        </button>
      </div>

      {run.isError && (
        <div className="panel danger">
          <p>Run failed: {String(run.error)}</p>
        </div>
      )}

      {run.isSuccess && run.data && (
        <div className="results">
          <div className="totals">
            <span>
              <strong>{run.data.totals.transactions}</strong> txs
            </span>
            <span>
              <strong>{run.data.totals.succeeded}</strong> succeeded
            </span>
            <span>
              <strong>{run.data.totals.failed}</strong> failed
            </span>
            <span>
              <strong>{run.data.totals.nfts_created}</strong> NFTs
            </span>
          </div>
          <h2>
            Scenario {run.data.scenario} · {run.data.date}
          </h2>
          <FlowVisualizer steps={run.data.steps} />
          <h2>Transactions</h2>
          <TransactionTable cards={run.data.transactions} />
        </div>
      )}
    </div>
  );
}