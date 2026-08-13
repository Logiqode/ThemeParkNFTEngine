import { useState } from 'react';
import ScenarioCard, { useRunScenario } from '../components/ScenarioCard';
import FlowVisualizer from '../components/FlowVisualizer';
import TransactionTable from '../components/TransactionTable';
import { Scenario } from '../api/demo';

const scenarios = [
  {
    scenario: '2a' as Scenario,
    title: 'NFC binding + wallet probe',
    description:
      '10 sponsored SUI transfers from the gas pool to derived wallets, proving each address is live on-chain.',
  },
  {
    scenario: '2b' as Scenario,
    title: 'Batch mint — guardians only',
    description:
      '10 guardians each take rides; 10 batch-mint transactions land in their own non-custodial wallets.',
  },
  {
    scenario: '2c' as Scenario,
    title: 'Batch mint — dependents only',
    description:
      '10 dependents ride all day; their NFTs mint into the guardian custodial wallet (no dependent wallet).',
  },
  {
    scenario: '2d' as Scenario,
    title: 'Batch mint — mixed day',
    description:
      '5 guardian + 5 dependent mints, randomized order, simulating a real family-heavy park day.',
  },
];

export default function Dashboard() {
  const run = useRunScenario();
  const [lastScenario, setLastScenario] = useState<Scenario | null>(null);

  const onRun = (s: Scenario) => {
    setLastScenario(s);
    run.mutate(s);
  };

  return (
    <div className="page">
      <h1>Dashboard</h1>
      <p className="muted">
        Run a scenario to watch the theme-park → on-chain flow. Each scenario
        fires exactly 10 real Sui-testnet transactions.
      </p>

      <div className="grid">
        {scenarios.map((s) => (
          <ScenarioCard key={s.scenario} {...s} onRun={onRun} />
        ))}
      </div>

      {run.isPending && (
        <div className="panel">
          <p>Running {run.variables}…</p>
        </div>
      )}

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
              <strong>{run.data.totals.nfts_created}</strong> NFTs
            </span>
          </div>
          <h2>
            Scenario {lastScenario} · {run.data.date}
          </h2>
          <FlowVisualizer steps={run.data.steps} />
          <h2>Transactions</h2>
          <TransactionTable cards={run.data.transactions} />
        </div>
      )}
    </div>
  );
}