import { FlowEvent, FlowKind } from '../api/demo';

const kindStyles: Record<FlowKind, { icon: string; cls: string }> = {
  offchain: { icon: '🟢', cls: 'step-offchain' },
  onchain: { icon: '⛓️', cls: 'step-onchain' },
  mock: { icon: '🔵', cls: 'step-mock' },
};

export default function FlowVisualizer({ steps }: { steps: FlowEvent[] }) {
  return (
    <ol className="flow">
      {steps.map((s) => {
        const style = kindStyles[s.kind] ?? kindStyles.mock;
        return (
          <li key={s.step} className={`flow-step ${style.cls}`}>
            <span className="step-icon">{style.icon}</span>
            <div className="step-body">
              <div className="step-label">
                {s.label}
                <span className="step-kind">{s.kind}</span>
              </div>
              <div className="step-detail muted">{s.detail}</div>
              {s.participant && (
                <div className="step-meta">
                  <span>{s.participant}</span>
                  {s.wallet && <code className="truncate">{s.wallet}</code>}
                </div>
              )}
              {s.txDigest && (
                <a
                  href={s.explorerUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="txlink"
                >
                  {s.txDigest}
                </a>
              )}
            </div>
          </li>
        );
      })}
    </ol>
  );
}