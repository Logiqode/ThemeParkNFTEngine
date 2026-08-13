import { ReactNode } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, HealthResponse } from '../api/demo';

export default function HealthGate({ children }: { children: ReactNode }) {
  const { data, isLoading, isError, error } = useQuery<HealthResponse>({
    queryKey: ['health'],
    queryFn: api.health,
    refetchInterval: 10000,
  });

  if (isLoading) {
    return <div className="panel">Checking backend health…</div>;
  }

  if (isError) {
    return (
      <div className="panel danger">
        <h2>Backend unreachable</h2>
        <p>{String(error)}</p>
        <p>
          Make sure the demo orchestrator is running on <code>:8090</code> and the
          Sui/PG/Kafka/Redis stack is up (<code>make up && make healthy</code>).
        </p>
      </div>
    );
  }

  if (!data?.healthy) {
    return (
      <div className="panel warning">
        <h2>Services not ready</h2>
        <ul>
          {Object.entries(data?.services ?? {}).map(([name, s]) => (
            <li key={name}>
              <span className={s.healthy ? 'dot ok' : 'dot bad'} /> {name}
              {s.healthy ? '' : ` — ${s.error}`}
            </li>
          ))}
        </ul>
        <p>Test-run buttons are disabled until every service is healthy.</p>
      </div>
    );
  }

  return <>{children}</>;
}