import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, ResetResult } from '../api/demo';

export default function Reset() {
  const queryClient = useQueryClient();
  const [confirmed, setConfirmed] = useState(false);
  const reset = useMutation<ResetResult, Error, void>({
    mutationFn: () => api.reset(),
    onSuccess: () => {
      queryClient.invalidateQueries();
      setConfirmed(false);
    },
  });

  return (
    <div className="page">
      <h1>Reset</h1>
      <p className="muted">
        Wipes all demo data: truncates the application tables and flushes Redis.
        Kafka topics are left intact (the consumer dedups by trace_id). This is
        the only action that resets the stack — scenario runs are additive.
      </p>

      <div className="panel warning">
        <label className="confirm">
          <input
            type="checkbox"
            checked={confirmed}
            onChange={(e) => setConfirmed(e.target.checked)}
          />
          I understand this clears all seeded participants, rides, and mint logs.
        </label>
        <button
          className="danger-btn"
          disabled={!confirmed || reset.isPending}
          onClick={() => reset.mutate()}
        >
          {reset.isPending ? 'Resetting…' : 'Reset full stack data'}
        </button>
      </div>

      {reset.isError && (
        <div className="panel danger">
          <p>Reset failed: {String(reset.error)}</p>
        </div>
      )}

      {reset.isSuccess && (
        <div className="panel success">
          <h2>Reset complete</h2>
          <p>Truncated tables: {reset.data.truncated_tables.join(', ')}.</p>
          <p>Redis flushed: {reset.data.redis_flushed ? 'yes' : 'no'}.</p>
        </div>
      )}
    </div>
  );
}