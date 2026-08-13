import { useMutation } from '@tanstack/react-query';
import { api, RunResult, Scenario } from '../api/demo';

interface Props {
  scenario: Scenario;
  title: string;
  description: string;
  onRun: (s: Scenario) => void;
}

function ScenarioCard({ scenario, title, description, onRun }: Props) {
  return (
    <div className="card">
      <div className="card-head">
        <span className="badge">{scenario}</span>
        <h3>{title}</h3>
      </div>
      <p className="muted">{description}</p>
      <button onClick={() => onRun(scenario)}>Run {scenario}</button>
    </div>
  );
}

export function useRunScenario() {
  return useMutation<RunResult, Error, Scenario>({
    mutationFn: (scenario) => api.run(scenario),
  });
}

export default ScenarioCard;