package demo

import "testing"

func TestDepsFor(t *testing.T) {
	cases := []struct {
		s          Scenario
		guardians  int
		dependents int
		probeTx    int
	}{
		{ScenarioProbe, 9, 1, 10},
		{ScenarioGuardianMint, 10, 0, 0},
		{ScenarioDependentMint, 0, 10, 0},
		{ScenarioMixedMint, 5, 5, 0},
	}
	for _, c := range cases {
		d := depsFor(c.s)
		if d.Guardians != c.guardians || d.Dependents != c.dependents || d.ProbeTx != c.probeTx {
			t.Errorf("depsFor(%s) = %+v, want guardians=%d dependents=%d probe=%d",
				c.s, d, c.guardians, c.dependents, c.probeTx)
		}
	}
}

func TestValidScenario(t *testing.T) {
	for _, s := range AllScenarios {
		if !validScenario(s) {
			t.Errorf("validScenario(%s) = false, want true", s)
		}
	}
	if validScenario("xx") {
		t.Errorf("validScenario(xx) = true, want false")
	}
}

func TestScenarioDateIsolation(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range AllScenarios {
		d := scenarioDate(s)
		if seen[d] {
			t.Errorf("scenario date %s is not unique", d)
		}
		seen[d] = true
	}
}

func TestDeterministicHelpers(t *testing.T) {
	if guardianEmail(1) != "guardian-0001@themepark.local" {
		t.Errorf("guardianEmail(1) = %s", guardianEmail(1))
	}
	if dependentName(1) != "dependent-0001" {
		t.Errorf("dependentName(1) = %s", dependentName(1))
	}
	if rideID(1) != "ride-001" {
		t.Errorf("rideID(1) = %s", rideID(1))
	}
}
