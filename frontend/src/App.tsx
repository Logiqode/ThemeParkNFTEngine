import { NavLink, Route, Routes } from 'react-router-dom';
import HealthGate from './components/HealthGate';
import Dashboard from './pages/Dashboard';
import ScenarioRunner from './pages/ScenarioRunner';
import WalletViewer from './pages/WalletViewer';
import Reset from './pages/Reset';

const navItems = [
  { to: '/', label: 'Dashboard', end: true },
  { to: '/tests', label: 'Tests' },
  { to: '/wallet', label: 'Wallet' },
  { to: '/reset', label: 'Reset' },
];

export default function App() {
  return (
    <div className="shell">
      <header className="topnav">
        <div className="brand">
          <span className="brand-mark">🎢</span>
          <span>ThemePark NFT · Test Runner</span>
        </div>
        <nav>
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) => (isActive ? 'nav active' : 'nav')}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </header>

      <main className="content">
        <HealthGate>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/tests" element={<ScenarioRunner />} />
            <Route path="/wallet" element={<WalletViewer />} />
            <Route path="/reset" element={<Reset />} />
          </Routes>
        </HealthGate>
      </main>
    </div>
  );
}