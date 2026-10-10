import { createRoot } from 'react-dom/client';
import App from './App.jsx';
import { ClusterProvider } from './lib/cluster.jsx';
import './styles.css';
import './styles-patterns.css';

createRoot(document.getElementById('root')).render(
  <ClusterProvider>
    <App />
  </ClusterProvider>
);
