import { useState, useEffect } from 'react';
import './App.css';
import { EventsOn } from '../wailsjs/runtime/runtime';
import { UploadForm } from './components/UploadForm';
import { ProgressPanel } from './components/ProgressPanel';
import { HistoryPanel } from './components/HistoryPanel';
import { IngestFile } from '../wailsjs/go/main/App';
import { DbStatus } from './types/events';

function App() {
  const [dbConnected, setDbConnected] = useState(true);
  const [dbError, setDbError] = useState('');
  const [isIngesting, setIsIngesting] = useState(false);
  const [refreshHistoryTrigger, setRefreshHistoryTrigger] = useState(0);

  useEffect(() => {
    // Listen to database connection status changes
    const unsubDb = EventsOn('db:status', (status: any) => {
      const parsedStatus: DbStatus = typeof status === 'string' ? JSON.parse(status) : status;
      setDbConnected(parsedStatus.connected);
      setDbError(parsedStatus.error || '');
    });

    return () => {
      unsubDb();
    };
  }, []);

  const handleIngest = async (filePath: string, dataSource: string, accountID: string) => {
    setIsIngesting(true);
    try {
      await IngestFile(filePath, dataSource, accountID);
      setRefreshHistoryTrigger((prev) => prev + 1);
    } catch (err) {
      console.error('Ingestion process finished with error or rejected:', err);
    }
  };

  const handleFinishedIngestion = () => {
    setIsIngesting(false);
    setRefreshHistoryTrigger((prev) => prev + 1);
  };

  return (
    <div className="app-container">
      <header className="app-header">
        <div className="header-left">
          <span className="logo-icon">🪐</span>
          <h1 className="app-title">Babylon Data Loader</h1>
        </div>
        <div className="header-right">
          <div className={`db-status-badge ${dbConnected ? 'connected' : 'disconnected'}`}>
            <span className="status-dot"></span>
            {dbConnected ? 'MongoDB Connected' : 'MongoDB Offline'}
          </div>
        </div>
      </header>

      <main className="app-main">
        {isIngesting ? (
          <div className="ingesting-layout">
            <ProgressPanel onFinished={handleFinishedIngestion} />
          </div>
        ) : (
          <div className="dashboard-layout">
            <div className="dashboard-left">
              <UploadForm
                dbConnected={dbConnected}
                dbError={dbError}
                onRetrySuccess={() => {
                  setDbConnected(true);
                  setDbError('');
                }}
                onIngest={handleIngest}
                isIngesting={isIngesting}
              />
            </div>
            <div className="dashboard-right">
              <HistoryPanel refreshTrigger={refreshHistoryTrigger} />
            </div>
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
