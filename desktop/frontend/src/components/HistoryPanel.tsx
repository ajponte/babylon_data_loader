import React, { useEffect, useState } from 'react';
import { GetHistory } from '../../wailsjs/go/main/App';
import { model } from '../../wailsjs/go/models';

interface HistoryPanelProps {
  refreshTrigger?: number;
}

export const HistoryPanel: React.FC<HistoryPanelProps> = ({ refreshTrigger = 0 }) => {
  const [history, setHistory] = useState<model.SyncLog[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchHistory = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const logs = await GetHistory();
      setHistory(logs || []);
    } catch (err: any) {
      console.error('Failed to fetch history:', err);
      setError(err?.message || err || 'Failed to load sync history.');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchHistory();
  }, [refreshTrigger]);

  const formatTimestamp = (ts: any) => {
    if (!ts) return 'N/A';
    try {
      const date = new Date(ts);
      if (isNaN(date.getTime())) return String(ts);
      return date.toLocaleString();
    } catch {
      return String(ts);
    }
  };

  return (
    <div className="history-panel">
      <div className="history-header">
        <h2 className="panel-title">Ingestion History</h2>
        <button
          type="button"
          className="btn btn-secondary btn-refresh"
          onClick={fetchHistory}
          disabled={isLoading}
        >
          {isLoading ? 'Refreshing...' : 'Refresh'}
        </button>
      </div>

      {error && <div className="history-error-alert">{error}</div>}

      {isLoading ? (
        <div className="history-loading">
          <div className="loading-dots">
            <span></span>
            <span></span>
            <span></span>
          </div>
          <p>Loading sync logs...</p>
        </div>
      ) : history.length === 0 ? (
        <div className="history-empty">
          <span className="empty-icon">📂</span>
          <p>No historical runs found.</p>
        </div>
      ) : (
        <div className="history-table-container">
          <table className="history-table">
            <thead>
              <tr>
                <th>Collection Ingested</th>
                <th>Records Ingested</th>
                <th>Ingestion Timestamp</th>
              </tr>
            </thead>
            <tbody>
              {history.map((log, index) => (
                <tr key={index}>
                  <td className="col-name">{log.collectionName}</td>
                  <td className="records-count">{String(log.recordsUploaded)}</td>
                  <td className="timestamp">{formatTimestamp(log.syncTimestamp)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};
