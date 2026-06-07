import React, { useState } from 'react';
import { RetryConnectDB } from '../../wailsjs/go/main/App';

interface UploadFormProps {
  dbConnected: boolean;
  dbError?: string;
  onRetrySuccess: () => void;
  onIngest: (filePath: string, dataSource: string, accountID: string) => void;
  isIngesting: boolean;
}

export const UploadForm: React.FC<UploadFormProps> = ({
  dbConnected,
  dbError,
  onRetrySuccess,
  onIngest,
  isIngesting,
}) => {
  const [filePath, setFilePath] = useState('');
  const [dataSource, setDataSource] = useState('chase');
  const [accountID, setAccountID] = useState('');
  const [errors, setErrors] = useState<{ filePath?: string; accountID?: string }>({});

  const handleSelectFile = async () => {
    try {
      const runtime = (window as any).runtime;
      if (runtime && typeof runtime.OpenFileDialog === 'function') {
        const selected = await runtime.OpenFileDialog({
          Title: 'Select CSV Statement File',
          Filters: [{ Name: 'CSV Files (*.csv)', Pattern: '*.csv' }],
        });
        if (selected) {
          setFilePath(selected);
          setErrors((prev) => ({ ...prev, filePath: undefined }));
        }
      } else {
        console.warn('Wails runtime not found, using prompt for mockup');
        const path = prompt('Enter mock file path:');
        if (path) {
          setFilePath(path);
          setErrors((prev) => ({ ...prev, filePath: undefined }));
        }
      }
    } catch (err) {
      console.error('Failed to open file dialog:', err);
    }
  };

  const handleAccountIdChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value;
    // Allow only digits
    if (/^\d*$/.test(val)) {
      setAccountID(val);
      setErrors((prev) => ({ ...prev, accountID: undefined }));
    }
  };

  const handleRetryConnection = async () => {
    const success = await RetryConnectDB();
    if (success) {
      onRetrySuccess();
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const newErrors: { filePath?: string; accountID?: string } = {};

    if (!filePath) {
      newErrors.filePath = 'File path is required';
    }
    if (!accountID) {
      newErrors.accountID = 'Account ID is required';
    } else if (accountID.length !== 4) {
      newErrors.accountID = 'Account ID must be exactly 4 digits';
    }

    if (Object.keys(newErrors).length > 0) {
      setErrors(newErrors);
      return;
    }

    onIngest(filePath, dataSource, accountID);
  };

  return (
    <div className="upload-form-container">
      {!dbConnected && (
        <div className="db-status-banner error" role="alert">
          <div className="banner-content">
            <span className="banner-icon">⚠️</span>
            <div className="banner-text">
              <strong>Database Disconnected:</strong> {dbError || 'Failed to connect to MongoDB.'}
            </div>
          </div>
          <button className="btn-retry" type="button" onClick={handleRetryConnection}>
            Retry Connection
          </button>
        </div>
      )}

      <form onSubmit={handleSubmit} className="upload-form">
        <h2 className="panel-title">Ingest Financial Feed</h2>
        
        <div className="form-group">
          <label htmlFor="filePath">File Path</label>
          <div className="file-input-wrapper">
            <input
              id="filePath"
              type="text"
              className={`input-field file-path-input ${errors.filePath ? 'input-error' : ''}`}
              placeholder="Select statement CSV file..."
              value={filePath}
              readOnly
            />
            <button
              type="button"
              className="btn btn-secondary select-file-btn"
              onClick={handleSelectFile}
              disabled={isIngesting}
            >
              Browse
            </button>
          </div>
          {errors.filePath && <span className="error-message">{errors.filePath}</span>}
        </div>

        <div className="form-row">
          <div className="form-group col">
            <label htmlFor="dataSource">Data Source</label>
            <select
              id="dataSource"
              className="input-field select-field"
              value={dataSource}
              onChange={(e) => setDataSource(e.target.value)}
              disabled={isIngesting}
            >
              <option value="chase">Chase</option>
              <option value="synthetic">Synthetic</option>
            </select>
          </div>

          <div className="form-group col">
            <label htmlFor="accountID">Account ID (4 Digits)</label>
            <input
              id="accountID"
              type="text"
              maxLength={4}
              placeholder="e.g. 1234"
              className={`input-field ${errors.accountID ? 'input-error' : ''}`}
              value={accountID}
              onChange={handleAccountIdChange}
              disabled={isIngesting}
            />
            {errors.accountID && <span className="error-message">{errors.accountID}</span>}
          </div>
        </div>

        <button
          type="submit"
          className="btn btn-primary btn-submit"
          disabled={isIngesting || !dbConnected}
        >
          {isIngesting ? 'Ingesting...' : 'Start Ingestion'}
        </button>
      </form>
    </div>
  );
};
