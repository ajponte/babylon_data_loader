import React, { useEffect, useState, useRef } from 'react';
import { EventsOn } from '../../wailsjs/runtime/runtime';
import { ProgressEvent, FrontendLog } from '../types/events';

interface ProgressPanelProps {
  onFinished: () => void;
}

export const ProgressPanel: React.FC<ProgressPanelProps> = ({ onFinished }) => {
  const [progress, setProgress] = useState<ProgressEvent | null>(null);
  const [logs, setLogs] = useState<FrontendLog[]>([]);
  const consoleEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // Listen to progress events
    const unsubProgress = EventsOn('ingest:progress', (event: any) => {
      const parsedEvent: ProgressEvent = typeof event === 'string' ? JSON.parse(event) : event;
      setProgress(parsedEvent);
    });

    // Listen to log events
    const unsubLog = EventsOn('ingest:log', (log: any) => {
      const parsedLog: FrontendLog = typeof log === 'string' ? JSON.parse(log) : log;
      setLogs((prev) => [...prev, parsedLog]);
    });

    return () => {
      unsubProgress();
      unsubLog();
    };
  }, []);

  // Auto-scroll logs to bottom
  useEffect(() => {
    if (consoleEndRef.current && typeof consoleEndRef.current.scrollIntoView === 'function') {
      consoleEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [logs]);

  if (!progress) {
    return (
      <div className="progress-panel-empty">
        <div className="spinner-glow"></div>
        <p>Waiting for ingestion to start...</p>
      </div>
    );
  }

  const {
    phase,
    message,
    fileName,
    currentRecord = 0,
    totalRecords = 0,
    upsertedCount = 0,
    duplicateCount = 0,
  } = progress;

  // Calculate percentage for upserting
  let percent = 0;
  let isIndeterminate = true;

  if (phase === 'upserting') {
    if (totalRecords > 0) {
      percent = Math.min(Math.round((Number(currentRecord) / Number(totalRecords)) * 100), 100);
      isIndeterminate = false;
    }
  } else if (phase === 'done') {
    percent = 100;
    isIndeterminate = false;
  }

  const getPhaseBadgeClass = () => {
    switch (phase) {
      case 'done':
        return 'badge-done';
      case 'failed':
        return 'badge-failed';
      default:
        return 'badge-active';
    }
  };

  return (
    <div className="progress-panel">
      <div className="progress-header">
        <h2 className="panel-title">Ingestion Progress</h2>
        <span className={`phase-badge ${getPhaseBadgeClass()}`}>
          {phase.toUpperCase()}
        </span>
      </div>

      <div className="progress-info">
        {fileName && (
          <div className="info-row">
            <span className="info-label">File:</span>
            <span className="info-value">{fileName}</span>
          </div>
        )}
        <div className="info-row">
          <span className="info-label">Status:</span>
          <span className="info-value">{message || 'Processing...'}</span>
        </div>
      </div>

      <div className="progress-bar-container">
        <div
          className={`progress-bar-fill ${isIndeterminate ? 'indeterminate' : ''}`}
          style={{ width: `${isIndeterminate ? 100 : percent}%` }}
        />
      </div>
      
      {!isIndeterminate && (
        <div className="progress-percentage">
          {percent}% ({currentRecord} / {totalRecords} records)
        </div>
      )}

      {/* Stats display */}
      {(phase === 'done' || Number(upsertedCount) > 0 || Number(duplicateCount) > 0) && (
        <div className="stats-grid">
          <div className="stat-card">
            <div className="stat-num text-success">{String(upsertedCount)}</div>
            <div className="stat-label">Upserted</div>
          </div>
          <div className="stat-card">
            <div className="stat-num text-warning">{String(duplicateCount)}</div>
            <div className="stat-label">Duplicates</div>
          </div>
          <div className="stat-card">
            <div className="stat-num text-info">{String(totalRecords)}</div>
            <div className="stat-label">Total Processed</div>
          </div>
        </div>
      )}

      {/* Logs console */}
      <div className="logs-console-container">
        <div className="logs-console-header">Live Ingestion Logs</div>
        <div className="logs-console">
          {logs.map((log, index) => {
            const timeStr = log.time ? log.time.split('T')[1]?.slice(0, 8) || log.time : '';
            return (
              <div key={index} className={`log-line level-${log.level.toLowerCase()}`}>
                <span className="log-time">[{timeStr}]</span>{' '}
                <span className="log-level">[{log.level}]</span>{' '}
                <span className="log-msg">{log.message}</span>
              </div>
            );
          })}
          <div ref={consoleEndRef} />
        </div>
      </div>

      {(phase === 'done' || phase === 'failed') && (
        <button className="btn btn-primary btn-close-progress" type="button" onClick={onFinished}>
          Done
        </button>
      )}
    </div>
  );
};
