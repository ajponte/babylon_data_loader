import { render, screen, waitFor } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { HistoryPanel } from './HistoryPanel';
import * as wailsApp from '../../wailsjs/go/main/App';
import { model } from '../../wailsjs/go/models';

// Mock the wails bindings
vi.mock('../../wailsjs/go/main/App', () => ({
  GetHistory: vi.fn(),
  RetryConnectDB: vi.fn(),
  IngestFile: vi.fn(),
}));

describe('HistoryPanel Component', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders loading state initially', () => {
    vi.mocked(wailsApp.GetHistory).mockReturnValue(new Promise(() => {}));
    
    render(<HistoryPanel />);

    expect(screen.getByText(/Loading sync logs.../i)).toBeInTheDocument();
  });

  it('renders empty historical run state', async () => {
    vi.mocked(wailsApp.GetHistory).mockResolvedValue([]);

    render(<HistoryPanel />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading sync logs.../i)).not.toBeInTheDocument();
    });
    expect(screen.getByText(/No historical runs found/i)).toBeInTheDocument();
  });

  it('renders log records correctly', async () => {
    const mockLogs = [
      model.SyncLog.createFrom({
        collectionName: 'transactions_chase',
        syncTimestamp: '2026-06-06T15:00:00Z',
        recordsUploaded: 25,
      }),
      model.SyncLog.createFrom({
        collectionName: 'transactions_synthetic',
        syncTimestamp: '2026-06-05T12:30:00Z',
        recordsUploaded: 142,
      }),
    ];

    vi.mocked(wailsApp.GetHistory).mockResolvedValue(mockLogs);

    render(<HistoryPanel />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    expect(screen.getByText('transactions_chase')).toBeInTheDocument();
    expect(screen.getByText('25')).toBeInTheDocument();
    expect(screen.getByText('transactions_synthetic')).toBeInTheDocument();
    expect(screen.getByText('142')).toBeInTheDocument();
  });

  it('handles error while fetching history', async () => {
    vi.mocked(wailsApp.GetHistory).mockRejectedValue(new Error('Connection timed out'));

    render(<HistoryPanel />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    expect(screen.getByText(/Connection timed out/i)).toBeInTheDocument();
  });
});
