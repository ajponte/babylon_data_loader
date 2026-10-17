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

  it('renders table headers correctly', async () => {
    vi.mocked(wailsApp.GetHistory).mockResolvedValue([
      model.SyncLog.createFrom({
        fileName: 'chase.csv',
        collectionName: 'transactions_chase',
        status: 'SUCCESS',
        upsertedCount: 10,
        matchedCount: 2,
        durationMs: 45,
        syncedAt: '2026-06-06T15:00:00Z',
      }),
    ]);

    render(<HistoryPanel />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    expect(screen.getByRole('columnheader', { name: 'Status' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'File Name' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Collection' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Records / Upserted' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Duplicates' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Duration' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Timestamp' })).toBeInTheDocument();
  });

  it('renders legacy log records correctly', async () => {
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
    const badges = screen.getAllByText('SUCCESS');
    expect(badges.length).toBe(2);
  });

  it('renders canonical success and failed log records with badges and accurate stats', async () => {
    const mockLogs = [
      model.SyncLog.createFrom({
        fileName: 'chase_jan.csv',
        collectionName: 'transactions_chase',
        status: 'SUCCESS',
        upsertedCount: 18,
        matchedCount: 2,
        durationMs: 120,
        syncedAt: '2026-06-06T15:00:00Z',
      }),
      model.SyncLog.createFrom({
        fileName: 'corrupt.csv',
        collectionName: 'transactions_chase',
        status: 'FAILED',
        upsertedCount: 0,
        matchedCount: 0,
        durationMs: 15,
        syncedAt: '2026-06-06T15:05:00Z',
      }),
    ];

    vi.mocked(wailsApp.GetHistory).mockResolvedValue(mockLogs);

    render(<HistoryPanel />);

    await waitFor(() => {
      expect(screen.queryByText(/Loading/i)).not.toBeInTheDocument();
    });

    expect(screen.getByText('chase_jan.csv')).toBeInTheDocument();
    expect(screen.getByText('corrupt.csv')).toBeInTheDocument();

    const successBadge = screen.getByText('SUCCESS');
    expect(successBadge).toHaveClass('badge-success');

    const failedBadge = screen.getByText('FAILED');
    expect(failedBadge).toHaveClass('badge-failed');

    expect(screen.getByText('18')).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText('120ms')).toBeInTheDocument();
    expect(screen.getByText('15ms')).toBeInTheDocument();
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
