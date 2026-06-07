import { render, screen, act, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ProgressPanel } from './ProgressPanel';

// Mock the Wails runtime
const callbacks: Record<string, Function> = {};
vi.mock('../../wailsjs/runtime/runtime', () => ({
  EventsOn: vi.fn((event: string, callback: Function) => {
    callbacks[event] = callback;
    return vi.fn(); // unsubscribe function
  }),
}));

describe('ProgressPanel Component', () => {
  const mockOnFinished = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    // Clear callbacks
    for (const key in callbacks) {
      delete callbacks[key];
    }
  });

  it('renders initial waiting state when no progress event is received yet', () => {
    render(<ProgressPanel onFinished={mockOnFinished} />);
    expect(screen.getByText(/Waiting for ingestion to start/i)).toBeInTheDocument();
  });

  it('updates state and displays progress bar when ingest:progress is received', () => {
    render(<ProgressPanel onFinished={mockOnFinished} />);

    const progressEvent = {
      phase: 'upserting',
      message: 'Ingesting records...',
      fileName: 'Chase1234.csv',
      currentRecord: 50,
      totalRecords: 100,
      upsertedCount: 40,
      duplicateCount: 10,
    };

    act(() => {
      callbacks['ingest:progress'](progressEvent);
    });

    expect(screen.getByText('UPSERTING')).toBeInTheDocument();
    expect(screen.getByText('Chase1234.csv')).toBeInTheDocument();
    expect(screen.getByText('Ingesting records...')).toBeInTheDocument();
    expect(screen.getByText(/50% \(50 \/ 100 records\)/i)).toBeInTheDocument();
  });

  it('displays log console and renders received ingest:log events', () => {
    render(<ProgressPanel onFinished={mockOnFinished} />);

    act(() => {
      callbacks['ingest:progress']({
        phase: 'parsing',
        message: 'Parsing statement',
        fileName: 'test.csv',
      });
    });

    const logEvent = {
      time: '2026-06-06T15:00:00.123Z',
      level: 'INFO',
      message: 'Beginning statement validation',
    };

    act(() => {
      callbacks['ingest:log'](logEvent);
    });

    expect(screen.getByText('Beginning statement validation')).toBeInTheDocument();
    expect(screen.getByText('[INFO]')).toBeInTheDocument();
  });

  it('renders Done button and stats when phase is done', () => {
    render(<ProgressPanel onFinished={mockOnFinished} />);

    act(() => {
      callbacks['ingest:progress']({
        phase: 'done',
        message: 'Completed successfully',
        fileName: 'Chase1234.csv',
        currentRecord: 100,
        totalRecords: 100,
        upsertedCount: 95,
        duplicateCount: 5,
      });
    });

    expect(screen.getByText('DONE')).toBeInTheDocument();
    expect(screen.getByText('95')).toBeInTheDocument(); // Upserted
    expect(screen.getByText('5')).toBeInTheDocument();  // Duplicates

    const doneBtn = screen.getByRole('button', { name: /Done/i });
    expect(doneBtn).toBeInTheDocument();
    
    fireEvent.click(doneBtn);
    expect(mockOnFinished).toHaveBeenCalled();
  });
});
