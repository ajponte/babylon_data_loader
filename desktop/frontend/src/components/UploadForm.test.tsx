import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { UploadForm } from './UploadForm';
import * as wailsApp from '../../wailsjs/go/main/App';

// Mock the wails bindings
vi.mock('../../wailsjs/go/main/App', () => ({
  RetryConnectDB: vi.fn(),
  IngestFile: vi.fn(),
  GetHistory: vi.fn(),
}));

describe('UploadForm Component', () => {
  const mockOnRetrySuccess = vi.fn();
  const mockOnIngest = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders form inputs correctly', () => {
    render(
      <UploadForm
        dbConnected={true}
        onRetrySuccess={mockOnRetrySuccess}
        onIngest={mockOnIngest}
        isIngesting={false}
      />
    );

    expect(screen.getByLabelText(/File Path/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Data Source/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Account ID/i)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Start Ingestion/i })).toBeInTheDocument();
  });

  it('displays validation errors on empty submission', async () => {
    render(
      <UploadForm
        dbConnected={true}
        onRetrySuccess={mockOnRetrySuccess}
        onIngest={mockOnIngest}
        isIngesting={false}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Start Ingestion/i }));

    expect(await screen.findByText(/File path is required/i)).toBeInTheDocument();
    expect(await screen.findByText(/Account ID is required/i)).toBeInTheDocument();
    expect(mockOnIngest).not.toHaveBeenCalled();
  });

  it('displays validation error if account ID is not exactly 4 digits', async () => {
    render(
      <UploadForm
        dbConnected={true}
        onRetrySuccess={mockOnRetrySuccess}
        onIngest={mockOnIngest}
        isIngesting={false}
      />
    );

    const fileInput = screen.getByLabelText(/File Path/i);
    fireEvent.change(fileInput, { target: { value: '/path/to/statement.csv' } });

    const accountInput = screen.getByLabelText(/Account ID/i);
    fireEvent.change(accountInput, { target: { value: '12' } });

    fireEvent.click(screen.getByRole('button', { name: /Start Ingestion/i }));

    expect(await screen.findByText(/Account ID must be exactly 4 digits/i)).toBeInTheDocument();
    expect(mockOnIngest).not.toHaveBeenCalled();
  });

  it('allows only digits for Account ID', () => {
    render(
      <UploadForm
        dbConnected={true}
        onRetrySuccess={mockOnRetrySuccess}
        onIngest={mockOnIngest}
        isIngesting={false}
      />
    );

    const accountInput = screen.getByLabelText(/Account ID/i) as HTMLInputElement;
    fireEvent.change(accountInput, { target: { value: '12ab' } });

    expect(accountInput.value).toBe('');
    
    fireEvent.change(accountInput, { target: { value: '1234' } });
    expect(accountInput.value).toBe('1234');
  });

  it('submits correctly with valid values', async () => {
    // Setup window.runtime mock
    const openFileDialogMock = vi.fn().mockResolvedValue('/my/test/file.csv');
    (window as any).runtime = {
      OpenFileDialog: openFileDialogMock
    };

    render(
      <UploadForm
        dbConnected={true}
        onRetrySuccess={mockOnRetrySuccess}
        onIngest={mockOnIngest}
        isIngesting={false}
      />
    );

    // Click Browse button
    const browseBtn = screen.getByRole('button', { name: /Browse/i });
    fireEvent.click(browseBtn);

    expect(openFileDialogMock).toHaveBeenCalled();

    // Wait for the state to update with the file path
    const fileInput = await screen.findByPlaceholderText(/Select statement CSV file.../i) as HTMLInputElement;
    await waitFor(() => {
      expect(fileInput.value).toBe('/my/test/file.csv');
    });

    const accountInput = screen.getByLabelText(/Account ID/i);
    fireEvent.change(accountInput, { target: { value: '9876' } });

    const sourceSelect = screen.getByLabelText(/Data Source/i);
    fireEvent.change(sourceSelect, { target: { value: 'synthetic' } });

    fireEvent.click(screen.getByRole('button', { name: /Start Ingestion/i }));

    await waitFor(() => {
      expect(mockOnIngest).toHaveBeenCalledWith('/my/test/file.csv', 'synthetic', '9876');
    });

    // Clean up
    delete (window as any).runtime;
  });

  it('displays connection status banner when MongoDB is offline', () => {
    render(
      <UploadForm
        dbConnected={false}
        dbError="Could not connect to host"
        onRetrySuccess={mockOnRetrySuccess}
        onIngest={mockOnIngest}
        isIngesting={false}
      />
    );

    expect(screen.getByRole('alert')).toBeInTheDocument();
    expect(screen.getByText(/Database Disconnected:/i)).toBeInTheDocument();
    expect(screen.getByText(/Could not connect to host/i)).toBeInTheDocument();

    const startBtn = screen.getByRole('button', { name: /Start Ingestion/i }) as HTMLButtonElement;
    expect(startBtn.disabled).toBe(true);
  });

  it('calls RetryConnectDB when clicking retry button', async () => {
    vi.mocked(wailsApp.RetryConnectDB).mockResolvedValue(true);

    render(
      <UploadForm
        dbConnected={false}
        dbError="Offline"
        onRetrySuccess={mockOnRetrySuccess}
        onIngest={mockOnIngest}
        isIngesting={false}
      />
    );

    const retryBtn = screen.getByRole('button', { name: /Retry Connection/i });
    fireEvent.click(retryBtn);

    expect(wailsApp.RetryConnectDB).toHaveBeenCalled();
    await waitFor(() => {
      expect(mockOnRetrySuccess).toHaveBeenCalled();
    });
  });
});
