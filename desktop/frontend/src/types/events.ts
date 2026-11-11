export type ProgressPhase =
  | 'validating'
  | 'parsing'
  | 'upserting'
  | 'saving'
  | 'done'
  | 'complete'
  | 'failed'
  | (string & {});

export interface ProgressEvent {
  phase: ProgressPhase;
  message: string;
  fileName: string;
  currentRecord: number;
  totalRecords: number;
  upsertedCount: number;
  duplicateCount: number;
}

export interface DbStatus {
  connected: boolean;
  error?: string;
}

export interface SyncLog {
  collectionName: string;
  syncTimestamp: string;
  recordsUploaded: number;
}

export interface FrontendLog {
  time: string;
  level: string;
  message: string;
  attrs?: Record<string, any>;
}
