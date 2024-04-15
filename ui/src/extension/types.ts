export type TaskStatus = 'pending' | 'running' | 'done' | 'failed' | 'interrupted' | 'saved';

export type Task = {
  id: string;
  api_task_id?: string;
  name?: string;
  type: string;
  status: TaskStatus;
  params: Record<string, any>;
  priority: number;
  result: string;
  bookmarked?: boolean;
  ack_tag?: number;
  editing?: boolean;
  created_at: number;
  started_at: number;
  finished_at: number;
  generation_time_seconds: number;
  queue_wait_seconds: number;
  updated_at: number;
};

export type ResponseStatus = {
  success: boolean;
  message: string;
};

export type TaskHistoryResponse = {
  tasks: Task[];
  total: number;
};

export type ProgressResponse = {
  active: boolean;
  completed: boolean;
  eta: number;
  id_live_preview: number;
  live_preview: string | null;
  paused: boolean;
  progress: number;
  queued: false;
};
