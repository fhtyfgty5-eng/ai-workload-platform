-- 支持按 Run 最近一次分发时间选择候选，保证跨扫描轮次的持久化公平性。
CREATE INDEX task_dispatches_run_created_idx
    ON task_dispatches (run_id, created_at DESC);
