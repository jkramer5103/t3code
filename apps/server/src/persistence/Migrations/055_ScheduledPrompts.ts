import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE scheduled_prompts (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    prompt TEXT NOT NULL,
    schedule_json TEXT NOT NULL,
    next_run_at TEXT NOT NULL,
    eligible_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'paused', 'completed', 'cancelled')),
    last_run_at TEXT,
    last_error TEXT,
    delivery_command_id TEXT,
    delivery_created_at TEXT,
    created_at TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX scheduled_prompts_due ON scheduled_prompts(status, eligible_at, next_run_at)`;
  yield* sql`CREATE INDEX scheduled_prompts_thread ON scheduled_prompts(thread_id, created_at)`;
});
