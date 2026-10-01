import {
  CommandId,
  MessageId,
  ThreadId,
  ScheduledPrompt,
  ScheduledPromptError,
  PromptSchedule,
  type CreateScheduledPromptInput,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Cron from "effect/Cron";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import {
  OrchestrationCommandInvariantError,
  OrchestrationCommandPreviouslyRejectedError,
} from "../orchestration/Errors.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import { threadHasQueuedTurnStart } from "../orchestration/ThreadSettlementPolicy.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";

const isCommandRejected = Schema.is(OrchestrationCommandInvariantError);
const isPreviouslyRejected = Schema.is(OrchestrationCommandPreviouslyRejectedError);
const encodeSchedule = Schema.encodeSync(Schema.fromJsonString(PromptSchedule));
const decodeSchedule = Schema.decodeUnknownSync(Schema.fromJsonString(PromptSchedule));
const iso = (millis: number) => DateTime.formatIso(DateTime.makeUnsafe(millis));

/** Computes the next occurrence after downtime without replaying missed intervals. */
export function nextOccurrence(
  schedule: PromptSchedule,
  now: number,
  previous?: number,
): string | null {
  switch (schedule.kind) {
    case "once":
      return previous === undefined ? DateTime.formatIso(DateTime.makeUnsafe(schedule.at)) : null;
    case "delay":
      return previous === undefined ? iso(now + schedule.seconds * 1000) : null;
    case "interval": {
      const interval = schedule.seconds * 1000;
      if (previous === undefined)
        return schedule.startAt
          ? DateTime.formatIso(DateTime.makeUnsafe(schedule.startAt))
          : iso(now + interval);
      return iso(previous + (Math.floor((now - previous) / interval) + 1) * interval);
    }
    case "cron": {
      if (schedule.expression.trim().split(/\s+/).length !== 5)
        throw new Error("Use a five-field cron expression (minute, hour, day, month, weekday).");
      return Cron.next(Cron.parseUnsafe(schedule.expression, schedule.timeZone), now).toISOString();
    }
  }
}

interface Row {
  readonly id: string;
  readonly thread_id: string;
  readonly prompt: string;
  readonly schedule_json: string;
  readonly next_run_at: string;
  readonly status: ScheduledPrompt["status"];
  readonly last_run_at: string | null;
  readonly last_error: string | null;
  readonly created_at: string;
  readonly delivery_command_id: string | null;
  readonly delivery_created_at: string | null;
}
const fromRow = (row: Row): ScheduledPrompt => ({
  id: row.id,
  prompt: row.prompt,
  schedule: decodeSchedule(row.schedule_json),
  nextRunAt: row.next_run_at,
  status: row.status,
  lastRunAt: row.last_run_at,
  lastError: row.last_error,
  createdAt: row.created_at,
});
const failure = (cause: unknown) =>
  new ScheduledPromptError({
    detail: cause instanceof Error ? cause.message : "Could not update scheduled prompts.",
  });

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const crypto = yield* Crypto.Crypto;
  const mutex = yield* Semaphore.make(1);
  const locked = <A, E>(effect: Effect.Effect<A, E>) =>
    mutex.withPermits(1)(effect).pipe(Effect.mapError(failure));

  const create = (threadId: ThreadId, input: CreateScheduledPromptInput) =>
    locked(
      Effect.gen(function* () {
        const thread = yield* snapshots.getThreadShellById(threadId);
        if (Option.isNone(thread) || thread.value.archivedAt !== null) {
          return yield* new ScheduledPromptError({
            detail: "Scheduling requires an active thread.",
          });
        }
        const now = yield* DateTime.now;
        const nextRunAt = yield* Effect.try({
          try: () => nextOccurrence(input.schedule, DateTime.toEpochMillis(now))!,
          catch: failure,
        });
        if (nextRunAt <= DateTime.formatIso(now)) {
          return yield* new ScheduledPromptError({
            detail: "The first scheduled time must be in the future.",
          });
        }
        const id = yield* crypto.randomUUIDv4;
        const createdAt = DateTime.formatIso(now);
        yield* sql`INSERT INTO scheduled_prompts
      (id, thread_id, prompt, schedule_json, next_run_at, eligible_at, status, created_at)
      VALUES (${id}, ${threadId}, ${input.prompt}, ${encodeSchedule(input.schedule)}, ${nextRunAt}, ${nextRunAt}, 'active', ${createdAt})`;
        return {
          id,
          ...input,
          nextRunAt,
          status: "active" as const,
          lastRunAt: null,
          lastError: null,
          createdAt,
        };
      }),
    );

  const list = (threadId: ThreadId) =>
    locked(
      sql<Row>`WITH terminal AS (
        SELECT * FROM scheduled_prompts WHERE thread_id = ${threadId} AND status IN ('completed', 'cancelled')
        ORDER BY created_at DESC LIMIT 100
      )
      SELECT * FROM scheduled_prompts WHERE thread_id = ${threadId} AND status IN ('active', 'paused')
      UNION ALL SELECT * FROM terminal ORDER BY created_at DESC`.pipe(
        Effect.map((rows) => rows.map(fromRow)),
      ),
    );

  const update = (threadId: ThreadId, id: string, action: "pause" | "resume" | "cancel") =>
    locked(
      Effect.gen(function* () {
        const [row] =
          yield* sql<Row>`SELECT * FROM scheduled_prompts WHERE id = ${id} AND thread_id = ${threadId}`;
        if (!row)
          return yield* new ScheduledPromptError({ detail: "Schedule not found in this thread." });
        if (row.status === "cancelled" || row.status === "completed") {
          if (action === "cancel") return fromRow(row);
          return yield* new ScheduledPromptError({
            detail: "Create a new schedule to replace a completed or cancelled one.",
          });
        }
        const status = action === "cancel" ? "cancelled" : action === "pause" ? "paused" : "active";
        yield* sql`UPDATE scheduled_prompts SET status = ${status}, last_error = NULL, eligible_at = next_run_at WHERE id = ${id}`;
        // Resume retains the original due time; overdue prompts run once.
        return fromRow({ ...row, status, last_error: null });
      }),
    );

  const runDue = locked(
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const nowIso = DateTime.formatIso(now);
      const rows =
        yield* sql<Row>`SELECT * FROM scheduled_prompts WHERE status = 'active' AND next_run_at <= ${nowIso} AND eligible_at <= ${nowIso} ORDER BY next_run_at LIMIT 32`;
      const dispatchedThreads = new Set<string>();
      for (const row of rows) {
        yield* Effect.gen(function* () {
          const threadId = ThreadId.make(row.thread_id);
          const thread = yield* snapshots.getThreadShellById(threadId);
          if (Option.isNone(thread) || thread.value.archivedAt !== null) {
            yield* sql`UPDATE scheduled_prompts SET status = 'paused', last_error = 'Thread is archived or deleted.' WHERE id = ${row.id}`;
            return;
          }
          const value = thread.value;
          if (
            dispatchedThreads.has(row.thread_id) ||
            value.latestTurn?.state === "running" ||
            value.session?.status === "starting" ||
            value.session?.status === "running" ||
            value.session?.activeTurnId ||
            value.hasPendingApprovals ||
            value.hasPendingUserInput ||
            threadHasQueuedTurnStart(value, nowIso)
          ) {
            yield* sql`UPDATE scheduled_prompts SET eligible_at = ${iso(DateTime.toEpochMillis(now) + 5000)} WHERE id = ${row.id}`;
            return;
          }
          const deliveredAt = row.delivery_created_at ?? nowIso;
          if (row.delivery_created_at === null) {
            yield* sql`UPDATE scheduled_prompts SET delivery_created_at = ${deliveredAt} WHERE id = ${row.id}`;
          }
          const occurrence = row.delivery_command_id ?? `schedule:${row.id}:${row.next_run_at}`;
          // Dispatch receipts deduplicate a crash after the turn was accepted but before the schedule advances.
          yield* engine.dispatch({
            type: "thread.turn.start",
            commandId: CommandId.make(occurrence),
            threadId,
            message: {
              messageId: MessageId.make(occurrence),
              role: "user",
              text: `[Scheduled prompt • ${row.next_run_at}]\n\n${row.prompt}`,
              attachments: [],
            },
            runtimeMode: value.runtimeMode,
            interactionMode: value.interactionMode,
            createdAt: deliveredAt,
            requireIdleAt: nowIso,
          });
          dispatchedThreads.add(row.thread_id);
          const next = yield* Effect.try(() =>
            nextOccurrence(
              decodeSchedule(row.schedule_json),
              DateTime.toEpochMillis(now),
              DateTime.toEpochMillis(DateTime.makeUnsafe(row.next_run_at)),
            ),
          );
          yield* sql`UPDATE scheduled_prompts SET next_run_at = ${next ?? row.next_run_at}, eligible_at = ${next ?? row.next_run_at},
          status = ${next === null ? "completed" : "active"}, last_run_at = ${deliveredAt}, last_error = NULL, delivery_command_id = NULL, delivery_created_at = NULL WHERE id = ${row.id}`;
        }).pipe(
          Effect.catch((cause) =>
            Effect.gen(function* () {
              // Rejected commands have durable receipts too. Give the retry a new ID,
              // while retaining accepted IDs when a subsequent SQL update fails.
              if (isCommandRejected(cause) || isPreviouslyRejected(cause)) {
                const retryId = yield* crypto.randomUUIDv4;
                yield* sql`UPDATE scheduled_prompts SET delivery_command_id = ${`schedule:${row.id}:${retryId}`}, delivery_created_at = NULL WHERE id = ${row.id}`;
              }
              yield* sql`UPDATE scheduled_prompts SET last_error = ${failure(cause).message}, eligible_at = ${iso(DateTime.toEpochMillis(now) + 60000)} WHERE id = ${row.id}`;
            }),
          ),
        );
      }
    }),
  );
  return { create, list, update, runDue };
});

export class ScheduledPrompts extends Context.Service<
  ScheduledPrompts,
  Effect.Success<typeof make>
>()("t3/scheduling/ScheduledPrompts") {}
export const layer = Layer.effect(ScheduledPrompts, make);
