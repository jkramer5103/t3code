import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  OrchestrationThreadShell,
  CreateScheduledPromptInput,
  type OrchestrationCommand,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { OrchestrationCommandInvariantError } from "../orchestration/Errors.ts";
import { make, nextOccurrence } from "./ScheduledPrompts.ts";
import { SchedulingToolkit } from "../mcp/toolkits/scheduling/tools.ts";
import { SchedulingToolkitHandlersLive } from "../mcp/toolkits/scheduling/handlers.ts";
import { ScheduledPrompts } from "./ScheduledPrompts.ts";
import { McpInvocationContext } from "../mcp/McpInvocationContext.ts";
import * as Stream from "effect/Stream";
import type { Tool } from "effect/unstable/ai";

const threadId = ThreadId.make("scheduled-thread");
const otherId = ThreadId.make("other-thread");
const thread = Schema.decodeUnknownSync(OrchestrationThreadShell)({
  id: threadId,
  projectId: ProjectId.make("project"),
  title: "Checks",
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  pullRequests: [],
  latestTurn: null,
  session: null,
  createdAt: "1970-01-01T00:00:00.000Z",
  updatedAt: "1970-01-01T00:00:00.000Z",
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
});

const harness = Effect.gen(function* () {
  const commands: OrchestrationCommand[] = [];
  const accepted = new Set<string>();
  let current: typeof thread | null = thread;
  let reject = false;
  const schedules = yield* make.pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionSnapshotQuery)({
          getThreadShellById: () => Effect.sync(() => Option.fromNullishOr(current)),
        }),
        Layer.mock(OrchestrationEngineService)({
          dispatch: (command) =>
            Effect.gen(function* () {
              if (reject)
                return yield* new OrchestrationCommandInvariantError({
                  commandType: command.type,
                  detail: "Busy",
                });
              if (!accepted.has(command.commandId)) {
                accepted.add(command.commandId);
                commands.push(command);
              }
              return { sequence: commands.length };
            }),
        }),
      ),
    ),
  );
  return {
    schedules,
    commands,
    setThread: (value: typeof current) => {
      current = value;
    },
    setReject: (value: boolean) => {
      reject = value;
    },
  };
});
const provide = Effect.provide(
  SqlitePersistenceMemory.pipe(Layer.provideMerge(NodeServices.layer)),
);

it.effect("delivers a one-time prompt in the same thread exactly once", () =>
  Effect.gen(function* () {
    const { schedules, commands } = yield* harness;
    const created = yield* schedules.create(threadId, {
      prompt: "Check the deployment",
      schedule: { kind: "delay", seconds: 60 },
    });
    yield* schedules.runDue;
    expect(commands).toHaveLength(0);
    yield* TestClock.adjust("1 minute");
    yield* schedules.runDue;
    yield* schedules.runDue;
    expect(commands).toHaveLength(1);
    expect(commands[0]).toMatchObject({
      type: "thread.turn.start",
      threadId,
      message: { role: "user", text: expect.stringContaining("Check the deployment") },
      runtimeMode: "full-access",
    });
    expect(yield* schedules.list(threadId)).toMatchObject([
      { id: created.id, status: "completed" },
    ]);
  }).pipe(provide),
);

it.effect("persists across service reconstruction and coalesces missed recurring runs", () =>
  Effect.gen(function* () {
    const first = yield* harness;
    yield* first.schedules.create(threadId, {
      prompt: "Check CI",
      schedule: { kind: "interval", seconds: 60 },
    });
    yield* TestClock.adjust("10 minutes");
    const restarted = yield* harness;
    yield* restarted.schedules.runDue;
    yield* restarted.schedules.runDue;
    expect(restarted.commands).toHaveLength(1);
    const [schedule] = yield* restarted.schedules.list(threadId);
    expect(schedule?.status).toBe("active");
    expect(schedule?.nextRunAt).toBe(
      DateTime.formatIso(DateTime.add(yield* DateTime.now, { minutes: 1 })),
    );
  }).pipe(provide),
);

it.effect("pauses, resumes overdue work once, cancels, and scopes changes to the thread", () =>
  Effect.gen(function* () {
    const { schedules, commands } = yield* harness;
    const created = yield* schedules.create(threadId, {
      prompt: "Check CI",
      schedule: { kind: "interval", seconds: 60 },
    });
    yield* schedules.update(threadId, created.id, "pause");
    yield* TestClock.adjust("3 hours");
    yield* schedules.runDue;
    expect(commands).toHaveLength(0);
    expect(yield* schedules.update(otherId, created.id, "cancel").pipe(Effect.flip)).toMatchObject({
      _tag: "ScheduledPromptError",
    });
    yield* schedules.update(threadId, created.id, "resume");
    yield* schedules.runDue;
    expect(commands).toHaveLength(1);
    yield* schedules.update(threadId, created.id, "cancel");
    yield* TestClock.adjust("1 hour");
    yield* schedules.runDue;
    expect(commands).toHaveLength(1);
  }).pipe(provide),
);

it.effect("waits for pending requests and pauses archived or deleted threads", () =>
  Effect.gen(function* () {
    const { schedules, commands, setThread } = yield* harness;
    yield* schedules.create(threadId, {
      prompt: "Check CI",
      schedule: { kind: "delay", seconds: 60 },
    });
    yield* TestClock.adjust("1 minute");
    setThread({ ...thread, hasPendingUserInput: true });
    yield* schedules.runDue;
    expect(commands).toHaveLength(0);
    setThread(thread);
    yield* TestClock.adjust("5 seconds");
    yield* schedules.runDue;
    expect(commands).toHaveLength(1);
    yield* schedules.create(threadId, {
      prompt: "Again",
      schedule: { kind: "delay", seconds: 60 },
    });
    yield* TestClock.adjust("1 minute");
    setThread({ ...thread, archivedAt: "1970-01-01T00:00:00.000Z" });
    yield* schedules.runDue;
    expect((yield* schedules.list(threadId)).some((item) => item.status === "paused")).toBe(true);
    setThread(null);
    expect(
      yield* schedules
        .create(threadId, { prompt: "Missing", schedule: { kind: "delay", seconds: 60 } })
        .pipe(Effect.flip),
    ).toMatchObject({ _tag: "ScheduledPromptError" });
  }).pipe(provide),
);

it.effect("retries a rejected delivery with a new command ID", () =>
  Effect.gen(function* () {
    const { schedules, commands, setReject } = yield* harness;
    yield* schedules.create(threadId, {
      prompt: "Check CI",
      schedule: { kind: "delay", seconds: 60 },
    });
    yield* TestClock.adjust("1 minute");
    setReject(true);
    yield* schedules.runDue;
    expect(commands).toHaveLength(0);
    expect((yield* schedules.list(threadId))[0]?.lastError).toContain("Busy");
    setReject(false);
    yield* TestClock.adjust("1 minute");
    yield* schedules.runDue;
    expect(commands).toHaveLength(1);
    expect(commands[0]?.commandId).not.toContain("1970-");
  }).pipe(provide),
);

it.effect(
  "recovers an accepted dispatch after a failed schedule update without posting twice",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const { schedules, commands } = yield* harness;
      yield* schedules.create(threadId, {
        prompt: "Check CI",
        schedule: { kind: "delay", seconds: 60 },
      });
      yield* sql`CREATE TRIGGER fail_schedule_advance BEFORE UPDATE OF last_run_at ON scheduled_prompts BEGIN SELECT RAISE(FAIL, 'simulated crash'); END`;
      yield* TestClock.adjust("1 minute");
      yield* schedules.runDue;
      expect(commands).toHaveLength(1);
      yield* sql`DROP TRIGGER fail_schedule_advance`;
      yield* TestClock.adjust("1 minute");
      yield* schedules.runDue;
      expect(commands).toHaveLength(1);
      expect((yield* schedules.list(threadId))[0]?.status).toBe("completed");
    }).pipe(provide),
);

it("normalizes offsets and handles cron time zones across daylight savings", () => {
  expect(nextOccurrence({ kind: "once", at: "2026-10-01T09:00:00+02:00" }, 0)).toBe(
    "2026-10-01T07:00:00.000Z",
  );
  const cron = { kind: "cron", expression: "0 9 * * *", timeZone: "Europe/Berlin" } as const;
  expect(
    nextOccurrence(cron, DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-24T08:00:00Z"))),
  ).toBe("2026-10-25T08:00:00.000Z");
  expect(() => nextOccurrence({ ...cron, expression: "invalid" }, 0)).toThrow();
  expect(() => nextOccurrence({ ...cron, timeZone: "invalid" }, 0)).toThrow();
});

it("rejects intervals below a minute and timestamps without a time zone", () => {
  const decode = Schema.decodeUnknownSync(CreateScheduledPromptInput);
  expect(() => decode({ prompt: "Check", schedule: { kind: "interval", seconds: 1 } })).toThrow();
  expect(() =>
    decode({ prompt: "Check", schedule: { kind: "once", at: "2026-10-01T09:00:00" } }),
  ).toThrow();
});

it.effect("MCP tools reject credentials without scheduling access", () =>
  Effect.gen(function* () {
    const { schedules } = yield* harness;
    const toolkit = yield* SchedulingToolkit.pipe(
      Effect.provide(
        SchedulingToolkitHandlersLive.pipe(
          Layer.provide(Layer.succeed(ScheduledPrompts, schedules)),
        ),
      ),
    );
    const result = yield* toolkit.handle("list_scheduled_prompts", {}).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.provideService(McpInvocationContext, {
        environmentId: EnvironmentId.make("environment"),
        threadId,
        providerSessionId: "session",
        providerInstanceId: ProviderInstanceId.make("codex"),
        issuedAt: 0,
        capabilities: new Set<"scheduling">(),
      }),
      Effect.provideService(ScheduledPrompts, schedules),
      Effect.flip,
    );
    expect(result).toMatchObject({ _tag: "McpCapabilityUnavailableError" });
  }).pipe(provide),
);

it.effect("agents can create, inspect, and cancel a schedule through MCP", () =>
  Effect.gen(function* () {
    const { schedules, commands } = yield* harness;
    const toolkit = yield* SchedulingToolkit.pipe(
      Effect.provide(
        SchedulingToolkitHandlersLive.pipe(
          Layer.provide(Layer.succeed(ScheduledPrompts, schedules)),
        ),
      ),
    );
    const context = Effect.provideService(McpInvocationContext, {
      environmentId: EnvironmentId.make("environment"),
      threadId,
      providerSessionId: "session",
      providerInstanceId: ProviderInstanceId.make("codex"),
      issuedAt: 0,
      capabilities: new Set(["scheduling"] as const),
    });
    const dependency = Effect.provideService(ScheduledPrompts, schedules);
    const created = yield* toolkit
      .handle("schedule_prompt", {
        prompt: "Check on this",
        schedule: { kind: "interval", seconds: 10800 },
      })
      .pipe(Stream.unwrap, Stream.runCollect, context, dependency);
    const { id } = created.at(-1)!.result as Tool.Success<
      typeof SchedulingToolkit.tools.schedule_prompt
    >;
    const listed = yield* toolkit
      .handle("list_scheduled_prompts", {})
      .pipe(Stream.unwrap, Stream.runCollect, context, dependency);
    expect(
      (listed.at(-1)!.result as Tool.Success<typeof SchedulingToolkit.tools.list_scheduled_prompts>)
        .schedules,
    ).toMatchObject([{ id, status: "active" }]);
    yield* toolkit
      .handle("update_scheduled_prompt", { id, action: "cancel" })
      .pipe(Stream.unwrap, Stream.runCollect, context, dependency);
    yield* TestClock.adjust("3 hours");
    yield* schedules.runDue;
    expect(commands).toHaveLength(0);
  }).pipe(provide),
);
