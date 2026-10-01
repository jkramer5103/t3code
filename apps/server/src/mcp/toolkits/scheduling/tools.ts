import {
  CreateScheduledPromptInput,
  McpCapabilityUnavailableError,
  ScheduledPrompt,
  ScheduledPromptError,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as ScheduledPrompts from "../../../scheduling/ScheduledPrompts.ts";

const dependencies = [McpInvocationContext.McpInvocationContext, ScheduledPrompts.ScheduledPrompts];
const failure = Schema.Union([ScheduledPromptError, McpCapabilityUnavailableError]);

export const SchedulingToolkit = Toolkit.make(
  Tool.make("schedule_prompt", {
    description:
      "Schedule a future user prompt that starts a new agent turn in this same chat. Use when the user asks to check later, at a specific time, or repeatedly. Supports once (ISO timestamp with offset), delay (seconds), interval (seconds, optional startAt), and cron (five fields, explicit IANA timeZone). Minimum delay/interval is 60 seconds. The environment server must be running; schedules persist across restarts and missed occurrences run once. Busy threads wait. Confirm the returned nextRunAt to the user. Do not schedule work unless the user requests it.",
    parameters: CreateScheduledPromptInput,
    success: ScheduledPrompt,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.OpenWorld, false)
    .annotate(Tool.Idempotent, false),
  Tool.make("list_scheduled_prompts", {
    description:
      "List all active and paused schedules in this chat, plus the latest 100 completed or cancelled schedules, including their IDs, next run, status, last run, and any delivery error. Use to inspect scheduled work before changing it.",
    success: Schema.Struct({ schedules: Schema.Array(ScheduledPrompt) }),
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.OpenWorld, false)
    .annotate(Tool.Idempotent, true),
  Tool.make("update_scheduled_prompt", {
    description:
      "Pause, resume, or cancel a schedule in this chat by its ID. Resume runs an overdue prompt once. Cancel permanently stops future runs; completed or cancelled schedules cannot resume. To change the prompt or timing, cancel it and create a replacement.",
    parameters: Schema.Struct({
      id: TrimmedNonEmptyString,
      action: Schema.Literals(["pause", "resume", "cancel"]),
    }),
    success: ScheduledPrompt,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.OpenWorld, false)
    .annotate(Tool.Idempotent, true),
);
