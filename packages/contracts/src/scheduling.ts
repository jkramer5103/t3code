import * as Schema from "effect/Schema";
import { IsoDateTime, TrimmedNonEmptyString } from "./baseSchemas.ts";

const Timestamp = IsoDateTime.check(Schema.isPattern(/T.*(?:Z|[+-]\d{2}:\d{2})$/));

const Seconds = Schema.Int.check(Schema.isBetween({ minimum: 60, maximum: 31_536_000 }));

export const PromptSchedule = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("once"), at: Timestamp }),
  Schema.Struct({ kind: Schema.Literal("delay"), seconds: Seconds }),
  Schema.Struct({
    kind: Schema.Literal("interval"),
    seconds: Seconds,
    startAt: Schema.optional(Timestamp),
  }),
  Schema.Struct({
    kind: Schema.Literal("cron"),
    expression: TrimmedNonEmptyString,
    timeZone: TrimmedNonEmptyString,
  }),
]);
export type PromptSchedule = typeof PromptSchedule.Type;

export const CreateScheduledPromptInput = Schema.Struct({
  prompt: TrimmedNonEmptyString.check(Schema.isMaxLength(120_000)),
  schedule: PromptSchedule,
});
export type CreateScheduledPromptInput = typeof CreateScheduledPromptInput.Type;

export const ScheduledPrompt = Schema.Struct({
  id: TrimmedNonEmptyString,
  prompt: Schema.String,
  schedule: PromptSchedule,
  nextRunAt: IsoDateTime,
  status: Schema.Literals(["active", "paused", "completed", "cancelled"]),
  lastRunAt: Schema.NullOr(IsoDateTime),
  lastError: Schema.NullOr(Schema.String),
  createdAt: IsoDateTime,
});
export type ScheduledPrompt = typeof ScheduledPrompt.Type;

export class ScheduledPromptError extends Schema.TaggedError<ScheduledPromptError>()(
  "ScheduledPromptError",
  { detail: Schema.String },
) {
  override get message() {
    return this.detail;
  }
}
