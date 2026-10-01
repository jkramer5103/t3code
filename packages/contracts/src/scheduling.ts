import * as Schema from "effect/Schema";
import { IsoDateTime, TrimmedNonEmptyString } from "./baseSchemas.ts";

const timestampPattern =
  /^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,3})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/;
const Timestamp = IsoDateTime.check(
  Schema.makeFilter((value) => {
    const parts = timestampPattern.exec(value);
    if (parts === null)
      return "Use a valid calendar timestamp with seconds and an explicit UTC offset.";
    const year = Number(parts[1]);
    const month = Number(parts[2]);
    const day = Number(parts[3]);
    const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
    const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    return day <= daysInMonth[month - 1]! || "The timestamp contains an impossible calendar date.";
  }),
);

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
