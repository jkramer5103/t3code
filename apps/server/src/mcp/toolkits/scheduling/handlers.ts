import * as Effect from "effect/Effect";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as ScheduledPrompts from "../../../scheduling/ScheduledPrompts.ts";
import { SchedulingToolkit } from "./tools.ts";

const make = Effect.gen(function* () {
  const schedules = yield* ScheduledPrompts.ScheduledPrompts;
  return SchedulingToolkit.of({
    schedule_prompt: (input) =>
      Effect.gen(function* () {
        const scope = yield* McpInvocationContext.requireMcpCapability("scheduling");
        return yield* schedules.create(scope.threadId, input);
      }),
    list_scheduled_prompts: () =>
      Effect.gen(function* () {
        const scope = yield* McpInvocationContext.requireMcpCapability("scheduling");
        return { schedules: yield* schedules.list(scope.threadId) };
      }),
    update_scheduled_prompt: (input) =>
      Effect.gen(function* () {
        const scope = yield* McpInvocationContext.requireMcpCapability("scheduling");
        return yield* schedules.update(scope.threadId, input.id, input.action);
      }),
  });
});
export const SchedulingToolkitHandlersLive = SchedulingToolkit.toLayer(make);
