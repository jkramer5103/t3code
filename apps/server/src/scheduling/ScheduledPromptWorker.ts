import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import { forkParked } from "../serverActivation.ts";
import { ServerRuntimeStartup } from "../serverRuntimeStartup.ts";
import { ScheduledPrompts } from "./ScheduledPrompts.ts";

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const schedules = yield* ScheduledPrompts;
    const startup = yield* ServerRuntimeStartup;
    yield* forkParked(
      startup.awaitCommandReady.pipe(
        Effect.andThen(
          schedules.runDue.pipe(
            Effect.catch((cause) => Effect.logWarning("Scheduled prompt sweep failed", cause)),
            Effect.repeat(Schedule.spaced("1 second")),
          ),
        ),
      ),
    );
  }),
);
