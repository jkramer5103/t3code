import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { TestClock } from "effect/testing";
import { ServerRuntimeStartup } from "../serverRuntimeStartup.ts";
import { ScheduledPrompts } from "./ScheduledPrompts.ts";
import * as ScheduledPromptWorker from "./ScheduledPromptWorker.ts";

it.effect("waits for command readiness and stops when the server scope closes", () =>
  Effect.gen(function* () {
    const ready = yield* Deferred.make<void>();
    const delivered = yield* Deferred.make<void>();
    let runs = 0;
    const services = Layer.mergeAll(
      Layer.mock(ServerRuntimeStartup)({ awaitCommandReady: Deferred.await(ready) }),
      Layer.mock(ScheduledPrompts)({
        runDue: Effect.gen(function* () {
          runs += 1;
          yield* Deferred.succeed(delivered, undefined);
        }),
      }),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        yield* Layer.build(ScheduledPromptWorker.layer.pipe(Layer.provide(services)));
        expect(runs).toBe(0);
        yield* Deferred.succeed(ready, undefined);
        yield* Deferred.await(delivered);
        expect(runs).toBe(1);
      }),
    );
    yield* TestClock.adjust("1 minute");
    expect(runs).toBe(1);
  }),
);
