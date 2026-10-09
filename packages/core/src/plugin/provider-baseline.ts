import { define } from "@opencode/plugin/effect/plugin"
import { Effect, Option, Schema, Stream } from "effect"
import { Bus } from "../bus.js"
import { Credential } from "../credential.js"
import { KV } from "../kv.js"

const key = "provider:known"
const decodeKnown = Schema.decodeUnknownOption(Schema.Array(Schema.String))

// Providers that did not exist on the first run (a newer catalog, build or plugin) start disabled, so they never
// become usable on their own through environment variables or ambient cloud credentials. Configuring the provider
// in opencode.json(c) enables it (the config provider plugin runs after this one), and so does connecting a
// credential for it, which also records it as known.
export const ProviderBaselinePlugin = define({
  id: "opencode.provider.baseline",
  effect: Effect.fn(function* (ctx) {
    const kv = yield* KV.Service
    const bus = yield* Bus.Service
    const stored = decodeKnown(yield* kv.get(key))
    let known = Option.isSome(stored) ? new Set(stored.value) : undefined
    const save = (ids: ReadonlySet<string>) => Effect.runFork(kv.set(key, [...ids].sort()))

    yield* ctx.provider.transform((providers) => {
      if (!known) {
        known = new Set(providers.list().map((record) => record.provider.id))
        save(known)
        return
      }
      for (const record of providers.list()) {
        const provider = record.provider
        if (known.has(provider.id) || (provider.integrationID && known.has(provider.integrationID))) continue
        providers.update(provider.id, (draft) => {
          draft.activation = "disabled"
        })
      }
    })

    yield* bus.subscribe(Credential.Event.Switched).pipe(
      Stream.runForEach((event) =>
        Effect.gen(function* () {
          if (!known || known.has(event.data.integrationID)) return
          known = new Set([...known, event.data.integrationID])
          yield* kv.set(key, [...known].sort())
          yield* ctx.provider.reload()
        }),
      ),
      Effect.forkScoped({ startImmediately: true }),
    )
  }),
})
