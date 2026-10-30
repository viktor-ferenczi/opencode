import { define } from "@opencode/plugin/effect/plugin"
import { Document } from "@opencode/schema/config"
import { Effect, Option, Schema, Stream } from "effect"
import { Bus } from "../bus.js"
import { Config } from "../config.js"
import { ConfigEntryObserver } from "../config/plugin/entry-observer.js"
import { Credential } from "../credential.js"
import { KV } from "../kv.js"
import { localProviderIDs } from "./provider/local.js"

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

const modelKey = "model:known"
const decodeKnownModels = Schema.decodeUnknownOption(Schema.Record(Schema.String, Schema.Array(Schema.String)))

// Models that show up later in an already recorded provider start disabled, so a rebase onto a newer bundled catalog
// or a remote model list (OpenCode Console, Zen) never makes them selectable on their own. Local models are left
// alone: listing a model under its provider in opencode.json(c) keeps it enabled, and models discovered from a local
// server (Ollama, LM Studio, vLLM) are not gated. A provider seen for the first time records all of its models,
// because ProviderBaselinePlugin already gates providers that are new.
export const ModelBaselinePlugin = define({
  id: "opencode.model.baseline",
  effect: Effect.fn(function* (ctx) {
    const kv = yield* KV.Service
    const config = yield* ConfigEntryObserver.observe(yield* Config.Service, ctx.event, ctx.model.reload())
    const load = kv
      .get(modelKey)
      .pipe(
        Effect.map((value) =>
          Option.getOrElse(decodeKnownModels(value), (): Record<string, readonly string[]> => ({})),
        ),
      )
    const known = new Map(Object.entries(yield* load).map(([id, models]) => [id, new Set(models)]))
    // Other Locations record providers as well, so merge into the stored map instead of overwriting it.
    const save = (added: Record<string, string[]>) =>
      Effect.runFork(load.pipe(Effect.flatMap((stored) => kv.set(modelKey, { ...added, ...stored }))))

    yield* ctx.model.transform((models) => {
      const configured = new Set(
        config.entries
          .filter((entry): entry is Document => entry.type === "document")
          .flatMap((file) => Object.entries(file.info.providers ?? {}))
          .flatMap(([providerID, provider]) => Object.keys(provider.models ?? {}).map((id) => `${providerID}/${id}`)),
      )
      const added: Record<string, string[]> = {}
      for (const record of models.provider.list()) {
        const providerID = record.provider.id
        if (localProviderIDs.has(providerID)) continue
        const recorded = known.get(providerID)
        if (!recorded) {
          if (record.models.size === 0) continue
          known.set(providerID, new Set(record.models.keys()))
          added[providerID] = [...record.models.keys()].sort()
          continue
        }
        for (const modelID of record.models.keys()) {
          if (recorded.has(modelID) || configured.has(`${providerID}/${modelID}`)) continue
          if (!models.get(providerID, modelID)) continue
          models.update(providerID, modelID, (draft) => {
            draft.enabled = false
          })
        }
      }
      if (Object.keys(added).length) save(added)
    })
  }),
})
