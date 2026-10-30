import { describe, expect } from "bun:test"
import { define } from "@opencode/plugin/effect/plugin"
import { Bus } from "@opencode/core/bus"
import { Config } from "@opencode/core/config"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { KV } from "@opencode/core/kv"
import { Plugin } from "@opencode/core/plugin"
import { PluginHost } from "@opencode/core/plugin/host"
import { Model } from "@opencode/core/model"
import { ModelBaselinePlugin, ProviderBaselinePlugin } from "@opencode/core/plugin/provider-baseline"
import "@opencode/core/plugin/provider/lmstudio"
import { Provider } from "@opencode/core/provider"
import { Document, Event, Info } from "@opencode/schema/config"
import { Effect, Schema } from "effect"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "./fixture"

const it = testEffect(PluginTestLayer)

const available = Effect.fn(function* () {
  const providers = yield* Provider.Service
  return (yield* providers.available()).map((provider) => String(provider.id))
})

describe("ProviderBaselinePlugin", () => {
  it.effect("records providers on the first run and disables ones added later until connected", () =>
    Effect.gen(function* () {
      const catalog = ["existing"]
      const plugin = yield* Plugin.Service
      const host = yield* PluginHost.make(plugin)
      yield* define({
        id: "test.catalog",
        effect: (ctx) =>
          ctx.provider.transform((providers) => {
            for (const id of catalog)
              providers.add({
                info: { ...Provider.Info.empty(Provider.ID.make(id)), activation: "enabled" },
                models: [],
              })
          }),
      }).effect(host)
      yield* ProviderBaselinePlugin.effect(host)

      expect(yield* available()).toContain("existing")
      const kv = yield* KV.Service
      expect(yield* kv.get("provider:known")).toContain("existing")

      catalog.push("fresh")
      yield* host.provider.reload()
      const providers = yield* Provider.Service
      expect((yield* providers.get(Provider.ID.make("fresh")))?.activation).toBe("disabled")
      expect(yield* available()).not.toContain("fresh")
      expect(yield* available()).toContain("existing")

      const credentials = yield* Credential.Service
      yield* credentials.create({ integrationID: Integration.ID.make("fresh"), value: { type: "key", key: "k" } })
      for (let attempt = 0; attempt < 100 && !(yield* available()).includes("fresh"); attempt++)
        yield* Effect.sleep("10 millis")
      expect(yield* available()).toContain("fresh")
      expect(yield* kv.get("provider:known")).toContain("fresh")
    }),
  )
})

const decodeConfig = Schema.decodeUnknownSync(Info)

const availableModels = Effect.fn(function* () {
  const models = yield* Model.Service
  return (yield* models.available()).map((model) => `${model.providerID}/${model.id}`)
})

describe("ModelBaselinePlugin", () => {
  it.effect("disables catalog models added later to a recorded provider until configured, but not local ones", () =>
    Effect.gen(function* () {
      const catalog: Record<string, string[]> = { zen: ["old"], lmstudio: ["loaded"] }
      const plugin = yield* Plugin.Service
      const host = yield* PluginHost.make(plugin)
      yield* define({
        id: "test.catalog",
        effect: (ctx) =>
          ctx.provider.transform((providers) => {
            for (const [id, models] of Object.entries(catalog)) {
              const providerID = Provider.ID.make(id)
              providers.add({
                info: { ...Provider.Info.empty(providerID), activation: "enabled" },
                models: models.map((model) => Model.Info.default(providerID, Model.ID.make(model))),
              })
            }
          }),
      }).effect(host)
      yield* ModelBaselinePlugin.effect(host)

      expect((yield* availableModels()).sort()).toEqual(["lmstudio/loaded", "zen/old"])
      const kv = yield* KV.Service
      for (let attempt = 0; attempt < 100 && !(yield* kv.get("model:known")); attempt++)
        yield* Effect.sleep("10 millis")
      expect(yield* kv.get("model:known")).toEqual({ zen: ["old"] })

      catalog.zen.push("launch", "preview")
      catalog.lmstudio.push("pulled")
      catalog.fresh = ["first"]
      yield* host.provider.reload()
      expect((yield* availableModels()).sort()).toEqual([
        "fresh/first",
        "lmstudio/loaded",
        "lmstudio/pulled",
        "zen/old",
      ])

      const config = yield* Config.Test
      yield* config.setEntries([
        new Document({ type: "document", info: decodeConfig({ providers: { zen: { models: { launch: {} } } } }) }),
      ])
      yield* (yield* Bus.Service).publish(Event.Updated, {})
      for (let attempt = 0; attempt < 100 && !(yield* availableModels()).includes("zen/launch"); attempt++)
        yield* Effect.sleep("10 millis")
      expect((yield* availableModels()).sort()).toEqual([
        "fresh/first",
        "lmstudio/loaded",
        "lmstudio/pulled",
        "zen/launch",
        "zen/old",
      ])
      for (let attempt = 0; attempt < 100 && Object.keys((yield* kv.get("model:known")) ?? {}).length < 2; attempt++)
        yield* Effect.sleep("10 millis")
      expect(yield* kv.get("model:known")).toEqual({ fresh: ["first"], zen: ["old"] })
    }),
  )
})
