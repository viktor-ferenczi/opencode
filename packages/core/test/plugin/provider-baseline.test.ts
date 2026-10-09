import { describe, expect } from "bun:test"
import { define } from "@opencode/plugin/effect/plugin"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { KV } from "@opencode/core/kv"
import { Plugin } from "@opencode/core/plugin"
import { PluginHost } from "@opencode/core/plugin/host"
import { ProviderBaselinePlugin } from "@opencode/core/plugin/provider-baseline"
import { Provider } from "@opencode/core/provider"
import { Effect } from "effect"
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
              providers.add({ info: { ...Provider.Info.empty(Provider.ID.make(id)), activation: "enabled" }, models: [] })
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
