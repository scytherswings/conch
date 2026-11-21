import { defineConfig } from "vitest/config";
import { cloudflareTest } from "@cloudflare/vitest-plugin";

export default defineConfig({
  plugins: [
    // The Workers Vitest integration (replaces @cloudflare/vitest-pool-workers,
    // which is deprecated and frozen). The plugin configures the pool and
    // provides the cloudflare:test module. isolated per-test storage is the
    // plugin's default; the wrangler config and miniflare bindings mirror the
    // old package's options exactly.
    cloudflareTest({
      wrangler: { configPath: "./wrangler.toml" },
      miniflare: { bindings: { CONCH_AUTH_SECRET: "test-secret", CONCH_AUTH_SECRET_NEXT: "next-secret" } },
    }),
  ],
});