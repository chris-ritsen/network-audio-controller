import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/webapp/browser",
  projects: [
    { name: "chromium", use: { browserName: "chromium" } },
    { name: "webkit", use: { browserName: "webkit" } },
  ],
  reporter: "list",
});
