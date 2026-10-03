import { defineConfig } from "@playwright/test";

export default defineConfig({
  projects: [
    { name: "chromium", use: { browserName: "chromium" } },
    { name: "webkit", use: { browserName: "webkit" } },
  ],
  reporter: "list",
  testDir: "./tests/webapp/browser",
  use: { colorScheme: "dark" },
});
