const { defineConfig, devices } = require('@playwright/test');

const externalBaseUrl = process.env.MONITOR_BASE_URL;

module.exports = defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  expect: {
    timeout: 5_000,
  },
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'list',
  use: {
    baseURL: externalBaseUrl || 'http://127.0.0.1:4173',
    trace: 'on-first-retry',
  },
  webServer: externalBaseUrl
    ? undefined
    : {
        command: 'bun run build && bun run preview -- --host 127.0.0.1 --port 4173',
        url: 'http://127.0.0.1:4173',
        reuseExistingServer: !process.env.CI,
        timeout: 120_000,
      },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
