const { defineConfig, devices } = require('@playwright/test');

const baseURL = process.env.MONITOR_BASE_URL;

if (!baseURL) {
  throw new Error(
    'MONITOR_BASE_URL is required and must point to the caller-owned monitor stack; the live Playwright suite never provisions one',
  );
}

module.exports = defineConfig({
  testDir: './e2e',
  testMatch: 'monitor.live.spec.ts',
  timeout: 30_000,
  expect: {
    timeout: 5_000,
  },
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: 0,
  workers: 1,
  reporter: 'list',
  use: {
    baseURL,
    trace: 'retain-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
