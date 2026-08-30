const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: '.',
  testMatch: 'reliability.spec.js',
  fullyParallel: false,
  workers: 1,
  timeout: 60_000,
  expect: { timeout: 15_000 },
  reporter: [['line']],
  use: {
    browserName: 'chromium',
    headless: true,
    ignoreHTTPSErrors: true,
    trace: 'off',
  },
});
