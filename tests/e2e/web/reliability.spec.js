const fs = require('node:fs');
const { test, expect } = require('@playwright/test');

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`missing ${name}`);
  return value;
}

async function codeLogin(page, baseUrl, code) {
  await page.goto(baseUrl);
  await expect(page.locator('#login')).toBeVisible();
  await page.locator('#code').fill(code);
  await page.getByRole('button', { name: 'Enter' }).click();
  await expect(page.locator('#app')).toBeVisible();
}

test('real viewer converges with a TUI after an SSE disconnect', async ({ browser }) => {
  const baseUrl = required('HEL_BROWSER_BASE_URL');
  const code = required('HEL_BROWSER_CODE');
  const qrLoginUrl = required('HEL_BROWSER_QR_URL');
  const title = required('HEL_BROWSER_TITLE');
  const projectDirectory = required('HEL_BROWSER_PROJECT_DIRECTORY');
  const readyMarker = required('HEL_BROWSER_READY_MARKER');
  const changedMarker = required('HEL_TUI_CHANGED_MARKER');
  const tracePath = required('HEL_BROWSER_TRACE');
  const screenshotPath = required('HEL_BROWSER_SCREENSHOT');
  const stage = value => process.stdout.write(`browser-stage: ${value}\n`);

  // A protected conversation route must remain a login page while its
  // snapshot request is unauthorized; route restoration cannot dereference
  // an absent snapshot.
  const lockedContext = await browser.newContext({ ignoreHTTPSErrors: true });
  const lockedPage = await lockedContext.newPage();
  const lockedErrors = [];
  lockedPage.on('pageerror', error => lockedErrors.push(error.message));
  await lockedPage.goto(`${baseUrl}/#conversation/not-authenticated`);
  await expect(lockedPage.locator('#login')).toBeVisible();
  await expect.poll(() => lockedErrors).toEqual([]);
  await lockedContext.close();

  // Authentication secrets are intentionally kept out of Playwright traces.
  stage('qr-login');
  const qrContext = await browser.newContext({ ignoreHTTPSErrors: true });
  const qrPage = await qrContext.newPage();
  await qrPage.goto(qrLoginUrl);
  await expect(qrPage.locator('#app')).toBeVisible();
  await expect(qrPage).toHaveURL(baseUrl + '/');
  await qrContext.close();

  stage('code-login');
  const context = await browser.newContext({
    ignoreHTTPSErrors: true,
    viewport: { width: 390, height: 844 },
  });
  const page = await context.newPage();
  const responseErrors = [];
  page.on('console', message => {
    const text = message.text();
    if (text.includes('Unexpected end of JSON input')) responseErrors.push(text);
  });
  page.on('pageerror', error => {
    if (error.message.includes("reading 'sessions'")) responseErrors.push(error.message);
  });
  try {
    await codeLogin(page, baseUrl, code);
    await context.tracing.start({ screenshots: true, snapshots: true, sources: true });

    stage('snapshot-rendered');
    await expect(page.locator('#configured')).toContainText('fake');
    await expect(page.locator('#configured')).toContainText('1 targets · 1 bundles');
    await page.locator('#new-title').fill(title);
    await page.locator('#new-project-directory').fill(projectDirectory);
    await page.getByRole('button', { name: 'Start' }).click();
    stage('session-requested');

    const session = page.locator('.session').filter({ hasText: title });
    await expect(session).toContainText('running');
    stage('session-running');
    await session.getByRole('button', { name: 'Open' }).click();
    await expect(page.locator('#conversation-title')).toHaveText(title);
    await page.getByRole('button', { name: 'Dashboard' }).click();

    await context.setOffline(true);
    fs.writeFileSync(readyMarker, 'browser offline and ready\n');
    stage('offline-ready');
    await expect.poll(() => fs.existsSync(changedMarker)).toBe(true);
    await context.setOffline(false);

    await expect(session).toContainText('Saved · no worker running');
    await session.getByRole('button', { name: 'Resume' }).click();
    await expect(session).toContainText('running');
    await session.getByRole('button', { name: 'Finish' }).click();
    const finish = page.locator('#finish-dialog');
    await expect(finish).toContainText('finish the current work');
    await expect(finish).toContainText('selected project directory will remain unchanged');
    await finish.getByRole('button', { name: 'Stop worker and save' }).click();
    await expect(session).toContainText('Saved · no worker running');
    await expect(page.locator('#action-error')).toHaveText('');
    expect(responseErrors).toEqual([]);

    await context.tracing.stop({ path: tracePath });

    // An expired browser cookie must reveal the login form, and explicit
    // logout must do the same after a fresh authenticated session.
    const cookies = await context.cookies();
    const sessionCookie = cookies.find(cookie => cookie.httpOnly);
    expect(sessionCookie).toBeTruthy();
    await context.addCookies([{ ...sessionCookie, expires: 1 }]);
    await page.reload();
    await expect(page.locator('#login')).toBeVisible();
    await codeLogin(page, baseUrl, code);
    expect(responseErrors).toEqual([]);
    await page.getByRole('button', { name: 'Sign out' }).click();
    await expect(page.locator('#login')).toBeVisible();
  } catch (error) {
    await page.locator('#code').fill('').catch(() => {});
    await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
    await context.tracing.stop({ path: tracePath }).catch(() => {});
    throw error;
  } finally {
    await context.close();
  }
});
