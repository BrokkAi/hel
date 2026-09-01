const fs = require('node:fs');
const { test, expect } = require('@playwright/test');

function escapeForRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

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
  // The login token must not survive in the URL, and the viewer lands on a
  // workspace rather than on a bare path.
  await expect(qrPage).toHaveURL(new RegExp('^' + escapeForRegExp(baseUrl) + '/(#workspace/.+)?$'));
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
    // The dashboard opens on a workspace, and the workspace is in the URL.
    await expect(page.locator('#workspaces .tab')).toHaveCount(1);
    await expect(page).toHaveURL(/#workspace\//);
    const workspaceHash = new URL(page.url()).hash;

    // Quota is a page reached from the menu, not a card on the dashboard.
    await page.locator('#menu-button').click();
    await page.getByRole('menuitem', { name: 'Quota' }).click();
    await expect(page).toHaveURL(/#quota$/);
    await expect(page.locator('#quota')).toContainText('fake');
    await page.getByRole('button', { name: 'Back' }).click();
    await expect(page).toHaveURL(new RegExp(escapeForRegExp(workspaceHash) + '$'));

    // The New flow asks one thing per screen and reviews before committing.
    await page.getByRole('button', { name: 'New session' }).click();
    await expect(page).toHaveURL(/\/new$/);
    await expect(page.locator('#new-progress')).toContainText('Profile');
    await page.getByRole('button', { name: 'Next' }).click();
    await expect(page.locator('#new-progress')).toContainText('Target');
    await page.getByRole('button', { name: 'Next' }).click();
    // The lab's only target is bare, so the project step asks for a directory
    // rather than offering a bundle.
    await expect(page.locator('#new-project-directory')).toBeVisible();
    await page.locator('#new-project-directory').fill(projectDirectory);
    await page.locator('#new-title').fill(title);
    await page.getByRole('button', { name: 'Next' }).click();
    await expect(page.locator('#new-progress')).toContainText('Review');
    await expect(page.locator('.review')).toContainText(title);
    await page.getByRole('button', { name: 'Start' }).click();
    stage('session-requested');

    // Scoped to the dashboard: the resume page renders session cards too, and
    // a hidden page's nodes are still in the document.
    const session = page.locator('#sessions .session').filter({ hasText: title });
    await expect(session).toContainText('running');
    stage('session-running');
    await session.getByRole('button', { name: 'Open' }).click();
    await expect(page).toHaveURL(/#conversation\//);
    await expect(page.locator('#conversation-title')).toHaveText(title);
    // The browser's own Back button returns to the dashboard rather than
    // leaving the application.
    await page.goBack();
    await expect(page.locator('#dashboard')).toBeVisible();

    await context.setOffline(true);
    fs.writeFileSync(readyMarker, 'browser offline and ready\n');
    stage('offline-ready');
    await expect.poll(() => fs.existsSync(changedMarker)).toBe(true);
    await context.setOffline(false);

    // A stopped session leaves the dashboard: it belongs to the resume flow,
    // which is where a person can do something about it.
    await expect(session).toHaveCount(0);
    await page.getByRole('button', { name: 'Resume a session' }).click();
    await expect(page).toHaveURL(/\/resume$/);
    const resumable = page.locator('#resumable .session').filter({ hasText: title });
    await expect(resumable).toBeVisible();
    await resumable.getByRole('button', { name: 'Resume' }).click();
    await page.getByRole('button', { name: 'Back' }).click();
    await expect(session).toContainText('running');
    // A stop needs the daemon's session manager to have adopted the session,
    // and adoption is asynchronous, so a stop issued moments after a resume can
    // fail with "is not managed". The terminal surface offers Retry stop for
    // exactly this; the phone leaves the button in place and marks the session
    // as needing attention, so retrying is what a person would do. The stop
    // itself then checkpoints and tears down a target, which takes materially
    // longer than a snapshot round trip.
    for (let attempt = 0; attempt < 4; attempt += 1) {
      if ((await session.count()) === 0) break;
      const stop = session.getByRole('button', { name: 'Stop' });
      if ((await stop.count()) === 0) {
        await page.waitForTimeout(1000);
        continue;
      }
      page.once('dialog', dialog => dialog.accept());
      await stop.click();
      await session
        .waitFor({ state: 'detached', timeout: 45_000 })
        .catch(() => {});
    }
    await expect(session).toHaveCount(0);
    // A stop that loses the adoption race fails after it was accepted, so its
    // reason never travels in the response: it reaches the phone as the
    // session's attention state and nothing else. This asserts that, and with
    // it that the controller's own wording — which names sessions and
    // workers — stays on the controller.
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
    await page.locator('#menu-button').click();
    await page.getByRole('menuitem', { name: 'Sign out' }).click();
    await expect(page.locator('#login')).toBeVisible();
  } catch (error) {
    await page
      .locator('#code')
      .fill('')
      .catch(() => {});
    await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
    await context.tracing.stop({ path: tracePath }).catch(() => {});
    throw error;
  } finally {
    await context.close();
  }
});
