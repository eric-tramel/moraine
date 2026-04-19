#!/usr/bin/env node
// Drives the moraine monitor at http://127.0.0.1:8080 in headless chromium
// and captures the screenshots embedded in house-style release notes.
//
// Usage:
//   node .claude/skills/release-notes/scripts/shoot-monitor.mjs \
//       "<session title substring>" [/tmp/moraine-shots]
//
// Output (in the given out-dir, default /tmp/moraine-shots):
//   01-sessions-landing.png
//   02-session-detail.png
//   03-session-flamegraph.png
//
// Preconditions:
//   - `moraine up` is running on the host.
//   - web/monitor/node_modules/playwright is installed (part of the
//     monitor's dev deps). If missing, run:
//       cd web/monitor && bun install --frozen-lockfile
//
// Written for the release-notes skill; see ../SKILL.md for the sensitivity
// review rules you owe the user before embedding these.

import { fileURLToPath } from 'node:url';
import { dirname, resolve as pathResolve } from 'node:path';
import { existsSync } from 'node:fs';

// Locate playwright relative to the repo root (this file is at
// .claude/skills/release-notes/scripts/shoot-monitor.mjs, so four parents up).
const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = pathResolve(here, '../../../..');
const playwrightEntry = pathResolve(
  repoRoot,
  'web/monitor/node_modules/playwright/index.mjs',
);
if (!existsSync(playwrightEntry)) {
  console.error(
    `playwright not found at ${playwrightEntry}. Run: cd web/monitor && bun install --frozen-lockfile`,
  );
  process.exit(1);
}
const { chromium } = await import(playwrightEntry);

const matchText = (process.argv[2] || '').toLowerCase();
const outDir = process.argv[3] || '/tmp/moraine-shots';
const monitorUrl = process.env.MORAINE_MONITOR_URL || 'http://127.0.0.1:8080/';

if (!matchText) {
  console.error(
    'usage: shoot-monitor.mjs <session title substring> [out-dir]',
  );
  console.error(
    'tip: pick a session whose title is already public (e.g. references a public PR #).',
  );
  process.exit(64);
}

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({
  viewport: { width: 1600, height: 1100 },
  deviceScaleFactor: 2,
});
const page = await ctx.newPage();

try {
  await page.goto(monitorUrl, { waitUntil: 'networkidle', timeout: 30_000 });
  await page.waitForSelector('text=Moraine Monitor', { timeout: 10_000 });
  // Analytics entrance animation + sessions list hydration.
  await page.waitForTimeout(3500);

  // --- 01: sessions landing ---
  await page.screenshot({
    path: `${outDir}/01-sessions-landing.png`,
    fullPage: false,
  });
  console.log('wrote 01-sessions-landing.png');

  // --- 02: click into the first session whose title includes matchText ---
  const clicked = await page.evaluate((needle) => {
    // Session cards are <button class="mv-card mv-card-library"> in the
    // current monitor build. Match on visible text.
    const cards = document.querySelectorAll(
      'button.mv-card.mv-card-library, button[class*="mv-card-library"]',
    );
    for (const el of cards) {
      const text = (el.textContent || '').toLowerCase();
      if (text.includes(needle)) {
        el.scrollIntoView({ block: 'center' });
        el.click();
        return text.slice(0, 120);
      }
    }
    return null;
  }, matchText);

  if (!clicked) {
    console.error(
      `no session card matched "${matchText}". check the sessions list and try a different substring.`,
    );
    process.exit(2);
  }
  console.log(`clicked session matching "${matchText}"`);

  // Detail view + flamegraph need time to fetch + render.
  await page.waitForTimeout(4500);
  await page.screenshot({
    path: `${outDir}/02-session-detail.png`,
    fullPage: false,
  });
  console.log('wrote 02-session-detail.png');

  // --- 03: switch to the flamegraph tab ---
  const flamed = await page.evaluate(() => {
    for (const el of document.querySelectorAll('button, [role="tab"], a')) {
      const t = (el.textContent || '').trim().toLowerCase();
      if (t === 'flamegraph' || t.includes('flamegraph')) {
        el.click();
        return t;
      }
    }
    return null;
  });
  if (!flamed) {
    console.error(
      'flamegraph tab not found — monitor build may have changed; skipping 03',
    );
  } else {
    await page.waitForTimeout(2500);
    await page.screenshot({
      path: `${outDir}/03-session-flamegraph.png`,
      fullPage: false,
    });
    console.log('wrote 03-session-flamegraph.png');
  }
} finally {
  await browser.close();
}
