#!/usr/bin/env node
/* Render web/landing/qa/og.html to a 1200x630 social card at assets/og.png. */
const path = require("path");
const { chromium } = require(path.resolve(__dirname, "../../monitor/node_modules/playwright"));

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1200, height: 630 }, deviceScaleFactor: 2 });
  await page.goto("file://" + path.resolve(__dirname, "og.html"), { waitUntil: "networkidle" });
  await page.waitForTimeout(200);
  const out = path.resolve(__dirname, "../assets/og.png");
  await page.screenshot({ path: out, clip: { x: 0, y: 0, width: 1200, height: 630 } });
  await browser.close();
  console.log("wrote " + out);
})().catch((e) => { console.error(e); process.exit(1); });
