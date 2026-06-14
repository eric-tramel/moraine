#!/usr/bin/env node
/*
 * Regenerate assets/monitor-desktop.png from qa/monitor-mock.html — a dark,
 * richly-populated mock of the Moraine Monitor used as the landing-page
 * product shot. Deterministic (seeded PRNG in the mock), so re-running gives
 * the same image. Run after editing qa/monitor-mock.html.
 */
const http = require("http");
const fs = require("fs");
const path = require("path");
const { chromium } = require(path.resolve(__dirname, "../../monitor/node_modules/playwright"));

const ROOT = path.resolve(__dirname, "..");
const MIME = { ".html": "text/html", ".css": "text/css", ".js": "text/javascript", ".svg": "image/svg+xml", ".png": "image/png", ".woff2": "font/woff2" };

(async () => {
  const srv = http.createServer((req, res) => {
    let u = decodeURIComponent(req.url.split("?")[0]);
    if (u === "/") u = "/qa/monitor-mock.html";
    fs.readFile(path.join(ROOT, u), (e, d) => {
      if (e) { res.writeHead(404); return res.end(); }
      res.writeHead(200, { "content-type": MIME[path.extname(u)] || "application/octet-stream" });
      res.end(d);
    });
  });
  await new Promise((r) => srv.listen(0, "127.0.0.1", r));
  const port = srv.address().port;

  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 1340, height: 900 }, deviceScaleFactor: 2 });
  await p.goto(`http://127.0.0.1:${port}/`, { waitUntil: "networkidle" });
  await p.waitForTimeout(350);
  const box = await p.evaluate(() => {
    document.body.style.width = "1340px";
    return { w: 1340, h: Math.ceil(document.body.getBoundingClientRect().height) };
  });
  await p.setViewportSize({ width: 1340, height: box.h });
  await p.waitForTimeout(150);
  const out = path.join(ROOT, "assets", "monitor-desktop.png");
  await p.screenshot({ path: out, clip: { x: 0, y: 0, width: 1340, height: box.h } });
  await b.close();
  srv.close();
  console.log(`wrote ${out} (${box.w}x${box.h} css, 2x → ${box.w * 2}x${box.h * 2})`);
})().catch((e) => { console.error(e); process.exit(1); });
