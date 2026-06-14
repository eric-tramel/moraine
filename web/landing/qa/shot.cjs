#!/usr/bin/env node
/*
 * Visual-QA screenshot harness for the Moraine landing page.
 *
 * Serves web/landing/ over a local HTTP server (so relative asset paths
 * resolve exactly as they will on the GitHub Pages project site) and captures
 * desktop + mobile, above-the-fold + full-page PNGs into web/landing/qa-shots/.
 *
 * Usage:  node web/landing/qa/shot.cjs [label]
 *   label  optional suffix for the output filenames (e.g. v1, v2)
 *
 * Reuses the Playwright + Chromium already vendored under web/monitor.
 */
const http = require("http");
const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "qa-shots");
const LABEL = process.argv[2] ? `-${process.argv[2]}` : "";

const { chromium } = require(path.resolve(
  __dirname,
  "../../monitor/node_modules/playwright",
));

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".woff2": "font/woff2",
  ".woff": "font/woff",
  ".ico": "image/x-icon",
  ".json": "application/json",
};

function serve(rootDir) {
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      let urlPath = decodeURIComponent(req.url.split("?")[0]);
      if (urlPath === "/") urlPath = "/index.html";
      const filePath = path.join(rootDir, urlPath);
      if (!filePath.startsWith(rootDir)) {
        res.writeHead(403);
        return res.end("forbidden");
      }
      fs.readFile(filePath, (err, data) => {
        if (err) {
          res.writeHead(404);
          return res.end("not found");
        }
        res.writeHead(200, {
          "content-type": MIME[path.extname(filePath)] || "application/octet-stream",
        });
        res.end(data);
      });
    });
    server.listen(0, "127.0.0.1", () => resolve(server));
  });
}

const VIEWPORTS = [
  { name: "desktop", width: 1440, height: 900, dpr: 2 },
  { name: "mobile", width: 390, height: 844, dpr: 3 },
];

(async () => {
  fs.mkdirSync(OUT, { recursive: true });
  const server = await serve(ROOT);
  const { port } = server.address();
  const base = `http://127.0.0.1:${port}/`;
  console.log(`serving ${ROOT} at ${base}`);

  const browser = await chromium.launch();
  const written = [];
  for (const vp of VIEWPORTS) {
    const ctx = await browser.newContext({
      viewport: { width: vp.width, height: vp.height },
      deviceScaleFactor: vp.dpr,
      reducedMotion: "reduce", // freeze animations for stable diffs
    });
    const page = await ctx.newPage();
    await page.goto(base, { waitUntil: "networkidle" });
    await page.waitForTimeout(400);

    const fold = path.join(OUT, `${vp.name}-fold${LABEL}.png`);
    await page.screenshot({ path: fold });
    written.push(fold);

    const full = path.join(OUT, `${vp.name}-full${LABEL}.png`);
    await page.screenshot({ path: full, fullPage: true });
    written.push(full);

    await ctx.close();
  }
  await browser.close();
  server.close();
  console.log("wrote:\n" + written.map((w) => "  " + w).join("\n"));
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
