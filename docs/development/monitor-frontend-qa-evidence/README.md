# Monitor Frontend QA Evidence

Playwright evidence for issues #352, #353, and #354.

The sandbox fixture used for these captures intentionally had empty `24h` and `7d`
analytics windows and a populated `30d` window, so the live monitor test exercises
the analytics range fallback path.

See `summary.json` for the captured viewport overflow metrics and seeded range
shape.
