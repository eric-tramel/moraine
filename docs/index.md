---
title: Moraine
hide:
  - navigation
  - toc
---

<main class="moraine-landing">
  <section class="moraine-hero" aria-labelledby="moraine-hero-title">
    <div class="moraine-hero__shell">
      <div class="moraine-hero__topline">
        <a class="moraine-brand" href="./index.html" aria-label="Moraine home">
          <span class="moraine-brand__mark" aria-hidden="true">
            <span></span>
            <span></span>
          </span>
          <span>Moraine</span>
        </a>
        <nav class="moraine-hero__nav" aria-label="Primary">
          <a href="./quickstart.html">Quickstart</a>
          <a href="./agent-mcp-search/index.html">MCP Search</a>
          <a href="https://github.com/eric-tramel/moraine">GitHub</a>
        </nav>
      </div>

      <div class="moraine-hero__grid">
        <div class="moraine-hero__copy">
          <p class="moraine-kicker">Local trace stack for agent work</p>
          <h1 id="moraine-hero-title">One trace stack across harnesses.</h1>
          <p class="moraine-hero__lede">
            Moraine watches agent session files, normalizes them into ClickHouse,
            serves a monitor UI, and exposes MCP tools so agents can search what
            happened before.
          </p>
          <div class="moraine-actions" aria-label="Get started">
            <a class="moraine-button moraine-button--primary" href="./quickstart.html">
              Quickstart
              <span aria-hidden="true">-&gt;</span>
            </a>
            <a class="moraine-button" href="./agent-mcp-search/index.html">
              MCP Search
              <span aria-hidden="true">-&gt;</span>
            </a>
          </div>
          <div class="moraine-hero__terminal" aria-label="Moraine startup commands">
            <span>&gt; moraine up</span>
            <span>&gt; watching session files...</span>
            <span>&gt; indexing to ClickHouse...</span>
            <span>&gt; MCP server ready</span>
          </div>
        </div>

        <div class="trace-window" aria-label="Moraine trace map">
          <div class="trace-window__chrome" aria-hidden="true">
            <span></span>
            <span></span>
            <span></span>
            <strong>Trace Stack</strong>
          </div>
          <div class="trace-map">
            <svg class="trace-lines" viewBox="0 0 760 420" aria-hidden="true" focusable="false">
              <path class="trace-line trace-line--cyan trace-line--dash" d="M176 82 C260 82 252 186 350 186" />
              <path class="trace-line trace-line--cyan trace-line--dash" d="M176 148 C246 148 268 196 350 196" />
              <path class="trace-line trace-line--green trace-line--dash" d="M176 214 C246 214 268 206 350 206" />
              <path class="trace-line trace-line--green trace-line--dash" d="M176 280 C260 280 252 216 350 216" />
              <path class="trace-line trace-line--cyan trace-line--dash" d="M176 346 C260 346 252 226 350 226" />
              <path class="trace-line trace-line--amber" d="M492 174 C574 174 574 92 650 92" />
              <path class="trace-line trace-line--green" d="M492 210 C574 210 574 210 650 210" />
              <path class="trace-line trace-line--cyan" d="M492 246 C574 246 574 328 650 328" />
            </svg>

            <div class="trace-column trace-column--sources">
              <p class="trace-label">Agent harnesses</p>
              <div class="trace-node"><span>Co</span>Codex</div>
              <div class="trace-node"><span>Cl</span>Claude Code</div>
              <div class="trace-node"><span>Ki</span>Kimi CLI</div>
              <div class="trace-node"><span>He</span>Hermes</div>
              <div class="trace-node"><span>Pi</span>Pi Coding Agent</div>
            </div>

            <div class="trace-core">
              <span class="trace-core__mark" aria-hidden="true">
                <span></span>
                <span></span>
              </span>
              <strong>Moraine</strong>
              <span>Ingest + Normalize</span>
              <span>Index + Search</span>
            </div>

            <div class="trace-column trace-column--outputs">
              <p class="trace-label">Outputs</p>
              <div class="trace-output trace-output--amber">
                <span class="trace-output__icon" aria-hidden="true"></span>
                ClickHouse
              </div>
              <div class="trace-output trace-output--green">
                <span class="trace-output__chart" aria-hidden="true"></span>
                Monitor UI
              </div>
              <div class="trace-output trace-output--cyan">
                <span class="trace-output__search" aria-hidden="true"></span>
                MCP Search
              </div>
            </div>
          </div>
        </div>
      </div>

      <div class="moraine-capabilities" aria-label="Core workflow">
        <article>
          <span class="moraine-icon moraine-icon--folder" aria-hidden="true"></span>
          <h2>Watch files</h2>
          <p>Detect new and updated session traces across local agent harnesses.</p>
        </article>
        <article>
          <span class="moraine-icon moraine-icon--database" aria-hidden="true"></span>
          <h2>Normalize events</h2>
          <p>Convert turns, tool calls, tokens, and timestamps into a shared schema.</p>
        </article>
        <article>
          <span class="moraine-icon moraine-icon--search" aria-hidden="true"></span>
          <h2>Search and open</h2>
          <p>Let agents retrieve the exact event, turn, or session they need.</p>
        </article>
        <article>
          <span class="moraine-icon moraine-icon--activity" aria-hidden="true"></span>
          <h2>Inspect live work</h2>
          <p>Use the monitor UI to see active sessions and indexing health.</p>
        </article>
      </div>
    </div>
  </section>

  <section class="moraine-section moraine-section--split" aria-labelledby="moraine-for-title">
    <div class="moraine-section__copy">
      <p class="moraine-kicker">Why it exists</p>
      <h2 id="moraine-for-title">A private record your tools can use.</h2>
      <p>
        Agent work is scattered across local JSONL files, terminal transcripts,
        tool calls, and harness-specific formats. Moraine keeps that history
        local, indexes it consistently, and gives both people and agents a way
        to inspect the same trace data.
      </p>
    </div>
    <div class="moraine-proof-grid">
      <article>
        <strong>For operators</strong>
        <p>Browse sessions, verify ingest health, and query the trace database directly.</p>
      </article>
      <article>
        <strong>For agents</strong>
        <p>Search earlier decisions, errors, commands, and active work through MCP.</p>
      </article>
      <article>
        <strong>For experiments</strong>
        <p>Build dashboards, evaluations, and retrieval workflows on normalized events.</p>
      </article>
    </div>
  </section>

  <section class="moraine-section moraine-start" aria-labelledby="moraine-start-title">
    <div>
      <p class="moraine-kicker">Start locally</p>
      <h2 id="moraine-start-title">Install, index, search.</h2>
      <p>
        The default stack runs on your machine and stores runtime state under
        <code>~/.moraine</code>. Connect your harness to <code>moraine run mcp</code>
        when you want agents to search prior sessions.
      </p>
      <div class="moraine-actions">
        <a class="moraine-button moraine-button--primary" href="./quickstart.html">
          Read the quickstart
          <span aria-hidden="true">-&gt;</span>
        </a>
        <a class="moraine-button" href="./agent-mcp-search/install.html">
          Connect a harness
          <span aria-hidden="true">-&gt;</span>
        </a>
      </div>
    </div>
    <pre class="moraine-code"><code>uv tool install moraine-cli
moraine up
moraine status
moraine run mcp</code></pre>
  </section>

  <section class="moraine-section moraine-harnesses" aria-labelledby="moraine-harnesses-title">
    <div>
      <p class="moraine-kicker">Supported sources</p>
      <h2 id="moraine-harnesses-title">Built for real agent harnesses.</h2>
    </div>
    <div class="moraine-harness-list" aria-label="Supported agent harnesses">
      <span>Codex</span>
      <span>Claude Code</span>
      <span>Cursor</span>
      <span>Kimi CLI</span>
      <span>Hermes</span>
      <span>Pi Coding Agent</span>
    </div>
  </section>
</main>
