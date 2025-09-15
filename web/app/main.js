(() => {
  "use strict";

  // ====== Config & helpers ======
  const API_BASE_KEY = "helpbot_api_base";
  const ADMIN_PHONE  = "5348929202";
  const HEALTH_TTL_MS = 10_000; // re-check health every 10s
  const FETCH_TIMEOUT_MS = 6000;

  const $  = (s) => document.querySelector(s);
  const chatEl   = () => $("#chat");
  const inputEl  = () => $("#q");
  const resultEl = () => $("#result");
  const errorEl  = () => $("#error");
  const statusEl = () => $("#status");
  const apiBaseInput = () => $("#api-base");
  const apiPing = () => $("#api-ping");
  const userInfoEl = () => $("#user-info");

  // state for health
  let lastHealth = { base:null, ok:false, ts:0, rows:0 };

  function getApiBase(){
    return (localStorage.getItem(API_BASE_KEY) || (apiBaseInput()?.value || "http://192.168.0.109:8000")).replace(/\/$/, "");
  }
  function setApiBase(v){
    localStorage.setItem(API_BASE_KEY, v);
    if (apiBaseInput()) apiBaseInput().value = v;
    lastHealth.base = v;
    lastHealth.ok = false;
    lastHealth.ts = 0;
    apiPing().textContent = ""; // will refresh on next probe
  }
  function setStatus(msg) { if (statusEl()) statusEl().textContent = msg || ""; }
  function setError(msg)  { if (errorEl())  errorEl().textContent  = msg || ""; }

  function escapeHtml(x) {
    return String(x ?? "")
      .replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
  }
  function sec1960ToISO(sec) {
    const base = new Date(Date.UTC(1960,0,1));
    const d = new Date(base.getTime() + (Number(sec)||0)*1000);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth()+1).padStart(2,"0");
    const day = String(d.getUTCDate()).padStart(2,"0");
    return `${y}-${m}-${day}`;
  }

  // ====== Minimal Puter auth badge (optional) ======
  async function ensurePuterReady(timeoutMs = 6000) {
    const t0 = Date.now();
    while (!(window.puter && puter.auth)) {
      await new Promise(r => setTimeout(r, 50));
      if (Date.now() - t0 > timeoutMs) break;
    }
  }
  async function getPuterAccount() {
    if (!window.puter || !puter.auth) return null;
    try { const u1 = await puter.auth.me?.();      if (u1 && (u1.username || u1.email)) return u1; } catch {}
    try { const u2 = await puter.auth.getUser?.(); if (u2 && (u2.username || u2.email)) return u2; } catch {}
    return null;
  }
  async function signInFlow() {
    await ensurePuterReady().catch(()=>{});
    const me0 = await getPuterAccount();
    if (me0) return me0;
    try { if (puter.auth?.signIn) {
      await puter.auth.signIn({ mode: "popup", redirectTo: location.href });
    }} catch {}
    return null;
  }
  async function signOutFlow() { try {
    await ensurePuterReady().catch(()=>{});
    if (puter.auth?.signOut) await puter.auth.signOut();
    else if (puter.auth?.logout) await puter.auth.logout();
  } catch {} }

  // ====== Network helpers (timeout + health) ======
  async function fetchWithTimeout(url, opts={}, timeoutMs=FETCH_TIMEOUT_MS) {
    const ctrl = new AbortController();
    const id = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const r = await fetch(url, { ...opts, signal: ctrl.signal });
      return r;
    } finally {
      clearTimeout(id);
    }
  }

  function candidateBases() {
    const saved = localStorage.getItem(API_BASE_KEY);
    const ui = (apiBaseInput()?.value || "").trim();
    const host = location.hostname || "localhost";
    const list = [];
    if (saved) list.push(saved);
    if (ui && /^https?:\/\//.test(ui)) list.push(ui);
    list.push(`http://${host}:8000`);              // same host, API port
    list.push(`http://192.168.0.109:8000`);        // your Mac M4 default
    list.push(`http://localhost:8000`);            // local fallback
    return [...new Set(list)].map(s => s.replace(/\/$/,""));
  }

  async function probe(base) {
    try {
      apiPing().textContent = "ping…";
      const r = await fetchWithTimeout(base + "/health", { method:"GET" }, 3000);
      const ok = r.ok;
      let rows = 0;
      if (ok) {
        const t = await r.text();
        try {
          const j = JSON.parse(t);
          rows = j?.rows || 0;
        } catch {}
      }
      return { ok, base, rows };
    } catch {
      return { ok:false, base, rows:0 };
    } finally {
      // ui ping set by caller
    }
  }

  async function autoPickApiBase() {
    const list = candidateBases();
    for (const b of list) {
      const res = await probe(b);
      if (res.ok) {
        setApiBase(b);
        lastHealth = { base:b, ok:true, ts:Date.now(), rows: res.rows|0 };
        apiPing().textContent = "API ✓";
        return b;
      }
    }
    // none worked; keep current but flag
    const fallback = getApiBase();
    lastHealth = { base:fallback, ok:false, ts:Date.now(), rows:0 };
    apiPing().textContent = "API ?";
    return null;
  }

  async function ensureHealthyApi(force=false) {
    const now = Date.now();
    const base = getApiBase();
    const stale = (now - (lastHealth.ts||0)) > HEALTH_TTL_MS || lastHealth.base !== base || force;
    if (!stale && lastHealth.ok) {
      apiPing().textContent = "API ✓";
      return base;
    }
    const res = await probe(base);
    lastHealth = { base, ok:res.ok, ts:Date.now(), rows: res.rows|0 };
    apiPing().textContent = res.ok ? "API ✓" : "API ?";
    if (!res.ok) {
      // try auto-pick among candidates
      const picked = await autoPickApiBase();
      if (!picked) throw new Error("API is unreachable. Check that the server is running and reachable on your LAN.");
      return picked;
    }
    return base;
  }

  async function postJSON(url, body) {
    try {
      await ensureHealthyApi();
    } catch (e) {
      throw new Error(e?.message || "API health check failed");
    }
    let r, t;
    try {
      r = await fetchWithTimeout(url, { method: "POST", headers: { "content-type":"application/json" }, body: JSON.stringify(body || {}) });
      t = await r.text();
    } catch (e) {
      // auto re-detect once and retry
      await autoPickApiBase();
      const base = getApiBase();
      r = await fetchWithTimeout(url.replace(/^https?:\/\/[^/]+/, base), { method: "POST", headers: { "content-type":"application/json" }, body: JSON.stringify(body || {}) });
      t = await r.text();
    }
    let j; try { j = JSON.parse(t); } catch { throw new Error(`${url} returned non-JSON: ${t.slice(0,160)}...`); }
    if (!r.ok) throw new Error(j.error || `HTTP ${r.status}`);
    return j;
  }

  const tools = {
    parse_date_range: (text) => postJSON(getApiBase() + "/tools/parse_date_range", { text }),
    lookup_company:   (name) => postJSON(getApiBase() + "/tools/lookup_company",   { name }),
    lookup_mudurluk:  (name) => postJSON(getApiBase() + "/tools/lookup_mudurluk",  { name }),
  };
  async function apiSearch(filters, limit=40) {
    return postJSON(getApiBase() + "/search", { filters, limit });
  }

  // ====== Guided chat state machine ======
  const session = resetSession();

  function resetSession() {
    return {
      step: "welcome",     // welcome -> date -> company -> city -> confirm
      date_from: null,
      date_to: null,
      company_code: null,
      company_label: null,
      city_code: null,
      city_label: null,
      awaitingPick: null,  // "company" | "city" for ambiguity choices
    };
  }

  // Casual/open-chat detection: keep it simple
  function isCasual(text) {
    const s = (text || "").trim().toLowerCase();
    if (!s) return false;
    const bad = ["nasılsın","naber","hava","fiyat","sohbet","konuş","espri","şaka","politik","siyaset","kimdir","nedir","ne haber","how are you"];
    return bad.some(w => s.includes(w));
  }

  // ====== Chat rendering ======
  function addMsg(role, html) {
    const div = document.createElement("div");
    div.className = "msg " + (role === "user" ? "user" : "bot");
    const b = document.createElement("div");
    b.className = "bubble";
    b.innerHTML = html;
    div.appendChild(b);
    chatEl().appendChild(div);
    chatEl().scrollTop = chatEl().scrollHeight;
  }
  function addOptions(options, handler) {
    const row = document.createElement("div");
    row.className = "options";
    options.forEach(opt => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "chip";
      btn.textContent = opt.label;
      btn.addEventListener("click", () => handler(opt));
      row.appendChild(btn);
    });
    chatEl().appendChild(row);
    chatEl().scrollTop = chatEl().scrollHeight;
  }

  // ====== Steps ======
  function askWelcome() {
    addMsg("bot", `
      <strong>Welcome!</strong><br/>
      Only <em>basic search</em> is available: we’ll gather <strong>date range → company → city</strong> and run a query.<br/>
      Open conversation is not permitted here. For help call <strong>${ADMIN_PHONE}</strong>.<br/><br/>
      <strong>Step 1 — Date range:</strong> Please enter a date range.<br/>
      Examples: <code>son 30 gün</code>, <code>2024-01..2024-03</code>, <code>Ocak 2025</code>, <code>2019</code>.
    `);
    session.step = "date";
  }
  async function handleDate(input) {
    const txt = input.trim();
    if (isCasual(txt)) {
      addMsg("bot", `Open conversation is not permitted. Please provide a <strong>date range</strong> (e.g., <code>son 30 gün</code>, <code>Ocak 2025</code>).`);
      return;
    }
    setStatus("parsing date…");
    try {
      const r = await tools.parse_date_range(txt);
      if (r?.status === "ok" && r.range) {
        session.date_from = r.range.from;
        session.date_to   = r.range.to;
        addMsg("bot", `✅ Date set: <code>${escapeHtml(session.date_from)}</code> → <code>${escapeHtml(session.date_to)}</code><br/><br/><strong>Step 2 — Company (optional):</strong> Enter a company name or type <code>skip</code>.`);
        session.step = "company";
      } else {
        addMsg("bot", `I couldn’t parse that date. Try: <code>son 30 gün</code>, <code>2024-01..2024-03</code>, <code>Ocak 2025</code>, <code>2019</code>.`);
      }
    } catch (e) {
      addMsg("bot", `Date tool error: ${escapeHtml(e.message||e)}. Trying to re-detect API…`);
      try { await autoPickApiBase(); } catch {}
      addMsg("bot", `Please enter the date again.`);
    } finally { setStatus(""); }
  }
  async function handleCompany(input) {
    const txt = input.trim();
    if (isCasual(txt)) {
      addMsg("bot", `Open conversation is not permitted. Please provide a <strong>company</strong> or type <code>skip</code>.`);
      return;
    }
    if (!txt || /^skip$/i.test(txt) || /^yok|atla|gec$/i.test(txt)) {
      session.company_code = null;
      session.company_label = null;
      addMsg("bot", `Skipping company.<br/><br/><strong>Step 3 — City (required):</strong> Enter a city name (e.g., İzmir, Denizli).`);
      session.step = "city";
      return;
    }
    setStatus("resolving company…");
    try {
      const r = await tools.lookup_company(txt);
      if (r?.status === "ok" && typeof r.code !== "undefined") {
        session.company_code = r.code;
        session.company_label = r.name || txt;
        addMsg("bot", `✅ Company set: <code>${escapeHtml(session.company_label)}</code><br/><br/><strong>Step 3 — City (required):</strong> Enter a city name (e.g., İzmir, Denizli).`);
        session.step = "city";
      } else if (r?.status === "ambiguous" && Array.isArray(r.options) && r.options.length) {
        addMsg("bot", `Multiple matches found. Please pick one:`);
        session.awaitingPick = "company";
        addOptions(r.options.slice(0,6).map(o => ({
          label: o.name || String(o.code),
          value: o.code
        })), (opt) => {
          session.company_code = opt.value;
          session.company_label = opt.label;
          addMsg("bot", `✅ Company set: <code>${escapeHtml(session.company_label)}</code><br/><br/><strong>Step 3 — City (required):</strong> Enter a city name (e.g., İzmir, Denizli).`);
          session.awaitingPick = null;
          session.step = "city";
        });
      } else {
        addMsg("bot", `I couldn’t resolve that company. Try another name or type <code>skip</code>.`);
      }
    } catch (e) {
      addMsg("bot", `Company tool error: ${escapeHtml(e.message||e)}. Trying to re-detect API…`);
      try { await autoPickApiBase(); } catch {}
      addMsg("bot", `Please enter the company again or type <code>skip</code>.`);
    } finally { setStatus(""); }
  }
  async function handleCity(input) {
    const txt = input.trim();
    if (isCasual(txt)) {
      addMsg("bot", `Open conversation is not permitted. Please provide a <strong>city</strong> (e.g., İzmir, Denizli).`);
      return;
    }
    setStatus("resolving city…");
    try {
      const r = await tools.lookup_mudurluk(txt);
      if (r?.status === "ok" && typeof r.code === "number") {
        session.city_code = r.code;
        session.city_label = r.name || txt;
        addMsg("bot", `✅ City set: <code>${escapeHtml(session.city_label)}</code><br/><br/><strong>Running search…</strong>`);
        session.step = "confirm";
        await runSearchNow();
      } else if (r?.status === "ambiguous" && Array.isArray(r.options) && r.options.length) {
        addMsg("bot", `Multiple cities found. Please pick one:`);
        session.awaitingPick = "city";
        addOptions(r.options.slice(0,6).map(o => ({
          label: o.name || String(o.code),
          value: o.code
        })), async (opt) => {
          session.city_code = opt.value;
          session.city_label = opt.label;
          addMsg("bot", `✅ City set: <code>${escapeHtml(session.city_label)}</code><br/><br/><strong>Running search…</strong>`);
          session.awaitingPick = null;
          session.step = "confirm";
          await runSearchNow();
        });
      } else {
        addMsg("bot", `I couldn’t resolve that city. Please try again (e.g., İzmir, Denizli).`);
      }
    } catch (e) {
      addMsg("bot", `City tool error: ${escapeHtml(e.message||e)}. Trying to re-detect API…`);
      try { await autoPickApiBase(); } catch {}
      addMsg("bot", `Please enter the city again.`);
    } finally { setStatus(""); }
  }

  async function runSearchNow() {
    if (!session.city_code) {
      addMsg("bot", `City is required for search. Please provide a city.`);
      session.step = "city";
      return;
    }
    const filters = {
      date_from: session.date_from,
      date_to:   session.date_to,
      city_code: session.city_code,
      type_code: null,
      company_code: session.company_code,
    };
    try {
      setStatus("searching…");
      const data = await apiSearch(filters, 40);
      renderResults(data?.hits || []);
      addMsg("bot", `Done. You can type <code>new</code> to start a new search.`);
      session.step = "done";
    } catch (e) {
      addMsg("bot", `Search failed: ${escapeHtml(e.message||e)}. Trying to re-detect API…`);
      try { await autoPickApiBase(); } catch {}
      addMsg("bot", `Type <code>new</code> to try again.`);
      session.step = "done";
    } finally { setStatus(""); }
  }

let lastRows = [];

function renderResults(rows) {
  // Keep a copy of the raw server rows
  lastRows = Array.isArray(rows) ? rows.slice() : [];
  applyClientFiltersSort();
}

function getUiFilters() {
  const get = (sel) => (document.querySelector(sel)?.value || "").trim();
  return {
    company: get("#flt-company").toLowerCase(),
    city:    get("#flt-city").toLowerCase(),
    type:    get("#flt-type").toLowerCase(),
    text:    get("#flt-text").toLowerCase(),
    from:    get("#flt-date-from"),
    to:      get("#flt-date-to"),
    sortBy:  get("#sort-by") || "date_int",
    sortDir: get("#sort-dir") || "desc",
  };
}

function sortRows(rows, sortBy, dir) {
  const mul = (dir === "asc") ? 1 : -1;
  const norm = (v) => (typeof v === "string" ? v.toLowerCase() : v);
  return rows.slice().sort((a, b) => {
    let va, vb;
    switch (sortBy) {
      case "ad_id":
        va = a.ad_id || 0; vb = b.ad_id || 0; break;
      case "company":
        va = a.company || ""; vb = b.company || ""; break;
      case "city":
        va = a.city || ""; vb = b.city || ""; break;
      case "type":
        va = a.type || ""; vb = b.type || ""; break;
      case "date_int":
      default:
        va = a.date_int || 0; vb = b.date_int || 0; break;
    }
    va = norm(va); vb = norm(vb);
    if (va < vb) return -1 * mul;
    if (va > vb) return  1 * mul;
    return 0;
  });
}

function applyClientFiltersSort() {
  if (!resultEl()) return;

  if (!lastRows || lastRows.length === 0) {
    resultEl().innerHTML = `<div class="muted">No records.</div>`;
    return;
  }

  const f = getUiFilters();
  const fromMs = f.from ? Date.parse(f.from) : NaN;
  const toMs   = f.to   ? Date.parse(f.to)   : NaN;

  const filtered = lastRows.filter(r => {
    if (f.company && !String(r.company || "").toLowerCase().includes(f.company)) return false;
    if (f.city    && !String(r.city    || "").toLowerCase().includes(f.city))     return false;
    if (f.type    && !String(r.type    || "").toLowerCase().includes(f.type))     return false;

    if (!Number.isNaN(fromMs) || !Number.isNaN(toMs)) {
      const ms = Date.parse(sec1960ToISO(r.date_int || 0));
      if (!Number.isNaN(fromMs) && ms < fromMs) return false;
      if (!Number.isNaN(toMs)   && ms > toMs)   return false;
    }

    if (f.text) {
      const hay = `${r.ad_id} ${sec1960ToISO(r.date_int||0)} ${r.company||""} ${r.type||""} ${r.city||""}`.toLowerCase();
      if (!hay.includes(f.text)) return false;
    }
    return true;
  });

  const sorted = sortRows(filtered, f.sortBy, f.sortDir);
  drawTable(sorted, lastRows.length);
}

function drawTable(rows, totalCount) {
  const badge = `<div class="muted" style="margin:6px 0;">Showing ${rows.length} of ${totalCount}.</div>`;
  if (!rows || rows.length === 0) {
    resultEl().innerHTML = badge + `<div class="muted">No records.</div>`;
    return;
  }

  const head = `
    <thead>
      <tr>
        <th>Ad ID</th>
        <th>Date</th>
        <th>City</th>
        <th>Type</th>
        <th>Company</th>
      </tr>
    </thead>
  `;

  const body = rows.map(r => {
    const iso = sec1960ToISO(r.date_int || 0);
    return `
      <tr>
        <td>${escapeHtml(String(r.ad_id || ""))}</td>
        <td>${escapeHtml(iso)}</td>
        <td>${escapeHtml(String(r.city || ""))}</td>
        <td>${escapeHtml(String(r.type || ""))}</td>
        <td>${escapeHtml(String(r.company || ""))}</td>
      </tr>
    `;
  }).join("");

  resultEl().innerHTML = badge + `<table class="grid">${head}<tbody>${body}</tbody></table>`;
}


  // ====== Controller ======
  async function onAsk(e) {
    e?.preventDefault?.();
    setError("");
    const val = (inputEl()?.value || "").trim();
    if (!val) return;
    addMsg("user", escapeHtml(val));
    inputEl().value = "";

    // quick health badge refresh (non-blocking)
    ensureHealthyApi().catch(()=>{});

    // 'new' resets flow
    if (/^new|yeni|baştan|reset$/i.test(val)) {
      while (chatEl().firstChild) chatEl().removeChild(chatEl().firstChild);
      Object.assign(session, resetSession());
      askWelcome();
      return;
    }

    // If user is picking from options, ignore free text
    if (session.awaitingPick) {
      addMsg("bot", `Please pick one of the options shown above.`);
      return;
    }

    switch (session.step) {
      case "welcome":
        askWelcome();
        break;
      case "date":
        await handleDate(val);
        break;
      case "company":
        await handleCompany(val);
        break;
      case "city":
        await handleCity(val);
        break;
      case "confirm":
      case "done":
      default:
        addMsg("bot", `Open conversation is not permitted. Type <code>new</code> to start a new search.`);
        break;
    }
  }

  // ====== Boot ======
  document.addEventListener("DOMContentLoaded", async () => {
    // Pre-fill API input with default
    if (apiBaseInput() && !apiBaseInput().value) apiBaseInput().value = "http://192.168.0.109:8000";
    const saved = localStorage.getItem(API_BASE_KEY);
    if (saved) { apiBaseInput().value = saved; }

    $("#api-save")?.addEventListener("click", () => {
      const v = (apiBaseInput()?.value || "").trim();
      if (!/^https?:\/\//.test(v)) { setError("Enter full API URL, e.g., http://192.168.0.109:8000"); return; }
      setApiBase(v);
      apiPing().textContent = "saved ✓";
      setTimeout(()=> apiPing().textContent="", 1200);
    });

    // Try auto pick on load
    await autoPickApiBase();

    // Auth buttons (optional badge)
    $("#sign-in-btn")?.addEventListener("click", async () => {
      setError("");
      try { await signInFlow(); const me = await getPuterAccount(); userInfoEl().textContent = me ? `Signed in: ${me.username || me.email || me.id}` : "Not signed in"; }
      catch (e) { setError("Sign-in failed: " + (e.message || e)); }
    });
    $("#sign-out-btn")?.addEventListener("click", async () => {
      try { await signOutFlow(); userInfoEl().textContent = "Not signed in"; }
      catch (e) { setError("Sign-out failed: " + (e.message || e)); }
    });

    // Wire form
    $("#ask-form")?.addEventListener("submit", onAsk);
    $("#ask-btn")?.addEventListener("click", onAsk);

    // Live re-filter / re-sort when the user types or changes controls
    const wire = (sel) => {
      const el = document.querySelector(sel);
      if (!el) return;
      el.addEventListener("input",  () => applyClientFiltersSort());
      el.addEventListener("change", () => applyClientFiltersSort());
    };
    ["#flt-company","#flt-city","#flt-type","#flt-date-from","#flt-date-to","#flt-text","#sort-by","#sort-dir"]
      .forEach(wire);
    
    // Start the guided flow
    askWelcome();
  });
})();
