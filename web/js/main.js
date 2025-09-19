// main.js — humanlike agent chat + global filter/sort + 100/page + status lights

// API base helper (reads from localStorage 'apiBase' or uses '/')
function api(path) {
  const stored = (localStorage.getItem('apiBase') || '/').trim();
  let base = stored === '' ? '/' : stored;
  if (base.endsWith('/')) base = base.slice(0, -1);
  return base + path;
}

// Bind API base controls if present
function bindApiControls() {
  const inp = document.getElementById('api-base');
  const save = document.getElementById('api-save');
  const ping = document.getElementById('api-ping');
  if (inp) inp.value = localStorage.getItem('apiBase') || '/';
  if (save) save.addEventListener('click', () => {
    const v = (inp.value || '/').trim();
    localStorage.setItem('apiBase', v || '/');
    pingServer(); // re-ping with new base
  });
  if (ping) ping.addEventListener('click', () => pingServer());
}

// ─────────────────────────────────────────────
// Status lights: server + Puter.js
// ─────────────────────────────────────────────
async function pingServer() {
  const dot = document.getElementById('server-dot');
  const label = document.getElementById('server-status');
  try {
    const res = await fetch(api('/health'), { cache: 'no-store' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const j = await res.json();
    dot.classList.remove('bad', 'warn'); dot.classList.add('ok');
    label.textContent = `up · ${j.rows ?? 0} rows`;
    return true;
  } catch (e) {
    dot.classList.remove('ok', 'warn'); dot.classList.add('bad');
    label.textContent = 'down';
    return false;
  }
}

function checkPuterReady() {
  const dot = document.getElementById('puter-dot');
  const label = document.getElementById('puter-status');
  const hasSDK = !!(window.Puter || window.puter);
  if (hasSDK) {
    dot.classList.remove('bad'); dot.classList.add('ok');
    label.textContent = 'sdk loaded';
    return true;
  } else {
    dot.classList.remove('ok'); dot.classList.add('bad');
    label.textContent = 'not loaded';
    return false;
  }
}

// ─────────────────────────────────────────────
// Global dataset + filtering/sorting/paging
// ─────────────────────────────────────────────
const STATE = {
  allHits: [],   // full dataset we fetched
  viewHits: [],  // filtered + sorted
  page: 1,
  pageSize: 100,
  sortBy: 'date_int',
  sortDir: 'desc',
  filterText: ''
};

function applyFilterAndSort() {
  const t = (STATE.filterText || '').trim().toLowerCase();
  let arr = STATE.allHits.slice();

  if (t) {
    arr = arr.filter(h =>
      String(h.company || '').toLowerCase().includes(t) ||
      String(h.city || '').toLowerCase().includes(t) ||
      String(h.type || '').toLowerCase().includes(t) ||
      String(h.ad_id || '').toLowerCase().includes(t)
    );
  }

  const key = STATE.sortBy;
  const dir = STATE.sortDir === 'desc' ? -1 : 1;
  arr.sort((a, b) => {
    const va = a[key]; const vb = b[key];
    if (va === vb) return 0;
    return (va > vb ? 1 : -1) * dir;
  });

  STATE.viewHits = arr;
  STATE.page = 1;
  renderPage();
}

function renderPage() {
  const total = STATE.viewHits.length;
  const pageSize = STATE.pageSize;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  if (STATE.page > pageCount) STATE.page = pageCount;

  const start = (STATE.page - 1) * pageSize;
  const end = Math.min(start + pageSize, total);
  const subset = STATE.viewHits.slice(start, end);

  const el = document.getElementById('result');
  el.innerHTML = renderResultsTable(subset);

  const info = document.getElementById('pager-info');
  if (info) info.textContent = `${start + 1}-${end} / ${total} (sayfa ${STATE.page}/${pageCount})`;
  const prev = document.getElementById('prev-page');
  const next = document.getElementById('next-page');
  if (prev) prev.disabled = STATE.page <= 1;
  if (next) next.disabled = STATE.page >= pageCount;
}

function renderResultsTable(rows) {
  if (!rows || rows.length === 0) {
    return '<div class="muted">Sonuç yok.</div>';
  }
  const th = `<tr>
      <th>Tarih</th><th>Şehir</th><th>Tür</th><th>Şirket</th><th>Ad ID</th>
    </tr>`;
  const trs = rows.map(h => {
    // Convert seconds since 1960-01-01 to ISO date
    const epoch1960 = Date.UTC(1960, 0, 1) / 1000;
    const iso = h.date_int ? new Date((h.date_int + epoch1960) * 1000).toISOString().slice(0, 10) : '';
    return `<tr>
      <td class="nowrap">${iso}</td>
      <td>${h.city || ''}</td>
      <td>${h.type || ''}</td>
      <td>${h.company || ''}</td>
      <td class="nowrap">${h.ad_id || ''}</td>
    </tr>`;
  }).join('');
  return `<table><thead>${th}</thead><tbody>${trs}</tbody></table>`;
}

function bindListControls() {
  const ft = document.getElementById('filter-text');
  if (ft) {
    let to = null;
    ft.addEventListener('input', (e) => {
      STATE.filterText = e.target.value;
      clearTimeout(to);
      to = setTimeout(applyFilterAndSort, 200);
    });
  }
  const sb = document.getElementById('sort-by');
  if (sb) sb.addEventListener('change', (e) => { STATE.sortBy = e.target.value; applyFilterAndSort(); });
  const sd = document.getElementById('sort-dir');
  if (sd) sd.addEventListener('change', (e) => { STATE.sortDir = e.target.value; applyFilterAndSort(); });
  const ps = document.getElementById('page-size');
  if (ps) {
    STATE.pageSize = parseInt(ps.value || '100', 10);
    ps.addEventListener('change', (e) => { STATE.pageSize = parseInt(e.target.value || '100', 10); renderPage(); });
  }
  const prev = document.getElementById('prev-page');
  if (prev) prev.addEventListener('click', () => { if (STATE.page > 1) { STATE.page--; renderPage(); } });
  const next = document.getElementById('next-page');
  if (next) next.addEventListener('click', () => { STATE.page++; renderPage(); });
}

// ─────────────────────────────────────────────
// Agentik diyalog (insan gibi, kısa) → /search
// ─────────────────────────────────────────────
const AGENT = { step: 0, company_code: null, type_code: null, date_from: null, date_to: null };

function say(text, who = 'ai') {
  const box = document.getElementById('dialog-transcript');
  const line = document.createElement('div');
  line.className = who === 'ai' ? 'muted' : '';
  line.textContent = (who === 'ai' ? '🤖 ' : '🧑 ') + text;
  box.appendChild(line);
  box.scrollTop = box.scrollHeight;
}

function showCompanyOptions(options) {
  const box = document.getElementById('dialog-transcript');
  const wrap = document.createElement('div');
  wrap.style.marginTop = '6px';

  (options || []).slice(0, 5).forEach(o => {
    const btn = document.createElement('button');
    btn.className = 'chip';
    btn.textContent = o.name || String(o.code);
    btn.style.marginRight = '6px';
    btn.addEventListener('click', () => {
      AGENT.company_code = parseInt(o.code, 10);
      say(`"${o.name}" seçildi. Hangi tarih aralığı? Örn: "son 3 yıl" ya da "2022-01-01..2024-12-31".`, 'ai');
      wrap.remove();      // clear suggestions
      AGENT.step = 1;     // advance to date question
    });
    wrap.appendChild(btn);
  });

  box.appendChild(wrap);
  box.scrollTop = box.scrollHeight;
}

async function lookupCompanyByName(name) {
  const res = await fetch(api('/tools/lookup_company'), {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name })
  });
  return res.json();
}
async function lookupType(term) {
  const res = await fetch(api('/tools/lookup_ilan_turu'), {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ term })
  });
  return res.json();
}
async function parseDateText(text) {
  const res = await fetch(api('/tools/parse_date_range'), {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text })
  });
  return res.json();
}

async function performSearch() {
  const filters = {
    company_code: AGENT.company_code ?? undefined,
    type_code: AGENT.type_code ?? undefined,
    date_from: AGENT.date_from ?? undefined,
    date_to: AGENT.date_to ?? undefined
  };
  // Büyük batch çek → sıralama/filtreleme tüm sonuçlar üzerinde
  const payload = { filters, limit: 5000 };
  const res = await fetch(api('/search'), {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const j = await res.json();
  STATE.allHits = j.hits || [];
  applyFilterAndSort();
  say(`Tamam. ${STATE.viewHits.length} sonuç buldum. Üstte sıralayıp süzebilirsiniz.`, 'ai');
}

async function onDialogSend() {
  const inp = document.getElementById('dialog-input');
  const text = (inp.value || '').trim();
  if (!text) return;
  say(text, 'user');
  inp.value = '';

  try {
    if (AGENT.step === 0) {
      const r = await lookupCompanyByName(text);
      if (r.status === 'ok') {
        AGENT.company_code = r.code;
        say(`Tamam, "${r.name}" ile ilerliyorum. Hangi tarih aralığına bakalım? Örn: "son 3 yıl" ya da "2022-01-01..2024-12-31".`, 'ai');
        AGENT.step = 1;
      } else if (r.status === 'ambiguous') {
        const opts = r.options.map(o => o.name).filter(Boolean).slice(0, 5).join(', ');
        say('Birden çok eşleşme buldum. Aşağıdan seçin:', 'ai');
        if (Array.isArray(r.options) && r.options.length) {
          showCompanyOptions(r.options);
        }        
      } else {
        say('Bulamadım. Şirket adını biraz daha net yazar mısınız?', 'ai');
      }
      return;
    }
    if (AGENT.step === 1) {
      const r = await parseDateText(text);
      if (r.status === 'ok') {
        AGENT.date_from = r.range.from;
        AGENT.date_to = r.range.to;
        say(`Anladım, ${AGENT.date_from} – ${AGENT.date_to}. Peki ilan türü? (örn: "Kuruluş", "Genel Kurul"; tümü için "hepsi" yazın)`, 'ai');
        AGENT.step = 2;
      } else {
        say('Tarih ifadesini anlayamadım. Örnek: "son 2 yıl" ya da "2024-01..2024-03".', 'ai');
      }
      return;
    }
    if (AGENT.step === 2) {
      if (text) {
        const tnorm = text.trim().toLowerCase();
        if (tnorm === 'hepsi') {
          AGENT.type_code = null; // no type filter
          say('Tüm ilan türleriyle arıyorum.', 'ai');
        } else {
          const r = await lookupType(text);
          if (r.status === 'ok') {
            AGENT.type_code = r.code;
          } else {
            say('Bu türü bulamadım; tür filtresi olmadan arıyorum.', 'ai');
          }
        }
      }
      say('Arıyorum…', 'ai');
      await performSearch();
      AGENT.step = 3;
      return;
    }
    // Sonrası: her yeni mesaj yeni akış başlatır
    AGENT.step = 0;
    AGENT.company_code = AGENT.type_code = null;
    AGENT.date_from = AGENT.date_to = null;
    say('Yeni bir şirket adıyla başlayalım. Hangi şirket?', 'ai');
  } catch (e) {
    console.error(e);
    say('Beklenmeyen bir hata oldu. Tekrar dener misiniz?', 'ai');
  }
}

function initDialog() {
  const send = document.getElementById('dialog-send');
  if (send) send.addEventListener('click', onDialogSend);
  const input = document.getElementById('dialog-input');
  if (input) input.addEventListener('keydown', (e) => { if (e.key === 'Enter') onDialogSend(); });
  say('Merhaba! Hangi şirketi aramak istiyorsunuz?', 'ai');
}

// ─────────────────────────────────────────────
// Boot
// ─────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  bindListControls();
  bindApiControls();
  initDialog();
  await pingServer();
  checkPuterReady();
});