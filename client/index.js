(() => {
  "use strict";

  // ===== Config / DOM
  const DEFAULT_MODEL = "gpt-4o-mini";
  const API_BASE_KEY  = "helpbot_api_base";
  const SUPPORT_PHONE = "5348929202";

  const $ = (s)=>document.querySelector(s);
  const elChat  = ()=>$("#chatlog");
  const elInput = ()=>$("#q");
  const elErr   = ()=>$("#error");
  const elStat  = ()=>$("#status");
  const elAns   = ()=>$("#answer");
  const elHits  = ()=>$("#hits");
  const elChips = ()=>$("#chips");
  const elApi   = ()=>$("#api-base");
  const elPing  = ()=>$("#api-ping");
  const elUser  = ()=>$("#user-info");
  const elModel = ()=>$("#llm-model");

  function apiBase(){ return (localStorage.getItem(API_BASE_KEY) || (elApi()?.value || "http://localhost:8000")).replace(/\/$/,""); }
  function setApiBase(v){ localStorage.setItem(API_BASE_KEY, v); if (elApi()) elApi().value = v; }
  function getModel(){ return elModel()?.value || DEFAULT_MODEL; }

  // ===== Puter helpers (for optional client-side summary)
  async function ensurePuterReady(timeoutMs = 8000){
    const t0=Date.now();
    while(!(window.puter && puter.auth && puter.ai && (typeof puter.ai.chat==="function" || puter.ai?.chat?.completions))){
      await new Promise(r=>setTimeout(r,50));
      if (Date.now()-t0>timeoutMs) throw new Error("Puter SDK failed to load.");
    }
  }
  async function llmChat(messages){
    await ensurePuterReady();
    if (typeof puter.ai.chat === "function"){
      return await puter.ai.chat(messages, { model:getModel(), temperature:0.1, stream:false, max_tokens:400 });
    }
    if (puter.ai?.chat?.completions?.create){
      return await puter.ai.chat.completions.create({ model:getModel(), messages, temperature:0.1 });
    }
    throw new Error("Puter LLM API unavailable");
  }
  function extractLLMText(resp){
    return resp?.text ?? resp?.output_text ?? resp?.choices?.[0]?.message?.content ?? "";
  }

  // ===== HTTP helpers
  async function postJSON(url, body){
    const r = await fetch(url, { method:"POST", headers:{ "content-type":"application/json" }, body: JSON.stringify(body||{}) });
    const t = await r.text();
    let j; try{ j=JSON.parse(t); }catch{ throw new Error(`${url} returned non-JSON: ${t.slice(0,160)}...`); }
    if (!r.ok) throw new Error(j.error || `HTTP ${r.status}`);
    return j;
  }

  // ===== Tools we call on your backend
  const Tools = {
    parse_date_range: (text)=> postJSON(`${apiBase()}/tools/parse_date_range`, { text }),
    lookup_company:   (name)=> postJSON(`${apiBase()}/tools/lookup_company`,   { name }),
    lookup_mudurluk:  (name)=> postJSON(`${apiBase()}/tools/lookup_mudurluk`,  { name }),
    search:           (payload)=> postJSON(`${apiBase()}/search`, payload),
    answer:           (payload)=> postJSON(`${apiBase()}/answer`, payload),
  };

  // ===== Chat rendering
  function msg(role, text){
    const row = document.createElement("div");
    row.className = "msg " + (role === "ai" ? "ai" : "user");
    const b = document.createElement("div");
    b.className = "bubble";
    b.textContent = text;
    row.appendChild(b);
    elChat().appendChild(row);
    elChat().scrollTop = elChat().scrollHeight;
  }

  function setError(txt){ if (elErr()) elErr().textContent = txt || ""; }
  function setStatus(txt){ if (elStat()) elStat().textContent = txt || ""; }
  function setAnswer(txt){ if (elAns()) elAns().textContent = txt || ""; }

  // ===== Wizard state (strict order): date -> company -> city
  const state = {
    step: 0,                 // 0=welcome, 1=date, 2=company, 3=city, 4=search
    date_from: null,
    date_to: null,
    company_code: null,
    company_name: null,
    city_code: null,
    city_name: null,
  };

  function resetWizard(){
    state.step = 0;
    state.date_from = state.date_to = null;
    state.company_code = state.company_name = null;
    state.city_code = state.city_name = null;
    elHits().innerHTML = "";
    setAnswer("");
    setError("");
    elChips().innerHTML = "";
    elInput().value = "";
    elChat().innerHTML = "";
    welcome();
  }

  function chips(){
    const c = [];
    if (state.date_from && state.date_to) c.push(`<span class="chip">Date: ${state.date_from} → ${state.date_to}</span>`);
    if (state.company_name) c.push(`<span class="chip">Company: ${state.company_name}</span>`);
    if (state.city_name) c.push(`<span class="chip">City: ${state.city_name}</span>`);
    elChips().innerHTML = c.join(" ");
  }

  // ===== Welcome + policy
  function welcome(){
    msg("ai", "Welcome! Basic search is enabled only. I will collect 3 items in order: (1) date range, (2) company name, (3) city.");
    msg("ai", `Note: Only a basic search logic is implemented for now. For assistance call ${SUPPORT_PHONE}.`);
    msg("ai", "First, please enter a date range (e.g., “son 30 gün”, “Ocak 2025”, or “2024-01-01..2024-03-31”).");
    state.step = 1;
  }

  // ===== Casual chat guard
  function isCasual(input){
    const s = input.toLowerCase().trim();
    // crude guard: greetings / chitchat keywords
    return /^(hi|hello|hey|nasılsın|naber|how are you|what's up|selam|merhaba)\b/.test(s) ||
           /(tell me a joke|who are you|weather|news|chat)/.test(s);
  }

  // ===== Step handlers
  async function handleDate(text){
    // Must call parse_date_range tool; no guessing
    try{
      const r = await Tools.parse_date_range(text);
      if (r?.status === "ok" && r.range?.from && r.range?.to){
        state.date_from = r.range.from;
        state.date_to   = r.range.to;
        chips();
        msg("ai", "Thanks. Now, please enter the company name (official trade name).");
        state.step = 2;
      } else {
        msg("ai", "I couldn’t understand that. Please provide a date range like “son 30 gün”, “Ocak 2025”, or “2024-01-01..2024-03-31”.");
      }
    }catch(e){
      setError("Date parser error: " + (e.message || e));
      msg("ai", "Please try the date again (examples: “son 30 gün”, “2024-01-01..2024-03-31”).");
    }
  }

  async function handleCompany(text){
    // Use backend resolver; if ambiguous, ask to pick; if unmapped, ask again
    try{
      const r = await Tools.lookup_company(text);
      if (r?.status === "ok" && r.code){
        state.company_code = r.code;
        state.company_name = r.name || text;
        chips();
        msg("ai", "Great. Lastly, please enter the city (müdürlük).");
        state.step = 3;
      } else if (r?.status === "ambiguous" && Array.isArray(r.options) && r.options.length){
        msg("ai", "I found multiple matches. Please pick one by replying with its exact name:");
        for (const opt of r.options.slice(0,5)){
          msg("ai", `• ${opt.name}`);
        }
      } else {
        msg("ai", "I could not resolve that company. Please provide the official trade name (as in the gazette).");
      }
    }catch(e){
      setError("Company lookup error: " + (e.message || e));
      msg("ai", "Please try the company name again.");
    }
  }

  async function handleCity(text){
    try{
      const r = await Tools.lookup_mudurluk(text);
      if (r?.status === "ok" && r.code){
        state.city_code = r.code;
        state.city_name = r.name || text;
        chips();
        msg("ai", "Thanks. Running a basic search now…");
        state.step = 4;
        await runSearchAndAnswer();
      } else if (r?.status === "ambiguous" && Array.isArray(r.options) && r.options.length){
        msg("ai", "Multiple city candidates found. Please reply with one of these:");
        for (const opt of r.options.slice(0,5)){
          msg("ai", `• ${opt.name}`);
        }
      } else {
        msg("ai", "City not recognized. Please enter the müdürlük (e.g., “İzmir”, “Ankara”).");
      }
    }catch(e){
      setError("City lookup error: " + (e.message || e));
      msg("ai", "Please try the city again (e.g., “İzmir”).");
    }
  }

  // ===== Retrieval + RAG
  function buildSearchPayload(){
    return {
      filters: {
        date_from: state.date_from,
        date_to:   state.date_to,
        company_code: state.company_code,
        city_code: state.city_code
      },
      limit: 40
    };
  }

  function renderHits(hits){
    const el = elHits();
    el.innerHTML = "";
    if (!hits || !hits.length){
      el.innerHTML = `<div class="muted">No records.</div>`;
      return;
    }
    for (const h of hits){
      const date = h.date_int ? toISOFrom1960Seconds(h.date_int) : (h.tarih || "");
      const card = document.createElement("div");
      card.className = "card";
      card.innerHTML = `
        <div><span class="chip">Ad ID: ${escapeHtml(h.ad_id || h.ilan_id || "")}</span>
             <span class="chip">${escapeHtml(h.city || h.loc_id || "")}</span>
             <span class="chip">${escapeHtml(h.type || h.type_id || "")}</span></div>
        <div style="margin-top:8px;font-weight:600">${escapeHtml(h.company || h.unvan || "")}</div>
        <div style="opacity:.8;margin-top:4px">Date: ${escapeHtml(date)}</div>
        <div style="opacity:.8;margin-top:4px">PDF: ${escapeHtml(h.ad_link || h.pdf_guid || "")}</div>
      `;
      el.appendChild(card);
    }
  }

  function escapeHtml(x){ return String(x ?? "").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;"); }
  function toISOFrom1960Seconds(di){
    const base = new Date(Date.UTC(1960,0,1));
    const d = new Date(base.getTime() + (parseInt(di,10)||0)*1000);
    return d.toISOString().slice(0,10);
  }

  async function runSearchAndAnswer(){
    setStatus("searching…");
    setError("");
    elHits().innerHTML = "";
    setAnswer("");
    // Ping API quickly
    try{
      elPing().textContent = "ping…";
      const r = await fetch(apiBase()+"/health");
      elPing().textContent = r.ok ? "API ✓" : "API ?";
    }catch{ elPing().textContent = "API ?"; }

    // Search
    let hits = [];
    try{
      const data = await Tools.search(buildSearchPayload());
      hits = data.hits || data.rows || [];
      renderHits(hits);
    }catch(e){
      setStatus("error");
      setError("Search failed: " + (e.message || e));
      msg("ai", `Search failed. For assistance, please call ${SUPPORT_PHONE}.`);
      return;
    }

    // Answer (server preferred)
    setStatus("answering…");
    try{
      const payload = { ...buildSearchPayload(), q_tr: `Tarih: ${state.date_from}..${state.date_to}; Şirket: ${state.company_name}; Şehir: ${state.city_name}`, max_ctx: 20 };
      const a = await Tools.answer(payload);
      if (a?.answer_tr){
        setAnswer(a.answer_tr);
      } else {
        throw new Error("No answer returned");
      }
      setStatus("ready");
    }catch{
      // Client-side fallback summary with Puter LLM
      try{
        const ctx = hits.slice(0,20).map(h=>{
          const date = h.date_int ? toISOFrom1960Seconds(h.date_int) : (h.tarih||"");
          return `- [${h.ad_id||h.ilan_id}] ${date} • ${h.city||h.loc_id||""} • ${h.type||h.type_id||""} • ${h.company||h.unvan||""} (PDF:${h.ad_link||h.pdf_guid||""})`;
        }).join("\n");
        const messages = [
          { role:"system", content:"You summarize Turkish commercial registry search results. Be concise (≤8 bullets). No chit-chat." },
          { role:"user", content:`Özet isteği:\nTarih: ${state.date_from}..${state.date_to}\nŞirket: ${state.company_name}\nŞehir: ${state.city_name}\n\nBağlam:\n${ctx}` }
        ];
        const resp = await llmChat(messages);
        setAnswer(extractLLMText(resp));
      }catch(e){
        setAnswer("");
        msg("ai", `Could not generate a summary. You can still review the results. For help call ${SUPPORT_PHONE}.`);
      }
      setStatus("ready");
    }
  }

  // ===== Controller: single input box driving the wizard
  async function onSubmit(){
    setError("");
    const text = (elInput()?.value || "").trim();
    if (!text) return;

    // Casual chat is not permitted
    if (isCasual(text)){
      msg("ai", `Open conversation is not permitted on this web app. For assistance call ${SUPPORT_PHONE}.`);
      elInput().value = "";
      return;
    }

    msg("user", text);
    elInput().value = "";

    if (state.step === 0){ welcome(); return; }
    if (state.step === 1){ await handleDate(text); return; }
    if (state.step === 2){ await handleCompany(text); return; }
    if (state.step === 3){ await handleCity(text); return; }
    if (state.step >= 4){
      // After search, any new message: remind policy and restart if user wants
      msg("ai", `Basic search completed. If you want to start a new search, type "restart". For assistance call ${SUPPORT_PHONE}.`);
      if (/^restart$/i.test(text)) resetWizard();
    }
  }

  // ===== Boot
  document.addEventListener("DOMContentLoaded", async ()=>{
    // API base persistence
    const saved = localStorage.getItem(API_BASE_KEY);
    if (saved) elApi().value = saved;
    $("#api-save")?.addEventListener("click", ()=>{
      const v = (elApi()?.value||"").trim();
      if (!/^https?:\/\//.test(v)){ setError("Enter full API URL, e.g., http://localhost:8000"); return; }
      setApiBase(v); elPing().textContent = "saved ✓"; setTimeout(()=> elPing().textContent="", 1200);
    });

    // Wire chat form
    $("#ask-form")?.addEventListener("submit", (e)=>{ e.preventDefault(); onSubmit(); });
    $("#ask-btn")?.addEventListener("click", (e)=>{ e.preventDefault(); onSubmit(); });

    // Puter auth (optional)
    $("#sign-in-btn")?.addEventListener("click", async ()=>{
      try{ await ensurePuterReady(); await puter.auth?.signIn?.({ mode:"popup", redirectTo: location.href }); }
      catch(e){ setError("Sign-in failed: " + (e.message||e)); }
    });
    $("#sign-out-btn")?.addEventListener("click", async ()=>{
      try{ await ensurePuterReady(); await puter.auth?.signOut?.(); }
      catch(e){ setError("Sign-out failed: " + (e.message||e)); }
    });

    // Initial welcome
    welcome();
  });
})();
