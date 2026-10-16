"use strict";
(() => {
  const $ = id => document.getElementById(id);
  const fmt = value => Number(value || 0).toLocaleString();
  const human = value => String(value || "unclassified").replaceAll("_", " ");
  const date = value => value ? new Date(value).toLocaleString() : "Not available";
  const pct = value => value == null ? "—" : `${(Number(value) * 100).toFixed(1)}%`;
  let key = "", connected = false, epoch = 0, detailEpoch = 0, sessionVersion = 0, selectedCall = null;
  let summary = null, selectedEvent = null, detailData = null, callSequence = 0;
  let theme = "", subject = "", subjectName = "";
  let eventAfter = null, subjectAfter = null, eventNext = null, subjectNext = null;
  let eventHistory = [], subjectHistory = [], calls = [];

  function node(tag, text, className) {
    const element = document.createElement(tag);
    if (text != null) element.textContent = text;
    if (className) element.className = className;
    return element;
  }
  function message(text = "") {
    $("error").textContent = text;
    $("error").hidden = !text;
  }
  function query(path, values = {}) {
    const params = new URLSearchParams();
    Object.entries(values).forEach(([name, value]) => {
      if (value != null && value !== "") params.set(name, value);
    });
    return path + (params.size ? "?" + params : "");
  }
  function safeUrl(value) {
    try {
      const url = new URL(value);
      return ["https:", "http:"].includes(url.protocol) ? url.href : null;
    } catch { return null; }
  }
  function sourceLink(url, title) {
    const href = safeUrl(url);
    if (!href) return node("span", title);
    const link = node("a", title);
    link.href = href; link.target = "_blank"; link.rel = "noopener noreferrer";
    return link;
  }
  function renderCalls() {
    $("call-count").textContent = calls.length;
    $("calls").replaceChildren(...calls.map(call => {
      const button = node("button", null, "list-row");
      button.setAttribute("aria-pressed", String(selectedCall === call));
      button.append(node("strong", `GET ${call.path}`), node("small", `${call.status} · ${call.ms} ms${call.requestId ? " · " + call.requestId : ""}`));
      button.onclick = () => { selectedCall = call; renderCalls(); renderResponse(); };
      return button;
    }));
  }
  function renderResponse() {
    $("response-title").textContent = selectedCall ? `GET ${selectedCall.path} · ${selectedCall.status}` : "Select a request";
    $("response").textContent = selectedCall ? JSON.stringify(selectedCall.body, null, 2) : "No requests selected.";
    $("copy-curl").disabled = !selectedCall;
  }
  async function api(path, publicRequest = false) {
    const started = performance.now(), id = ++callSequence, version = sessionVersion;
    let status = "NETWORK ERROR", body = null, requestId = null;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 45000);
    try {
      const response = await fetch(path, {
        headers: !publicRequest && key ? {Authorization: `Bearer ${key}`} : {},
        cache: "no-store", signal: controller.signal, redirect: "error",
      });
      status = response.status;
      requestId = response.headers.get("x-request-id");
      const text = await response.text();
      try { body = JSON.parse(text); } catch { body = {detail: text.slice(0, 2000)}; }
      if (!response.ok) {
        const error = new Error(`${status}: ${body.detail || response.statusText}`);
        error.status = status;
        throw error;
      }
      return body;
    } catch (error) {
      if (error.name === "AbortError") throw new Error("Request timed out after 45 seconds. Retry using Refresh.");
      throw error;
    } finally {
      clearTimeout(timeout);
      const call = {id, path, status, body, requestId, ms: Math.round(performance.now() - started)};
      if (version === sessionVersion) {
        calls.unshift(call); calls = calls.slice(0, 30);
        // An API key never appears in a request URL, inspector header, storage or cURL example.
        if (!selectedCall) { selectedCall = call; renderResponse(); }
        renderCalls();
      }
    }
  }
  function clearPages() {
    eventAfter = subjectAfter = eventNext = subjectNext = null;
    eventHistory = []; subjectHistory = [];
  }
  function clearDetail() {
    detailEpoch++; selectedEvent = null; detailData = null;
    $("detail").replaceChildren(node("p", "Select an event to trace every mapping and inspect its source observations.", "muted"));
  }
  function disconnect() {
    epoch++; sessionVersion++; key = ""; connected = false; summary = null;
    $("api-key").value = ""; $("connect-form").hidden = false;
    $("workspace").hidden = true; $("disconnect").hidden = $("refresh").hidden = true;
    $("connection-message").textContent = "Disconnected. Enter an existing read key to load live data.";
    calls = []; selectedCall = null; renderCalls(); renderResponse();
    $("events").replaceChildren(); $("subjects").replaceChildren(); $("themes").replaceChildren();
    $("stats").replaceChildren(); $("insights").replaceChildren();
    theme = subject = subjectName = ""; clearPages(); clearDetail();
  }
  function handleError(error) {
    if (error.status === 401) disconnect();
    message(error.message);
  }
  function renderSummary() {
    if (!summary) return;
    const c = summary.coverage;
    $("stats").replaceChildren(...[
      [c.events, "Active events"], [c.subjects, "Subjects / event families"],
      [c.themes, "Stored themes"], [c.cross_source_events, "Events across 2+ sources"],
    ].map(([value, label]) => {
      const box = node("div", null, "stat"); box.append(node("strong", fmt(value)), node("span", label)); return box;
    }));
    $("themes").replaceChildren(...summary.themes.map(item => {
      const button = node("button", null, "list-row");
      button.setAttribute("aria-pressed", String(item.theme_id === theme));
      button.append(node("strong", human(item.theme_id)), node("small", `${fmt(item.event_count)} events · ${fmt(item.subject_count)} subjects`));
      button.onclick = () => { theme = item.theme_id; subject = subjectName = ""; clearPages(); clearDetail(); renderSummary(); loadLists(); };
      return button;
    }));
    const fraction = c.events ? (c.uncategorized_events / c.events * 100).toFixed(1) : "0.0";
    const insights = [
      `${fmt(c.uncategorized_events)} events (${fraction}%) belong to “other” or “unclassified” themes. These categories include uncovered topics; they are not an error count.`,
      `${fmt(c.cross_source_events)} canonical events have active mappings across multiple sources. Related family members remain distinct outcomes, not interchangeable probabilities.`,
      `${fmt(c.formal_subject_events)} live events have a formal subject entity. The ${fmt(c.subjects)} subjects shown above are event families; ${fmt(summary.reference.entities)} active named entities are available separately for mention evidence.`,
      `${fmt(summary.reference.instruments)} reference securities exist; ${fmt(summary.reference.instruments_with_issuer)} have an established issuer entity. Security exposure mappings require a separate evidenced model.`,
    ];
    if (c.events_without_family) insights.push(`${fmt(c.events_without_family)} active events have no event family; their subject mapping is missing.`);
    $("insights").replaceChildren(...insights.map(text => node("li", text)));
    $("snapshot").textContent = `Summary loaded ${date(summary.as_of)}. Lists are separate live reads. ${summary.basis}`;
  }
  function renderEventList(data) {
    eventNext = data.next_after;
    $("events-count").textContent = `${data.count} on this page`;
    $("events").replaceChildren(...data.items.map(item => {
      const button = node("button", null, "list-row");
      button.dataset.eventId = item.canonical_event_id;
      button.setAttribute("aria-pressed", String(item.canonical_event_id === selectedEvent));
      button.append(node("strong", item.canonical_name), node("small", `${item.subject_name || "No subject mapped"} → ${human(item.theme_id)}`), node("small", `${item.sources} · ${fmt(item.contract_count)} contract(s) · received ${date(item.latest_received_at)}`));
      button.onclick = () => selectEvent(item.canonical_event_id);
      return button;
    }));
    if (!data.count) $("events").append(node("p", "No active events match these filters. Clear filters or try another search.", "empty"));
    $("events-prev").disabled = !eventHistory.length; $("events-next").disabled = !eventNext;
  }
  function renderSubjectList(data) {
    subjectNext = data.next_after;
    $("subjects").replaceChildren(...data.items.map(item => {
      const button = node("button", null, "list-row");
      button.setAttribute("aria-pressed", String(item.subject_id === subject));
      button.append(node("strong", item.subject_name), node("small", `${human(item.theme_id)} · ${fmt(item.event_count)} events`), node("small", item.subject_id));
      button.onclick = () => { subject = item.subject_id; subjectName = item.subject_name; eventAfter = null; eventHistory = []; clearDetail(); loadLists(); };
      return button;
    }));
    if (!data.count) $("subjects").append(node("p", "No subjects match these filters.", "empty"));
    $("subjects-prev").disabled = !subjectHistory.length; $("subjects-next").disabled = !subjectNext;
  }
  async function loadLists(refreshSummary = false) {
    if (!connected) return;
    const currentEpoch = ++epoch;
    message();
    const common = {q: $("search").value.trim(), source: $("source").value, theme, limit: 30};
    $("active-filter").textContent = [theme && `Theme: ${human(theme)}`, subject && `Subject: ${subjectName}`].filter(Boolean).join(" · ");
    $("events").replaceChildren(node("p", "Loading live events…", "empty"));
    $("subjects").replaceChildren(node("p", "Loading subjects…", "empty"));
    for (const id of ["events-prev", "events-next", "subjects-prev", "subjects-next"]) $(id).disabled = true;
    const tasks = [
      api(query("/v1/explorer/events", {...common, subject_id: subject, after: eventAfter})),
      api(query("/v1/explorer/subjects", {...common, after: subjectAfter})),
    ];
    if (refreshSummary) tasks.push(api(query("/v1/explorer/summary", {source: common.source})));
    const results = await Promise.allSettled(tasks);
    if (currentEpoch !== epoch) return;
    const error = results.find(result => result.status === "rejected");
    if (error?.reason.status === 401) { handleError(error.reason); return; }
    if (results[0].status === "fulfilled") renderEventList(results[0].value);
    else $("events").replaceChildren(node("p", "Events unavailable. Retry using Refresh.", "empty"));
    if (results[1].status === "fulfilled") renderSubjectList(results[1].value);
    else $("subjects").replaceChildren(node("p", "Subjects unavailable. Retry using Refresh.", "empty"));
    if (refreshSummary && results[2].status === "fulfilled") { summary = results[2].value; renderSummary(); }
    if (error) handleError(error.reason);
  }
  function evidenceDetails(contract) {
    const details = node("details", null, "evidence");
    details.append(node("summary", "Mapping method, validity & resolution rules"));
    const dl = node("dl");
    for (const [label, value] of [
      ["Mapping ID", contract.mapping_id], ["Type", contract.mapping_type],
      ["Method", `${contract.mapping_method} / ${contract.method_version || "—"}`],
      ["Review", contract.review_status], ["Confidence", contract.confidence ?? "—"],
      ["Valid from", date(contract.valid_from)], ["Valid to", contract.valid_to ? date(contract.valid_to) : "Open interval"],
      ["Contract ID", contract.source_contract_id], ["Market ID", contract.source_market_id],
      ["Last polled", date(contract.last_polled_at)], ["Observed", date(contract.observed_at)],
      ["Observation method", contract.methodology_version || "—"],
    ]) dl.append(node("dt", label), node("dd", value));
    details.append(dl);
    if (contract.review_notes) details.append(node("p", contract.review_notes));
    details.append(node("blockquote", contract.current_resolution_rules || "No resolution rules available."));
    return details;
  }
  function renderDetail(data) {
    const root = $("detail"), top = node("div", null, "detail-top");
    const title = node("div"); title.append(node("h2", data.event.canonical_name), node("div", data.event.canonical_event_id, "id"));
    const toggle = node("label", null, "inline-label"), check = node("input");
    check.type = "checkbox"; check.checked = data.include_expired;
    check.onchange = () => selectEvent(data.event.canonical_event_id, check.checked);
    toggle.append(check, node("span", "Include expired mappings")); top.append(title, toggle);
    const path = node("div", null, "mapping-path");
    for (const [label, name, id] of [
      ["1. Event", data.event.canonical_name, data.event.canonical_event_id],
      ["2. Subject / event family", data.subject?.canonical_name || "No subject mapped", data.subject?.event_family_id || "Missing family"],
      ["3. Theme / family category", human(data.theme.theme_id), data.theme.family_version?.method_version || "Stored category; no version metadata available"],
    ]) {
      const box = node("div", null, "mapping-step"); box.append(node("span", label), node("strong", name), node("div", id, "id")); path.append(box);
    }
    const insight = data.insights;
    const notes = node("ul");
    const lines = [
      `${fmt(insight.current_contract_count)} active contract(s) across ${insight.source_ids.join(", ") || "no active sources"}. Resolution deadline: ${date(data.event.resolution_deadline)}.`,
      insight.probability_range == null ? "A cross-source probability comparison is unavailable: fewer than two distinct sources supply a current price." : `Observed price range across distinct sources: ${(insight.probability_range * 100).toFixed(1)} percentage points. This is descriptive; mapping and resolution rules require inspection.`,
      `Event class: ${human(data.event.event_class)}; family theme: ${human(data.theme.theme_id)}. ${insight.theme_needs_review ? "The stored theme is broad or unclassified." : "Classification uses stored taxonomy."}`,
    ];
    notes.append(...lines.map(line => node("li", line)));
    root.replaceChildren(top, path, notes, node("h3", "Source contracts & observations"));
    const wrap = node("div", null, "table-wrap"), table = node("table"), head = node("thead"), headRow = node("tr");
    for (const label of ["Source / contract", "Probability", "Bid / ask", "Spread", "Received", "Mapping"]) headRow.append(node("th", label));
    head.append(headRow); table.append(head);
    const body = node("tbody");
    for (const contract of data.source_contracts) {
      const row = node("tr", null, contract.is_current ? "" : "expired");
      const source = node("td"); source.append(node("strong", contract.source_id), node("div", contract.is_current ? "Current mapping" : "Inactive / historical mapping", "muted"), sourceLink(contract.source_url, contract.current_title));
      const mapping = node("td"); mapping.append(node("div", `${contract.mapping_type} · ${contract.review_status}`), evidenceDetails(contract));
      row.append(source, node("td", pct(contract.displayed_probability)), node("td", `${pct(contract.yes_bid)} / ${pct(contract.yes_ask)}`), node("td", contract.spread == null ? "—" : `${(contract.spread * 100).toFixed(1)} pp`), node("td", date(contract.received_at)), mapping);
      body.append(row);
    }
    table.append(body); wrap.append(table); root.append(wrap);
    if (!data.source_contracts.length) root.append(node("p", "No mappings found in the selected validity scope.", "muted"));
    root.append(node("h3", "Named entities mentioned · separate from subject assignment"));
    if (!data.entity_mentions.length) root.append(node("p", "No current evidenced mentions found. This does not prove that the event lacks relevant entities.", "muted"));
    for (const edge of data.entity_mentions) {
      const details = node("details", null, "evidence");
      details.append(node("summary", `${edge.entity_name} · ${edge.relationship_type} · ${edge.edge_class}`), node("p", `${edge.inference_method || "Explicit source mention"} / ${edge.method_version || "—"}`));
      details.append(node("div", edge.edge_id, "id"));
      for (const item of edge.evidence) {
        details.append(node("blockquote", item.excerpt || "No excerpt available."), node("div", `${item.source_id || ""} · ${item.source_record_id || ""} · ${date(item.observed_at)}`, "muted"));
        if (item.source_url) details.append(sourceLink(item.source_url, "Source evidence ↗"));
      }
      root.append(details);
    }
    root.append(node("p", data.basis, "muted"));
  }
  async function selectEvent(id, includeExpired = false) {
    const currentDetail = ++detailEpoch;
    selectedEvent = id; detailData = null; message();
    $("events").querySelectorAll("button[data-event-id]").forEach(button => button.setAttribute("aria-pressed", String(button.dataset.eventId === id)));
    $("detail").replaceChildren(node("p", "Loading mapping evidence…", "muted"));
    try {
      const data = await api(query(`/v1/explorer/events/${encodeURIComponent(id)}/mappings`, {include_expired: includeExpired}));
      if (currentDetail !== detailEpoch || !connected) return;
      detailData = data; renderDetail(data);
    } catch (error) {
      if (currentDetail !== detailEpoch) return;
      $("detail").replaceChildren(node("p", "Mapping evidence unavailable. Select the event to retry.", "muted"));
      handleError(error);
    }
  }
  $("connect-form").onsubmit = async event => {
    event.preventDefault(); key = $("api-key").value.trim(); if (!key) return;
    const attempt = ++epoch; message(); $("connect").disabled = true;
    $("connection-message").textContent = "Connecting to live EventRisk data…";
    try {
      const data = await api("/v1/explorer/summary");
      if (attempt !== epoch) return;
      summary = data; connected = true; $("api-key").value = "";
      $("workspace").hidden = false; $("connect-form").hidden = true;
      $("disconnect").hidden = $("refresh").hidden = false;
      $("connection-message").textContent = "Connected · read-only API · key held in this tab's memory";
      $("source").replaceChildren(new Option("All sources", ""), ...summary.sources.map(item => new Option(item.source_name || item.source_id, item.source_id)));
      clearPages(); renderSummary(); await loadLists();
    } catch (error) {
      if (attempt !== epoch) return;
      key = ""; message(error.message); $("connection-message").textContent = "Connection failed. Check your API key and retry.";
    } finally { $("connect").disabled = false; }
  };
  $("filters").onsubmit = event => { event.preventDefault(); subject = subjectName = ""; clearPages(); clearDetail(); loadLists(); };
  $("source").onchange = () => { subject = subjectName = ""; clearPages(); clearDetail(); loadLists(true); };
  $("reset").onclick = () => { $("search").value = ""; $("source").value = ""; theme = subject = subjectName = ""; clearPages(); clearDetail(); loadLists(true); };
  $("all-themes").onclick = () => { theme = subject = subjectName = ""; clearPages(); clearDetail(); renderSummary(); loadLists(); };
  $("disconnect").onclick = () => { disconnect(); message(); };
  $("refresh").onclick = () => { const event = selectedEvent; loadLists(true); if (event) selectEvent(event, detailData?.include_expired || false); };
  for (const kind of ["events", "subjects"]) {
    $(kind + "-next").onclick = () => {
      if (kind === "events") { eventHistory.push(eventAfter); eventAfter = eventNext; }
      else { subjectHistory.push(subjectAfter); subjectAfter = subjectNext; }
      loadLists();
    };
    $(kind + "-prev").onclick = () => {
      if (kind === "events") eventAfter = eventHistory.pop() ?? null;
      else subjectAfter = subjectHistory.pop() ?? null;
      loadLists();
    };
  }
  $("copy-curl").onclick = async () => {
    if (!selectedCall) return;
    const command = `curl -sS '${location.origin}${selectedCall.path.replaceAll("'", "'\\''")}'` + (selectedCall.path.startsWith("/v1/") ? " -H 'Authorization: Bearer <EVENTRISK_API_KEY>'" : "");
    try { await navigator.clipboard.writeText(command); $("copy-curl").textContent = "Copied"; setTimeout(() => { $("copy-curl").textContent = "Copy cURL"; }, 1500); }
    catch { message("Clipboard unavailable. The request path is shown in the inspector."); }
  };
  api("/health", true).then(data => {
    $("health").textContent = `${data.ok ? "API online" : "API reports quality issues"} · ${(data.sources || []).map(item => `${item.source_id}: ${item.health_status}`).join(" · ")}`;
    $("health").className = data.ok ? "status-good" : "status-bad";
  }).catch(error => { $("health").textContent = `Health check failed: ${error.message}`; $("health").className = "status-bad"; });
})();
