const nf = new Intl.NumberFormat();
const $ = (id) => document.getElementById(id);
const esc = (value) =>
  String(value).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

// A snapshot can arrive with empty totals, so live counting starts from 0 rather than undefined.
const num = (value) => (Number.isFinite(value) ? value : 0);

// A recorded request outlives the key it came in on, so a row whose key is gone says so instead
// of showing an empty cell.
const keyLabel = (event) => event.key || 'key removed';

const ICONS = {
  overview: '<path d="M4 17a8 8 0 0 1 16 0"/><path d="M12 14l4-3"/>',
  activity: '<path d="M3 12h4l3-8 4 16 3-8h4"/>',
  keys: '<circle cx="8.5" cy="12" r="3.5"/><path d="M12 12h9M18 12v4M21 12v-3"/>',
  connections: '<path d="M9 3v5M15 3v5M8 8h8v6a4 4 0 0 1-8 0z"/>',
  client: '<path d="M9 7l-5 5 5 5M15 7l5 5-5 5"/>',
  accounts: '<circle cx="12" cy="9" r="4"/><path d="M4 20a8 8 0 0 1 16 0"/>',
  sandbox: '<path d="M4 5h16v11H9l-5 4z"/><path d="M8 10h8"/>',
  check: '<circle cx="12" cy="12" r="9"/><path d="M8.5 12.5l2.5 2.5 5-5"/>',
  alert: '<path d="M12 3l9 17H3z"/><path d="M12 9v4M12 16.5h.01"/>',
  pending: '<circle cx="12" cy="12" r="9"/><path d="M12 7.5V12l3 2"/>',
  chevron: '<path d="M9 6l6 6-6 6"/>',
  panel: '<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M9 4v16"/>',
  exit: '<path d="M15 4H6a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h9"/><path d="M16 12h5M19 9l3 3-3 3"/>',
  // Half of the circle is filled, which is what reads as light and dark; the rest of the set is outline only.
  theme: '<circle cx="12" cy="12" r="8"/><path d="M12 4a8 8 0 0 1 0 16z" fill="currentColor"/>',
};

function icon(name) {
  return `<svg viewBox="0 0 24 24" aria-hidden="true">${ICONS[name]}</svg>`;
}

function paintIcons(root) {
  for (const el of root.querySelectorAll('[data-icon]')) el.innerHTML = icon(el.dataset.icon);
}

// The stored health is unknown until a check has run on a connection, which is not the same as down: the
// gateway only puts a connection whose check failed behind the others, so one that was never checked does
// not get the warning. A value this page does not know is shown as it is stored rather than read as one of
// these.
const HEALTH = {
  ok: ['ok', 'check', 'ok'],
  down: ['down', 'alert', 'down'],
  unknown: ['', 'pending', 'not checked yet'],
};

function healthChip(health) {
  const [kind, glyph, label] = Object.hasOwn(HEALTH, health) ? HEALTH[health] : ['', null, health];
  return `<span class="chip ${kind}">${glyph ? `<span class="icon">${icon(glyph)}</span>` : ''}<span>${esc(label)}</span></span>`;
}

const state = {
  me: null,
  summary: { self: {}, global: {}, hourly: [], daily: [], models: [], users: [] },
  keys: [],
  // Keys that belong to another account, read only by an admin, and which of them the page shows.
  otherKeys: [],
  keyOwner: '',
  connections: [],
  events: [],
  hours: [],
  days: [],
  live: null,
  view: 'overview',
  connectKey: null,
  theme: 'system',
  // The entry screen falls back to offering Register when the setting could not be read, so a
  // first account is never blocked by a failed request.
  settings: { open_registration: true, require_approval: true, has_accounts: false },
  presence: { online: 0, generating: 0, users: [] },
  // The streams in flight, as the gateway last said: every stream's share of what is being generated,
  // and the streams themselves as far as this account may see them.
  progress: { shares: [], streams: [] },
  chat: [],
  models: [],
  modelNote: '',
  sandboxKey: null,
  sandboxModel: null,
};

const VIEWS = ['overview', 'activity', 'keys', 'connections', 'connect', 'sandbox', 'accounts'];

// Views an account that is not an admin cannot open, so they are not in the navigation either.
const ADMIN_ONLY = ['accounts', 'connections'];

function viewAllowed(name) {
  return state.me.role === 'admin' || !ADMIN_ONLY.includes(name);
}

async function api(path, init) {
  const response = await fetch(path, { credentials: 'same-origin', ...init });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body?.error?.message || `request failed (${response.status})`);
  return body;
}

function postJson(path, body) {
  return api(path, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
}

function notice(message, kind) {
  const target = $('shell').hidden ? $('auth-error') : $('notice');
  if (!target) return;
  target.textContent = message || '';
  target.className = kind === 'error' ? 'notice error' : 'notice';
}

// A plain sentence for the user; the raw driver text stays in the console.
function report(err) {
  const message = String(err?.message || '');
  notice(message && message.length < 140 ? message : 'Something went wrong. Check the values and try again.', 'error');
  console.error(err);
}

// A copy button says Copied for a moment after it copies; this puts back the label it had before.
function restoreLabel(button) {
  clearTimeout(button.copiedTimer);
  if (button.dataset.label) button.textContent = button.dataset.label;
}

// The clipboard is only exposed on a secure origin, so this page served over http on a LAN
// address has no navigator.clipboard. Selecting a temporary textarea still copies there.
async function copy(text) {
  if (navigator.clipboard) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // Permission denied for this origin — fall through to the selection path.
    }
  }
  const area = document.createElement('textarea');
  area.value = text;
  area.setAttribute('readonly', '');
  area.style.position = 'fixed';
  area.style.left = '-100%';
  document.body.appendChild(area);
  area.select();
  const copied = document.execCommand('copy');
  area.remove();
  return copied;
}

// Menus replace the native select: a button plus a list, closed on outside click and
// Escape, navigable with the arrow keys. A glyph is for the menu that has to work in a
// collapsed rail, where the label text is not shown. The button's name carries the choice
// as well as what is being chosen, because a name on a button replaces the text inside it.
const menuName = (label, choice) => (choice ? `${label}: ${choice}` : label);

function renderMenu(container, name, options, current, label, glyph) {
  const picked = options.find((o) => o.value === current) || options[0];
  container.dataset.name = name;
  container.dataset.value = picked ? picked.value : '';
  container.innerHTML = `
    <button type="button" class="menu-button" aria-haspopup="listbox" aria-expanded="false" aria-label="${esc(menuName(label, picked?.label))}">
      ${glyph ? `<span class="icon">${icon(glyph)}</span>` : ''}
      <span>${esc(picked ? picked.label : label)}</span>
      <span class="icon">${icon('chevron')}</span>
    </button>
    <ul class="menu" role="listbox" aria-label="${esc(label)}" hidden>
      ${options.map((o) => `<li role="option" tabindex="-1" data-value="${esc(o.value)}" aria-selected="${o.value === (picked?.value ?? '')}">${esc(o.label)}</li>`).join('')}
    </ul>`;
}

function closeMenus() {
  for (const menu of document.querySelectorAll('.menu')) {
    menu.hidden = true;
    menu.closest('.menu-wrap').querySelector('.menu-button').setAttribute('aria-expanded', 'false');
  }
}

function openMenu(wrap) {
  const menu = wrap.querySelector('.menu');
  closeMenus();
  menu.hidden = false;
  wrap.querySelector('.menu-button').setAttribute('aria-expanded', 'true');

  // The theme list is the one that can end up past the edge of the window, because its control sits at
  // the bottom of the shell and a short screen cannot show the whole list above it. The room is measured
  // when the menu is shown: a stylesheet knows the window size but not where the button is. The list is
  // capped to that room so it stays inside the window.
  if (wrap.dataset.name === 'theme') {
    const room = wrap.querySelector('.menu-button').getBoundingClientRect().top - 8;
    menu.style.maxHeight = `${room}px`;
  }

  const first = menu.querySelector('[aria-selected="true"]') || menu.querySelector('[role="option"]');
  if (first) first.focus();
}

function chooseOption(option) {
  const wrap = option.closest('.menu-wrap');
  wrap.dataset.value = option.dataset.value;
  // In a menu button the glyph and the chevron carry .icon, so the label is the only span without a
  // class — the same span the collapsed-rail rule in web/style.css hides. It cannot be taken as the
  // first child, because a menu that has a glyph puts that first.
  wrap.querySelector('.menu-button span:not(.icon)').textContent = option.textContent;
  wrap.querySelector('.menu-button').setAttribute('aria-label', menuName(wrap.querySelector('.menu').getAttribute('aria-label'), option.textContent));
  for (const item of wrap.querySelectorAll('[role="option"]')) item.setAttribute('aria-selected', String(item === option));
  closeMenus();
  if (wrap.dataset.name === 'theme') {
    applyTheme(option.dataset.value);
    labelRail();
  }
  if (wrap.dataset.name === 'connect-key') {
    state.connectKey = option.dataset.value;
    // Copied was about the key that was showing, so it does not carry over to the one just picked.
    restoreLabel($('connect-copy'));
    renderConnect();
  }
  if (wrap.dataset.name === 'sandbox-key') {
    state.sandboxKey = option.dataset.value;
    loadModels().then(() => renderSandbox());
  }
  if (wrap.dataset.name === 'sandbox-model') state.sandboxModel = option.dataset.value;
  if (wrap.dataset.name === 'registration') setRegistration(option.dataset.value === 'open');
  if (wrap.dataset.name === 'approval') setApproval(option.dataset.value === 'needed');
  if (wrap.dataset.name === 'key-owner') {
    state.keyOwner = option.dataset.value;
    renderKeys();
  }
}

// Views

const RENDER = {
  overview: renderOverview,
  activity: renderActivity,
  keys: renderKeys,
  connections: renderConnections,
  connect: renderConnect,
  sandbox: renderSandbox,
  accounts: renderAccounts,
};

function currentView() {
  const name = (location.hash || '').replace(/^#\/?/, '');
  if (!VIEWS.includes(name)) return 'overview';
  if (!viewAllowed(name)) return 'overview';
  return name;
}

function route() {
  state.view = currentView();
  for (const link of document.querySelectorAll('.nav a')) {
    if (!viewAllowed(link.dataset.view)) {
      link.hidden = true;
      continue;
    }
    link.hidden = false;
    if (link.dataset.view === state.view) link.setAttribute('aria-current', 'page');
    else link.removeAttribute('aria-current');
  }
  for (const view of document.querySelectorAll('.view')) view.hidden = view.dataset.view !== state.view;
  render(false);
  // A model list comes from the upstream, so it is read when the Sandbox is opened rather than
  // on every page load.
  if (state.view === 'sandbox' && !state.models.length && state.keys.length) loadModels().then(() => renderSandbox());
}

function render(arrived) {
  renderNav();
  (RENDER[state.view] || renderOverview)(arrived);
}

function renderNav() {
  $('count-keys').textContent = state.keys.length ? String(state.keys.length) : '';
  const badge = $('count-connections');
  const down = state.connections.filter((c) => c.health === 'down').length;
  badge.textContent = state.connections.length ? String(state.connections.length) : '';
  const warned = $('count-connections-down');
  warned.textContent = down ? `${down} down` : '';
  warned.hidden = down === 0;
}

// Data

async function load() {
  // What was in flight belongs to the session that was open before; this one is told its own.
  state.progress = { shares: [], streams: [] };
  state.settings = await api('/api/settings');
  state.summary = await api('/api/stats/summary');
  state.keys = (await api('/api/keys')).keys;
  state.connections = (await api('/api/connections')).connections;
  state.events = (await api('/api/stats/recent')).events;
  await loadOtherKeys();
  await refreshPresence();
  buildAxes();
  showShell();
  route();
  subscribe();
}

// Only an admin can read another account's keys, so a member never asks for them.
async function loadOtherKeys() {
  state.otherKeys = state.me?.role === 'admin' ? (await api('/api/keys/others')).keys : [];
}

// Names of the accounts this page has seen generating. A live presence event carries ids but no names,
// which only /api/presence gives, and only to an admin; the names are kept for as long as the page is
// open, so an account is only looked up the first time it is seen rather than every time it starts.
const names = new Map();
let naming = null;

async function refreshPresence() {
  state.presence = await api('/api/presence');
  for (const user of state.presence.users || []) names.set(user.id, user.username);
}

function namedPresence(presence) {
  const ids = presence.generating_ids || [];
  return { ...presence, users: ids.filter((id) => names.has(id)).map((id) => ({ id, username: names.get(id) })) };
}

// One lookup at a time, and only the names are taken from it: the counts on the page stay the ones the
// latest event carried, which a slower answer to an older lookup would otherwise overwrite. An account
// that started while a lookup was out is looked up when it returns, and an answer that arrives after
// sign out is dropped, because the names belong to the admin who asked.
const unnamed = () => state.me?.role === 'admin' && state.presence.users.length < (state.presence.generating_ids || []).length;

function nameUnknown() {
  if (naming) return;
  naming = api('/api/presence')
    .then((presence) => {
      if (!state.me) return;
      for (const user of presence.users || []) names.set(user.id, user.username);
      state.presence = namedPresence(state.presence);
      render(false);
    })
    .catch(() => {})
    .finally(() => {
      naming = null;
      if (unnamed()) nameUnknown();
    });
}

async function refresh(path) {
  if (path === 'keys') {
    state.keys = (await api('/api/keys')).keys;
    await loadOtherKeys();
  }
  if (path === 'connections') state.connections = (await api('/api/connections')).connections;
  if (path === 'stats') state.summary = await api('/api/stats/summary');
  render(false);
}

// A counter moves with the same event the feed does. The top counter covers every account, so
// any event counts, including the figures a member is sent for another account's row; an account's
// own figures only move for the event that belongs to it.
// Averages are not updated here: they are the mean of the stored rows, and one event cannot move a
// mean that was computed over all of them.
const isToday = (value) => String(value).slice(0, 10) === new Date().toISOString().slice(0, 10);

function countTotals(totals, event) {
  if (!totals) return;
  totals.prompt_tokens = num(totals.prompt_tokens) + num(event.prompt_tokens);
  // What a model generated is the answer side on its own, so the generation counter keeps its own
  // figure rather than reading the total.
  totals.completion_tokens = num(totals.completion_tokens) + num(event.completion_tokens);
  // The cached part only counts when the engine reported it, which is what the page's note checks.
  if (event.cached_tokens !== null && event.cached_tokens !== undefined) {
    totals.cached_tokens = num(totals.cached_tokens) + event.cached_tokens;
    totals.cached_reported = num(totals.cached_reported) + 1;
  }
  totals.requests = num(totals.requests) + 1;
  // The day figures are what a daily account limit is measured against.
  if (isToday(event.created_at)) {
    totals.today_tokens = num(totals.today_tokens) + event.total_tokens;
    totals.today_requests = num(totals.today_requests) + 1;
    totals.today_prompt_tokens = num(totals.today_prompt_tokens) + num(event.prompt_tokens);
    totals.today_completion_tokens = num(totals.today_completion_tokens) + num(event.completion_tokens);
  }
}

function subscribe() {
  if (state.live) state.live.close();
  state.live = new EventSource('/api/live');
  // A live stream that is refused rather than dropped, as an expired session's is, is not tried again, so
  // what it last said about the streams in flight would stand for good; it is taken off the page instead.
  state.live.onerror = () => {
    if (state.live?.readyState !== EventSource.CLOSED) return;
    state.progress = { shares: [], streams: [] };
    renderProgress();
  };
  state.live.onmessage = (message) => {
    const payload = JSON.parse(message.data);
    if (payload.type === 'snapshot') {
      state.summary.self = payload.self;
      state.summary.global = payload.global;
      // The snapshot is what was recorded when the stream opened, so progress from before it, such as a
      // stream that ended while the page was reconnecting, no longer applies; the next tick says what does.
      state.progress = { shares: [], streams: [] };
      render(false);
      return;
    }
    if (payload.type === 'presence') {
      state.presence = namedPresence(payload.presence);
      if (unnamed()) nameUnknown();
      render(false);
      return;
    }
    if (payload.type === 'progress') {
      state.progress = payload;
      renderProgress();
      return;
    }
    // Another account's row, sent to a member as its figures alone: it only moves the platform total, and
    // takes its stream's share out of what is still being generated.
    if (payload.type === 'total') {
      retireStream(payload.event.stream);
      countTotals(state.summary.global, payload.event);
      renderProgress();
      return;
    }
    state.events.unshift(payload.event);
    state.events = state.events.slice(0, 50);
    const event = payload.event;
    retireStream(event.stream);
    // Every event counts toward the total the gate has produced, but only the caller's own events
    // move the account figures or the charts.
    countTotals(state.summary.global, event);
    if (event.user_id === state.me.id) countTotals(state.summary.self, event);
    // An account's row in the Accounts view moves with the same event, so its input and output do not
    // stand still beside a day figure that has moved.
    const account = state.summary.users.find((user) => user.id === event.user_id);
    if (account) {
      account.requests = num(account.requests) + 1;
      account.prompt_tokens = num(account.prompt_tokens) + num(event.prompt_tokens);
      account.completion_tokens = num(account.completion_tokens) + num(event.completion_tokens);
      if (isToday(event.created_at)) account.used_today = num(account.used_today) + event.total_tokens;
    }
    // The charts are per account, so an event from another account does not move them.
    if (event.user_id === state.me.id) addEvent(event);
    const wire = $('gate-wire');
    wire.classList.add('arrive');
    setTimeout(() => wire.classList.remove('arrive'), 180);
    render(true);
  };
}

// A recorded row takes the place of the stream it settles, in the same render that adds the row's
// figures, so the two are never counted together while the next progress update is on its way.
function retireStream(id) {
  if (id === null || id === undefined) return;
  state.progress.shares = state.progress.shares.filter((s) => s.stream !== id);
  state.progress.streams = state.progress.streams.filter((s) => s.stream !== id);
}

// Progress arrives up to four times a second while anything is generating, so it repaints only what it
// moves, on the Overview, rather than the whole view: a full render rebuilds every table and menu on the
// page, and an open menu or a half-typed field would not survive it that often.
function renderProgress() {
  if (state.view !== 'overview') return;
  renderReadout();
  renderFeed(false);
}

// Time axes. Buckets persist between renders so a live event can add to them.
function buildAxes() {
  state.hours = [];
  state.days = [];
  const now = Date.now();
  for (let i = 23; i >= 0; i -= 1) {
    const at = new Date(now - i * 3600 * 1000);
    state.hours.push({ key: at.toISOString().slice(0, 13) + ':00:00Z', label: String(at.getUTCHours()).padStart(2, '0'), tokens: 0, requests: 0, errors: 0 });
  }
  for (let i = 13; i >= 0; i -= 1) {
    const at = new Date(now - i * 86400 * 1000);
    state.days.push({ key: at.toISOString().slice(0, 10), label: at.toISOString().slice(5, 10), tokens: 0, requests: 0, errors: 0 });
  }
  for (const row of state.summary.hourly) {
    const bucket = state.hours.find((b) => b.key === row.bucket);
    if (bucket) Object.assign(bucket, row);
  }
  for (const row of state.summary.daily) {
    const bucket = state.days.find((b) => b.key === row.bucket);
    if (bucket) Object.assign(bucket, row);
  }
}

function addEvent(event) {
  const hour = state.hours.find((b) => b.key === String(event.created_at).slice(0, 13) + ':00:00Z');
  if (hour) {
    hour.tokens += event.total_tokens;
    hour.requests += 1;
    hour.errors += event.status >= 400 ? 1 : 0;
  }
  const day = state.days.find((b) => b.key === String(event.created_at).slice(0, 10));
  if (day) {
    day.tokens += event.total_tokens;
    day.requests += 1;
    day.errors += event.status >= 400 ? 1 : 0;
  }
}

// Charts

function svg(inner, height, label) {
  return `<svg viewBox="0 0 900 ${height}" role="img" aria-label="${esc(label)}">${inner}</svg>`;
}

function areaChart(buckets) {
  const height = 190;
  const innerHeight = height - 24;
  const max = Math.max(1, ...buckets.map((b) => b.tokens));
  const step = 884 / Math.max(1, buckets.length - 1);
  const points = buckets.map((b, i) => [10 + i * step, innerHeight - 6 - (b.tokens / max) * (innerHeight - 14)]);
  const line = points.map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)} ${y.toFixed(1)}`).join(' ');
  const area = `${line} L${points.at(-1)[0].toFixed(1)} ${innerHeight} L${points[0][0].toFixed(1)} ${innerHeight} Z`;
  const grid = [0.25, 0.5, 0.75]
    .map((f) => `<line class="grid" x1="10" x2="894" y1="${(innerHeight - f * (innerHeight - 14)).toFixed(1)}" y2="${(innerHeight - f * (innerHeight - 14)).toFixed(1)}" />`)
    .join('');
  const labels = buckets
    .filter((_, i) => i % 4 === 0)
    .map((b, i) => `<text class="axis" x="${(10 + i * 4 * step).toFixed(1)}" y="${height - 6}">${b.label}</text>`)
    .join('');
  return svg(`${grid}<path class="area" d="${area}"/><path class="line" d="${line}"/>${labels}`, height, `Peak ${nf.format(max)} input and output tokens in an hour`);
}

function barChart(buckets) {
  const height = 160;
  const max = Math.max(1, ...buckets.map((b) => b.requests));
  const slot = 884 / buckets.length;
  const bars = buckets
    .map((b, i) => {
      const h = Math.max(1, (b.requests / max) * (height - 34));
      const peak = b.requests === max && b.requests > 0;
      return `<rect class="bar${peak ? ' peak' : ''}" x="${(10 + i * slot + 2).toFixed(1)}" y="${(height - 24 - h).toFixed(1)}" width="${(slot - 4).toFixed(1)}" height="${h.toFixed(1)}"/>`;
    })
    .join('');
  const labels = buckets
    .map((b, i) => `<text class="axis" text-anchor="middle" x="${(10 + i * slot + slot / 2).toFixed(1)}" y="${height - 6}">${b.label}</text>`)
    .join('');
  return svg(`<line class="grid" x1="10" x2="894" y1="${height - 24}" y2="${height - 24}"/>${bars}${labels}`, height, `Peak ${nf.format(max)} requests in a day`);
}

function empty(message) {
  return `<p class="note">${esc(message)}</p>`;
}

// Overview

function renderOverview(arrived) {
  renderGate();
  renderReadout();
  renderPresence();
  renderCharts();
  renderFeed(arrived);
}

// The connection the gateway tries first for a key, in the order it uses: the key's own connection
// while it is enabled, then the rest by priority, with every connection marked down after the ones
// that are not, in that same order. A key bound to a connection marked down therefore starts with
// another one that is not, and one marked down comes first only when every one is.
function firstConnection(key) {
  const enabled = state.connections.filter((c) => c.enabled).sort((a, b) => a.priority - b.priority);
  const bound = key && key.connection_id ? enabled.find((c) => c.id === key.connection_id) : null;
  const ordered = bound ? [bound, ...enabled.filter((c) => c !== bound)] : enabled;
  return ordered.find((c) => c.health !== 'down') || ordered[0] || null;
}

function renderGate() {
  // The path shown has to be the path a request actually took, so the key comes from the
  // last event and the connection from the order the gateway tries for that key.
  const last = state.events[0];
  const key = last ? state.keys.find((k) => k.name === last.key) : null;
  const conn = firstConnection(key);
  $('gate-key').textContent = last ? keyLabel(last) : (state.keys.length ? state.keys[0].name : 'no key yet');
  // The name is clamped to the width of a leg, and the chip sits beside it rather than inside it, so a
  // long name is cut short and the health still shows.
  $('gate-upstream').textContent = conn ? conn.name : 'no connection yet';
  $('gate-health').innerHTML = conn ? healthChip(conn.health) : '';

  if (!last) {
    $('gate-read').textContent = 'Nothing has crossed the gate yet.';
    return;
  }
  $('gate-read').innerHTML = [
    'last request',
    `<b>${esc(last.created_at.slice(11, 19))}</b>`,
    esc(last.model || 'no model'),
    `${rowTokens(last, last.prompt_tokens)} input and ${rowTokens(last, last.completion_tokens)} output tokens`,
    `<span class="${last.status >= 400 ? 'danger' : ''}">${esc(last.status)}</span>`,
  ].join(' ');
}

// Both pairs ask the same two questions of a different scope: the top one is the whole gate, the
// one below the gate diagram is this account. The generated side is the headline, and the input is
// shown on its own beside it. An agentic client sends the whole conversation again on every turn, so
// the input figure is mostly repeats and the generated figure is the part that says what happened.
// The support panel below repeats the generated number as part of its breakdown; it is the same
// figure, not a second one.
function renderReadout() {
  const own = state.progress.streams.filter((s) => s.user_id === state.me.id);
  const shares = state.progress.shares;
  renderPair(['generated-tokens', 'global-generated-note', 'global-input-tokens', 'global-input-note'], state.summary.global, 'every account and key', {
    tokens: shares.reduce((sum, s) => sum + num(s.completion_tokens), 0),
    estimated: shares.some((s) => s.estimated),
  });
  renderPair(['account-generated-tokens', 'account-generated-note', 'account-input-tokens', 'account-input-note'], state.summary.self, 'this account', {
    tokens: own.reduce((sum, s) => sum + num(s.completion_tokens), 0),
    estimated: own.some((s) => s.estimated),
  });
  renderSupport(state.summary.self);
}

// The cached part of the input is only a note when an engine reported it. Nothing is invented here:
// a row that reported no cache is stored as unknown, so a zero cache across the whole instance reads
// as "no engine reported a cache hit", not as a cache that never hit. The notes open with the scope,
// because the two pairs show the same figures for a different set of requests.
// What a stream still running has generated counts toward the generated figure as it arrives, so the
// number moves with the answer. The note says how much of the figure is still in flight and, while the
// gateway is counting it rather than the upstream, that it is an estimate until the upstream reports it.
// Recording the request does not end that: an upstream that never reports is recorded as the estimate.
function renderPair([generatedId, generatedNoteId, inputId, inputNoteId], totals, scope, inflight) {
  const t = totals || {};
  $(generatedId).textContent = nf.format(num(t.completion_tokens) + inflight.tokens);
  const running = inflight.estimated ? 'still being generated, estimated by the gateway until the upstream reports it' : 'still being generated';
  $(generatedNoteId).textContent = inflight.tokens
    ? `what the models generated for ${scope}, including ${nf.format(inflight.tokens)} ${running}`
    : `what the models generated for ${scope}`;
  $(inputId).textContent = nf.format(num(t.prompt_tokens));
  const note = $(inputNoteId);
  if (num(t.cached_reported)) {
    note.textContent = `what ${scope} sent in, of which ${nf.format(num(t.cached_tokens))} came from the prompt cache`;
  } else {
    note.textContent = `what ${scope} sent in. No engine reported a prompt cache hit.`;
  }
}

function renderSupport(t) {
  const rows = [
    ['Requests', nf.format(t.requests || 0)],
    ['Requests today', nf.format(t.today_requests || 0)],
    [
      'Tokens today (input + output)',
      `${nf.format(t.today_tokens || 0)} — ${nf.format(t.today_prompt_tokens || 0)} input, ${nf.format(t.today_completion_tokens || 0)} output`,
    ],
    ['Average latency', `${nf.format(t.avg_latency_ms || 0)} ms`],
    ['Errors (status 400 or worse)', nf.format(t.errors || 0)],
  ];
  // These appear only for an engine that reports timings. The API gives null when no row has one, so
  // the row is left out rather than shown as a zero; the speed is measured over the rows whose counts
  // the upstream reported, so an estimated row cannot move it. Averages belong to the stored rows and
  // do not move with a live event.
  if (t.avg_prompt_ms !== null && t.avg_prompt_ms !== undefined) {
    rows.push(['Average prompt processing', `${nf.format(t.avg_prompt_ms)} ms`]);
  }
  if (t.generation_tokens_per_second !== null && t.generation_tokens_per_second !== undefined) {
    rows.push(['Average generation speed', `${nf.format(t.generation_tokens_per_second)} output tokens/s`]);
  }
  $('support').innerHTML = rows
    .map(([label, value]) => `<div class="row"><dt>${esc(label)}</dt><dd>${esc(value)}</dd></div>`)
    .join('');
}

// Presence is a live count, so it comes from the hub rather than from stored rows.
function renderPresence() {
  const present = state.presence;
  const rows = [
    ['Online accounts', present.online ? nf.format(present.online) : 'none'],
    ['Generating now', present.generating ? nf.format(present.generating) : 'none'],
  ];
  // Names are scoped like the rest of the stats: the endpoint only returns them to an admin.
  if (state.me.role === 'admin') {
    for (const user of present.users) rows.push([user.username, 'generating']);
  }
  $('presence').innerHTML = rows
    .map(([label, value]) => `<div class="row"><dt>${esc(label)}</dt><dd>${esc(value)}</dd></div>`)
    .join('');
}

function renderCharts() {
  const anyTraffic = state.hours.some((b) => b.requests > 0);
  $('chart-hourly').innerHTML = anyTraffic ? areaChart(state.hours) : empty('Nothing measured yet. Send a request through the gateway and it appears here.');
  $('chart-daily').innerHTML = state.days.some((b) => b.requests > 0) ? barChart(state.days) : empty('No requests in the last 14 days.');
}

// An admin's stream carries every account's requests, and anyone else's only their own, so the heading
// says which of the two the feed is.
// A stream still running is a row of its own above the recorded ones, with live in place of a status it
// does not have yet. Its tokens are the input and what has been generated so far, in the same two columns
// a recorded row fills, and each is marked with a ~ while it is the gateway's estimate. Each column has its
// own flag, because an upstream can report the input before what it generated, and a flag that is missing
// is read as an estimate rather than as a reported count.
function streamTokens(tokens, estimated) {
  const how = estimated ? 'Estimated by the gateway: the upstream has not reported this count yet' : 'As the upstream has reported so far';
  return `<span class="tokens" title="${how}">${estimated ? '~' : ''}${nf.format(num(tokens))}</span>`;
}

// A recorded row is marked the same way when the gateway counted it because the upstream reported nothing:
// an upstream that never reports usage, a stream cut short before its usage chunk, a request that reached
// no connection. The row carries one flag for its counts, so the input and the output are marked together,
// and a missing flag is read as an estimate here as well.
function rowTokens(event, tokens) {
  const count = nf.format(num(tokens));
  return event.estimated !== false ? `<span title="Estimated by the gateway: the upstream did not report this count">~${count}</span>` : count;
}

function streamRow(stream) {
  return `<li class="inflight">
    <span>${esc(stream.started_at.slice(11, 19))}</span>
    <span>${esc(stream.key)}</span>
    <span>${esc(stream.model || 'no model')}</span>
    <span>live</span>
    ${streamTokens(stream.prompt_tokens, stream.prompt_estimated !== false)}
    ${streamTokens(stream.completion_tokens, stream.estimated !== false)}
  </li>`;
}

function renderFeed(arrived) {
  $('feed-title').textContent = state.me.role === 'admin' ? 'Live traffic, every account' : 'Live traffic for this account';
  // The feed holds twelve rows, streams first, so as many streams as the gateway runs at once do not push
  // the page down; the recorded rows fill whatever is left.
  const streams = [...state.progress.streams].sort((a, b) => b.stream - a.stream).slice(0, 12);
  $('feed-head').hidden = !state.events.length && !streams.length;
  const recorded = state.events
    .slice(0, 12 - streams.length)
    .map(
      (event, i) =>
        `<li class="${arrived && i === 0 ? ' arrived' : ''}">
          <span>${esc(event.created_at.slice(11, 19))}</span>
          <span>${esc(keyLabel(event))}</span>
          <span>${esc(event.model || 'no model')}</span>
          <span class="${event.status >= 400 ? 'danger' : ''}">${esc(event.status)}</span>
          <span class="tokens">${rowTokens(event, event.prompt_tokens)}</span>
          <span class="tokens">${rowTokens(event, event.completion_tokens)}</span>
        </li>`,
    )
    .join('');
  $('feed').innerHTML =
    streams.length || state.events.length
      ? streams.map(streamRow).join('') + recorded
      : empty('No requests yet. Create a key and send one, and the row lands here as it happens.');
}

// Activity

// The recent rows and the models are an admin's for every account and anyone else's for their own, the
// same scope the live stream keeps, so the lede says which.
function renderActivity() {
  const admin = state.me.role === 'admin';
  $('activity-lede').textContent = `${
    admin ? 'The last 50 requests that crossed the gate from every account' : 'The last 50 requests this account sent through the gate'
  }, newest first, with what each cost. New rows arrive as they happen. Times are UTC.`;
  const columns = ['Time', 'Key', admin ? 'Account' : null, 'Model', 'Input tokens', 'Output tokens', 'Latency', 'Status'].filter(Boolean);
  $('activity-head').innerHTML = `<tr>${columns.map((c) => `<th>${esc(c)}</th>`).join('')}</tr>`;

  $('activity-body').innerHTML = state.events.length
    ? state.events
        .map(
          (event) =>
            `<tr>
              <td>${esc(event.created_at.slice(11, 19))}</td>
              <td><span class="clamp">${esc(keyLabel(event))}</span></td>
              ${admin ? `<td><span class="clamp">${esc(event.user)}</span></td>` : ''}
              <td><span class="clamp">${esc(event.model || 'no model')}</span></td>
              <td class="num">${rowTokens(event, event.prompt_tokens)}</td>
              <td class="num">${rowTokens(event, event.completion_tokens)}</td>
              <td class="num">${nf.format(event.latency_ms)} ms</td>
              <td class="num${event.status >= 400 ? ' danger' : ''}">${esc(event.status)}</td>
            </tr>`,
        )
        .join('')
    : `<tr><td colspan="${columns.length}">No requests recorded yet. Send one through the gateway and it appears here.</td></tr>`;

  $('models-body').innerHTML = state.summary.models.length
    ? state.summary.models
        .map(
          (model) =>
            `<tr><td><span class="clamp">${esc(model.model)}</span></td><td class="num">${nf.format(model.requests)}</td><td class="num">${nf.format(model.prompt_tokens)}</td><td class="num">${nf.format(model.completion_tokens)}</td></tr>`,
        )
        .join('')
    : `<tr><td colspan="4">No model has been used yet. Add a connection, then send a request with a key.</td></tr>`;
}

// Keys

function connectionOptions() {
  return [{ value: '', label: 'Any enabled connection' }, ...state.connections.map((c) => ({ value: String(c.id), label: c.name }))];
}

// For chat and completions, the gateway takes a key's own model first, then the default of the connection that
// serves it, and the model the client asks for only when neither is set; an embedding takes the model the
// client asks for first, which this column does not show. A key that is not bound to one connection can
// be served by any, so it can only say which of the two it may be, and so can a key whose connection is
// marked down, because the gateway starts with another one.
function keyModel(key, bound) {
  if (key.model) return key.model;
  if (bound && bound.enabled && firstConnection(key) === bound) return bound.default_model || 'the model the client asks for';
  return state.connections.some((c) => c.enabled && c.default_model)
    ? "the connection's default, or the model the client asks for"
    : 'the model the client asks for';
}

// When a key was last used, from its last recorded request, and when it stops working, in UTC like every
// time on the page.
function keyTimes(key) {
  const used = key.last_used_at ? `last used ${key.last_used_at.slice(0, 16).replace('T', ' ')} UTC` : 'never used';
  if (!key.expires_at) return esc(used);
  // An expiry given as a date is the first moment of the day after it, so it reads as the day it works through.
  const at = key.expires_at.endsWith('T00:00:00Z')
    ? `${new Date(Date.parse(key.expires_at) - 1).toISOString().slice(0, 10)} UTC, through the day`
    : `${key.expires_at.slice(0, 16).replace('T', ' ')} UTC`;
  return new Date(key.expires_at) > new Date()
    ? `${esc(used)} · expires ${esc(at)}`
    : `${esc(used)} · <span class="danger">expired ${esc(at)}</span>`;
}

function keyRow(key, showOwner) {
  // A binding to a disabled connection does not hold: the key uses another one.
  const bound = key.connection_id ? state.connections.find((c) => c.id === key.connection_id) : null;
  const binding = bound && !bound.enabled ? `${esc(bound.name)} <span class="muted">binding inactive</span>` : esc(key.connection || 'any');
  // A key that is off is still a key with a history, so the row stays and only reads quieter; the
  // Enable button in it says plainly which state it is in.
  return `<tr class="${key.enabled ? '' : 'off'}">
    ${showOwner ? `<td>${esc(key.owner)}</td>` : ''}
    <td>
      <span class="stack">
        <span class="clamp" title="${esc(key.key_hint)}">${esc(key.name)}</span>
        <span class="muted">${keyTimes(key)}</span>
      </span>
    </td>
    <td>${binding}</td>
    <td>${esc(keyModel(key, bound))}</td>
    <td>${key.rpm_limit ? `${key.rpm_limit}/min` : 'no rate limit'}<br>${key.token_cap ? `${nf.format(key.token_cap)} input + output tokens` : 'no token cap'}</td>
    <td class="num">${nf.format(key.requests)}</td>
    <td class="num">${nf.format(key.prompt_tokens)}</td>
    <td class="num">${nf.format(key.completion_tokens)}</td>
    <td class="num">
      ${key.key ? `<button class="link" data-copy="${esc(key.key)}">Copy key</button>` : ''}
      <button class="link" data-rotate="${key.id}">Rotate</button>
      <button class="link" data-toggle="${key.id}" data-enabled="${key.enabled}">${key.enabled ? 'Disable' : 'Enable'}</button>
      <button class="link danger" data-delete="${key.id}">Delete</button>
    </td>
  </tr>`;
}

// Only accounts that have a key can be picked, so the list is the keys themselves.
function keyOwnerOptions() {
  const owners = [...new Set(state.otherKeys.map((key) => key.owner))].sort((a, b) => a.localeCompare(b));
  return [{ value: '', label: 'None' }, { value: 'all', label: 'All other accounts' }, ...owners.map((name) => ({ value: name, label: name }))];
}

function renderKeys() {
  const admin = state.me.role === 'admin';
  // A date that has already ended in UTC is refused, so it is not offered.
  $('key-expires').min = new Date().toISOString().slice(0, 10);
  // The table above is this account's own keys for everyone; another account's keys are a
  // separate read an admin can ask for.
  $('other-keys-field').hidden = !admin;
  $('keys-body').innerHTML = state.keys.length
    ? state.keys.map((key) => keyRow(key, false)).join('')
    : `<tr><td colspan="8">No key yet. Create one above, then use it as the Bearer token, or as x-api-key, against /v1/chat/completions.</td></tr>`;

  renderMenu($('connection-menu'), 'connection', connectionOptions(), $('connection-menu').dataset.value || '', 'Connection');
  if (!admin) return;

  const options = keyOwnerOptions();
  // A picked account can stop having keys, because its key or the account is deleted, so a
  // choice that is no longer offered is dropped instead of leaving an empty table.
  if (!options.some((o) => o.value === state.keyOwner)) state.keyOwner = '';

  const hasOthers = state.otherKeys.length > 0;
  $('key-owner-menu').hidden = !hasOthers;
  $('other-keys-note').hidden = hasOthers;
  $('other-keys-note').textContent = hasOthers ? '' : 'No key belongs to another account yet.';
  if (hasOthers) renderMenu($('key-owner-menu'), 'key-owner', options, state.keyOwner, 'Keys from other accounts');

  const rows = state.otherKeys.filter((key) => state.keyOwner && (state.keyOwner === 'all' || key.owner === state.keyOwner));
  $('other-keys-table').hidden = !rows.length;
  $('other-keys-body').innerHTML = rows.map((key) => keyRow(key, true)).join('');
}

// Connections

function renderConnections() {
  $('connections-body').innerHTML = state.connections.length
    ? state.connections
        .map(
          (connection) => `<tr>
            <td><span class="clamp">${esc(connection.name)}</span></td>
            <td><span class="clamp">${esc(connection.base_url)}</span></td>
            <td>${esc(connection.default_model || 'the model the client asks for')}</td>
            <td class="num">${esc(connection.priority)}</td>
            <td>${healthChip(connection.health)} ${modelsNote(connection.models)}</td>
            <td class="num">
              <button class="link" data-check="${connection.id}">Check health</button>
              <button class="link danger" data-drop="${connection.id}">Remove</button>
            </td>
          </tr>`,
        )
        .join('')
    : `<tr><td colspan="6">No connection yet. Add one above. Any endpoint that speaks the OpenAI API works, including llama.cpp, Ollama and vLLM.</td></tr>`;
}

// What the last check read of a connection's model list, which is what a request for a model is routed by.
// A list that was never read is said to be unknown rather than shown as empty.
function modelsNote(models) {
  if (!Array.isArray(models)) return '<span class="muted" title="No model list has been read from this connection yet">models unknown</span>';
  const label = models.length === 1 ? '1 model' : `${nf.format(models.length)} models`;
  return `<span class="muted" title="${esc(models.join(', '))}">${label}</span>`;
}

// Connect a client

function renderConnect() {
  const base = `${location.origin}/v1`;
  $('connect-base').textContent = base;

  renderMenu(
    $('connect-key-menu'),
    'connect-key',
    state.keys.length ? state.keys.map((k) => ({ value: k.key, label: k.name })) : [{ value: '', label: 'no key yet' }],
    state.connectKey || '',
    'API key',
  );

  const key = state.keys.find((k) => k.key === state.connectKey) || state.keys[0];
  $('connect-model').textContent = key
    ? key.model
      ? `This key resolves to ${key.model} for chat and completions; an embedding uses the model the client names. Use GET ${base}/models to see the models the connections serve.`
      : `Use whatever the key resolves to, or list it with GET ${base}/models.`
    : 'Create a key first. The key is what the client sends.';

  $('connect-curl').textContent = key
    ? `curl ${base}/chat/completions \\\n  -H "authorization: Bearer ${key.key}" \\\n  -H 'content-type: application/json' \\\n  -d '{"model":"<model>","messages":[{"role":"user","content":"hi"}]}'`
    : 'No key yet. Create one on the Keys page.';

  $('connect-copy').dataset.copy = key ? key.key : '';
  $('connect-copy').disabled = !key;
}

// Sandbox. Chat through the same gateway a client uses, so the key is sent as the Bearer
// token and the request is counted like any other. The conversation itself stays in this page.

async function loadModels() {
  const key = state.sandboxKey || state.keys[0]?.key;
  state.models = [];
  state.modelNote = '';
  if (!key) {
    state.modelNote = 'Create a key first. The Sandbox sends it as the Bearer token.';
    return;
  }
  try {
    const data = await api('/v1/models', { headers: { authorization: `Bearer ${key}` } });
    state.models = (data.data ?? []).map((model) => model.id);
    if (!state.models.length) state.modelNote = 'No connection listed any models. Check them on the Connections page.';
  } catch (err) {
    state.modelNote = String(err?.message || 'The model list could not be read.');
  }
}

function renderSandbox() {
  const key = state.keys.find((k) => k.key === state.sandboxKey) || state.keys[0];
  state.sandboxKey = key ? key.key : '';
  renderMenu(
    $('sandbox-key-menu'),
    'sandbox-key',
    state.keys.length ? state.keys.map((k) => ({ value: k.key, label: k.name })) : [{ value: '', label: 'no key yet' }],
    state.sandboxKey,
    'Key',
  );

  const options = state.models.length ? state.models.map((id) => ({ value: id, label: id })) : [{ value: '', label: 'no model available' }];
  if (!options.some((o) => o.value === state.sandboxModel)) state.sandboxModel = options[0].value;
  renderMenu($('sandbox-model-menu'), 'sandbox-model', options, state.sandboxModel, 'Model');

  const note = $('sandbox-model-note');
  note.textContent = state.modelNote;
  note.hidden = !state.modelNote;

  renderTranscript();
  $('chat-send').disabled = !state.sandboxKey || !state.sandboxModel;
}

// A turn is the speaker line, then the reasoning the model streamed as its own section, then the
// answer. Reasoning is kept apart because it is the model's working, not what it is saying to you.
function turnHtml(turn) {
  const name = turn.role === 'user' ? 'You' : esc(turn.model || 'model');
  const reasoning = turn.role === 'assistant'
    ? `<div class="reasoning"${turn.reasoning ? '' : ' hidden'}><span class="label">Reasoning</span><p>${esc(turn.reasoning || '')}</p></div>`
    : '';
  return `<li class="turn ${turn.role}"><span class="who">${name}</span>${reasoning}<p class="answer">${esc(turn.content)}</p></li>`;
}

function renderTranscript() {
  const list = $('transcript');
  list.innerHTML = state.chat.length
    // Each turn keeps the model that answered it, so a later change of model does not
    // relabel an earlier answer.
    ? state.chat.map(turnHtml).join('')
    : '<li class="turn empty">Nothing said yet. Pick a key and a model, then ask something.</li>';
}

async function sendChat(text) {
  const key = state.sandboxKey;
  const model = state.sandboxModel;
  if (!key || !model) {
    notice('Pick a key and a model first.', 'error');
    return;
  }
  state.chat.push({ role: 'user', content: text });
  const turn = { role: 'assistant', content: '', reasoning: '', model };
  state.chat.push(turn);

  // Read the stream the way a client would: complete SSE lines, keeping the answer as it
  // arrives. A non-streaming answer still works because the body falls back to JSON. The last
  // turn is this one, so it is looked up again on each paint rather than held.
  const paint = () => {
    const row = $('transcript').lastElementChild;
    if (!row) return;
    const section = row.querySelector('.reasoning');
    if (section) {
      section.hidden = !turn.reasoning;
      section.querySelector('p').textContent = turn.reasoning;
    }
    const answer = row.querySelector('.answer');
    if (answer) answer.textContent = turn.content;
  };
  renderTranscript();
  $('chat-send').disabled = true;
  try {
    const response = await fetch('/v1/chat/completions', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json' },
      body: JSON.stringify({ model, stream: true, messages: state.chat.map(({ role, content }) => ({ role, content })) }),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      turn.content = body?.error?.message || `The gateway answered ${response.status}.`;
    } else if ((response.headers.get('content-type') || '').includes('text/event-stream')) {
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let finished = false;
      while (!finished) {
        const chunk = await reader.read();
        if (chunk.done) break;
        buffer += decoder.decode(chunk.value, { stream: true });
        for (;;) {
          const index = buffer.indexOf('\n');
          if (index === -1) break;
          const line = buffer.slice(0, index).trim();
          buffer = buffer.slice(index + 1);
          if (!line.startsWith('data:')) continue;
          const data = line.slice(5).trim();
          if (data === '[DONE]') {
            finished = true;
            break;
          }
          let value;
          try {
            value = JSON.parse(data);
          } catch {
            continue; // an upstream line we cannot read is skipped, not fatal
          }
          const delta = value.choices?.[0]?.delta;
          if (!delta) continue;
          // Reasoning arrives in its own field, before or beside the answer, depending on the
          // upstream. Both are kept so the section fills in as it streams.
          const thinking = delta.reasoning_content ?? delta.reasoning;
          if (typeof thinking === 'string' && thinking) turn.reasoning += thinking;
          if (typeof delta.content === 'string' && delta.content) turn.content += delta.content;
          if (turn.reasoning || turn.content) paint();
        }
      }
    } else {
      const body = await response.json();
      const message = body?.choices?.[0]?.message;
      turn.reasoning = message?.reasoning_content ?? message?.reasoning ?? '';
      turn.content = message?.content || '';
    }
    if (!turn.content && !turn.reasoning) turn.content = 'The answer came back empty.';
  } catch (err) {
    turn.content = String(err?.message || 'The request failed.');
    console.error(err);
  }
  paint();
  $('chat-send').disabled = false;
}

// Accounts. The view is admin-only, so the limit control is always editable here.
function renderAccounts() {
  renderAccountSettings();
  $('accounts-body').innerHTML = state.summary.users.length
    ? state.summary.users
        .map((user) => {
          const cap = user.daily_token_cap;
          const over = cap !== null && user.used_today >= cap;
          const disabled = !user.enabled;
          const name = esc(user.username);
          return `<tr>
            <td>
              <span class="stack">
                <span class="inline-field">
                  <input class="username" value="${name}" data-for="${user.id}" aria-label="Username for ${name}">
                  <button class="secondary" data-rename="${user.id}">Rename</button>
                </span>
                <span class="inline-field">
                  <input class="password" data-for="${user.id}" placeholder="new password" aria-label="New password for ${name}">
                  <button class="secondary" data-password="${user.id}">Set password</button>
                </span>
                ${user.pending ? '<span class="muted">waiting for approval</span>' : ''}
                ${disabled ? '<span class="danger">disabled</span>' : ''}
              </span>
            </td>
            <td>
              <span class="inline-field">
                <input class="cap" type="number" min="1" value="${cap ?? ''}" placeholder="no limit" data-for="${user.id}" aria-label="Daily limit for ${name}">
                <button class="secondary" data-limit="${user.id}">Set limit</button>
              </span>
            </td>
            <td class="num">${nf.format(user.used_today)}${over ? ' <span class="danger">over</span>' : ''}</td>
            <td class="num">${nf.format(user.requests)}</td>
            <td class="num">${nf.format(user.prompt_tokens)}</td>
            <td class="num">${nf.format(user.completion_tokens)}</td>
            <td class="num">
              ${user.pending ? `<button class="link" data-approve="${user.id}">Approve</button>` : ''}
              <button class="link" data-status="${user.id}" data-enabled="${user.enabled}">${disabled ? 'Enable' : 'Disable'}</button>
              ${user.id === state.me.id ? '<span class="muted">signed in</span>' : `<button class="link danger" data-account="${user.id}">Delete</button>`}
            </td>
          </tr>`;
        })
        .join('')
    : '<tr><td colspan="7">No account yet. The first account to register becomes the admin.</td>';
}

// The settings on this page are for an admin: whether anyone may register, whether a new account
// has to be approved, and the form that makes an account without opening registration.
function renderAccountSettings() {
  const admin = state.me.role === 'admin';
  for (const id of ['registration-field', 'approval-field', 'account-form']) $(id).hidden = !admin;
  if (!admin) return;
  renderMenu(
    $('registration-menu'),
    'registration',
    [
      { value: 'closed', label: 'Closed' },
      { value: 'open', label: 'Open' },
    ],
    state.settings.open_registration ? 'open' : 'closed',
    'Self-registration',
  );
  renderMenu(
    $('approval-menu'),
    'approval',
    [
      { value: 'needed', label: 'Needed' },
      { value: 'not needed', label: 'Not needed' },
    ],
    state.settings.require_approval ? 'needed' : 'not needed',
    'Approval for new accounts',
  );
}

async function setRegistration(open) {
  try {
    await api('/api/settings', {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ open_registration: open }),
    });
    state.settings.open_registration = open;
    renderAccounts();
    notice(open ? 'Self-registration is open. Anyone can make an account.' : 'Self-registration is closed.');
  } catch (err) {
    report(err);
  }
}

async function setApproval(needed) {
  try {
    await api('/api/settings', {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ require_approval: needed }),
    });
    state.settings.require_approval = needed;
    renderAccounts();
    notice(needed ? 'A new account now waits for approval before it can sign in.' : 'Approval is off, so a new account can sign in straight away.');
  } catch (err) {
    report(err);
  }
}

// Shell and auth

function showAuth() {
  $('auth').hidden = false;
  $('shell').hidden = true;
  $('account').textContent = '';
  renderAuth();
}

// The mode the entry screen is in. Declared above the functions that read it, since it is only ever
// used from a render or a click, both of which happen after this has run.
let mode = 'login';

// The entry screen opens on Sign in. An account with a password signs in, so that is the default; an
// instance with no accounts has nothing to sign in to, so a first account opens on Register. The mode
// is chosen on every render rather than kept, because a mode the screen hides is one the server
// refuses: Register is only offered while registration is open, and `POST /api/register` answers 403
// when it is not.
function renderAuth() {
  const open = state.settings.open_registration;
  $('first-account-note').hidden = state.settings.has_accounts;
  $('mode-register').hidden = !open;
  $('register-note').hidden = open;
  const firstAccount = !state.settings.has_accounts;
  setMode(open && (firstAccount || mode === 'register') ? 'register' : 'login');
}

// An account made by an admin has no password until the person signs in for the first time, so
// the entry screen says which password becomes theirs. It only applies once accounts exist.
function showFirstPasswordNote() {
  $('first-password-note').hidden = !(state.settings.has_accounts && mode === 'login');
}

function showShell() {
  $('auth').hidden = true;
  $('shell').hidden = false;
  const role = state.me.role === 'admin' ? '<br>admin' : '';
  $('account').innerHTML = `<b>${esc(state.me.username)}</b>${role}`;
  renderMenu($('theme-menu'), 'theme', [
    { value: 'system', label: 'System' },
    { value: 'light', label: 'Light' },
    { value: 'dark', label: 'Dark' },
  ], state.theme, 'Theme', 'theme');
  labelRail();
}

function setMode(next) {
  mode = next;
  $('mode-register').setAttribute('aria-pressed', String(mode === 'register'));
  $('mode-login').setAttribute('aria-pressed', String(mode === 'login'));
  $('auth-submit').textContent = mode === 'register' ? 'Register' : 'Sign in';
  showFirstPasswordNote();
}

// Registering can end without a session: an account that has to be approved is not signed in,
// so the entry screen says what happens next instead of loading the app.
async function submitAuth(form) {
  const data = Object.fromEntries(new FormData(form));
  try {
    const account = await postJson(`/api/${mode}`, data);
    if (account.pending) {
      notice(`"${account.username}" is registered. An admin has to approve it before it can sign in.`);
      return;
    }
    state.me = account;
    await load();
  } catch (err) {
    report(err);
  }
}

// Confirm dialog for destructive actions

function confirmAction(title, text, action, run) {
  const dialog = $('confirm');
  $('confirm-title').textContent = title;
  $('confirm-text').textContent = text;
  $('confirm-ok').textContent = action;
  $('confirm-ok').onclick = async () => {
    dialog.close();
    try {
      await run();
    } catch (err) {
      report(err);
    }
  };
  dialog.showModal();
}

// Theme and sidebar

function applyTheme(value) {
  state.theme = value;
  if (value === 'system') document.documentElement.removeAttribute('data-theme');
  else document.documentElement.dataset.theme = value;
  localStorage.setItem('dreamgate-theme', value);
}

const narrow = window.matchMedia('(max-width: 768px)');

function toggleRail() {
  const shell = $('shell');
  if (narrow.matches) {
    if (shell.dataset.drawer === 'open') shell.removeAttribute('data-drawer');
    else shell.dataset.drawer = 'open';
  } else {
    shell.dataset.collapsed = String(shell.dataset.collapsed !== 'true');
  }
  labelRail();
}

// The toggle is a glyph alone, so its name says what a click does now and shows as a tooltip. The name
// carries the state, so the button does not state it a second time with aria-expanded; under 768px it
// opens a drawer that starts closed, so the state is read rather than assumed. A collapsed rail shows
// only glyphs for the links, Sign out and the theme menu, so each one takes its name as a tooltip there;
// for a link or Sign out that is also the name a screen reader reads while the text is hidden.
function labelRail() {
  const shell = $('shell');
  const toggle = $('toggle-rail');
  const open = narrow.matches ? shell.dataset.drawer === 'open' : shell.dataset.collapsed !== 'true';
  const action = narrow.matches ? (open ? 'Close sidebar' : 'Open sidebar') : open ? 'Collapse sidebar' : 'Expand sidebar';
  toggle.setAttribute('aria-label', action);
  toggle.title = action;
  const hidden = shell.dataset.collapsed === 'true';
  for (const el of document.querySelectorAll('.nav a, #sign-out, #theme-menu .menu-button')) {
    const name = el.getAttribute('aria-label') || el.querySelector('span:not(.icon)').textContent;
    if (hidden) el.title = name;
    else el.removeAttribute('title');
  }
}

// Events

$('auth-form').addEventListener('submit', (event) => {
  event.preventDefault();
  submitAuth(event.target);
});

// A message belongs to the mode that produced it, so switching modes clears it. This is done at the
// click rather than in setMode, because renderAuth calls setMode too and must not erase a message it
// has just written, such as a settings read that failed before the screen was drawn.
$('mode-register').addEventListener('click', () => {
  notice('');
  setMode('register');
});
$('mode-login').addEventListener('click', () => {
  notice('');
  setMode('login');
});
$('toggle-rail').addEventListener('click', toggleRail);
$('scrim').addEventListener('click', () => {
  $('shell').removeAttribute('data-drawer');
  labelRail();
});
narrow.addEventListener('change', labelRail);
$('confirm-cancel').addEventListener('click', () => $('confirm').close());

for (const [button, fields] of [['key-advanced', 'key-advanced-fields'], ['connection-advanced', 'connection-advanced-fields']]) {
  $(button).addEventListener('click', () => {
    const open = $(button).getAttribute('aria-expanded') === 'true';
    $(button).setAttribute('aria-expanded', String(!open));
    $(fields).hidden = open;
  });
}

document.addEventListener('click', (event) => {
  const button = event.target.closest('.menu-button');
  if (button) {
    const wrap = button.closest('.menu-wrap');
    if (button.getAttribute('aria-expanded') === 'true') closeMenus();
    else openMenu(wrap);
    return;
  }
  const option = event.target.closest('[role="option"]');
  if (option) {
    chooseOption(option);
    return;
  }
  if (!event.target.closest('.menu-wrap')) closeMenus();
});

document.addEventListener('keydown', (event) => {
  const option = event.target.closest('[role="option"]');
  if (!option) return;
  const items = [...option.closest('.menu').querySelectorAll('[role="option"]')];
  const index = items.indexOf(option);
  const next = event.key === 'ArrowDown' ? items[index + 1] : event.key === 'ArrowUp' ? items[index - 1] : null;
  if (next) {
    event.preventDefault();
    next.focus();
  } else if (event.key === 'Enter') {
    event.preventDefault();
    chooseOption(option);
  } else if (event.key === 'Escape') {
    closeMenus();
    option.closest('.menu-wrap').querySelector('.menu-button').focus();
  }
});

document.addEventListener('keydown', (event) => {
  const button = event.target.closest('.menu-button');
  if (button && (event.key === 'ArrowDown' || event.key === 'ArrowUp')) {
    event.preventDefault();
    openMenu(button.closest('.menu-wrap'));
  }
});

$('sign-out').addEventListener('click', async () => {
  try {
    await api('/api/logout', { method: 'POST' });
  } catch (err) {
    report(err);
    return;
  }
  state.me = null;
  if (state.live) state.live.close();
  state.live = null;
  // An admin's streams and names are other accounts' keys, models and usernames, so none of it is left
  // for whoever signs in next.
  state.progress = { shares: [], streams: [] };
  names.clear();
  showAuth();
});

window.addEventListener('hashchange', () => {
  if (state.me) route();
});

$('key-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const form = event.target;
  const data = Object.fromEntries(new FormData(form));
  const picked = $('connection-menu').dataset.value;
  const body = {
    name: data.name,
    connection_id: picked ? Number(picked) : null,
    model: data.model || null,
    rpm_limit: data.rpm_limit ? Number(data.rpm_limit) : null,
    token_cap: data.token_cap ? Number(data.token_cap) : null,
    expires_at: data.expires_at || null,
  };
  try {
    const created = await postJson('/api/keys', body);
    state.connectKey = created.key;
    form.reset();
    renderMenu($('connection-menu'), 'connection', connectionOptions(), '', 'Connection');
    await refresh('keys');
    notice(`Created key "${data.name}". Use it as the Bearer token.`);
  } catch (err) {
    report(err);
  }
});

$('connection-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const form = event.target;
  const data = Object.fromEntries(new FormData(form));
  const body = {
    name: data.name,
    base_url: data.base_url,
    api_key: data.api_key || null,
    default_model: data.default_model || null,
    priority: data.priority ? Number(data.priority) : null,
  };
  try {
    const created = await postJson('/api/connections', body);
    form.reset();
    await api(`/api/connections/${created.id}/check`, { method: 'POST' });
    await refresh('connections');
    notice(`Added connection "${data.name}" and checked it.`);
  } catch (err) {
    report(err);
  }
});

// An admin can make an account while self-registration is closed, so this form is the only way
// in for that person; the password is theirs when they sign in for the first time.
$('account-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const form = event.target;
  const data = Object.fromEntries(new FormData(form));
  const body = { username: data.username, daily_token_cap: data.daily_token_cap ? Number(data.daily_token_cap) : null };
  try {
    const created = await postJson('/api/users', body);
    form.reset();
    await refresh('stats');
    notice(`Made account "${created.username}". The password is theirs when they sign in for the first time.`);
  } catch (err) {
    report(err);
  }
});

$('chat-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const form = event.target;
  const text = (Object.fromEntries(new FormData(form)).message || '').trim();
  if (!text) return;
  form.reset();
  await sendChat(text);
});

// The conversation is only in this page, so clearing it needs no confirmation until there is
// something to lose.
$('chat-clear').addEventListener('click', () => {
  if (!state.chat.length) return;
  confirmAction('Clear chat', 'Clear this conversation? It is kept in this page only.', 'Clear', () => {
    state.chat = [];
    renderTranscript();
  });
});

// One field per request, so changing a password does not touch the limit. None of these are
// destructive except deleting the account, so they run straight away.
async function changeAccount(user, body, message) {
  try {
    await api(`/api/users/${user.id}`, {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
    await refresh('stats');
    notice(message);
  } catch (err) {
    report(err);
  }
}

function account(id) {
  return state.summary.users.find((user) => user.id === Number(id));
}

function fieldValue(user, kind) {
  return document.querySelector(`input.${kind}[data-for="${user.id}"]`).value.trim();
}

async function setLimit(user, raw) {
  const value = raw === '' ? null : Number(raw);
  if (value !== null && (!Number.isInteger(value) || value < 1)) {
    notice('Daily limit must be a whole number of at least 1, or empty to clear it.', 'error');
    return;
  }
  await changeAccount(
    user,
    { daily_token_cap: value },
    `Set the daily limit for ${user.username} to ${value ? `${nf.format(value)} input and output tokens` : 'none'}.`,
  );
}

document.addEventListener('click', async (event) => {
  const button = event.target.closest('button.link, button.secondary');
  if (!button) return;
  if (button.dataset.copy || button.dataset.copyFrom) {
    const text = button.dataset.copy || $(button.dataset.copyFrom).textContent.trim();
    if (!(await copy(text))) {
      notice('Nothing was copied. Select the value and copy it yourself.', 'error');
      return;
    }
    // Copied is only true for a moment after the click, so the label goes back on its own; a second click
    // starts the moment again rather than ending it early.
    clearTimeout(button.copiedTimer);
    button.dataset.label ??= button.textContent;
    button.textContent = 'Copied';
    button.copiedTimer = setTimeout(() => restoreLabel(button), 1500);
    return;
  }
  if (button.dataset.limit) {
    const user = account(button.dataset.limit);
    await setLimit(user, fieldValue(user, 'cap'));
    return;
  }
  if (button.dataset.rename) {
    const user = account(button.dataset.rename);
    const value = fieldValue(user, 'username');
    if (!value) {
      notice('Type a username first.', 'error');
      return;
    }
    await changeAccount(user, { username: value }, `Renamed "${user.username}" to "${value}".`);
    return;
  }
  if (button.dataset.password) {
    const user = account(button.dataset.password);
    const value = fieldValue(user, 'password');
    if (value.length < 8) {
      notice('A new password must be at least 8 characters.', 'error');
      return;
    }
    await changeAccount(user, { password: value }, `Set a new password for "${user.username}".`);
    return;
  }
  if (button.dataset.status) {
    const user = account(button.dataset.status);
    const enabled = button.dataset.enabled !== 'true';
    await changeAccount(
      user,
      { enabled },
      enabled ? `Enabled "${user.username}".` : `Disabled "${user.username}". They are signed out and their keys do not work.`,
    );
    return;
  }
  if (button.dataset.approve) {
    const user = account(button.dataset.approve);
    await changeAccount(user, { pending: false }, `Approved "${user.username}". They can sign in now.`);
    return;
  }
  if (button.dataset.account) {
    const user = account(button.dataset.account);
    confirmAction('Delete account', `Delete "${user.username}"? Their keys and everything recorded for this account go with it.`, 'Delete', async () => {
      await api(`/api/users/${user.id}`, { method: 'DELETE' });
      await refresh('stats');
      notice(`Deleted "${user.username}".`);
    });
    return;
  }
  if (button.dataset.toggle) {
    try {
      await api(`/api/keys/${button.dataset.toggle}`, {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ enabled: button.dataset.enabled !== 'true' }),
      });
      await refresh('keys');
    } catch (err) {
      report(err);
    }
    return;
  }
  if (button.dataset.rotate) {
    const id = Number(button.dataset.rotate);
    const own = state.keys.find((k) => k.id === id);
    const key = own || state.otherKeys.find((k) => k.id === id);
    const who = own ? '' : ` (${key.owner})`;
    // Another account's new key is not shown to an admin, so the confirm says where it can be read.
    const after = own ? 'The new key is in this list.' : `${key.owner} reads the new key from their own list.`;
    confirmAction(
      'Rotate key',
      `Give "${key.name}"${who} a new key? The old key stops working at once, so every client that uses it needs the new one. The key keeps its limits, its binding and its usage. ${after}`,
      'Rotate',
      async () => {
        const rotated = await postJson(`/api/keys/${key.id}/rotate`, {});
        // The Connect view and the Sandbox go on with the key they had picked, under its new secret.
        if (own && key.key === state.connectKey) state.connectKey = rotated.key;
        if (own && key.key === state.sandboxKey) state.sandboxKey = rotated.key;
        await refresh('keys');
        notice(`Rotated "${key.name}". The old key no longer works.`);
      },
    );
    return;
  }
  if (button.dataset.delete) {
    const id = Number(button.dataset.delete);
    const own = state.keys.find((k) => k.id === id);
    const key = own || state.otherKeys.find((k) => k.id === id);
    // An admin can delete another account's key, so the confirm names whose key it is.
    const who = own ? '' : ` (${key.owner})`;
    confirmAction('Delete key', `Delete "${key.name}"${who}? Requests already recorded for it stay.`, 'Delete', async () => {
      await api(`/api/keys/${key.id}`, { method: 'DELETE' });
      if (key.key === state.connectKey) state.connectKey = null;
      await refresh('keys');
    });
    return;
  }
  if (button.dataset.check) {
    try {
      const result = await api(`/api/connections/${button.dataset.check}/check`, { method: 'POST' });
      await refresh('connections');
      if (result.health !== 'ok') notice('Health check reported down. Check the base URL and that the endpoint answers GET /v1/models.', 'error');
    } catch (err) {
      report(err);
    }
    return;
  }
  if (button.dataset.drop) {
    const connection = state.connections.find((c) => c.id === Number(button.dataset.drop));
    confirmAction('Remove connection', `Remove "${connection.name}"? Keys pointing at it will use the next healthy connection.`, 'Remove', async () => {
      await api(`/api/connections/${connection.id}`, { method: 'DELETE' });
      await refresh('connections');
    });
  }
});

// Start

paintIcons(document);
applyTheme(localStorage.getItem('dreamgate-theme') || 'system');

// Settings come first: the entry screen decides whether Register is offered before anyone is
// signed in.
api('/api/settings')
  .then((settings) => {
    state.settings = settings;
    renderAuth();
  })
  .catch((err) => notice(String(err?.message || 'Could not read whether registration is open.'), 'error'))
  .then(() => api('/api/me'))
  .then((account) => {
    state.me = account;
    return load();
  })
  .catch(() => showAuth());

// The online window passes with time as well as with an event, so the panel is refreshed on a
// timer in addition to the live stream.
setInterval(() => {
  if (!state.me) return;
  refreshPresence().then(() => render(false)).catch(() => {});
}, 30000);
