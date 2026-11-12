const nf = new Intl.NumberFormat();
const $ = (id) => document.getElementById(id);
const esc = (value) =>
  String(value).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

// A snapshot can arrive with empty totals, so live counting starts from 0 rather than undefined.
const num = (value) => (Number.isFinite(value) ? value : 0);

const ICONS = {
  overview: '<path d="M4 17a8 8 0 0 1 16 0"/><path d="M12 14l4-3"/>',
  activity: '<path d="M4 6h16M4 12h12M4 18h8"/>',
  keys: '<circle cx="8.5" cy="12" r="3.5"/><path d="M12 12h9M18 12v4M21 12v-3"/>',
  connections: '<path d="M9 3v5M15 3v5M8 8h8v6a4 4 0 0 1-8 0z"/>',
  client: '<path d="M9 7l-5 5 5 5M15 7l5 5-5 5"/>',
  accounts: '<circle cx="12" cy="9" r="4"/><path d="M4 20a8 8 0 0 1 16 0"/>',
  check: '<circle cx="12" cy="12" r="9"/><path d="M8.5 12.5l2.5 2.5 5-5"/>',
  alert: '<path d="M12 4l8 14H4z"/><path d="M12 9v4"/>',
  chevron: '<path d="M9 6l6 6-6 6"/>',
  panel: '<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M9 4v16"/>',
  exit: '<path d="M15 4H6a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h9"/><path d="M16 12h5M19 9l3 3-3 3"/>',
};

function icon(name) {
  return `<svg viewBox="0 0 24 24" aria-hidden="true">${ICONS[name]}</svg>`;
}

function paintIcons(root) {
  for (const el of root.querySelectorAll('[data-icon]')) el.innerHTML = icon(el.dataset.icon);
}

const state = {
  me: null,
  summary: { totals: {}, hourly: [], daily: [], models: [], users: [] },
  keys: [],
  connections: [],
  events: [],
  hours: [],
  days: [],
  live: null,
  view: 'overview',
  connectKey: null,
  theme: 'system',
};

const VIEWS = ['overview', 'activity', 'keys', 'connections', 'connect', 'accounts'];

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

// Menus replace the native select: a button plus a list, closed on outside click and
// Escape, navigable with the arrow keys.
function renderMenu(container, name, options, current, label) {
  const picked = options.find((o) => o.value === current) || options[0];
  container.dataset.name = name;
  container.dataset.value = picked ? picked.value : '';
  container.innerHTML = `
    <button type="button" class="menu-button" aria-haspopup="listbox" aria-expanded="false" aria-label="${esc(label)}">
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
  const first = menu.querySelector('[aria-selected="true"]') || menu.querySelector('[role="option"]');
  if (first) first.focus();
}

function chooseOption(option) {
  const wrap = option.closest('.menu-wrap');
  wrap.dataset.value = option.dataset.value;
  wrap.querySelector('.menu-button span:first-child').textContent = option.textContent;
  for (const item of wrap.querySelectorAll('[role="option"]')) item.setAttribute('aria-selected', String(item === option));
  closeMenus();
  if (wrap.dataset.name === 'theme') applyTheme(option.dataset.value);
  if (wrap.dataset.name === 'connect-key') {
    state.connectKey = option.dataset.value;
    renderConnect();
  }
}

// Views

const RENDER = {
  overview: renderOverview,
  activity: renderActivity,
  keys: renderKeys,
  connections: renderConnections,
  connect: renderConnect,
  accounts: renderAccounts,
};

function currentView() {
  const name = (location.hash || '').replace(/^#\/?/, '');
  if (!VIEWS.includes(name)) return 'overview';
  if (name === 'accounts' && state.me.role !== 'admin') return 'overview';
  return name;
}

function route() {
  state.view = currentView();
  for (const link of document.querySelectorAll('.nav a')) {
    link.hidden = link.dataset.view === 'accounts' && state.me.role !== 'admin';
    if (link.hidden) continue;
    if (link.dataset.view === state.view) link.setAttribute('aria-current', 'page');
    else link.removeAttribute('aria-current');
  }
  for (const view of document.querySelectorAll('.view')) view.hidden = view.dataset.view !== state.view;
  render(false);
}

function render(arrived) {
  renderNav();
  (RENDER[state.view] || renderOverview)(arrived);
}

function renderNav() {
  $('nav-accounts').hidden = state.me.role !== 'admin';
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
  state.summary = await api('/api/stats/summary');
  state.keys = (await api('/api/keys')).keys;
  state.connections = (await api('/api/connections')).connections;
  state.events = (await api('/api/stats/recent')).events;
  buildAxes();
  showShell();
  route();
  subscribe();
}

async function refresh(path) {
  if (path === 'keys') state.keys = (await api('/api/keys')).keys;
  if (path === 'connections') state.connections = (await api('/api/connections')).connections;
  if (path === 'stats') state.summary = await api('/api/stats/summary');
  render(false);
}

function subscribe() {
  if (state.live) state.live.close();
  state.live = new EventSource('/api/live');
  state.live.onmessage = (message) => {
    const payload = JSON.parse(message.data);
    if (payload.type === 'snapshot') {
      state.summary.totals = payload.totals;
      render(false);
      return;
    }
    state.events.unshift(payload.event);
    state.events = state.events.slice(0, 50);
    const event = payload.event;
    const totals = state.summary.totals;
    totals.total_tokens = num(totals.total_tokens) + event.total_tokens;
    totals.requests = num(totals.requests) + 1;
    // The day figures are what a daily account limit is measured against, so they have to
    // move with the same event the feed does.
    if (String(event.created_at).slice(0, 10) === new Date().toISOString().slice(0, 10)) {
      totals.today_tokens = num(totals.today_tokens) + event.total_tokens;
      totals.today_requests = num(totals.today_requests) + 1;
      const account = state.summary.users.find((user) => user.id === event.user_id);
      if (account) account.used_today = num(account.used_today) + event.total_tokens;
    }
    addEvent(event);
    const wire = $('gate-wire');
    wire.classList.add('arrive');
    setTimeout(() => wire.classList.remove('arrive'), 180);
    setSignal('ok', 'listening');
    render(true);
  };
  state.live.onerror = () => setSignal('down', 'connection lost, retrying');
}

function setSignal(kind, text) {
  const chip = $('signal');
  chip.className = `chip ${kind}`;
  chip.querySelector('[data-icon]').innerHTML = icon(kind === 'ok' ? 'check' : 'alert');
  $('signal-text').textContent = text;
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
  return svg(`${grid}<path class="area" d="${area}"/><path class="line" d="${line}"/>${labels}`, height, `Token rate, last 24 hours, peak ${nf.format(max)} tokens in an hour`);
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
  return svg(`<line class="grid" x1="10" x2="894" y1="${height - 24}" y2="${height - 24}"/>${bars}${labels}`, height, `Requests per day, last 14 days, peak ${nf.format(max)} requests in a day`);
}

function empty(message) {
  return `<p class="note">${esc(message)}</p>`;
}

// Overview

function renderOverview(arrived) {
  renderGate();
  renderReadout();
  renderCharts();
  renderFeed(arrived);
}

function topConnection() {
  const enabled = state.connections.filter((c) => c.enabled).sort((a, b) => a.priority - b.priority);
  // Same order the gateway uses: highest priority, skipping a connection that is down
  // unless it is the only one left.
  return enabled.find((c) => c.health !== 'down') || enabled[0] || null;
}

function renderGate() {
  // The path shown has to be the path a request actually took, so the key comes from the
  // last event and the connection from that key's binding.
  const last = state.events[0];
  const key = last ? state.keys.find((k) => k.name === last.key) : null;
  const bound = key && key.connection_id ? state.connections.find((c) => c.id === key.connection_id && c.enabled) : null;
  const conn = bound || topConnection();
  $('gate-key').textContent = last ? last.key : (state.keys.length ? state.keys[0].name : 'no key yet');
  $('gate-upstream').innerHTML = conn
    ? `${esc(conn.name)} <span class="chip ${conn.health === 'ok' ? 'ok' : 'down'}">${icon(conn.health === 'ok' ? 'check' : 'alert')}<span>${esc(conn.health)}</span></span>`
    : 'no connection yet';

  if (!last) {
    $('gate-read').textContent = 'Nothing has crossed the gate yet.';
    return;
  }
  $('gate-read').innerHTML = [
    'last request',
    `<b>${esc(last.created_at.slice(11, 19))}</b>`,
    esc(last.model || 'no model'),
    `${nf.format(last.total_tokens)} tokens`,
    `<span class="${last.status >= 400 ? 'danger' : ''}">${esc(last.status)}</span>`,
  ].join(' ');
}

function renderReadout() {
  const t = state.summary.totals;
  $('total-tokens').textContent = nf.format(t.total_tokens || 0);
  $('readout-note').textContent =
    state.me.role === 'admin' ? 'tokens counted so far, every account' : 'tokens counted so far, this account only';

  const rows = [
    ['Requests', nf.format(t.requests || 0)],
    ['Requests today', nf.format(t.today_requests || 0)],
    ['Tokens today', nf.format(t.today_tokens || 0)],
    ['Prompt tokens', nf.format(t.prompt_tokens || 0)],
    ['Completion tokens', nf.format(t.completion_tokens || 0)],
    ['Average latency', `${nf.format(t.avg_latency_ms || 0)} ms`],
    ['Errors (status 400 or worse)', nf.format(t.errors || 0)],
  ];
  $('support').innerHTML = rows
    .map(([label, value]) => `<div class="row"><dt>${esc(label)}</dt><dd>${esc(value)}</dd></div>`)
    .join('');
}

function renderCharts() {
  const anyTraffic = state.hours.some((b) => b.requests > 0);
  $('chart-hourly').innerHTML = anyTraffic ? areaChart(state.hours) : empty('Nothing measured yet. Send a request through the gateway and it appears here.');
  $('chart-daily').innerHTML = state.days.some((b) => b.requests > 0) ? barChart(state.days) : empty('No requests in the last 14 days.');
}

function renderFeed(arrived) {
  $('feed').innerHTML = state.events.length
    ? state.events
        .slice(0, 12)
        .map(
          (event, i) =>
            `<li class="${arrived && i === 0 ? ' arrived' : ''}">
              <span>${esc(event.created_at.slice(11, 19))}</span>
              <span>${esc(event.key)}</span>
              <span>${esc(event.model || 'no model')}</span>
              <span class="${event.status >= 400 ? 'danger' : ''}">${esc(event.status)}</span>
              <span class="tokens">${nf.format(event.total_tokens)}</span>
            </li>`,
        )
        .join('')
    : empty('No requests yet. Create a key and send one, and the row lands here as it happens.');
}

// Activity

function renderActivity() {
  const admin = state.me.role === 'admin';
  const columns = ['time', 'key', admin ? 'account' : null, 'model', 'tokens', 'latency', 'status'].filter(Boolean);
  $('activity-head').innerHTML = `<tr>${columns.map((c) => `<th>${esc(c)}</th>`).join('')}</tr>`;

  $('activity-body').innerHTML = state.events.length
    ? state.events
        .map(
          (event) =>
            `<tr>
              <td>${esc(event.created_at.slice(11, 19))}</td>
              <td><span class="clamp">${esc(event.key)}</span></td>
              ${admin ? `<td><span class="clamp">${esc(event.user)}</span></td>` : ''}
              <td><span class="clamp">${esc(event.model || 'no model')}</span></td>
              <td class="num">${nf.format(event.total_tokens)}</td>
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
            `<tr><td><span class="clamp">${esc(model.model)}</span></td><td class="num">${nf.format(model.requests)}</td><td class="num">${nf.format(model.tokens)}</td></tr>`,
        )
        .join('')
    : `<tr><td colspan="3">No model has been used yet. Add a connection, then send a request with a key.</td></tr>`;
}

// Keys

function connectionOptions() {
  return [{ value: '', label: 'Any enabled connection' }, ...state.connections.map((c) => ({ value: String(c.id), label: c.name }))];
}

function renderKeys() {
  $('keys-body').innerHTML = state.keys.length
    ? state.keys
        .map(
          (key) => {
            // A binding to a disabled connection does not hold: the key uses another one.
            const bound = key.connection_id ? state.connections.find((c) => c.id === key.connection_id) : null;
            const binding = bound && !bound.enabled ? `${esc(bound.name)} <span class="muted">binding inactive</span>` : esc(key.connection || 'any');
            return `<tr>
            <td><span class="clamp" title="${esc(key.key)}">${esc(key.name)}</span></td>
            <td>${binding}</td>
            <td>${esc(key.model || 'the client model')}</td>
            <td>${key.rpm_limit ? `${key.rpm_limit}/min` : 'no limit'}<br>${key.token_cap ? nf.format(key.token_cap) : 'no cap'}</td>
            <td class="num">${nf.format(key.requests)}</td>
            <td class="num">${nf.format(key.tokens)}</td>
            <td class="num">
              <button class="link" data-copy="${esc(key.key)}">Copy key</button>
              <button class="link" data-toggle="${key.id}" data-enabled="${key.enabled}">${key.enabled ? 'Disable' : 'Enable'}</button>
              <button class="link danger" data-delete="${key.id}">Delete</button>
            </td>
          </tr>`;
          },
        )
        .join('')
    : `<tr><td colspan="7">No key yet. Create one above, then use it as the Bearer token, or as x-api-key, against /v1/chat/completions.</td></tr>`;

  renderMenu($('connection-menu'), 'connection', connectionOptions(), $('connection-menu').dataset.value || '', 'Connection');
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
            <td><span class="chip ${connection.health === 'ok' ? 'ok' : 'down'}">${icon(connection.health === 'ok' ? 'check' : 'alert')}<span>${esc(connection.health)}</span></span></td>
            <td class="num">
              <button class="link" data-check="${connection.id}">Check health</button>
              <button class="link danger" data-drop="${connection.id}">Remove</button>
            </td>
          </tr>`,
        )
        .join('')
    : `<tr><td colspan="6">No connection yet. Add one above. Any endpoint that speaks the OpenAI API works, including llama.cpp, Ollama and vLLM.</td></tr>`;
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
      ? `This key always resolves to ${key.model}. Use GET ${base}/models to see what the connection serves.`
      : `Use whatever the key resolves to, or list it with GET ${base}/models.`
    : 'Create a key first. The key is what the client sends.';

  $('connect-curl').textContent = key
    ? `curl ${base}/chat/completions \\\n  -H "authorization: Bearer ${key.key}" \\\n  -H 'content-type: application/json' \\\n  -d '{"model":"<model>","messages":[{"role":"user","content":"hi"}]}'`
    : 'No key yet. Create one on the Keys page.';

  $('connect-copy').dataset.copy = key ? key.key : '';
  $('connect-copy').disabled = !key;
}

// Accounts. The view is admin-only, so the limit control is always editable here.
function renderAccounts() {
  $('accounts-body').innerHTML = state.summary.users.length
    ? state.summary.users
        .map((user) => {
          const cap = user.daily_token_cap;
          const over = cap !== null && user.used_today >= cap;
          return `<tr>
            <td><span class="clamp">${esc(user.name)}</span><br><span class="clamp">${esc(user.email)}</span></td>
            <td>
              <span class="inline-field">
                <input class="cap" type="number" min="1" value="${cap ?? ''}" placeholder="no limit" data-for="${user.id}" aria-label="Daily limit for ${esc(user.name)}">
                <button class="secondary" data-limit="${user.id}">Set limit</button>
              </span>
            </td>
            <td class="num">${nf.format(user.used_today)}${over ? ' <span class="danger">over</span>' : ''}</td>
            <td class="num">${nf.format(user.requests)}</td>
            <td class="num">${nf.format(user.tokens)}</td>
          </tr>`;
        })
        .join('')
    : '<tr><td colspan="5">No account yet. The first account to register becomes the admin.</td>';
}

// Shell and auth

function showAuth() {
  $('auth').hidden = false;
  $('shell').hidden = true;
  $('account').textContent = '';
}

function showShell() {
  $('auth').hidden = true;
  $('shell').hidden = false;
  const role = state.me.role === 'admin' ? '<br>admin' : '';
  $('account').innerHTML = `<b>${esc(state.me.name)}</b><br>${esc(state.me.email)}${role}`;
  renderMenu($('theme-menu'), 'theme', [
    { value: 'system', label: 'System' },
    { value: 'light', label: 'Light' },
    { value: 'dark', label: 'Dark' },
  ], state.theme, 'Theme');
}

let mode = 'register';

function setMode(next) {
  mode = next;
  $('mode-register').setAttribute('aria-pressed', String(mode === 'register'));
  $('mode-login').setAttribute('aria-pressed', String(mode === 'login'));
  $('auth-submit').textContent = mode === 'register' ? 'Register' : 'Sign in';
  $('name-field').hidden = mode === 'login';
  notice('');
}

async function submitAuth(form) {
  const data = Object.fromEntries(new FormData(form));
  try {
    state.me = await postJson(`/api/${mode}`, data);
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

function toggleRail() {
  const shell = $('shell');
  if (window.matchMedia('(max-width: 768px)').matches) {
    const open = shell.dataset.drawer === 'open';
    if (open) shell.removeAttribute('data-drawer');
    else shell.dataset.drawer = 'open';
    $('toggle-rail').setAttribute('aria-expanded', String(!open));
    return;
  }
  const collapsed = shell.dataset.collapsed === 'true';
  shell.dataset.collapsed = String(!collapsed);
  $('toggle-rail').setAttribute('aria-expanded', String(collapsed));
}

// Events

$('auth-form').addEventListener('submit', (event) => {
  event.preventDefault();
  submitAuth(event.target);
});

$('mode-register').addEventListener('click', () => setMode('register'));
$('mode-login').addEventListener('click', () => setMode('login'));
$('toggle-rail').addEventListener('click', toggleRail);
$('scrim').addEventListener('click', () => $('shell').removeAttribute('data-drawer'));
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

// Setting a limit is not destructive, so it runs straight away.
async function setLimit(user, raw) {
  const value = raw === '' ? null : Number(raw);
  if (value !== null && (!Number.isInteger(value) || value < 1)) {
    notice('Daily limit must be a whole number of at least 1, or empty to clear it.', 'error');
    return;
  }
  try {
    await api(`/api/users/${user.id}`, {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ daily_token_cap: value }),
    });
    await refresh('stats');
    notice(`Set limit for ${user.name} to ${value ? nf.format(value) : 'none'}.`);
  } catch (err) {
    report(err);
  }
}

document.addEventListener('click', async (event) => {
  const button = event.target.closest('button.link, button.secondary');
  if (!button) return;
  if (button.dataset.copy) {
    await navigator.clipboard.writeText(button.dataset.copy);
    button.textContent = 'Copied';
    return;
  }
  if (button.dataset.copyFrom) {
    await navigator.clipboard.writeText($(button.dataset.copyFrom).textContent.trim());
    button.textContent = 'Copied';
    return;
  }
  if (button.dataset.limit) {
    const user = state.summary.users.find((u) => u.id === Number(button.dataset.limit));
    await setLimit(user, document.querySelector(`input.cap[data-for="${user.id}"]`).value.trim());
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
  if (button.dataset.delete) {
    const key = state.keys.find((k) => k.id === Number(button.dataset.delete));
    confirmAction('Delete key', `Delete "${key.name}"? Requests already recorded for it stay.`, 'Delete', async () => {
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
setSignal('down', 'not connected');

api('/api/me')
  .then((account) => {
    state.me = account;
    return load();
  })
  .catch(() => showAuth());
