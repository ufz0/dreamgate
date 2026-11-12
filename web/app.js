const nf = new Intl.NumberFormat();
const $ = (id) => document.getElementById(id);
const esc = (value) =>
  String(value).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

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

function notice(message) {
  const target = $('shell').hidden ? $('auth-error') : $('notice');
  if (target) target.textContent = message || '';
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
  const keys = $('count-keys');
  keys.textContent = state.keys.length ? String(state.keys.length) : '';
  const badge = $('count-connections');
  const down = state.connections.filter((c) => c.health === 'down').length;
  badge.textContent = state.connections.length ? String(state.connections.length) : '';
  badge.classList.toggle('bad', down > 0);
  if (down) badge.title = `${down} of ${state.connections.length} report down`;
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
    state.summary.totals.total_tokens += payload.event.total_tokens;
    state.summary.totals.requests += 1;
    addEvent(payload.event);
    flash($('signal'), 'flash');
    flash($('gate-wire'), 'arrive');
    render(true);
    $('signal').classList.remove('stale');
    $('signal-text').textContent = 'listening';
  };
  state.live.onerror = () => {
    $('signal').classList.add('stale');
    $('signal-text').textContent = 'connection lost, retrying';
  };
}

function flash(el, className) {
  if (!el) return;
  el.classList.remove(className);
  void el.offsetWidth;
  el.classList.add(className);
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
    state.days.push({ key: at.toISOString().slice(0, 10), label: String(at.getUTCDay()), tokens: 0, requests: 0, errors: 0 });
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
      return `<rect class="bar" x="${(10 + i * slot + 2).toFixed(1)}" y="${(height - 24 - h).toFixed(1)}" width="${(slot - 4).toFixed(1)}" height="${h.toFixed(1)}"/>`;
    })
    .join('');
  const labels = buckets.map((b, i) => `<text class="axis" x="${(10 + i * slot + slot / 2 - 6).toFixed(1)}" y="${height - 6}">${b.label}</text>`).join('');
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
  return state.connections.filter((c) => c.enabled).sort((a, b) => a.priority - b.priority)[0] || null;
}

function renderGate() {
  const key = state.keys[0];
  const conn = topConnection();
  $('gate-key').textContent = key ? key.name : 'no key yet';
  $('gate-upstream').innerHTML = conn
    ? `${esc(conn.name)} <span class="state ${esc(conn.health)}">${esc(conn.health)}</span>`
    : 'no connection yet';

  const last = state.events[0];
  if (!last) {
    $('gate-read').textContent = 'Nothing has crossed the gate yet.';
    return;
  }
  $('gate-read').innerHTML = [
    'last request',
    `<b>${esc(last.created_at.slice(11, 19))}</b>`,
    esc(last.model || 'no model'),
    `${nf.format(last.total_tokens)} tokens`,
    `<span class="${last.status >= 400 ? 'bad' : ''}">${esc(last.status)}</span>`,
  ].map((part) => `<span>${part}</span>`).join('');
}

function renderReadout() {
  const t = state.summary.totals;
  $('total-tokens').textContent = nf.format(t.total_tokens || 0);
  $('readout-note').textContent =
    state.me.role === 'admin' ? 'tokens counted so far, every account' : 'tokens counted so far, this account';

  const rows = [
    ['Requests', nf.format(t.requests || 0)],
    ['Requests today', nf.format(t.today_requests || 0)],
    ['Tokens today', nf.format(t.today_tokens || 0)],
    ['Prompt tokens', nf.format(t.prompt_tokens || 0)],
    ['Completion tokens', nf.format(t.completion_tokens || 0)],
    ['Average latency', `${nf.format(t.avg_latency_ms || 0)} ms`],
    ['Refused by limits', nf.format(t.errors || 0)],
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
              <span class="${event.status >= 400 ? 'bad' : ''}">${esc(event.status)}</span>
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
              <td>${esc(event.key)}</td>
              ${admin ? `<td>${esc(event.user)}</td>` : ''}
              <td>${esc(event.model || 'no model')}</td>
              <td class="num">${nf.format(event.total_tokens)}</td>
              <td class="num">${nf.format(event.latency_ms)} ms</td>
              <td class="num${event.status >= 400 ? ' bad' : ''}">${esc(event.status)}</td>
            </tr>`,
        )
        .join('')
    : `<tr><td colspan="${columns.length}">No requests recorded yet. Send one through the gateway and it appears here.</td></tr>`;

  $('models-body').innerHTML = state.summary.models.length
    ? state.summary.models
        .map(
          (model) =>
            `<tr><td>${esc(model.model)}</td><td class="num">${nf.format(model.requests)}</td><td class="num">${nf.format(model.tokens)}</td></tr>`,
        )
        .join('')
    : `<tr><td colspan="3">No model has been used yet. Add a connection, then send a request with a key.</td></tr>`;
}

// Keys

function renderKeys() {
  $('keys-body').innerHTML = state.keys.length
    ? state.keys
        .map(
          (key) => `<tr>
            <td><span title="${esc(key.key)}">${esc(key.name)}</span></td>
            <td>${esc(key.connection || 'any')}</td>
            <td>${esc(key.model || 'default')}</td>
            <td>${key.rpm_limit ? `${key.rpm_limit}/min` : 'no limit'}<br>${key.token_cap ? nf.format(key.token_cap) : 'no cap'}</td>
            <td class="num">${nf.format(key.requests)}</td>
            <td class="num">${nf.format(key.tokens)}</td>
            <td class="num">
              <button class="link" data-copy="${esc(key.key)}">Copy key</button>
              <button class="link" data-toggle="${key.id}" data-enabled="${key.enabled}">${key.enabled ? 'Disable' : 'Enable'}</button>
              <button class="link" data-delete="${key.id}">Delete</button>
            </td>
          </tr>`,
        )
        .join('')
    : `<tr><td colspan="7">No key yet. Create one below, then use it as the Bearer token, or as x-api-key, against /v1/chat/completions.</td></tr>`;

  const options = state.connections.map((c) => `<option value="${c.id}">${esc(c.name)}</option>`).join('');
  $('key-form').connection.innerHTML = `<option value="">Any enabled connection</option>${options}`;
}

// Connections

function renderConnections() {
  $('connections-body').innerHTML = state.connections.length
    ? state.connections
        .map(
          (connection) => `<tr>
            <td>${esc(connection.name)}</td>
            <td>${esc(connection.base_url)}</td>
            <td>${esc(connection.default_model || 'the model the client asks for')}</td>
            <td class="num">${esc(connection.priority)}</td>
            <td><span class="state ${esc(connection.health)}">${esc(connection.health)}</span></td>
            <td class="num">
              <button class="link" data-check="${connection.id}">Check health</button>
              <button class="link" data-drop="${connection.id}">Remove</button>
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
  $('connect-health').textContent = `${location.origin}/health`;

  const select = $('connect-key');
  if (!state.connectKey && state.keys.length) state.connectKey = state.keys[0].key;
  const key = state.keys.find((k) => k.key === state.connectKey) || state.keys[0];

  select.innerHTML = state.keys.length
    ? state.keys.map((k) => `<option value="${esc(k.key)}"${k.key === key?.key ? ' selected' : ''}>${esc(k.name)}</option>`).join('')
    : '<option value="">no key yet</option>';

  $('connect-model').textContent = key
    ? key.model
      ? `This key always resolves to ${key.model}. Use GET ${base}/models to see what the connection serves.`
      : 'Use whatever the key resolves to, or list it with GET ' + base + '/models.'
    : 'Create a key first. The key is what the client sends.';

  $('connect-curl').textContent = key
    ? `curl ${base}/chat/completions \\\n  -H "authorization: Bearer ${key.key}" \\\n  -H 'content-type: application/json' \\\n  -d '{"model":"<model>","messages":[{"role":"user","content":"hi"}]}'`
    : 'No key yet. Create one on the Keys page.';

  $('connect-copy').dataset.copy = key ? key.key : '';
}

// Accounts

function renderAccounts() {
  $('accounts-body').innerHTML = state.summary.users.length
    ? state.summary.users
        .map(
          (user) =>
            `<tr><td>${esc(user.name)}<br>${esc(user.email)}</td><td class="num">${nf.format(user.requests)}</td><td class="num">${nf.format(user.tokens)}</td></tr>`,
        )
        .join('')
    : `<tr><td colspan="3">No usage from any account yet. The first account to register becomes the admin.</td></tr>`;
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
  $('account').innerHTML = `${esc(state.me.name)}<br>${esc(state.me.email)}${state.me.role === 'admin' ? '<br>admin' : ''}`;
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
    notice(err.message);
  }
}

// Events

$('auth-form').addEventListener('submit', (event) => {
  event.preventDefault();
  submitAuth(event.target);
});

$('mode-register').addEventListener('click', () => setMode('register'));
$('mode-login').addEventListener('click', () => setMode('login'));

$('sign-out').addEventListener('click', async () => {
  try {
    await api('/api/logout', { method: 'POST' });
  } catch (err) {
    notice(err.message);
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
  const body = {
    name: data.name,
    connection_id: data.connection ? Number(data.connection) : null,
    model: data.model || null,
    rpm_limit: data.rpm_limit ? Number(data.rpm_limit) : null,
    token_cap: data.token_cap ? Number(data.token_cap) : null,
  };
  try {
    const created = await postJson('/api/keys', body);
    state.connectKey = created.key;
    form.reset();
    await refresh('keys');
  } catch (err) {
    notice(err.message);
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
  } catch (err) {
    notice(err.message);
  }
});

$('connect-key').addEventListener('change', (event) => {
  state.connectKey = event.target.value;
  renderConnect();
});

document.addEventListener('click', async (event) => {
  const button = event.target.closest('button.link');
  if (!button) return;
  try {
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
    if (button.dataset.toggle) {
      await api(`/api/keys/${button.dataset.toggle}`, {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ enabled: button.dataset.enabled !== 'true' }),
      });
      await refresh('keys');
      return;
    }
    if (button.dataset.delete) {
      const gone = state.keys.find((k) => k.id === Number(button.dataset.delete));
      await api(`/api/keys/${button.dataset.delete}`, { method: 'DELETE' });
      if (gone && gone.key === state.connectKey) state.connectKey = null;
      await refresh('keys');
      return;
    }
    if (button.dataset.check) {
      const result = await api(`/api/connections/${button.dataset.check}/check`, { method: 'POST' });
      await refresh('connections');
      if (result.health !== 'ok') notice('Health check reported down. Check the base URL and that the endpoint answers GET /v1/models.');
      return;
    }
    if (button.dataset.drop) {
      await api(`/api/connections/${button.dataset.drop}`, { method: 'DELETE' });
      await refresh('connections');
    }
  } catch (err) {
    notice(err.message);
  }
});

// Start: an existing session goes straight to the dashboard.
api('/api/me')
  .then((account) => {
    state.me = account;
    return load();
  })
  .catch(() => showAuth());
