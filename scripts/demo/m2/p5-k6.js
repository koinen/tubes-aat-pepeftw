// M2 P5 (C12) load for K3 (other-source latency) and K5 (priority shedding). Run via scripts/demo/m2/p5.sh k6 <k3|k5>.
//
// MODE=k3: READ_VUS bnpb-ops readers, alternating GET /hazards?type=SEISMIC (BMKG) and type=VOLCANIC (PVMBG).
//          Thresholds: p95 < 300 ms per source (the M1 criterion stated in the README).
// MODE=k5: MEDIA_VUS media readers without think time + FIELD_VUS field-team readers (1 req/s each).
//          Every response is classed per role: ok, shed_503 (503 "overloaded" + Retry-After), rate_429,
//          upstream_503 (any other 503), other. Latency is reported per role.
//
// Access tokens live 60 s, so each VU refreshes its own token after 45 s.
// The per-subject rate limit (RATE_LIMIT_MAX) must be raised for both modes; see the runbook.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter } from 'k6/metrics';

const MODE = __ENV.MODE || 'k5';
const API = __ENV.API;
const AUTH = __ENV.AUTH;
const DURATION = __ENV.DURATION || '5m';

const responses = new Counter('responses');

const CLIENTS = {
  media: { id: 'media', secret: __ENV.MEDIA_SECRET },
  field: { id: 'field-team', secret: __ENV.LAPANGAN_SECRET },
  ops: { id: 'bnpb-ops', secret: __ENV.BNPB_SECRET },
};

let cached = {};

function token(kind) {
  const c = cached[kind];
  if (c && Date.now() - c.at < 45000) return c.token;
  const res = http.post(`${AUTH}/token`, JSON.stringify({
    grant_type: 'client_credentials', client_id: CLIENTS[kind].id, client_secret: CLIENTS[kind].secret,
  }), { headers: { 'Content-Type': 'application/json' }, tags: { name: 'token' } });
  if (res.status !== 200) {
    responses.add(1, { role: kind, class: `token_${res.status}` });
    return c ? c.token : '';
  }
  cached[kind] = { token: res.json('access_token'), at: Date.now() };
  return cached[kind].token;
}

function classify(res) {
  if (res.status >= 200 && res.status < 300) return 'ok';
  if (res.status === 429) return 'rate_429';
  if (res.status === 503) {
    let code = '';
    try { code = res.json('error.code') || ''; } catch (e) { code = ''; }
    if (String(code).toLowerCase() === 'overloaded' && res.headers['Retry-After']) return 'shed_503';
    return 'upstream_503';
  }
  if (res.status === 0) return 'transport_error';
  return `other_${res.status}`;
}

function get(kind, role, path, tags) {
  const res = http.get(`${API}${path}`, {
    headers: { Authorization: `Bearer ${token(kind)}` },
    tags: Object.assign({ role, name: path.split('?')[0] }, tags || {}),
    timeout: '30s',
  });
  responses.add(1, { role, class: classify(res) });
  return res;
}

const classes = ['ok', 'shed_503', 'rate_429', 'upstream_503', 'transport_error'];

function displayThresholds(roles) {
  // k6 prints tagged sub-metrics only when a threshold names them; these always pass.
  const t = {};
  for (const r of roles) {
    t[`http_req_duration{role:${r}}`] = ['p(95)>=0'];
    for (const c of classes) t[`responses{role:${r},class:${c}}`] = ['count>=0'];
  }
  return t;
}

export const options = MODE === 'k3' ? {
  scenarios: {
    reads: { executor: 'constant-vus', vus: Number(__ENV.READ_VUS || 20), duration: DURATION, exec: 'k3' },
  },
  thresholds: Object.assign(displayThresholds(['ops']), {
    'http_req_duration{source:BMKG}': ['p(95)<300'],
    'http_req_duration{source:PVMBG}': ['p(95)<300'],
  }),
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
} : {
  scenarios: {
    media: { executor: 'constant-vus', vus: Number(__ENV.MEDIA_VUS || 400), duration: DURATION, exec: 'media' },
    field: { executor: 'constant-vus', vus: Number(__ENV.FIELD_VUS || 50), duration: DURATION, exec: 'field' },
  },
  thresholds: displayThresholds(['media', 'field']),
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function k3() {
  get('ops', 'ops', '/hazards?type=SEISMIC&limit=20', { source: 'BMKG' });
  sleep(0.25);
  get('ops', 'ops', '/hazards?type=VOLCANIC&limit=20', { source: 'PVMBG' });
  sleep(0.25);
}

export function media() {
  get('media', 'media', '/hazards?limit=20');
}

export function field() {
  get('field', 'field', '/hazards?limit=20');
  sleep(1);
}
