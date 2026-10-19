import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis, publish, subscribe } from '../redis.js';
import { rejectIfCrashed, sleep } from '../lifecycle.js';
import { emit } from '../events.js';

/**
 * Circuit breaker in front of a flaky downstream dependency.
 *
 *   CLOSED    - calls pass through; consecutive failures are counted
 *   OPEN      - after FAILURE_THRESHOLD failures, calls fail fast without touching the
 *               dependency, giving it room to recover
 *   HALF_OPEN - after OPEN_MS, one trial call is let through; success closes the breaker,
 *               failure re-opens it
 *
 * Modes:
 *   none   - every request waits for the dependency's timeout
 *   local  - each instance has its own breaker; each must discover the outage separately
 *   shared - breaker state lives in Redis (updated by Lua scripts), so the first instances
 *            to see failures trip it for the whole cluster
 */
const FAILURE_THRESHOLD = 5;
const OPEN_MS = 5000;
const TIMEOUT_MS = 700;
const CONFIG_KEY = 'cb:downstream';
const CALLS_KEY = 'cb:downstream:calls';
const SHARED_KEY = 'cb:shared';
const RESET_CHANNEL = 'cb:reset';

// ---- simulated downstream -------------------------------------------------------------
let downstream = { failRate: 0, latencyMs: 60 };
async function refreshConfig() {
  const cfg = await redis.hgetall(CONFIG_KEY);
  if (cfg.failRate !== undefined) downstream = { failRate: Number(cfg.failRate), latencyMs: Number(cfg.latencyMs) };
}

async function callDownstream() {
  await redis.incr(CALLS_KEY);
  if (Math.random() < downstream.failRate) {
    await sleep(TIMEOUT_MS); // a struggling service usually times out rather than failing fast
    throw new Error('timeout');
  }
  await sleep(downstream.latencyMs);
  return 'ok';
}

// ---- local breaker ----------------------------------------------------------------------
const local = { state: 'closed', failures: 0, openedAt: 0, trialInFlight: false };

function localAcquire() {
  if (local.state === 'open' && Date.now() - local.openedAt >= OPEN_MS) transitionLocal('half_open');
  if (local.state === 'closed') return 'allow';
  if (local.state === 'half_open' && !local.trialInFlight) {
    local.trialInFlight = true;
    return 'trial';
  }
  return 'reject';
}

function localRecord(ok, wasTrial) {
  if (wasTrial) local.trialInFlight = false;
  // Late results from calls that started before the breaker opened don't change an open breaker.
  if (local.state === 'open') return;
  if (ok) {
    local.failures = 0;
    if (local.state === 'half_open') transitionLocal('closed');
    return;
  }
  local.failures++;
  if (local.state === 'half_open' || local.failures >= FAILURE_THRESHOLD) {
    local.openedAt = Date.now();
    transitionLocal('open');
  }
}

function transitionLocal(to) {
  const from = local.state;
  if (from === to) return;
  local.state = to;
  emit('cb.transition', { mode: 'local', from, to });
}

// ---- shared breaker (Redis + Lua, using Redis' clock) --------------------------------------
const NOW = `local t = redis.call('TIME') local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)`;

const SHARED_ACQUIRE = `${NOW}
local state = redis.call('hget', KEYS[1], 'state') or 'closed'
if state == 'open' and now - tonumber(redis.call('hget', KEYS[1], 'openedAt') or '0') >= tonumber(ARGV[1]) then
  redis.call('hset', KEYS[1], 'state', 'half_open', 'trial', '0')
  state = 'half_open'
end
if state == 'closed' then return {'allow', state} end
if state == 'half_open' and redis.call('hget', KEYS[1], 'trial') ~= '1' then
  redis.call('hset', KEYS[1], 'trial', '1')
  return {'trial', state}
end
return {'reject', state}`;

const SHARED_RECORD = `${NOW}
local state = redis.call('hget', KEYS[1], 'state') or 'closed'
if ARGV[3] == '1' then redis.call('hset', KEYS[1], 'trial', '0') end
if state == 'open' then return {state, state} end
if ARGV[1] == '1' then
  redis.call('hset', KEYS[1], 'failures', 0, 'state', 'closed')
  return {state, 'closed'}
end
local failures = redis.call('hincrby', KEYS[1], 'failures', 1)
if state == 'half_open' or failures >= tonumber(ARGV[2]) then
  redis.call('hset', KEYS[1], 'state', 'open', 'openedAt', now)
  return {state, 'open'}
end
return {state, state}`;

async function sharedAcquire() {
  const [decision, state] = await redis.eval(SHARED_ACQUIRE, 1, SHARED_KEY, OPEN_MS);
  return { decision, state };
}

async function sharedRecord(ok, wasTrial) {
  const [from, to] = await redis.eval(SHARED_RECORD, 1, SHARED_KEY, ok ? '1' : '0', FAILURE_THRESHOLD, wasTrial ? '1' : '0');
  if (from !== to) emit('cb.transition', { mode: 'shared', from, to });
}

export async function startCircuitBreaker() {
  await refreshConfig();
  setInterval(() => refreshConfig().catch(() => {}), 500);
  await subscribe(RESET_CHANNEL, () => Object.assign(local, { state: 'closed', failures: 0, openedAt: 0, trialInFlight: false }));
}

export const cbRouter = Router();

cbRouter.post('/call', rejectIfCrashed, async (req, res) => {
  const mode = req.body?.mode ?? 'none';
  const started = Date.now();
  let decision = 'allow';
  if (mode === 'local') decision = localAcquire();
  if (mode === 'shared') decision = (await sharedAcquire()).decision;

  let result;
  if (decision === 'reject') {
    result = 'short_circuit';
  } else {
    try {
      await callDownstream();
      result = 'ok';
    } catch {
      result = 'error';
    }
    if (mode === 'local') localRecord(result === 'ok', decision === 'trial');
    if (mode === 'shared') await sharedRecord(result === 'ok', decision === 'trial');
  }
  res.json({ instance: INSTANCE_ID, mode, result, trial: decision === 'trial', ms: Date.now() - started });
});

cbRouter.get('/state', async (req, res) => {
  const [shared, calls] = await Promise.all([redis.hgetall(SHARED_KEY), redis.get(CALLS_KEY)]);
  res.json({
    instance: INSTANCE_ID,
    local: { state: local.state, failures: local.failures, openedAt: local.openedAt },
    shared: { state: shared.state ?? 'closed', failures: Number(shared.failures ?? 0), openedAt: Number(shared.openedAt ?? 0) },
    downstream,
    downstreamCalls: Number(calls ?? 0),
    config: { threshold: FAILURE_THRESHOLD, openMs: OPEN_MS, timeoutMs: TIMEOUT_MS },
    now: Date.now(),
  });
});

cbRouter.post('/downstream', async (req, res) => {
  const failRate = Math.min(Math.max(Number(req.body?.failRate ?? 0), 0), 1);
  const latencyMs = Math.min(Math.max(Number(req.body?.latencyMs ?? 60), 0), 2000);
  await redis.hset(CONFIG_KEY, 'failRate', failRate, 'latencyMs', latencyMs);
  await refreshConfig();
  emit('cb.downstream', { failRate, latencyMs });
  res.json({ failRate, latencyMs });
});

cbRouter.post('/reset', async (req, res) => {
  await redis.del(SHARED_KEY, CALLS_KEY);
  await publish(RESET_CHANNEL, { by: INSTANCE_ID });
  emit('cb.reset');
  res.json({ ok: true });
});
