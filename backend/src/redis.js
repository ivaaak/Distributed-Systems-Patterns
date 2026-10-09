import Redis from 'ioredis';
import { REDIS_URL } from './config.js';

export function createConnection(name) {
  const conn = new Redis(REDIS_URL, { connectionName: name, maxRetriesPerRequest: null });
  conn.on('error', (err) => console.error(`[redis:${name}]`, err.message));
  return conn;
}

// Regular commands.
export const redis = createConnection('cmd');
// A connection in SUBSCRIBE mode can't run other commands, so pub/sub gets its own.
const sub = createConnection('sub');

const handlers = new Map(); // channel -> Set<fn>

sub.on('message', (channel, raw) => {
  const set = handlers.get(channel);
  if (!set) return;
  let data;
  try {
    data = JSON.parse(raw);
  } catch {
    return;
  }
  for (const fn of set) fn(data);
});

export async function subscribe(channel, fn) {
  if (!handlers.has(channel)) {
    handlers.set(channel, new Set());
    await sub.subscribe(channel);
  }
  handlers.get(channel).add(fn);
}

export function publish(channel, data) {
  return redis.publish(channel, JSON.stringify(data));
}

// Compare-and-delete: only remove a key if we still own it. Used by locks, presence and leadership.
export const DEL_IF_OWNER = `
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0`;
