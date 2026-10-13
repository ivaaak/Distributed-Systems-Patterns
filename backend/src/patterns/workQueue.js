import { randomUUID } from 'node:crypto';
import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis, createConnection } from '../redis.js';
import { lifecycle, rejectIfCrashed, sleep } from '../lifecycle.js';
import { emit } from '../events.js';

/**
 * Competing consumers with Redis Streams + a consumer group.
 *
 * Every instance is a consumer in group `workers`. XREADGROUP hands each job to exactly
 * one consumer and records it in the Pending Entries List (PEL) until XACK. If a consumer
 * crashes mid-job the entry stays pending; a reclaimer on the surviving instances notices
 * it has been idle too long and XCLAIMs it -> at-least-once delivery.
 */
const STREAM = 'jobs:stream';
const GROUP = 'workers';
const RECLAIM_IDLE_MS = 5000;
const RECLAIM_EVERY_MS = 1500;

const blocking = createConnection('stream-reader'); // XREADGROUP BLOCK ties up its connection
const inbox = []; // reclaimed jobs waiting for this worker
let current = null;

const toObject = (fields) => {
  const obj = {};
  for (let i = 0; i < fields.length; i += 2) obj[fields[i]] = fields[i + 1];
  return obj;
};

async function ensureGroup() {
  try {
    await redis.xgroup('CREATE', STREAM, GROUP, '$', 'MKSTREAM');
  } catch (err) {
    if (!String(err.message).includes('BUSYGROUP')) throw err;
  }
}

async function runJob({ streamId, fields, reclaimedFrom, deliveries }) {
  const job = toObject(fields);
  const durationMs = Number(job.durationMs);
  current = { jobId: job.jobId, startedAt: Date.now(), durationMs };
  emit('job.started', { jobId: job.jobId, streamId, durationMs, reclaimedFrom, deliveries });

  const deadline = Date.now() + durationMs;
  while (Date.now() < deadline) {
    if (lifecycle.crashed) {
      // Process "died" mid-job: no ACK, the entry stays in the PEL.
      current = null;
      return;
    }
    await sleep(100);
  }

  await redis.xack(STREAM, GROUP, streamId);
  current = null;
  emit('job.completed', { jobId: job.jobId, streamId, tookMs: durationMs });
}

async function workerLoop() {
  while (true) {
    try {
      if (lifecycle.crashed) {
        await sleep(300);
        continue;
      }
      if (inbox.length) {
        await runJob(inbox.shift());
        continue;
      }
      const res = await blocking.xreadgroup(
        'GROUP', GROUP, INSTANCE_ID, 'COUNT', 1, 'BLOCK', 1000, 'STREAMS', STREAM, '>'
      );
      if (!res) continue;
      for (const [, entries] of res) {
        for (const [streamId, fields] of entries) await runJob({ streamId, fields });
      }
    } catch (err) {
      if (String(err.message).includes('NOGROUP')) await ensureGroup();
      else console.error('[worker]', err.message);
      await sleep(500);
    }
  }
}

async function reclaimTick() {
  if (lifecycle.crashed || inbox.length || current) return;
  // Pending entries idle for longer than the threshold belong to a consumer that's probably dead.
  const pending = await redis.xpending(STREAM, GROUP, 'IDLE', RECLAIM_IDLE_MS, '-', '+', 5);
  const stale = pending.filter(([, consumer]) => consumer !== INSTANCE_ID);
  if (!stale.length) return;
  const [id, from, , deliveries] = stale[0];
  const claimed = await redis.xclaim(STREAM, GROUP, INSTANCE_ID, RECLAIM_IDLE_MS, id);
  for (const [streamId, fields] of claimed) {
    if (!fields) continue; // entry was trimmed from the stream
    const job = toObject(fields);
    emit('job.reclaimed', { jobId: job.jobId, streamId, from, deliveries: deliveries + 1 });
    inbox.push({ streamId, fields, reclaimedFrom: from, deliveries: deliveries + 1 });
  }
}

export async function startWorkQueue() {
  await ensureGroup();
  workerLoop();
  setInterval(() => reclaimTick().catch((err) => console.error('[reclaim]', err.message)), RECLAIM_EVERY_MS);
}

export const currentJob = () => current;

export const jobsRouter = Router();

jobsRouter.post('/', rejectIfCrashed, async (req, res) => {
  const count = Math.min(Math.max(Number(req.body?.count) || 1, 1), 50);
  const jobs = [];
  for (let i = 0; i < count; i++) {
    const jobId = `job-${randomUUID().slice(0, 5)}`;
    const durationMs = 1500 + Math.floor(Math.random() * 2500);
    const streamId = await redis.xadd(
      STREAM, 'MAXLEN', '~', 1000, '*', 'jobId', jobId, 'durationMs', durationMs
    );
    emit('job.enqueued', { jobId, streamId, durationMs });
    jobs.push({ jobId, streamId, durationMs });
  }
  res.json({ jobs });
});

jobsRouter.get('/stats', async (req, res) => {
  const [length, summary] = await Promise.all([redis.xlen(STREAM), redis.xpending(STREAM, GROUP)]);
  const [pending, , , consumers] = summary;
  res.json({ length, pending, consumers: Object.fromEntries(consumers ?? []) });
});
