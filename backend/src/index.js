import http from 'node:http';
import express from 'express';
import cors from 'cors';
import { WebSocketServer } from 'ws';
import { PORT, INSTANCE_ID } from './config.js';
import { redis } from './redis.js';
import { lifecycle } from './lifecycle.js';
import { emit, handleMonitorSocket, startEventForwarding } from './events.js';
import {
  startRegistry,
  setStatsProvider,
  getClusterSnapshot,
  forgetDeadInstances,
  deregister,
} from './registry.js';
import {
  notificationsRouter,
  handleClientSocket,
  startNotifications,
  localClientCount,
} from './patterns/notifications.js';
import { leaderRouter, startLeaderElection, isLeader, resignLeadership } from './patterns/leaderElection.js';
import { lockRouter } from './patterns/distributedLock.js';
import { rateLimitRouter, startRateLimiter } from './patterns/rateLimiter.js';
import { jobsRouter, startWorkQueue, currentJob } from './patterns/workQueue.js';
import { shardRouter } from './patterns/sharding.js';
import { cacheRouter, startCaching } from './patterns/caching.js';
import { paymentsRouter } from './patterns/idempotency.js';
import { cbRouter, startCircuitBreaker } from './patterns/circuitBreaker.js';
import { sagaRouter, startSagas } from './patterns/saga.js';
import { outboxRouter, startOutbox } from './patterns/outbox.js';
import { quorumRouter } from './patterns/quorum.js';
import { bulkheadRouter, startBulkhead } from './patterns/bulkhead.js';

const app = express();
app.use(cors());
app.use(express.json());
app.use((req, res, next) => {
  res.setHeader('X-Instance-Id', INSTANCE_ID);
  next();
});

app.get('/api/health', (req, res) => res.json({ instance: INSTANCE_ID, crashed: lifecycle.crashed }));
app.get('/api/cluster', async (req, res) => res.json(await getClusterSnapshot()));
app.post('/api/cluster/forget-dead', async (req, res) => res.json({ removed: await forgetDeadInstances() }));

app.post('/api/chaos/crash', (req, res) => {
  lifecycle.crash();
  emit('instance.crashed');
  res.json({ instance: INSTANCE_ID, crashed: true });
});
app.post('/api/chaos/recover', (req, res) => {
  lifecycle.recover();
  emit('instance.recovered');
  res.json({ instance: INSTANCE_ID, crashed: false });
});

app.use('/api/notify', notificationsRouter);
app.use('/api/leader', leaderRouter);
app.use('/api/lock', lockRouter);
app.use('/api/ratelimit', rateLimitRouter);
app.use('/api/jobs', jobsRouter);
app.use('/api/shard', shardRouter);
app.use('/api/cache', cacheRouter);
app.use('/api/payments', paymentsRouter);
app.use('/api/cb', cbRouter);
app.use('/api/saga', sagaRouter);
app.use('/api/outbox', outboxRouter);
app.use('/api/quorum', quorumRouter);
app.use('/api/bulkhead', bulkheadRouter);

app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: err.message });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname !== '/ws/client' && url.pathname !== '/ws/monitor') {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    if (url.pathname === '/ws/client') handleClientSocket(ws, url);
    else handleMonitorSocket(ws);
  });
});

setStatsProvider(() => ({
  clients: localClientCount(),
  isLeader: isLeader(),
  job: currentJob()?.jobId ?? null,
}));

async function main() {
  await redis.ping();
  await startEventForwarding();
  await startNotifications();
  await startRateLimiter();
  await startWorkQueue();
  await startCaching();
  await startCircuitBreaker();
  await startSagas();
  await startBulkhead();
  startOutbox();
  startLeaderElection();
  await startRegistry();
  server.listen(PORT, () => console.log(`[${INSTANCE_ID}] listening on :${PORT}`));
}

async function shutdown() {
  console.log(`[${INSTANCE_ID}] shutting down`);
  try {
    emit('instance.stopped');
    await resignLeadership();
    await deregister();
  } finally {
    process.exit(0);
  }
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().catch((err) => {
  console.error(`[${INSTANCE_ID}] failed to start:`, err.message);
  process.exit(1);
});
