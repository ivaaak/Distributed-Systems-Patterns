import { EventEmitter } from 'node:events';

/**
 * Simulated failure state. A "crashed" instance keeps its HTTP port open (so the UI can
 * recover it) but otherwise behaves as if the process died: it stops heartbeating, drops
 * its WebSocket clients, stops renewing leadership and stops consuming jobs.
 */
class Lifecycle extends EventEmitter {
  crashed = false;

  crash() {
    if (this.crashed) return;
    this.crashed = true;
    this.emit('crash');
  }

  recover() {
    if (!this.crashed) return;
    this.crashed = false;
    this.emit('recover');
  }
}

export const lifecycle = new Lifecycle();

export function rejectIfCrashed(req, res, next) {
  if (lifecycle.crashed) {
    return res.status(503).json({ error: 'instance is crashed (simulated)' });
  }
  next();
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
