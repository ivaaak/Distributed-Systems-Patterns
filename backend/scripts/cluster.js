// Spawns N backend instances locally (default 3) on consecutive ports, like a mini cluster.
// Usage: node scripts/cluster.js [count] [basePort]
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const count = Number(process.argv[2] || process.env.INSTANCES || 3);
const basePort = Number(process.argv[3] || 4001);
const entry = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const colors = [36, 35, 33, 32, 34, 31];

const children = [];
for (let i = 0; i < count; i++) {
  const port = basePort + i;
  const id = `api-${i + 1}`;
  const tag = `\x1b[${colors[i % colors.length]}m[${id}]\x1b[0m`;
  const child = spawn(process.execPath, ['--watch', entry], {
    env: { ...process.env, PORT: String(port), INSTANCE_ID: id },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const pipe = (stream, out) =>
    stream.on('data', (buf) =>
      buf.toString().split(/\r?\n/).filter(Boolean).forEach((line) => out.write(`${tag} ${line}\n`))
    );
  pipe(child.stdout, process.stdout);
  pipe(child.stderr, process.stderr);
  children.push(child);
}

const stop = () => {
  children.forEach((c) => c.kill('SIGTERM'));
  setTimeout(() => process.exit(0), 500);
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
