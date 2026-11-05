#!/usr/bin/env node
// Nimbus Local — a zero-dependency AWS emulator: S3, SQS, DynamoDB, SNS, Secrets Manager, STS.
//   node nimbus.mjs                    → http://localhost:4566  (console at /_nimbus)
//   node nimbus.mjs --persist ./.nimbus --port 4566 --host 0.0.0.0
import { createServer } from 'node:http';
import { readFile, writeFile, mkdir, rename } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { AwsError, detectService, decodeAwsChunked, regionOf, uuid } from './lib/common.mjs';
import { createS3 } from './services/s3.mjs';
import { createSqs } from './services/sqs.mjs';
import { createDynamo } from './services/dynamodb.mjs';
import { createSecrets, createSns, createSts } from './services/misc.mjs';

const ROOT = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const flag = (n, d) => {
  const i = args.indexOf('--' + n);
  return i < 0 ? d : args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : true;
};
const PORT = Number(flag('port', process.env.NIMBUS_PORT || 4566));
const HOST = flag('host', process.env.NIMBUS_HOST || '127.0.0.1');
const PERSIST = flag('persist', process.env.NIMBUS_PERSIST || '');
const QUIET = !!flag('quiet', false);
const t0 = performance.now();

export async function startNimbus({ port = PORT, host = HOST, persist = PERSIST, quiet = QUIET } = {}) {
  let state = {};
  const file = persist ? join(persist === true ? '.nimbus' : persist, 'state.json') : null;
  if (file && existsSync(file)) state = JSON.parse(await readFile(file, 'utf8'));
  let baseUrl = `http://localhost:${port}`;
  const s3 = createS3(state);
  const sqs = createSqs(state, { baseUrl: () => baseUrl });
  const services = { s3, sqs, dynamodb: createDynamo(state), secretsmanager: createSecrets(state), sns: createSns(state, { sqs }), sts: createSts() };

  let saveT;
  const save = () => {
    if (!file) return;
    clearTimeout(saveT);
    saveT = setTimeout(async () => {
      await mkdir(dirname(file), { recursive: true });
      await writeFile(file + '.tmp', JSON.stringify(state));
      await rename(file + '.tmp', file);
    }, 500);
  };

  // request log for the console
  const log = [];
  const listeners = new Set();
  const record = (e) => {
    log.unshift(e);
    if (log.length > 300) log.length = 300;
    for (const res of listeners) res.write(`data: ${JSON.stringify(e)}\n\n`);
    if (!quiet) console.log(`${e.status >= 400 ? '✗' : '·'} ${e.service.padEnd(14)} ${e.action.padEnd(28)} ${e.status} ${e.ms.toFixed(1)}ms`);
  };

  const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,PUT,POST,DELETE,HEAD,OPTIONS', 'Access-Control-Allow-Headers': '*', 'Access-Control-Expose-Headers': 'ETag,x-amz-request-id,x-amz-version-id,Content-Range,Content-Length' };

  const server = createServer(async (req, res) => {
    const started = performance.now();
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    baseUrl = `http://${req.headers.host || `localhost:${port}`}`;
    const reqId = uuid();
    const send = (status, headers, body) => {
      res.writeHead(status, { ...CORS, 'x-amz-request-id': reqId, 'x-amzn-RequestId': reqId, ...headers });
      res.end(body);
    };
    if (req.method === 'OPTIONS') return send(204, {}, '');

    // ---- Nimbus console & health
    if (url.pathname === '/_nimbus/health' || url.pathname === '/_localstack/health') {
      return send(200, { 'Content-Type': 'application/json' }, JSON.stringify({ services: Object.fromEntries(Object.keys(services).map((s) => [s === 'secretsmanager' ? 'secretsmanager' : s, 'running'])), edition: 'nimbus', version: '1.0.0', uptimeSec: Math.round(process.uptime()) }));
    }
    if (url.pathname === '/_nimbus' || url.pathname === '/_nimbus/') return send(200, { 'Content-Type': 'text/html; charset=utf-8' }, await readFile(join(ROOT, 'console.html')));
    if (url.pathname === '/_nimbus/api/state') return send(200, { 'Content-Type': 'application/json' }, JSON.stringify(summary(state)));
    if (url.pathname === '/_nimbus/api/log') return send(200, { 'Content-Type': 'application/json' }, JSON.stringify(log));
    if (url.pathname === '/_nimbus/api/events') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', Connection: 'keep-alive' });
      res.write('retry: 2000\n\n');
      listeners.add(res);
      req.on('close', () => listeners.delete(res));
      return;
    }
    if (url.pathname === '/_nimbus/api/reset' && req.method === 'POST') {
      for (const k of Object.keys(state)) for (const kk of Object.keys(state[k])) state[k][kk] = {};
      save();
      return send(200, { 'Content-Type': 'application/json' }, '{"ok":true}');
    }
    const item = url.pathname.match(/^\/_nimbus\/api\/(table|queue|object|secret)$/);
    if (item) return send(200, { 'Content-Type': 'application/json' }, JSON.stringify(peek(state, item[1], url.searchParams)));

    // ---- AWS APIs
    const chunks = [];
    for await (const c of req) chunks.push(c);
    let body = Buffer.concat(chunks);
    const sha = req.headers['x-amz-content-sha256'] || '';
    if (sha.startsWith('STREAMING') || /aws-chunked/.test(req.headers['content-encoding'] || '')) body = decodeAwsChunked(body);
    let form = null;
    if ((req.headers['content-type'] || '').startsWith('application/x-www-form-urlencoded')) form = new URLSearchParams(body.toString());
    else if (url.searchParams.has('Action')) form = url.searchParams;

    // virtual-hosted-style bucket: <bucket>.localhost[:port] or <bucket>.s3.<anything>
    const hostName = (req.headers.host || '').split(':')[0];
    const vh = hostName.match(/^(.+?)\.(?:s3[.-][^.]*\.)?(?:localhost|s3\.localhost\.localstack\.cloud|localhost\.localstack\.cloud)$/) || hostName.match(/^(.+?)\.s3\.[\w.-]+$/);
    const svcName = detectService(req, form);
    const svc = services[svcName] || services.s3;
    const action =
      (req.headers['x-amz-target'] || '').split('.')[1] ||
      form?.get('Action') ||
      `${req.method} ${url.pathname.split('/').filter(Boolean).length > 1 ? 'object' : url.pathname === '/' ? 'service' : 'bucket'}${[...url.searchParams.keys()].filter((k) => !k.startsWith('X-Amz')).map((k) => '?' + k).join('')}`;
    let status = 200;
    try {
      const out = await svc.handle({ req, url, body, form, region: regionOf(req), bucketFromHost: svc === services.s3 && vh ? vh[1] : null });
      if (out.json !== undefined) send(200, { 'Content-Type': out.contentType || 'application/json' }, JSON.stringify(out.json));
      else if (out.xml !== undefined) send(200, { 'Content-Type': 'text/xml' }, out.xml);
      else {
        status = out.status;
        send(out.status, out.headers || {}, out.body ?? '');
      }
      if (req.method !== 'GET' && req.method !== 'HEAD' && !/^(Get|List|Describe|Receive|Query|Scan|BatchGet|TransactGet)/.test(action)) save();
      if (/^ReceiveMessage|DeleteMessage/.test(action)) save();
    } catch (e) {
      const err = e instanceof AwsError ? e : new AwsError('InternalFailure', e.message, 500);
      if (!(e instanceof AwsError)) console.error(e);
      status = err.status;
      if (svc.jsonError || req.headers['x-amz-target']) {
        const type = (svc.errorPrefix || '') + err.code;
        send(err.status, { 'Content-Type': 'application/x-amz-json-1.0', 'x-amzn-query-error': `${err.code};Sender` }, JSON.stringify({ __type: type, message: err.message, Message: err.message, ...(err.extra || {}) }));
      } else if (svc === services.s3) {
        send(err.status, { 'Content-Type': 'application/xml' }, req.method === 'HEAD' ? '' : svc.errorBody(err));
      } else {
        send(err.status, { 'Content-Type': 'text/xml' }, `<?xml version="1.0"?><ErrorResponse><Error><Type>Sender</Type><Code>${err.code}</Code><Message>${err.message}</Message></Error><RequestId>${reqId}</RequestId></ErrorResponse>`);
      }
    } finally {
      record({ t: Date.now(), service: svc.name, action, method: req.method, path: url.pathname, status, ms: performance.now() - started, bytes: body.length });
    }
  });
  await new Promise((r) => server.listen(port, host, r));
  const actual = server.address().port;
  baseUrl = `http://localhost:${actual}`;
  if (!quiet) {
    console.log(`\n  ☁  Nimbus Local ready in ${(performance.now() - t0).toFixed(0)} ms  →  http://${host === '0.0.0.0' ? 'localhost' : host}:${actual}`);
    console.log(`     services: s3 · sqs · dynamodb · sns · secretsmanager · sts${file ? `   persist: ${file}` : '   (in-memory)'}`);
    console.log(`     console:  http://localhost:${actual}/_nimbus\n`);
  }
  return { server, port: actual, state, close: () => new Promise((r) => server.close(r)) };
}

function summary(state) {
  const now = Date.now();
  return {
    s3: Object.values(state.s3?.buckets || {}).map((b) => ({ name: b.name, objects: Object.keys(b.objects).length, bytes: Object.values(b.objects).reduce((s, o) => s + o.size, 0), keys: Object.keys(b.objects).sort().slice(0, 200).map((k) => ({ key: k, size: b.objects[k].size, type: b.objects[k].contentType, modified: b.objects[k].lastModified })) })),
    sqs: Object.values(state.sqs?.queues || {}).map((q) => ({ name: q.name, visible: q.messages.filter((m) => m.visibleAt <= now).length, inflight: q.messages.filter((m) => m.visibleAt > now && m.receiveCount).length, delayed: q.messages.filter((m) => m.visibleAt > now && !m.receiveCount).length, dlq: q.attributes.RedrivePolicy ? JSON.parse(q.attributes.RedrivePolicy).deadLetterTargetArn.split(':').pop() : null })),
    dynamodb: Object.values(state.dynamodb?.tables || {}).map((t) => ({ name: t.TableName, items: Object.keys(t.items).length, keys: t.KeySchema.map((k) => `${k.AttributeName} (${k.KeyType})`).join(', '), indexes: [...(t.GlobalSecondaryIndexes || []), ...(t.LocalSecondaryIndexes || [])].map((i) => i.IndexName) })),
    sns: Object.values(state.sns?.topics || {}).map((t) => ({ name: t.name, subscriptions: t.subs.map((s) => `${s.protocol}: ${s.endpoint.split(':').pop()}`) })),
    secrets: Object.values(state.secrets?.secrets || {}).filter((s) => !s.DeletedDate).map((s) => ({ name: s.Name, versions: Object.keys(s.versions).length, changed: s.LastChangedDate })),
  };
}

function peek(state, kind, q) {
  if (kind === 'table') return Object.values(state.dynamodb?.tables?.[q.get('name')]?.items || {}).slice(0, 200);
  if (kind === 'queue') return (state.sqs?.queues?.[q.get('name')]?.messages || []).slice(0, 100).map((m) => ({ id: m.id, body: m.body.slice(0, 2000), receiveCount: m.receiveCount, visible: m.visibleAt <= Date.now(), sentAt: m.sentAt }));
  if (kind === 'object') {
    const o = state.s3?.buckets?.[q.get('bucket')]?.objects?.[q.get('key')];
    return o ? { ...o, body: undefined, preview: /^(text|application\/(json|xml|javascript))/.test(o.contentType || '') || o.size < 4096 ? Buffer.from(o.body, 'base64').toString('utf8').slice(0, 8000) : null } : null;
  }
  if (kind === 'secret') {
    const s = state.secrets?.secrets?.[q.get('name')];
    return s ? { name: s.Name, arn: s.ARN, versions: Object.entries(s.versions).map(([id, v]) => ({ id, stages: v.stages, created: v.CreatedDate, length: (v.SecretString || '').length })) } : null; // values never leave via the console
  }
  return null;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const n = await startNimbus();
  const stop = () => n.close().then(() => process.exit(0));
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
}
