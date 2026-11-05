// Shared helpers: XML building, errors, ids, aws-chunked decoding.
import { createHash, randomUUID } from 'node:crypto';

export const ACCOUNT = '000000000000';
export const REGION_DEFAULT = 'us-east-1';

export class AwsError extends Error {
  constructor(code, message, status = 400) {
    super(message);
    this.code = code;
    this.status = status;
  }
}

export const md5 = (b) => createHash('md5').update(b).digest('hex');
export const uuid = () => randomUUID();
export const nowIso = () => new Date().toISOString();

export function xmlEscape(s) {
  return String(s ?? '').replace(/[<>&'"]/g, (c) => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', "'": '&apos;', '"': '&quot;' })[c]);
}

/** Tiny object → XML serializer: arrays repeat the tag, objects nest, null/undefined are skipped. */
export function toXml(name, value, ns) {
  const attrs = ns ? ` xmlns="${ns}"` : '';
  if (value == null) return '';
  if (Array.isArray(value)) return value.map((v) => toXml(name, v)).join('');
  if (typeof value === 'object') return `<${name}${attrs}>${Object.entries(value).map(([k, v]) => toXml(k, v)).join('')}</${name}>`;
  return `<${name}${attrs}>${xmlEscape(value)}</${name}>`;
}

export const xmlDoc = (name, value, ns) => `<?xml version="1.0" encoding="UTF-8"?>\n${toXml(name, value, ns)}`;

/** Minimal XML reader for the small request bodies S3 sends (CompleteMultipartUpload, Delete). */
export function xmlValues(xml, tag) {
  const out = [];
  const re = new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, 'g');
  let m;
  while ((m = re.exec(xml))) out.push(m[1]);
  return out;
}
export const xmlUnescape = (s) => String(s).replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&amp;/g, '&');

/** Decode an `aws-chunked` body (SigV4 streaming / trailing-checksum uploads used by newer SDKs). */
export function decodeAwsChunked(buf) {
  const parts = [];
  let i = 0;
  while (i < buf.length) {
    const eol = buf.indexOf('\r\n', i);
    if (eol < 0) break;
    const header = buf.subarray(i, eol).toString('latin1');
    const size = parseInt(header.split(';')[0], 16);
    if (!Number.isFinite(size)) break;
    i = eol + 2;
    if (size === 0) break; // trailers follow; ignore
    parts.push(buf.subarray(i, i + size));
    i += size + 2;
  }
  return Buffer.concat(parts);
}

/** Which service is this request for? (X-Amz-Target, then SigV4 credential scope, then S3.) */
export function detectService(req, form) {
  const target = req.headers['x-amz-target'] || '';
  if (target.startsWith('DynamoDB_')) return 'dynamodb';
  if (target.startsWith('AmazonSQS')) return 'sqs';
  if (target.startsWith('secretsmanager')) return 'secretsmanager';
  if (target.startsWith('AWSSecurityTokenService')) return 'sts';
  const auth = req.headers.authorization || '';
  const scope = auth.match(/Credential=[^/]+\/\d{8}\/([^/]+)\/([^/]+)\//);
  if (scope) return scope[2];
  const qScope = (form?.get?.('X-Amz-Credential') || '').split('/')[3];
  if (qScope) return qScope;
  if (form?.get?.('Action')) {
    const a = form.get('Action');
    if (/Queue|Message/.test(a)) return 'sqs';
    if (/Topic|Publish|Subscri/.test(a)) return 'sns';
    if (/CallerIdentity|AssumeRole|SessionToken/.test(a)) return 'sts';
  }
  return 's3';
}

export function regionOf(req) {
  const m = (req.headers.authorization || '').match(/Credential=[^/]+\/\d{8}\/([^/]+)\//);
  return m ? m[1] : REGION_DEFAULT;
}
