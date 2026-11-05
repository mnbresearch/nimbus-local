// S3 (REST/XML): buckets, objects, ranges, metadata, copy, batch delete, multipart, presigned URLs.
import { AwsError, md5, xmlDoc, xmlValues, xmlUnescape, uuid } from '../lib/common.mjs';

const NS = 'http://s3.amazonaws.com/doc/2006-03-01/';
const SUBRESOURCES = ['acl', 'cors', 'policy', 'tagging', 'lifecycle', 'encryption', 'website', 'logging', 'notification', 'ownershipControls', 'publicAccessBlock', 'requestPayment', 'accelerate', 'replication', 'object-lock', 'analytics', 'metrics', 'inventory', 'intelligent-tiering'];

export function createS3(state) {
  state.s3 ||= { buckets: {} };
  const db = state.s3;

  const bucket = (name) => {
    const b = db.buckets[name];
    if (!b) throw new AwsError('NoSuchBucket', 'The specified bucket does not exist', 404);
    return b;
  };
  const obj = (b, key) => {
    const o = b.objects[key];
    if (!o) throw new AwsError('NoSuchKey', 'The specified key does not exist.', 404);
    return o;
  };
  const ok = (body = '', headers = {}, status = 200) => ({ status, headers: body ? { 'Content-Type': 'application/xml', ...headers } : headers, body });
  const metaHeaders = (o) => ({
    ETag: `"${o.etag}"`,
    'Last-Modified': new Date(o.lastModified).toUTCString(),
    'Content-Type': o.contentType || 'binary/octet-stream',
    'Accept-Ranges': 'bytes',
    ...(o.contentEncoding ? { 'Content-Encoding': o.contentEncoding } : {}),
    ...(o.cacheControl ? { 'Cache-Control': o.cacheControl } : {}),
    ...(o.contentDisposition ? { 'Content-Disposition': o.contentDisposition } : {}),
    ...Object.fromEntries(Object.entries(o.meta || {}).map(([k, v]) => [`x-amz-meta-${k}`, v])),
  });
  const putObject = (b, key, body, h) => {
    const o = {
      body: body.toString('base64'),
      size: body.length,
      etag: md5(body),
      lastModified: Date.now(),
      contentType: h['content-type'],
      contentEncoding: (h['content-encoding'] || '').replace(/,?\s*aws-chunked/, '') || undefined,
      cacheControl: h['cache-control'],
      contentDisposition: h['content-disposition'],
      meta: Object.fromEntries(Object.entries(h).filter(([k]) => k.startsWith('x-amz-meta-')).map(([k, v]) => [k.slice(11), v])),
    };
    b.objects[key] = o;
    return o;
  };

  function list(b, q, v2) {
    const prefix = q.get('prefix') || '';
    const delim = q.get('delimiter') || '';
    const max = Math.min(1000, Number(q.get('max-keys') || 1000));
    const after = v2 ? (q.get('continuation-token') ? Buffer.from(q.get('continuation-token'), 'base64').toString() : q.get('start-after') || '') : q.get('marker') || '';
    const keys = Object.keys(b.objects).filter((k) => k.startsWith(prefix) && k > after).sort();
    const contents = [];
    const prefixes = new Set();
    let last = null;
    let truncated = false;
    for (const k of keys) {
      if (contents.length + prefixes.size >= max) {
        truncated = true;
        break;
      }
      if (delim) {
        const i = k.indexOf(delim, prefix.length);
        if (i >= 0) {
          const cp = k.slice(0, i + delim.length);
          if (!prefixes.has(cp)) prefixes.add(cp);
          last = k;
          continue;
        }
      }
      const o = b.objects[k];
      contents.push({ Key: k, LastModified: new Date(o.lastModified).toISOString(), ETag: `"${o.etag}"`, Size: o.size, StorageClass: 'STANDARD' });
      last = k;
    }
    const res = {
      Name: b.name,
      Prefix: prefix,
      ...(delim ? { Delimiter: delim } : {}),
      MaxKeys: max,
      IsTruncated: truncated,
      Contents: contents,
      CommonPrefixes: [...prefixes].map((p) => ({ Prefix: p })),
    };
    if (v2) {
      res.KeyCount = contents.length + prefixes.size;
      if (q.get('continuation-token')) res.ContinuationToken = q.get('continuation-token');
      if (truncated) res.NextContinuationToken = Buffer.from(last).toString('base64');
    } else {
      res.Marker = after;
      if (truncated) res.NextMarker = last;
    }
    return ok(xmlDoc('ListBucketResult', res, NS));
  }

  return {
    name: 's3',
    async handle({ req, url, body, bucketFromHost }) {
      const q = url.searchParams;
      const method = req.method;
      const h = req.headers;
      let path = decodeURIComponent(url.pathname.replace(/\+/g, '%20'));
      let name;
      let key;
      if (bucketFromHost) {
        name = bucketFromHost;
        key = path.slice(1);
      } else {
        const parts = path.slice(1).split('/');
        name = parts.shift();
        key = parts.join('/');
      }

      if (!name) {
        if (method !== 'GET') throw new AwsError('MethodNotAllowed', 'Method not allowed', 405);
        return ok(xmlDoc('ListAllMyBucketsResult', { Owner: { ID: 'nimbus', DisplayName: 'nimbus' }, Buckets: { Bucket: Object.values(db.buckets).map((b) => ({ Name: b.name, CreationDate: new Date(b.created).toISOString() })) } }, NS));
      }

      if (!key) {
        // ---- bucket operations
        if (method === 'PUT') {
          if (SUBRESOURCES.some((s) => q.has(s)) || q.has('versioning')) {
            bucket(name);
            if (q.has('versioning')) db.buckets[name].versioning = /<Status>Enabled/.test(body.toString()) ? 'Enabled' : 'Suspended';
            return ok();
          }
          if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(name)) throw new AwsError('InvalidBucketName', 'The specified bucket is not valid.');
          if (!db.buckets[name]) db.buckets[name] = { name, created: Date.now(), objects: {}, uploads: {} };
          return ok('', { Location: '/' + name });
        }
        const b = bucket(name);
        if (method === 'HEAD') return ok('', { 'x-amz-bucket-region': 'us-east-1' });
        if (method === 'DELETE') {
          if (SUBRESOURCES.some((s) => q.has(s))) return ok('', {}, 204);
          if (Object.keys(b.objects).length) throw new AwsError('BucketNotEmpty', 'The bucket you tried to delete is not empty', 409);
          delete db.buckets[name];
          return ok('', {}, 204);
        }
        if (method === 'POST' && q.has('delete')) {
          const xml = body.toString();
          const keys = xmlValues(xml, 'Key').map(xmlUnescape);
          const quiet = /<Quiet>true<\/Quiet>/i.test(xml);
          for (const k of keys) delete b.objects[k];
          return ok(xmlDoc('DeleteResult', { Deleted: quiet ? [] : keys.map((k) => ({ Key: k })) }, NS));
        }
        if (method === 'GET') {
          if (q.has('location')) return ok(xmlDoc('LocationConstraint', '', NS));
          if (q.has('versioning')) return ok(xmlDoc('VersioningConfiguration', b.versioning ? { Status: b.versioning } : {}, NS));
          if (q.has('uploads')) return ok(xmlDoc('ListMultipartUploadsResult', { Bucket: name, Upload: Object.entries(b.uploads).map(([id, u]) => ({ Key: u.key, UploadId: id, Initiated: new Date(u.created).toISOString() })) }, NS));
          if (q.has('policy')) throw new AwsError('NoSuchBucketPolicy', 'The bucket policy does not exist', 404);
          if (q.has('tagging')) throw new AwsError('NoSuchTagSet', 'The TagSet does not exist', 404);
          if (q.has('cors')) throw new AwsError('NoSuchCORSConfiguration', 'The CORS configuration does not exist', 404);
          if (SUBRESOURCES.some((s) => q.has(s))) return ok(xmlDoc('Configuration', {}, NS));
          return list(b, q, q.get('list-type') === '2');
        }
        throw new AwsError('MethodNotAllowed', 'Method not allowed', 405);
      }

      // ---- object operations
      const b = bucket(name);
      if (method === 'POST' && q.has('uploads')) {
        const id = uuid().replace(/-/g, '');
        b.uploads[id] = { key, created: Date.now(), parts: {}, headers: { ...h } };
        return ok(xmlDoc('InitiateMultipartUploadResult', { Bucket: name, Key: key, UploadId: id }, NS));
      }
      if (method === 'PUT' && q.has('uploadId')) {
        const u = b.uploads[q.get('uploadId')];
        if (!u) throw new AwsError('NoSuchUpload', 'The specified upload does not exist.', 404);
        const n = Number(q.get('partNumber'));
        u.parts[n] = { body: body.toString('base64'), etag: md5(body), size: body.length };
        return ok('', { ETag: `"${u.parts[n].etag}"` });
      }
      if (method === 'POST' && q.has('uploadId')) {
        const id = q.get('uploadId');
        const u = b.uploads[id];
        if (!u) throw new AwsError('NoSuchUpload', 'The specified upload does not exist.', 404);
        const nums = xmlValues(body.toString(), 'PartNumber').map(Number);
        const order = nums.length ? nums : Object.keys(u.parts).map(Number).sort((x, y) => x - y);
        const bufs = order.map((n) => {
          if (!u.parts[n]) throw new AwsError('InvalidPart', `Part ${n} was not uploaded`);
          return Buffer.from(u.parts[n].body, 'base64');
        });
        const o = putObject(b, key, Buffer.concat(bufs), u.headers);
        o.etag = `${md5(Buffer.concat(order.map((n) => Buffer.from(u.parts[n].etag, 'hex'))))}-${order.length}`;
        delete b.uploads[id];
        return ok(xmlDoc('CompleteMultipartUploadResult', { Location: `/${name}/${key}`, Bucket: name, Key: key, ETag: `"${o.etag}"` }, NS));
      }
      if (method === 'DELETE' && q.has('uploadId')) {
        delete b.uploads[q.get('uploadId')];
        return ok('', {}, 204);
      }
      if (method === 'GET' && q.has('uploadId')) {
        const u = b.uploads[q.get('uploadId')];
        if (!u) throw new AwsError('NoSuchUpload', 'The specified upload does not exist.', 404);
        return ok(xmlDoc('ListPartsResult', { Bucket: name, Key: key, UploadId: q.get('uploadId'), Part: Object.entries(u.parts).map(([n, p]) => ({ PartNumber: n, ETag: `"${p.etag}"`, Size: p.size })) }, NS));
      }
      if (q.has('tagging') || q.has('acl')) {
        obj(b, key);
        if (method === 'GET') return ok(xmlDoc(q.has('tagging') ? 'Tagging' : 'AccessControlPolicy', q.has('tagging') ? { TagSet: '' } : {}, NS));
        return ok();
      }
      if (method === 'PUT') {
        const src = h['x-amz-copy-source'];
        if (src) {
          const s = decodeURIComponent(src.split('?')[0]).replace(/^\//, '');
          const [sb, ...rest] = s.split('/');
          const so = obj(bucket(sb), rest.join('/'));
          const copy = { ...so, lastModified: Date.now(), meta: { ...so.meta } };
          if ((h['x-amz-metadata-directive'] || '').toUpperCase() === 'REPLACE') {
            copy.meta = Object.fromEntries(Object.entries(h).filter(([k]) => k.startsWith('x-amz-meta-')).map(([k, v]) => [k.slice(11), v]));
            copy.contentType = h['content-type'] || copy.contentType;
          }
          b.objects[key] = copy;
          return ok(xmlDoc('CopyObjectResult', { LastModified: new Date(copy.lastModified).toISOString(), ETag: `"${copy.etag}"` }, NS));
        }
        const o = putObject(b, key, body, h);
        return ok('', { ETag: `"${o.etag}"` });
      }
      if (method === 'DELETE') {
        delete b.objects[key];
        return ok('', {}, 204);
      }
      if (method === 'GET' || method === 'HEAD') {
        const o = obj(b, key);
        const headers = metaHeaders(o);
        const full = Buffer.from(o.body, 'base64');
        const range = h.range && h.range.match(/bytes=(\d*)-(\d*)/);
        if (range) {
          let start = range[1] === '' ? Math.max(0, o.size - Number(range[2])) : Number(range[1]);
          let end = range[1] === '' ? o.size - 1 : range[2] === '' ? o.size - 1 : Math.min(o.size - 1, Number(range[2]));
          if (start >= o.size) throw new AwsError('InvalidRange', 'The requested range is not satisfiable', 416);
          const part = full.subarray(start, end + 1);
          return { status: 206, headers: { ...headers, 'Content-Range': `bytes ${start}-${end}/${o.size}`, 'Content-Length': String(part.length) }, body: method === 'HEAD' ? '' : part };
        }
        return { status: 200, headers: { ...headers, 'Content-Length': String(o.size) }, body: method === 'HEAD' ? '' : full };
      }
      throw new AwsError('MethodNotAllowed', 'Method not allowed', 405);
    },
    errorBody: (e) => xmlDoc('Error', { Code: e.code, Message: e.message, RequestId: uuid() }),
  };
}
