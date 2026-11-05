// SQS — both the JSON protocol (current SDKs/CLI) and the legacy Query/XML protocol.
// Visibility timeouts, delays, long polling, DLQ redrive, FIFO groups + dedup, batches.
import { AwsError, ACCOUNT, md5, uuid, xmlDoc } from '../lib/common.mjs';

const NS = 'http://queue.amazonaws.com/doc/2012-11-05/';

export function createSqs(state, { baseUrl }) {
  state.sqs ||= { queues: {} };
  const db = state.sqs;
  const arnOf = (name, region = 'us-east-1') => `arn:aws:sqs:${region}:${ACCOUNT}:${name}`;
  const urlOf = (name) => `${baseUrl()}/${ACCOUNT}/${name}`;
  const byUrl = (u) => {
    const name = String(u || '').split('/').pop();
    const q = db.queues[name];
    if (!q) throw new AwsError('AWS.SimpleQueueService.NonExistentQueue', 'The specified queue does not exist.', 400);
    return q;
  };

  function visible(q, now) {
    return q.messages.filter((m) => m.visibleAt <= now);
  }

  function redrive(q, now) {
    const rp = q.attributes.RedrivePolicy && JSON.parse(q.attributes.RedrivePolicy);
    if (!rp) return;
    const dlq = Object.values(db.queues).find((x) => x.arn === rp.deadLetterTargetArn);
    if (!dlq) return;
    const max = Number(rp.maxReceiveCount);
    const move = q.messages.filter((m) => m.visibleAt <= now && m.receiveCount >= max);
    for (const m of move) {
      q.messages.splice(q.messages.indexOf(m), 1);
      dlq.messages.push({ ...m, visibleAt: now, receiveCount: 0 });
    }
  }

  function send(q, p) {
    if (!p.MessageBody) throw new AwsError('MissingParameter', 'The request must contain the parameter MessageBody.');
    if (Buffer.byteLength(p.MessageBody) > 262144) throw new AwsError('InvalidParameterValue', 'Message must be shorter than 262144 bytes.');
    const now = Date.now();
    const fifo = q.name.endsWith('.fifo');
    if (fifo && !p.MessageGroupId) throw new AwsError('MissingParameter', 'The request must contain the parameter MessageGroupId.');
    const dedup = fifo ? p.MessageDeduplicationId || (q.attributes.ContentBasedDeduplication === 'true' ? md5(p.MessageBody) : null) : null;
    if (fifo && !dedup) throw new AwsError('InvalidParameterValue', 'The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly');
    if (dedup) {
      q.dedup = (q.dedup || []).filter((d) => d.t > now - 300000);
      const seen = q.dedup.find((d) => d.id === dedup);
      if (seen) return { MessageId: seen.messageId, MD5OfMessageBody: md5(p.MessageBody) };
    }
    const delay = Number(p.DelaySeconds ?? q.attributes.DelaySeconds ?? 0);
    const m = { id: uuid(), body: p.MessageBody, md5: md5(p.MessageBody), sentAt: now, visibleAt: now + delay * 1000, receiveCount: 0, groupId: p.MessageGroupId, attributes: p.MessageAttributes || {} };
    q.messages.push(m);
    if (dedup) q.dedup.push({ id: dedup, messageId: m.id, t: now });
    return { MessageId: m.id, MD5OfMessageBody: m.md5, ...(fifo ? { SequenceNumber: String(now * 1000 + q.messages.length) } : {}) };
  }

  async function receive(q, p) {
    const max = Math.min(10, Number(p.MaxNumberOfMessages || 1));
    const vt = Number(p.VisibilityTimeout ?? q.attributes.VisibilityTimeout ?? 30);
    const wait = Math.min(20, Number(p.WaitTimeSeconds ?? q.attributes.ReceiveMessageWaitTimeSeconds ?? 0));
    const deadline = Date.now() + wait * 1000;
    for (;;) {
      const now = Date.now();
      // expire retention
      const ret = Number(q.attributes.MessageRetentionPeriod || 345600) * 1000;
      q.messages = q.messages.filter((m) => m.sentAt > now - ret);
      redrive(q, now);
      let ready = visible(q, now);
      if (q.name.endsWith('.fifo')) {
        const busy = new Set(q.messages.filter((m) => m.visibleAt > now && m.receiveCount).map((m) => m.groupId));
        ready = ready.filter((m) => !busy.has(m.groupId));
      }
      const picked = ready.slice(0, max);
      if (picked.length || now >= deadline) {
        return picked.map((m) => {
          m.visibleAt = now + vt * 1000;
          m.receiveCount += 1;
          m.firstReceive ||= now;
          m.receipt = Buffer.from(`${q.name}|${m.id}|${uuid()}`).toString('base64');
          return {
            MessageId: m.id,
            ReceiptHandle: m.receipt,
            MD5OfBody: m.md5,
            Body: m.body,
            Attributes: { SentTimestamp: String(m.sentAt), ApproximateReceiveCount: String(m.receiveCount), ApproximateFirstReceiveTimestamp: String(m.firstReceive), ...(m.groupId ? { MessageGroupId: m.groupId } : {}) },
            ...(Object.keys(m.attributes).length ? { MessageAttributes: m.attributes } : {}),
          };
        });
      }
      await new Promise((r) => setTimeout(r, 100));
    }
  }

  const findByReceipt = (q, rh) => q.messages.find((m) => m.receipt === rh);

  const actions = {
    CreateQueue(p) {
      const name = p.QueueName;
      if (!/^[\w-]{1,80}(\.fifo)?$/.test(name || '')) throw new AwsError('InvalidParameterValue', 'Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length');
      if (!db.queues[name]) {
        db.queues[name] = { name, arn: arnOf(name), created: Date.now(), attributes: { VisibilityTimeout: '30', DelaySeconds: '0', MessageRetentionPeriod: '345600', ReceiveMessageWaitTimeSeconds: '0', ...(name.endsWith('.fifo') ? { FifoQueue: 'true' } : {}), ...(p.Attributes || {}) }, messages: [], tags: p.tags || {} };
      }
      return { QueueUrl: urlOf(name) };
    },
    GetQueueUrl(p) {
      if (!db.queues[p.QueueName]) throw new AwsError('AWS.SimpleQueueService.NonExistentQueue', 'The specified queue does not exist.');
      return { QueueUrl: urlOf(p.QueueName) };
    },
    ListQueues(p) {
      return { QueueUrls: Object.keys(db.queues).filter((n) => n.startsWith(p.QueueNamePrefix || '')).map(urlOf) };
    },
    DeleteQueue(p) {
      delete db.queues[byUrl(p.QueueUrl).name];
      return {};
    },
    PurgeQueue(p) {
      byUrl(p.QueueUrl).messages = [];
      return {};
    },
    GetQueueAttributes(p) {
      const q = byUrl(p.QueueUrl);
      const now = Date.now();
      const all = {
        ...q.attributes,
        QueueArn: q.arn,
        ApproximateNumberOfMessages: String(q.messages.filter((m) => m.visibleAt <= now).length),
        ApproximateNumberOfMessagesNotVisible: String(q.messages.filter((m) => m.visibleAt > now && m.receiveCount).length),
        ApproximateNumberOfMessagesDelayed: String(q.messages.filter((m) => m.visibleAt > now && !m.receiveCount).length),
        CreatedTimestamp: String(Math.floor(q.created / 1000)),
        LastModifiedTimestamp: String(Math.floor(q.created / 1000)),
      };
      const names = p.AttributeNames || ['All'];
      return { Attributes: names.includes('All') ? all : Object.fromEntries(names.filter((n) => n in all).map((n) => [n, all[n]])) };
    },
    SetQueueAttributes(p) {
      Object.assign(byUrl(p.QueueUrl).attributes, p.Attributes || {});
      return {};
    },
    SendMessage: (p) => send(byUrl(p.QueueUrl), p),
    SendMessageBatch(p) {
      const q = byUrl(p.QueueUrl);
      const Successful = [];
      const Failed = [];
      for (const e of p.Entries || []) {
        try {
          Successful.push({ Id: e.Id, ...send(q, e) });
        } catch (err) {
          Failed.push({ Id: e.Id, Code: err.code, Message: err.message, SenderFault: true });
        }
      }
      return { Successful, Failed };
    },
    async ReceiveMessage(p) {
      const msgs = await receive(byUrl(p.QueueUrl), p);
      return msgs.length ? { Messages: msgs } : {};
    },
    DeleteMessage(p) {
      const q = byUrl(p.QueueUrl);
      const m = findByReceipt(q, p.ReceiptHandle);
      if (m) q.messages.splice(q.messages.indexOf(m), 1);
      return {};
    },
    DeleteMessageBatch(p) {
      const q = byUrl(p.QueueUrl);
      const Successful = [];
      for (const e of p.Entries || []) {
        const m = findByReceipt(q, e.ReceiptHandle);
        if (m) q.messages.splice(q.messages.indexOf(m), 1);
        Successful.push({ Id: e.Id });
      }
      return { Successful, Failed: [] };
    },
    ChangeMessageVisibility(p) {
      const q = byUrl(p.QueueUrl);
      const m = findByReceipt(q, p.ReceiptHandle);
      if (!m) throw new AwsError('ReceiptHandleIsInvalid', 'The input receipt handle is invalid.');
      m.visibleAt = Date.now() + Number(p.VisibilityTimeout) * 1000;
      return {};
    },
    TagQueue(p) {
      Object.assign(byUrl(p.QueueUrl).tags, p.Tags || {});
      return {};
    },
    ListQueueTags: (p) => ({ Tags: byUrl(p.QueueUrl).tags }),
  };

  // Query protocol → JSON-style params.
  function fromQuery(form) {
    const p = {};
    const entries = {};
    const attrs = {};
    const names = [];
    for (const [k, v] of form) {
      let m;
      if ((m = k.match(/^(?:\w+RequestEntry|Entries(?:\.member)?)\.(\d+)\.(.+)$/))) (entries[m[1]] ||= {})[m[2]] = v;
      else if ((m = k.match(/^Attribute(?:s)?(?:\.entry)?\.(\d+)\.(Name|Value|key|value)$/))) (attrs[m[1]] ||= {})[m[2].toLowerCase()] = v;
      else if ((m = k.match(/^AttributeName(?:s)?(?:\.member)?\.(\d+)$/))) names.push(v);
      else if ((m = k.match(/^Tag(?:s)?\.(\d+)\.(Key|Value)$/))) (p._tags ||= {})[m[1]] = { ...(p._tags?.[m[1]] || {}), [m[2]]: v };
      else p[k] = v;
    }
    if (Object.keys(entries).length) p.Entries = Object.values(entries);
    if (Object.keys(attrs).length) p.Attributes = Object.fromEntries(Object.values(attrs).map((a) => [a.name, a.value]));
    if (names.length) p.AttributeNames = names;
    return p;
  }

  function toQueryXml(action, out) {
    const result = {};
    if (out.QueueUrl) result.QueueUrl = out.QueueUrl;
    if (out.QueueUrls) result.QueueUrl = out.QueueUrls;
    if (out.MessageId) Object.assign(result, { MessageId: out.MessageId, MD5OfMessageBody: out.MD5OfMessageBody });
    if (out.Messages || action === 'ReceiveMessage') result.Message = (out.Messages || []).length === 0 ? [] : out.Messages.map((m) => ({ MessageId: m.MessageId, ReceiptHandle: m.ReceiptHandle, MD5OfBody: m.MD5OfBody, Body: m.Body, Attribute: Object.entries(m.Attributes).map(([Name, Value]) => ({ Name, Value })) }));
    if (out.Attributes) result.Attribute = Object.entries(out.Attributes).map(([Name, Value]) => ({ Name, Value }));
    if (out.Successful) {
      const tag = action === 'SendMessageBatch' ? 'SendMessageBatchResultEntry' : 'DeleteMessageBatchResultEntry';
      result[tag] = out.Successful;
      result.BatchResultErrorEntry = out.Failed;
    }
    return xmlDoc(`${action}Response`, { [`${action}Result`]: result, ResponseMetadata: { RequestId: uuid() } }, NS);
  }

  return {
    name: 'sqs',
    actions,
    async handle({ req, url, body, form }) {
      const target = req.headers['x-amz-target'];
      if (target) {
        const action = target.split('.')[1];
        if (!actions[action]) throw new AwsError('InvalidAction', `Unsupported SQS action ${action}`);
        const p = body.length ? JSON.parse(body.toString()) : {};
        if (!p.QueueUrl && url.pathname.split('/').length >= 3 && url.pathname !== '/') p.QueueUrl = url.pathname;
        return { json: await actions[action](p), contentType: 'application/x-amz-json-1.0' };
      }
      const p = fromQuery(form);
      if (!p.QueueUrl && /^\/\d{12}\/[\w.-]+$/.test(url.pathname)) p.QueueUrl = url.pathname;
      if (p._tags) p.tags = Object.fromEntries(Object.values(p._tags).map((t) => [t.Key, t.Value]));
      const action = p.Action;
      if (!actions[action]) throw new AwsError('InvalidAction', `Unsupported SQS action ${action}`);
      return { xml: toQueryXml(action, await actions[action](p)) };
    },
    jsonError: true,
  };
}
