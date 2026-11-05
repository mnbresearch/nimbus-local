// DynamoDB (JSON 1.0): tables + GSIs/LSIs, CRUD with expressions, Query/Scan with pagination,
// batches, transactions, TTL.
import { AwsError, ACCOUNT } from '../lib/common.mjs';
import { compileCondition, parseKeyCondition, applyUpdate, project, cmpVal, getPath } from './expressions.mjs';

export function createDynamo(state) {
  state.dynamodb ||= { tables: {} };
  const db = state.dynamodb;
  const notFound = (n) => new AwsError('ResourceNotFoundException', `Requested resource not found: Table: ${n} not found`);
  const table = (n) => db.tables[n] || (() => { throw notFound(n); })();
  const keyNames = (schema) => ({ hash: schema.find((k) => k.KeyType === 'HASH')?.AttributeName, range: schema.find((k) => k.KeyType === 'RANGE')?.AttributeName });
  const keyOf = (t, item) => {
    const { hash, range } = keyNames(t.KeySchema);
    if (!item[hash]) throw new AwsError('ValidationException', `One of the required keys was not given a value: ${hash}`);
    if (range && !item[range]) throw new AwsError('ValidationException', `One of the required keys was not given a value: ${range}`);
    return JSON.stringify([item[hash], range ? item[range] : null]);
  };
  const keyOnly = (schema, item) => Object.fromEntries(schema.map((k) => [k.AttributeName, item[k.AttributeName]]));
  const clone = (x) => (x == null ? x : JSON.parse(JSON.stringify(x)));
  const failCond = () => new AwsError('ConditionalCheckFailedException', 'The conditional request failed');

  function live(t) {
    if (!t.ttl) return;
    const now = Date.now() / 1000;
    for (const [k, it] of Object.entries(t.items)) if (it[t.ttl]?.N && Number(it[t.ttl].N) < now) delete t.items[k];
  }

  function describe(t) {
    const count = Object.keys(t.items).length;
    const size = Buffer.byteLength(JSON.stringify(t.items));
    const idx = (list, kind) =>
      list?.length
        ? list.map((g) => ({ IndexName: g.IndexName, KeySchema: g.KeySchema, Projection: g.Projection, IndexStatus: 'ACTIVE', ItemCount: count, IndexSizeBytes: size, IndexArn: `${t.arn}/index/${g.IndexName}`, ...(kind === 'g' ? { ProvisionedThroughput: { ReadCapacityUnits: 0, WriteCapacityUnits: 0 } } : {}) }))
        : undefined;
    return {
      TableName: t.TableName,
      TableArn: t.arn,
      TableId: t.id,
      TableStatus: 'ACTIVE',
      KeySchema: t.KeySchema,
      AttributeDefinitions: t.AttributeDefinitions,
      CreationDateTime: t.created / 1000,
      ItemCount: count,
      TableSizeBytes: size,
      BillingModeSummary: { BillingMode: t.BillingMode || 'PAY_PER_REQUEST' },
      ProvisionedThroughput: { ReadCapacityUnits: 0, WriteCapacityUnits: 0, NumberOfDecreasesToday: 0 },
      GlobalSecondaryIndexes: idx(t.GlobalSecondaryIndexes, 'g'),
      LocalSecondaryIndexes: idx(t.LocalSecondaryIndexes, 'l'),
    };
  }

  function checkCondition(p, existing) {
    if (p.ConditionExpression && !compileCondition(p.ConditionExpression, p.ExpressionAttributeNames, p.ExpressionAttributeValues)(existing)) throw failCond();
  }

  function validateItem(t, item) {
    for (const def of t.AttributeDefinitions) {
      const v = item[def.AttributeName];
      if (v && !(def.AttributeType in v)) throw new AwsError('ValidationException', `One or more parameter values were invalid: Type mismatch for key ${def.AttributeName} expected: ${def.AttributeType}`);
    }
    if (Buffer.byteLength(JSON.stringify(item)) > 400 * 1024) throw new AwsError('ValidationException', 'Item size has exceeded the maximum allowed size');
  }

  function put(p) {
    const t = table(p.TableName);
    validateItem(t, p.Item);
    const k = keyOf(t, p.Item);
    const old = t.items[k];
    checkCondition(p, old);
    t.items[k] = clone(p.Item);
    return p.ReturnValues === 'ALL_OLD' && old ? { Attributes: old } : {};
  }
  function update(p) {
    const t = table(p.TableName);
    const k = keyOf(t, p.Key);
    const old = t.items[k];
    checkCondition(p, old);
    const item = clone(old) || clone(p.Key);
    if (p.UpdateExpression) applyUpdate(item, p.UpdateExpression, p.ExpressionAttributeNames, p.ExpressionAttributeValues);
    for (const kn of Object.keys(p.Key)) if (JSON.stringify(item[kn]) !== JSON.stringify(p.Key[kn])) throw new AwsError('ValidationException', 'One or more parameter values were invalid: Cannot update attribute ' + kn + '. This attribute is part of the key');
    validateItem(t, item);
    t.items[k] = item;
    const rv = p.ReturnValues || 'NONE';
    if (rv === 'ALL_NEW') return { Attributes: item };
    if (rv === 'ALL_OLD') return old ? { Attributes: old } : {};
    if (rv === 'UPDATED_NEW' || rv === 'UPDATED_OLD') {
      const src = rv === 'UPDATED_NEW' ? item : old || {};
      const changed = Object.keys({ ...item, ...(old || {}) }).filter((a) => JSON.stringify(item[a]) !== JSON.stringify(old?.[a]) && !(a in p.Key));
      return { Attributes: Object.fromEntries(changed.filter((a) => a in src).map((a) => [a, src[a]])) };
    }
    return {};
  }
  function del(p) {
    const t = table(p.TableName);
    const k = keyOf(t, p.Key);
    const old = t.items[k];
    checkCondition(p, old);
    delete t.items[k];
    return p.ReturnValues === 'ALL_OLD' && old ? { Attributes: old } : {};
  }

  function search(p, isQuery) {
    const t = table(p.TableName);
    live(t);
    let schema = t.KeySchema;
    if (p.IndexName) {
      const ix = [...(t.GlobalSecondaryIndexes || []), ...(t.LocalSecondaryIndexes || [])].find((i) => i.IndexName === p.IndexName);
      if (!ix) throw new AwsError('ValidationException', `The table does not have the specified index: ${p.IndexName}`);
      schema = ix.KeySchema;
    }
    const { hash, range } = keyNames(schema);
    let items = Object.values(t.items);
    if (p.IndexName) items = items.filter((it) => it[hash] && (!range || it[range]));
    if (isQuery) {
      if (!p.KeyConditionExpression) throw new AwsError('ValidationException', 'Query requires KeyConditionExpression');
      const kc = parseKeyCondition(p.KeyConditionExpression, p.ExpressionAttributeNames, p.ExpressionAttributeValues);
      const hashPart = kc.parts.find((x) => x.op === '=' && x.a.path?.[0] === hash);
      if (!hashPart) throw new AwsError('ValidationException', 'Query condition missed key schema element: ' + hash);
      items = items.filter(kc.test);
    }
    const sortKey = range || (isQuery ? null : null);
    if (sortKey) items.sort((a, b) => cmpVal(a[sortKey], b[sortKey]) || (cmpVal(a[hash], b[hash]) || 0));
    else if (!isQuery) items.sort((a, b) => keyOf(t, a).localeCompare(keyOf(t, b)));
    if (isQuery && p.ScanIndexForward === false) items.reverse();
    if (p.ExclusiveStartKey) {
      const startK = JSON.stringify(t.KeySchema.map((k) => p.ExclusiveStartKey[k.AttributeName]));
      const at = items.findIndex((it) => JSON.stringify(t.KeySchema.map((k) => it[k.AttributeName])) === startK);
      items = at >= 0 ? items.slice(at + 1) : items;
    }
    if (p.TotalSegments > 1) items = items.filter((it) => [...keyOf(t, it)].reduce((s, c) => s + c.charCodeAt(0), 0) % p.TotalSegments === p.Segment);
    let evaluated = items;
    let lek;
    if (p.Limit && items.length > p.Limit) {
      evaluated = items.slice(0, p.Limit);
      const last = evaluated.at(-1);
      lek = { ...keyOnly(t.KeySchema, last), ...(p.IndexName ? keyOnly(schema, last) : {}) };
    }
    const filter = compileCondition(p.FilterExpression, p.ExpressionAttributeNames, p.ExpressionAttributeValues);
    const matched = evaluated.filter(filter);
    const out = { Count: matched.length, ScannedCount: evaluated.length };
    if (p.Select !== 'COUNT') out.Items = matched.map((it) => clone(project(it, p.ProjectionExpression, p.ExpressionAttributeNames)));
    if (lek) out.LastEvaluatedKey = lek;
    return out;
  }

  const actions = {
    CreateTable(p) {
      if (db.tables[p.TableName]) throw new AwsError('ResourceInUseException', `Table already exists: ${p.TableName}`);
      if (!p.KeySchema?.length) throw new AwsError('ValidationException', 'KeySchema is required');
      const t = { ...clone(p), id: crypto.randomUUID(), arn: `arn:aws:dynamodb:us-east-1:${ACCOUNT}:table/${p.TableName}`, created: Date.now(), items: {} };
      db.tables[p.TableName] = t;
      return { TableDescription: describe(t) };
    },
    DescribeTable: (p) => ({ Table: describe(table(p.TableName)) }),
    ListTables(p) {
      const names = Object.keys(db.tables).sort().filter((n) => !p.ExclusiveStartTableName || n > p.ExclusiveStartTableName);
      const lim = p.Limit || 100;
      return { TableNames: names.slice(0, lim), ...(names.length > lim ? { LastEvaluatedTableName: names[lim - 1] } : {}) };
    },
    DeleteTable(p) {
      const t = table(p.TableName);
      delete db.tables[p.TableName];
      return { TableDescription: { ...describe(t), TableStatus: 'DELETING' } };
    },
    UpdateTable(p) {
      const t = table(p.TableName);
      for (const u of p.GlobalSecondaryIndexUpdates || []) {
        if (u.Create) (t.GlobalSecondaryIndexes ||= []).push(u.Create);
        if (u.Delete) t.GlobalSecondaryIndexes = (t.GlobalSecondaryIndexes || []).filter((g) => g.IndexName !== u.Delete.IndexName);
      }
      if (p.AttributeDefinitions) t.AttributeDefinitions = [...new Map([...t.AttributeDefinitions, ...p.AttributeDefinitions].map((a) => [a.AttributeName, a])).values()];
      if (p.BillingMode) t.BillingMode = p.BillingMode;
      return { TableDescription: describe(t) };
    },
    UpdateTimeToLive(p) {
      const t = table(p.TableName);
      t.ttl = p.TimeToLiveSpecification.Enabled ? p.TimeToLiveSpecification.AttributeName : null;
      return { TimeToLiveSpecification: p.TimeToLiveSpecification };
    },
    DescribeTimeToLive(p) {
      const t = table(p.TableName);
      return { TimeToLiveDescription: t.ttl ? { TimeToLiveStatus: 'ENABLED', AttributeName: t.ttl } : { TimeToLiveStatus: 'DISABLED' } };
    },
    PutItem: put,
    UpdateItem: update,
    DeleteItem: del,
    GetItem(p) {
      const t = table(p.TableName);
      live(t);
      const it = t.items[keyOf(t, p.Key)];
      return it ? { Item: clone(project(it, p.ProjectionExpression, p.ExpressionAttributeNames)) } : {};
    },
    Query: (p) => search(p, true),
    Scan: (p) => search(p, false),
    BatchWriteItem(p) {
      for (const [tn, reqs] of Object.entries(p.RequestItems || {})) {
        if (reqs.length > 25) throw new AwsError('ValidationException', 'Too many items requested for the BatchWriteItem call');
        for (const r of reqs) {
          if (r.PutRequest) put({ TableName: tn, Item: r.PutRequest.Item });
          if (r.DeleteRequest) del({ TableName: tn, Key: r.DeleteRequest.Key });
        }
      }
      return { UnprocessedItems: {} };
    },
    BatchGetItem(p) {
      const Responses = {};
      for (const [tn, spec] of Object.entries(p.RequestItems || {})) {
        const t = table(tn);
        live(t);
        Responses[tn] = spec.Keys.map((k) => t.items[keyOf(t, k)]).filter(Boolean).map((it) => clone(project(it, spec.ProjectionExpression, spec.ExpressionAttributeNames)));
      }
      return { Responses, UnprocessedKeys: {} };
    },
    TransactWriteItems(p) {
      const backup = clone(db.tables);
      const reasons = [];
      let failed = false;
      for (const op of p.TransactItems || []) {
        try {
          if (op.Put) put(op.Put);
          else if (op.Update) update(op.Update);
          else if (op.Delete) del(op.Delete);
          else if (op.ConditionCheck) {
            const t = table(op.ConditionCheck.TableName);
            checkCondition(op.ConditionCheck, t.items[keyOf(t, op.ConditionCheck.Key)]);
          }
          reasons.push({ Code: 'None' });
        } catch (e) {
          failed = true;
          reasons.push({ Code: e.code === 'ConditionalCheckFailedException' ? 'ConditionalCheckFailed' : e.code, Message: e.message });
        }
      }
      if (failed) {
        db.tables = backup;
        state.dynamodb.tables = backup;
        const err = new AwsError('TransactionCanceledException', `Transaction cancelled, please refer cancellation reasons for specific reasons [${reasons.map((r) => r.Code).join(', ')}]`);
        err.extra = { CancellationReasons: reasons };
        throw err;
      }
      return {};
    },
    TransactGetItems(p) {
      return { Responses: (p.TransactItems || []).map(({ Get }) => {
        const t = table(Get.TableName);
        const it = t.items[keyOf(t, Get.Key)];
        return it ? { Item: clone(project(it, Get.ProjectionExpression, Get.ExpressionAttributeNames)) } : {};
      }) };
    },
    DescribeEndpoints: () => ({ Endpoints: [{ Address: 'localhost', CachePeriodInMinutes: 1440 }] }),
    ListTagsOfResource: () => ({ Tags: [] }),
    TagResource: () => ({}),
    DescribeContinuousBackups: (p) => (table(p.TableName), { ContinuousBackupsDescription: { ContinuousBackupsStatus: 'DISABLED' } }),
  };

  return {
    name: 'dynamodb',
    actions,
    get db() {
      return state.dynamodb;
    },
    async handle({ req, body }) {
      const action = (req.headers['x-amz-target'] || '').split('.')[1];
      if (!actions[action]) throw new AwsError('UnknownOperationException', `Unsupported DynamoDB operation ${action}`);
      const p = body.length ? JSON.parse(body.toString()) : {};
      return { json: actions[action](p), contentType: 'application/x-amz-json-1.0' };
    },
    jsonError: true,
    errorPrefix: 'com.amazonaws.dynamodb.v20120810#',
    getPath,
  };
}
