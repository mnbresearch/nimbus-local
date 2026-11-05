import { test } from 'node:test';
import assert from 'node:assert/strict';
import { compileCondition, applyUpdate, project, tokenize } from '../services/expressions.mjs';
import { decodeAwsChunked, detectService, toXml } from '../lib/common.mjs';
import { startNimbus } from '../nimbus.mjs';

const item = { pk: { S: 'u#1' }, age: { N: '42' }, tags: { SS: ['a', 'b'] }, profile: { M: { city: { S: 'Delhi' }, langs: { L: [{ S: 'hi' }, { S: 'en' }] } } } };

test('condition expressions', () => {
  const c = (e, v = {}, n = {}) => compileCondition(e, n, v)(item);
  assert.ok(c('age BETWEEN :a AND :b', { ':a': { N: '40' }, ':b': { N: '50' } }));
  assert.ok(c('begins_with(pk, :p) AND contains(tags, :t)', { ':p': { S: 'u#' }, ':t': { S: 'b' } }));
  assert.ok(c('#p.city = :c', { ':c': { S: 'Delhi' } }, { '#p': 'profile' }));
  assert.ok(c('profile.langs[1] = :l', { ':l': { S: 'en' } }));
  assert.ok(c('NOT attribute_exists(missing) OR age < :x', { ':x': { N: '1' } }));
  assert.ok(c('size(tags) = :two', { ':two': { N: '2' } }));
  assert.ok(c('age IN (:a, :b)', { ':a': { N: '1' }, ':b': { N: '42.0' } }));
  assert.ok(!c('(age > :x) AND (pk <> :pk)', { ':x': { N: '10' }, ':pk': { S: 'u#1' } }));
  assert.throws(() => c('age = :missing'), /not defined/);
  assert.throws(() => tokenize('age = $x'), /Invalid expression/);
});

test('update expressions', () => {
  const it = JSON.parse(JSON.stringify(item));
  applyUpdate(it, 'SET age = age - :one, profile.city = :c, #n = if_not_exists(#n, :zero) REMOVE tags', { '#n': 'visits' }, { ':one': { N: '1' }, ':c': { S: 'Mumbai' }, ':zero': { N: '0' } });
  assert.equal(it.age.N, '41');
  assert.equal(it.profile.M.city.S, 'Mumbai');
  assert.equal(it.visits.N, '0');
  assert.equal(it.tags, undefined);
  applyUpdate(it, 'ADD visits :n, labels :s DELETE labels :d', {}, { ':n': { N: '3' }, ':s': { SS: ['x', 'y'] }, ':d': { SS: ['x'] } });
  assert.equal(it.visits.N, '3');
  assert.deepEqual(it.labels.SS, ['y']);
});

test('projection', () => {
  assert.deepEqual(Object.keys(project(item, 'pk, profile.city')), ['pk', 'profile']);
  assert.deepEqual(project(item, 'profile.city').profile.M, { city: { S: 'Delhi' } });
});

test('aws-chunked decoding and service detection', () => {
  const body = Buffer.from('5;chunk-signature=abc\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n');
  assert.equal(decodeAwsChunked(body).toString(), 'hello world');
  const req = (h) => ({ headers: h });
  assert.equal(detectService(req({ 'x-amz-target': 'DynamoDB_20120810.PutItem' })), 'dynamodb');
  assert.equal(detectService(req({ authorization: 'AWS4-HMAC-SHA256 Credential=test/20261105/us-east-1/sqs/aws4_request' })), 'sqs');
  assert.equal(detectService(req({})), 's3');
  assert.equal(toXml('A', { B: [1, 2], C: 'x<y' }), '<A><B>1</B><B>2</B><C>x&lt;y</C></A>');
});

test('server: S3 + DynamoDB round-trip over HTTP with persistence', async () => {
  const dir = `/tmp/nimbus-test-${process.pid}`;
  let n = await startNimbus({ port: 0, persist: dir, quiet: true });
  const base = `http://127.0.0.1:${n.port}`;
  const auth = (svc) => ({ authorization: `AWS4-HMAC-SHA256 Credential=test/20261105/us-east-1/${svc}/aws4_request, SignedHeaders=host, Signature=x` });
  assert.equal((await fetch(`${base}/bucket-one`, { method: 'PUT', headers: auth('s3') })).status, 200);
  await fetch(`${base}/bucket-one/hello.txt`, { method: 'PUT', headers: auth('s3'), body: 'hi there' });
  const ddb = (target, b) => fetch(base, { method: 'POST', headers: { ...auth('dynamodb'), 'x-amz-target': `DynamoDB_20120810.${target}`, 'content-type': 'application/x-amz-json-1.0' }, body: JSON.stringify(b) }).then((r) => r.json());
  await ddb('CreateTable', { TableName: 't', KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }], AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }] });
  await ddb('PutItem', { TableName: 't', Item: { id: { S: '1' }, v: { N: '7' } } });
  await new Promise((r) => setTimeout(r, 700)); // let the snapshot flush
  await n.close();
  n = await startNimbus({ port: 0, persist: dir, quiet: true });
  const base2 = `http://127.0.0.1:${n.port}`;
  assert.equal(await (await fetch(`${base2}/bucket-one/hello.txt`, { headers: auth('s3') })).text(), 'hi there');
  const got = await fetch(base2, { method: 'POST', headers: { ...auth('dynamodb'), 'x-amz-target': 'DynamoDB_20120810.GetItem' }, body: JSON.stringify({ TableName: 't', Key: { id: { S: '1' } } }) }).then((r) => r.json());
  assert.equal(got.Item.v.N, '7');
  const health = await (await fetch(`${base2}/_localstack/health`)).json();
  assert.equal(health.services.s3, 'running');
  await n.close();
});
