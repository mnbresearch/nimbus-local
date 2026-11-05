// Secrets Manager (JSON 1.1), SNS (Query/XML, fan-out to SQS + HTTP), STS (Query/XML).
import { AwsError, ACCOUNT, uuid, xmlDoc, md5 } from '../lib/common.mjs';

export function createSecrets(state) {
  state.secrets ||= { secrets: {} };
  const db = state.secrets;
  const find = (id) => {
    const s = db.secrets[id] || Object.values(db.secrets).find((x) => x.ARN === id);
    if (!s || (s.DeletedDate && !s.restorable)) throw new AwsError('ResourceNotFoundException', "Secrets Manager can't find the specified secret.");
    return s;
  };
  const pub = (s) => ({ ARN: s.ARN, Name: s.Name, Description: s.Description, CreatedDate: s.CreatedDate / 1000, LastChangedDate: s.LastChangedDate / 1000, Tags: s.Tags, ...(s.DeletedDate ? { DeletedDate: s.DeletedDate / 1000 } : {}) });
  const actions = {
    CreateSecret(p) {
      if (db.secrets[p.Name] && !db.secrets[p.Name].DeletedDate) throw new AwsError('ResourceExistsException', `The operation failed because the secret ${p.Name} already exists.`);
      const ver = p.ClientRequestToken || uuid();
      const s = { Name: p.Name, ARN: `arn:aws:secretsmanager:us-east-1:${ACCOUNT}:secret:${p.Name}-${uuid().slice(0, 6)}`, Description: p.Description, Tags: p.Tags || [], CreatedDate: Date.now(), LastChangedDate: Date.now(), versions: { [ver]: { SecretString: p.SecretString, SecretBinary: p.SecretBinary, stages: ['AWSCURRENT'], CreatedDate: Date.now() } } };
      db.secrets[p.Name] = s;
      return { ARN: s.ARN, Name: s.Name, VersionId: ver };
    },
    GetSecretValue(p) {
      const s = find(p.SecretId);
      if (s.DeletedDate) throw new AwsError('InvalidRequestException', 'You can’t perform this operation on the secret because it was marked for deletion.');
      const [vid, v] = Object.entries(s.versions).find(([id, x]) => (p.VersionId ? id === p.VersionId : x.stages.includes(p.VersionStage || 'AWSCURRENT'))) || [];
      if (!v) throw new AwsError('ResourceNotFoundException', "Secrets Manager can't find the specified secret value for staging label: AWSCURRENT");
      return { ARN: s.ARN, Name: s.Name, VersionId: vid, SecretString: v.SecretString, SecretBinary: v.SecretBinary, VersionStages: v.stages, CreatedDate: v.CreatedDate / 1000 };
    },
    PutSecretValue(p) {
      const s = find(p.SecretId);
      const ver = p.ClientRequestToken || uuid();
      for (const v of Object.values(s.versions)) {
        v.stages = v.stages.filter((x) => x !== 'AWSPREVIOUS');
        if (v.stages.includes('AWSCURRENT')) v.stages = v.stages.filter((x) => x !== 'AWSCURRENT').concat('AWSPREVIOUS');
      }
      s.versions[ver] = { SecretString: p.SecretString, SecretBinary: p.SecretBinary, stages: ['AWSCURRENT'], CreatedDate: Date.now() };
      s.LastChangedDate = Date.now();
      return { ARN: s.ARN, Name: s.Name, VersionId: ver, VersionStages: ['AWSCURRENT'] };
    },
    UpdateSecret(p) {
      const s = find(p.SecretId);
      if (p.Description != null) s.Description = p.Description;
      if (p.SecretString != null || p.SecretBinary != null) return actions.PutSecretValue(p);
      return { ARN: s.ARN, Name: s.Name };
    },
    DescribeSecret: (p) => pub(find(p.SecretId)),
    ListSecrets: () => ({ SecretList: Object.values(db.secrets).filter((s) => !s.DeletedDate).map(pub) }),
    DeleteSecret(p) {
      const s = find(p.SecretId);
      if (p.ForceDeleteWithoutRecovery) delete db.secrets[s.Name];
      else s.DeletedDate = Date.now();
      return { ARN: s.ARN, Name: s.Name, DeletionDate: Date.now() / 1000 };
    },
    RestoreSecret(p) {
      const s = db.secrets[p.SecretId];
      if (!s) throw new AwsError('ResourceNotFoundException', "Secrets Manager can't find the specified secret.");
      delete s.DeletedDate;
      return { ARN: s.ARN, Name: s.Name };
    },
    GetRandomPassword: (p) => {
      const n = p.PasswordLength || 32;
      const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789' + (p.ExcludePunctuation ? '' : '!#$%&*+-=?@^_');
      const bytes = crypto.getRandomValues(new Uint8Array(n));
      return { RandomPassword: [...bytes].map((b) => chars[b % chars.length]).join('') };
    },
  };
  return {
    name: 'secretsmanager',
    actions,
    async handle({ req, body }) {
      const action = (req.headers['x-amz-target'] || '').split('.')[1];
      if (!actions[action]) throw new AwsError('InvalidRequestException', `Unsupported Secrets Manager action ${action}`);
      return { json: actions[action](body.length ? JSON.parse(body.toString()) : {}), contentType: 'application/x-amz-json-1.1' };
    },
    jsonError: true,
  };
}

const SNS_NS = 'http://sns.amazonaws.com/doc/2010-03-31/';
export function createSns(state, { sqs }) {
  state.sns ||= { topics: {} };
  const db = state.sns;
  const topic = (arn) => db.topics[arn] || (() => { throw new AwsError('NotFound', 'Topic does not exist', 404); })();
  const r = (action, result) => ({ xml: xmlDoc(`${action}Response`, { [`${action}Result`]: result, ResponseMetadata: { RequestId: uuid() } }, SNS_NS) });
  const actions = {
    CreateTopic(p) {
      const arn = `arn:aws:sns:us-east-1:${ACCOUNT}:${p.Name}`;
      db.topics[arn] ||= { arn, name: p.Name, subs: [], created: Date.now() };
      return r('CreateTopic', { TopicArn: arn });
    },
    ListTopics: () => r('ListTopics', { Topics: { member: Object.keys(db.topics).map((TopicArn) => ({ TopicArn })) } }),
    DeleteTopic(p) {
      delete db.topics[p.TopicArn];
      return r('DeleteTopic', {});
    },
    Subscribe(p) {
      const t = topic(p.TopicArn);
      const sub = { arn: `${t.arn}:${uuid()}`, protocol: p.Protocol, endpoint: p.Endpoint, raw: false };
      t.subs.push(sub);
      return r('Subscribe', { SubscriptionArn: sub.arn });
    },
    Unsubscribe(p) {
      for (const t of Object.values(db.topics)) t.subs = t.subs.filter((s) => s.arn !== p.SubscriptionArn);
      return r('Unsubscribe', {});
    },
    ListSubscriptionsByTopic: (p) => r('ListSubscriptionsByTopic', { Subscriptions: { member: topic(p.TopicArn).subs.map((s) => ({ SubscriptionArn: s.arn, Protocol: s.protocol, Endpoint: s.endpoint, TopicArn: p.TopicArn })) } }),
    SetSubscriptionAttributes(p) {
      for (const t of Object.values(db.topics)) for (const s of t.subs) if (s.arn === p.SubscriptionArn && p.AttributeName === 'RawMessageDelivery') s.raw = p.AttributeValue === 'true';
      return r('SetSubscriptionAttributes', {});
    },
    async Publish(p) {
      const t = topic(p.TopicArn || p.TargetArn);
      const id = uuid();
      const envelope = JSON.stringify({ Type: 'Notification', MessageId: id, TopicArn: t.arn, Subject: p.Subject, Message: p.Message, Timestamp: new Date().toISOString() });
      for (const s of t.subs) {
        if (s.protocol === 'sqs') {
          const name = s.endpoint.split(':').pop();
          const q = state.sqs?.queues?.[name];
          if (q) await sqs.actions.SendMessage({ QueueUrl: name, MessageBody: s.raw ? p.Message : envelope, MessageGroupId: p.MessageGroupId, MessageDeduplicationId: p.MessageDeduplicationId || (q.name.endsWith('.fifo') ? md5(envelope) : undefined) });
        } else if (s.protocol === 'http' || s.protocol === 'https') {
          fetch(s.endpoint, { method: 'POST', headers: { 'Content-Type': 'text/plain', 'x-amz-sns-message-type': 'Notification' }, body: envelope }).catch(() => {});
        }
      }
      return r('Publish', { MessageId: id });
    },
  };
  return {
    name: 'sns',
    async handle({ form }) {
      const p = Object.fromEntries(form);
      if (!actions[p.Action]) throw new AwsError('InvalidAction', `Unsupported SNS action ${p.Action}`);
      return actions[p.Action](p);
    },
  };
}

export function createSts() {
  return {
    name: 'sts',
    async handle({ form, req }) {
      const action = form?.get('Action') || (req.headers['x-amz-target'] || '').split('.')[1];
      if (action === 'GetCallerIdentity') return { xml: xmlDoc('GetCallerIdentityResponse', { GetCallerIdentityResult: { Arn: `arn:aws:iam::${ACCOUNT}:root`, UserId: ACCOUNT, Account: ACCOUNT }, ResponseMetadata: { RequestId: uuid() } }, 'https://sts.amazonaws.com/doc/2011-06-15/') };
      if (action === 'AssumeRole' || action === 'GetSessionToken') {
        const exp = new Date(Date.now() + 3600e3).toISOString();
        return { xml: xmlDoc(`${action}Response`, { [`${action}Result`]: { Credentials: { AccessKeyId: 'ASIANIMBUSLOCAL', SecretAccessKey: 'nimbus', SessionToken: 'nimbus', Expiration: exp }, ...(action === 'AssumeRole' ? { AssumedRoleUser: { Arn: form.get('RoleArn'), AssumedRoleId: 'NIMBUS:' + form.get('RoleSessionName') } } : {}) }, ResponseMetadata: { RequestId: uuid() } }, 'https://sts.amazonaws.com/doc/2011-06-15/') };
      }
      throw new AwsError('InvalidAction', `Unsupported STS action ${action}`);
    },
  };
}
