"""End-to-end test against a running Nimbus (node nimbus.mjs). Run: python3 tests/integration_boto3.py"""
import os, json, time, boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

EP = os.environ.get("NIMBUS_URL", "http://localhost:4566")
kw = dict(endpoint_url=EP, region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
ok = lambda name: print(f"  ✓ {name}")

# ---------------- DynamoDB (resource API exercises the expression engine heavily)
ddb = boto3.resource("dynamodb", **kw)
client = boto3.client("dynamodb", **kw)
try: client.delete_table(TableName="orders")
except ClientError: pass
t = ddb.create_table(TableName="orders", BillingMode="PAY_PER_REQUEST",
    KeySchema=[{"AttributeName": "customer", "KeyType": "HASH"}, {"AttributeName": "order_id", "KeyType": "RANGE"}],
    AttributeDefinitions=[{"AttributeName": "customer", "AttributeType": "S"}, {"AttributeName": "order_id", "AttributeType": "N"}, {"AttributeName": "status", "AttributeType": "S"}],
    GlobalSecondaryIndexes=[{"IndexName": "by_status", "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}, {"AttributeName": "order_id", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}}])
t.wait_until_exists(); ok("create_table + waiter")
with t.batch_writer() as bw:
    for i in range(1, 31):
        bw.put_item(Item={"customer": "alice" if i % 2 else "bob", "order_id": i, "status": "shipped" if i % 3 == 0 else "open", "total": i * 10, "tags": {"rush"} if i % 5 == 0 else {"std"}, "meta": {"src": "web", "n": i}})
ok("batch_writer 30 items")
r = t.query(KeyConditionExpression=Key("customer").eq("alice") & Key("order_id").between(5, 15))
assert [i["order_id"] for i in r["Items"]] == [5, 7, 9, 11, 13, 15], r["Items"]; ok("query + between")
r = t.query(KeyConditionExpression=Key("customer").eq("bob"), ScanIndexForward=False, Limit=3)
assert [i["order_id"] for i in r["Items"]] == [30, 28, 26] and "LastEvaluatedKey" in r; ok("query desc + limit + LastEvaluatedKey")
r2 = t.query(KeyConditionExpression=Key("customer").eq("bob"), ScanIndexForward=False, Limit=3, ExclusiveStartKey=r["LastEvaluatedKey"])
assert [i["order_id"] for i in r2["Items"]] == [24, 22, 20]; ok("pagination")
r = t.query(IndexName="by_status", KeyConditionExpression=Key("status").eq("shipped"), FilterExpression=Attr("total").gt(100))
assert [int(i["order_id"]) for i in r["Items"]] == [12, 15, 18, 21, 24, 27, 30]; ok("GSI query + filter")
r = t.scan(FilterExpression=Attr("tags").contains("rush") & Attr("meta.src").eq("web"), ProjectionExpression="order_id, #m.n", ExpressionAttributeNames={"#m": "meta"})
assert sorted(int(i["order_id"]) for i in r["Items"]) == [5, 10, 15, 20, 25, 30] and "total" not in r["Items"][0] and r["Items"][0]["meta"]["n"]; ok("scan + contains + nested path + projection")
assert t.scan(Select="COUNT")["Count"] == 30; ok("scan COUNT")
r = t.update_item(Key={"customer": "alice", "order_id": 1}, UpdateExpression="SET total = total + :inc, notes = list_append(if_not_exists(notes, :empty), :n) ADD tags :t REMOVE meta",
    ExpressionAttributeValues={":inc": 5, ":empty": [], ":n": ["gift wrap"], ":t": {"vip"}}, ReturnValues="ALL_NEW")
a = r["Attributes"]; assert a["total"] == 15 and a["notes"] == ["gift wrap"] and a["tags"] == {"std", "vip"} and "meta" not in a; ok("update SET/ADD/REMOVE/list_append/if_not_exists")
try:
    t.put_item(Item={"customer": "alice", "order_id": 1}, ConditionExpression="attribute_not_exists(customer)"); raise SystemExit("condition should fail")
except ClientError as e: assert e.response["Error"]["Code"] == "ConditionalCheckFailedException"; ok("ConditionalCheckFailedException")
try:
    client.transact_write_items(TransactItems=[
        {"Put": {"TableName": "orders", "Item": {"customer": {"S": "carol"}, "order_id": {"N": "99"}}}},
        {"ConditionCheck": {"TableName": "orders", "Key": {"customer": {"S": "alice"}, "order_id": {"N": "1"}}, "ConditionExpression": "total > :x", "ExpressionAttributeValues": {":x": {"N": "1000"}}}}])
    raise SystemExit("transaction should fail")
except ClientError as e: assert e.response["Error"]["Code"] == "TransactionCanceledException"
assert "Item" not in t.get_item(Key={"customer": "carol", "order_id": 99}); ok("transaction rollback")
got = client.batch_get_item(RequestItems={"orders": {"Keys": [{"customer": {"S": "bob"}, "order_id": {"N": "2"}}, {"customer": {"S": "alice"}, "order_id": {"N": "3"}}]}})
assert len(got["Responses"]["orders"]) == 2; ok("batch_get_item")
t.delete_item(Key={"customer": "bob", "order_id": 2}); assert "Item" not in t.get_item(Key={"customer": "bob", "order_id": 2}); ok("delete_item")
assert "orders" in client.list_tables()["TableNames"]; ok("list_tables")

# ---------------- S3 via boto3 (presigned URL, copy, metadata, range)
s3 = boto3.client("s3", **kw)
s3.create_bucket(Bucket="assets")
s3.put_object(Bucket="assets", Key="a/b.json", Body=b'{"x":1}', ContentType="application/json", Metadata={"owner": "mridul"})
h = s3.head_object(Bucket="assets", Key="a/b.json"); assert h["Metadata"]["owner"] == "mridul" and h["ContentType"] == "application/json"; ok("put/head + metadata")
s3.copy_object(Bucket="assets", Key="copy.json", CopySource={"Bucket": "assets", "Key": "a/b.json"})
assert s3.get_object(Bucket="assets", Key="copy.json", Range="bytes=1-3")["Body"].read() == b'"x"'; ok("copy + range")
import urllib.request
url = s3.generate_presigned_url("get_object", Params={"Bucket": "assets", "Key": "a/b.json"}, ExpiresIn=60)
os.environ["no_proxy"] = "localhost,127.0.0.1"
assert urllib.request.urlopen(url).read() == b'{"x":1}'; ok("presigned URL")
lst = s3.list_objects_v2(Bucket="assets", Delimiter="/"); assert [p["Prefix"] for p in lst["CommonPrefixes"]] == ["a/"]; ok("list_objects_v2 delimiter")
s3.delete_objects(Bucket="assets", Delete={"Objects": [{"Key": "a/b.json"}, {"Key": "copy.json"}]}); assert s3.list_objects_v2(Bucket="assets")["KeyCount"] == 0; ok("delete_objects")
try: s3.get_object(Bucket="assets", Key="missing")
except ClientError as e: assert e.response["Error"]["Code"] == "NoSuchKey"; ok("NoSuchKey error")

# ---------------- SQS via boto3 (JSON protocol) incl. long polling
sqs = boto3.client("sqs", **kw)
q = sqs.create_queue(QueueName="py-jobs")["QueueUrl"]
start = time.time(); r = sqs.receive_message(QueueUrl=q, WaitTimeSeconds=1); assert "Messages" not in r and time.time() - start >= 0.9; ok("long poll waits")
sqs.send_message(QueueUrl=q, MessageBody="hi", DelaySeconds=1)
assert "Messages" not in sqs.receive_message(QueueUrl=q); time.sleep(1.1)
m = sqs.receive_message(QueueUrl=q)["Messages"][0]; sqs.delete_message(QueueUrl=q, ReceiptHandle=m["ReceiptHandle"]); ok("delay + receive + delete (MD5 validated by botocore)")

sm = boto3.client("secretsmanager", **kw)
sm.create_secret(Name="api/key", SecretString=json.dumps({"k": "v"})); assert json.loads(sm.get_secret_value(SecretId="api/key")["SecretString"])["k"] == "v"; ok("secrets JSON")
print("ALL GOOD")
