# Pulsar Admin config — namespaces, topics, quotas (examples):

Run these on a Pulsar admin host with pulsar-admin (adjust host / ports). Replace pulsar:// and http:// endpoints as appropriate.

# 1. Create a tenant (example)
pulsar-admin tenants create front-office \
  --allowed-clusters "standalone"

# 2. Create namespace
pulsar-admin namespaces create front-office/trades

# 3. Configure replication and retention
pulsar-admin namespaces set-retention --time 7d --size 10G front-office/trades

# 4. Set namespace policies: max producers/consumers / rates (examples)
pulsar-admin namespaces set-max-producers --max-producers 1000 front-office/trades
pulsar-admin namespaces set-retention --time 7d --size 50G front-office/trades

# 5. Set publish throttling quotas (example starting points)
pulsar-admin namespaces set-publish-rate --msg-rate 200000 --byte-rate 20000000 front-office/trades
# per-producer quotas (optional)
pulsar-admin namespaces set-publish-rate-policies --publishThrottlingTimeInSecond 1 \
  --publishThrottlingBucketInByte 20000000 front-office/trades

# Create partitioned topics for hot topics (trade ingestion):

# create partitioned topic with N partitions (tune N to your throughput)
pulsar-admin topics create-partitioned-topic persistent://front-office/trades/trades.incoming.normal -p 32

# manifest topic for bulk
pulsar-admin topics create-partitioned-topic persistent://front-office/trades/files.manifests -p 4

# DLQ topic
pulsar-admin topics create persistent://front-office/trades/ingest.dlq


# Recommended starting points (adjust per load):

Partition count for trades.incoming.normal: 32 — adjust by expected throughput (each partition ~ few thousand msgs/sec).
Publish rate (namespace-level): start at 200k msgs/sec or lower; tune upward.
Retention for trade topics: keep short (e.g. 7 days) if downstream commit to DB; keep manifests longer (30–90 days).
Topic compaction: enable compaction for state-topic-like topics if you store latest state keyed by tradeId.
Set namespace-level dispatch-rate and backlog quotas to protect Pulsar and downstream DBs:

pulsar-admin namespaces set-backlog-quotas --limit-bytes 10737418240 --limit-time 604800 front-office/trades

# Security TLS:

# generate token using pulsar-admin (example using a private key)
pulsar-admin tokens create --subject "nifi-producer" \
  --private-key-file /path/to/private.key > nifi-token.jwt

# In NiFi PulsarClientService set auth token or in NiFi, place token in Pulsar client config.

# Use the token on a consumer as follows:
client = pulsar.Client(
    service_url='pulsar+ssl://pulsar-broker:6651',
    authentication=pulsar.AuthenticationToken('YOUR_TOKEN_HERE'),
    tls_trust_certs_file_path='/etc/ssl/certs/ca.pem',  # if using TLS
    validate_hostname=True
)

# Typical Subscription Type and Ordering:

For trade processing you typically want ordering per key (tradeId) and high throughput:

1. Use Key_Shared subscription type (preserves key ordering and allows multiple consumers) — best for scaling consumers while maintaining ordering by key.
2. Use a consumer group (same subscription name) for all realtime Flink consumer instances.

consumer = client.subscribe(
    topic='persistent://front-office/trades/trades.incoming.normal',
    subscription_name='realtime-ingest-sub',
    subscription_type=pulsar.SubscriptionType.Key_Shared,
    consumer_name='flink-worker-1',
    receiver_queue_size=1000
)

Use Key_Shared with sticky hashing if you need ordering <tradeId> → partition mapping and consumer instance mapping matter. If NiFi flow sets a Message Key equal to tradeId, Pulsar will hash/route messages by key across partitions.
