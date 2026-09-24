# Upgrading from 6.x to 7.x

## Summary of changes

**Minimum requirements raised.** PHP 7.x is no longer supported; PHP 8.1 or later is required. librdkafka 1.6.0 or later is required (previously 1.0.0).

**Compile-time feature flags removed.** Several methods were previously compiled in only when the installed librdkafka was new enough to support them (guarded by `#ifdef HAS_RD_KAFKA_OAUTHBEARER`, `HAS_RD_KAFKA_TRANSACTIONS`, `HAS_RD_KAFKA_PURGE`, `HAS_RD_KAFKA_CONTROLLERID`, `HAVE_RD_KAFKA_MESSAGE_HEADERS`, `HAS_RD_KAFKA_INCREMENTAL_ASSIGN`). Because the minimum librdkafka is now 1.6.0, which provides all of these features, the guards have been removed and the methods are always available.

**New methods on `KafkaConsumer`.** The high-level consumer gained `poll()`, `oauthbearerSetToken()`, `oauthbearerSetTokenFailure()`, `getRebalanceProtocol()`, and `getConsumerGroupMetadata()`, and now supports SASL/SSL OAUTHBEARER authentication end-to-end.

**Internal fixes.** A missing `zend_restore_error_handling()` call in the KafkaConsumer error path was corrected. Several internal type mismatches were fixed.

**Callback and topic lifetimes corrected.** Callback zvals and the parent references held by topic and queue wrappers were invisible to PHP's cycle collector, so cycles through them kept clients alive until request shutdown. These cycles are now collected, which means an unreachable producer or consumer can be destroyed when cycle collection runs. `KafkaConsumerTopic` now keeps its `KafkaConsumer` alive and is invalidated when that consumer closes, preventing the native topic handle from outliving its client.

**`KafkaConsumer::close()` rejected inside callbacks.** The method now throws `RdKafka\Exception` when called from one of the consumer's callbacks. Close the consumer after the method that invoked the callback returns.

**Asynchronous consumer close.** `KafkaConsumer::closeAsync()` starts closing the consumer and `isClosed()` reports when it has finished, so an application can keep calling `consume()` while the group hands its partitions over. Both require librdkafka 1.9.0 or later.

**Queue access for event loops.** `RdKafka::getMainQueue()`, `KafkaConsumer::getConsumerQueue()` and `KafkaConsumer::splitPartitionQueue()` return the client's queues. `Queue::getLength()` reports how many events are waiting, and `Queue::ioEventEnable()` writes to a stream when a queue has work, so `stream_select()` or an event loop can wait for Kafka work instead of polling on a timer.

**Callback exceptions stop consumption and polling.** When a callback throws, the method that called it now returns before handing out another message or calling another callback. Messages and delivery reports that were not handed out stay queued for the next call. A consumer's error callback is the exception and keeps the previous behavior.

**Close and destruction fixes.** `KafkaConsumer::close()` now reports close errors instead of ignoring them. Destroying a consumer that uses the cooperative protocol no longer hangs or fails with librdkafka 1.6 to 1.8, and closing a consumer no longer hangs when its rebalance callback throws or returns without acknowledging the revocation. Constructing a client again after a successful construction now throws instead of leaking, and a failed construction no longer leaks its configuration or callbacks and can be retried.

**PHP 7 compatibility shims removed.** Internal compatibility code for PHP 7 has been cleaned up; this has no effect on behaviour for PHP 8 users.

---

## User-impacting changes

### `KafkaConsumer::consume()` returns `null` on timeout

Previously, `KafkaConsumer::consume()` returned a `Message` object with `err` set to `RD_KAFKA_RESP_ERR__TIMED_OUT` when no message arrived within the timeout window. It now returns `null` in that case, matching librdkafka's own behavior.

**Before:**
```php
$msg = $consumer->consume(1000);
if ($msg->err === RD_KAFKA_RESP_ERR__TIMED_OUT) {
    // no message
}
```

**After:**
```php
$msg = $consumer->consume(1000);
if ($msg === null) {
    // no message
}
```

`RD_KAFKA_RESP_ERR__TIMED_OUT` on a returned `Message` now means an actual timeout error originating from librdkafka, not a poll window expiry.

### `KafkaConsumer::close()` cannot be called from a callback

Calling `KafkaConsumer::close()` from a consumer callback destroyed the native client while librdkafka was still using it. It now throws `RdKafka\Exception` with code `RD_KAFKA_RESP_ERR__STATE`. A callback can record that shutdown was requested, then the application can close the consumer after the method that invoked the callback returns.

### `KafkaConsumer::close()` reports close errors

`close()` used to ignore the error returned by librdkafka. It now releases the consumer and then throws `RdKafka\Exception` with the native error code, for example after the consumer hit a fatal error. Destroying a consumer without calling `close()` reports the same error as a warning. Once `close()` starts closing the consumer, the consumer is closed and released when `close()` returns or throws. `close()` throws without closing anything when it is called from one of the consumer's callbacks, or while another `close()` call is still running, for example from an error handler.

If a rebalance callback throws, or returns without calling `assign()`, `incrementalAssign()` or `incrementalUnassign()` while `close()` is revoking partitions, the consumer now releases the partitions itself so that closing can finish. The callback's exception is thrown once the consumer is closed.

### Callback exceptions stop consumption and polling

Previously, when a callback threw, librdkafka kept dispatching the queue. Later callbacks were skipped because an exception was pending, so delivery reports and messages passed to `ConsumerTopic::consumeCallback()` were lost, and `consume()` could hand out a message that PHP then discarded while its position still advanced.

Now the call that invoked the callback returns first. Remaining delivery reports, messages and callbacks stay queued for the next `poll()`, `consume()` or `consumeCallback()` call, and the callback's exception is not replaced by an interruption error. The error callback of a consumer still keeps dispatching, because librdkafka's batch consumption does not yet stop promptly after a callback asks it to.

If a callback throws during `ConsumerTopic::consumeBatch()`, the exception replaces the returned array. Messages the batch had already collected are not returned, their positions have already advanced, and nothing rewinds them. Use `consume()` when every message must be handed to the application before a callback exception propagates.

### Clients cannot be constructed twice

Calling `__construct()` again on an `RdKafka\Producer`, `RdKafka\Consumer` or `RdKafka\KafkaConsumer` that was constructed successfully used to replace the native client and leak the old one. On a `KafkaConsumer` after `close()`, it leaked the callbacks copied by the first construction. It now throws `RdKafka\Exception` with code `RD_KAFKA_RESP_ERR__STATE`. Create a new object instead.

A construction that fails, for example because of an invalid configuration, releases what it copied, so it can be retried with a corrected configuration.

### Invalidated topics report that their client was closed

Methods called on a topic whose client was closed, or that was never constructed, now throw an exception with the message `RdKafka\Topic is not initialized or its client has been closed`. Queues use the same wording. Previously the message said that `__construct()` had not been called.

### Conf::dump() does not include topic-level properties

`Conf::set()` accepts both global and topic-level properties, silently routing topic-level properties to an embedded `default_topic_conf`. However, `Conf::dump()` only returns global configuration properties.

**If you were using Conf::dump() for debugging or testing** and expecting to see topic-level properties (like `auto.offset.reset`, `compression.codec`, `auto.commit.enable`, etc.), you must now explicitly access them via `getDefaultTopicConf()`.

```php
// Before (incorrect assumption):
$conf = new RdKafka\Conf;
$conf->set('group.id', 'my-group');           // global property
$conf->set('auto.offset.reset', 'earliest');  // topic-level property

$dump = $conf->dump();
// Incorrectly assumed 'auto.offset.reset' would appear here

// After (correct usage):
$conf = new RdKafka\Conf;
$conf->set('group.id', 'my-group');           // global property
$conf->set('auto.offset.reset', 'earliest');  // topic-level property

// Get global properties:
$globalProps = $conf->dump();
echo $globalProps['group.id'];  // 'my-group'

// Get topic-level properties:
$topicConf = $conf->getDefaultTopicConf();

if ($topicConf !== null) {
    $topicProps = $topicConf->dump();
    echo $topicProps['auto.offset.reset'];  // 'earliest'

    // Or get a single topic-level property:
    $value = $topicConf->get('auto.offset.reset');  // 'earliest'
}

// Alternatively, use Conf::get() for global properties and Conf::getDefaultTopicConf()->get() for topic-level properties:
$groupId = $conf->get('group.id');                                          // global
$autoOffsetReset = $conf->getDefaultTopicConf()->get('auto.offset.reset');  // topic-level
```

Note: This behavior existed in php-rdkafka 6.x but was not well-documented. Tests or debugging code that relied on dump() containing topic-level properties will need to be updated.
Refer to librdkafka CONFIGURATION.md (https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) to determine which properties are topic-level (marked with * in the C/P column) vs global.

### PHP 8.1 now required

php-rdkafka 7.x requires PHP 8.1 or later. PHP 7.x is no longer supported.

### librdkafka 1.6.0 now required

librdkafka 1.6.0 is the new minimum. Versions older than 1.6.0 are not supported.

### Previously conditional methods are now always available

The following methods were only compiled in when the build-time librdkafka was sufficiently new. They are now unconditionally available (librdkafka 1.6.0 supports all of them):

| Class | Method |
|-------|--------|
| `RdKafka\Conf` | `setOauthbearerTokenRefreshCb()` |
| `RdKafka\Producer` | `purge()` |
| `RdKafka\Producer` | `initTransactions()`, `beginTransaction()`, `commitTransaction()`, `abortTransaction()` |
| `RdKafka\Producer` | `oauthbearerSetToken()`, `oauthbearerSetTokenFailure()` |
| `RdKafka\Producer` | `getControllerId()` |
| `RdKafka\KafkaConsumer` | `getControllerId()` |
| `RdKafka\KafkaConsumer` | `incrementalAssign()`, `incrementalUnassign()` |
| `RdKafka\ProducerTopic` | `producev()` |

If your code checked `method_exists()` before calling any of these, those guards can be removed.

### New methods on `KafkaConsumer`

`RdKafka\KafkaConsumer` gained five new methods:

```php
KafkaConsumer::poll(int $timeout_ms): int
KafkaConsumer::oauthbearerSetToken(string $token_value, int $lifetime_ms, string $principal_name, array $extensions = []): void
KafkaConsumer::oauthbearerSetTokenFailure(string $error): void
KafkaConsumer::getRebalanceProtocol(): string
KafkaConsumer::getConsumerGroupMetadata(): ConsumerGroupMetadata
```

`poll()` allows the high-level consumer to service callbacks (including the OAUTHBEARER token refresh callback) without consuming a message. This is the same method that exists on the low-level `RdKafka\Consumer`.

### New `ConsumerGroupMetadata` API

`RdKafka\ConsumerGroupMetadata` can be constructed with full group metadata on every supported librdkafka version. Its getter methods require librdkafka 2.8.0 and throw `RdKafka\Exception` on older versions.

### Asynchronous consumer close

With librdkafka 1.9.0 or later, `KafkaConsumer::closeAsync()` starts leaving the consumer group without blocking. Keep calling `consume()` so rebalance and commit callbacks run, then call `close()` once `isClosed()` returns `true`:

```php
$consumer->closeAsync();

while (!$consumer->isClosed()) {
    $message = $consumer->consume(50);
    // Handle any message or error that is still returned.
}

$consumer->close();
```

`close()` still releases the consumer and can still block while librdkafka shuts down. Calling it before the asynchronous close has finished waits for that close to complete.

Until `close()` is called, the consumer's methods stay callable. Once the group has finished closing, calls that need the group fail with librdkafka's errors, and `getRebalanceProtocol()` throws `RdKafka\Exception`.

### Client queues and notifications

`RdKafka::getMainQueue()` returns the queue that a producer's or legacy consumer's `poll()` serves, and `KafkaConsumer::getConsumerQueue()` returns the queue that `KafkaConsumer::consume()` serves. Each call returns the same `Queue` object while it is in use. A queue keeps its client alive. The client's final `close()` or destruction invalidates the queue; an asynchronous close finishing does not.

`Queue::getLength()` returns the number of events waiting in a queue, including delivery reports, callback events and messages. It does not serve them, and it is not a count of messages or of outstanding deliveries.

On Linux and macOS, `Queue::ioEventEnable($stream)` makes librdkafka write to a non-blocking socket or pipe when the queue has new work, so an application can wait with `stream_select()` or an event loop. `ioEventEnable(null)` disables notifications.

Notifications are hints. Several events can produce one notification, and work that was queued before notifications were enabled produces none. Serve the queue once after enabling notifications, and again whenever a pass leaves work behind. A consumer still has to call `KafkaConsumer::consume()` within `max.poll.interval.ms`.

The queue keeps its own duplicate of the stream's descriptor until notifications are disabled or the queue is released, so closing the PHP stream does not redirect notifications to another resource. The duplicate shares the stream's blocking mode, so keep the stream non-blocking while it is registered. Encrypted, filtered and user-space streams are rejected.

librdkafka writes from its own threads and ignores write errors. If the reading end is closed, a write can raise `SIGPIPE`, which the PHP CLI ignores but other runtimes might not. The optional payload is written with a single `write()` that can be cut short when the stream's buffer is nearly full, so keep it short; the default is one byte.

```php
[$read, $write] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
stream_set_blocking($read, false);
stream_set_blocking($write, false);

$queue = $producer->getMainQueue();
$queue->ioEventEnable($write);

try {
    $producer->poll(0); // Also serve work that was queued before notifications were enabled.

    while ($producer->getOutQLen() > 0) {
        $readable = [$read];
        $writable = $except = [];

        if (stream_select($readable, $writable, $except, 1) === false) {
            throw new RuntimeException('Unable to wait for Kafka notifications.');
        }

        while (($notification = fread($read, 8192)) !== '') {
            if ($notification === false) {
                throw new RuntimeException('Unable to read Kafka notifications.');
            }
        }

        $producer->poll(0);
    }
} finally {
    $queue->ioEventEnable(null);
    fclose($write);
    fclose($read);
}
```

### Partition queues

`KafkaConsumer::splitPartitionQueue($topic, $partition)` stops forwarding a partition's messages to the consumer queue and returns the partition's own queue, which `Queue::consume()` reads. Split partitions inside the rebalance callback before acknowledging the assignment, and release their queues after the partitions are revoked. Messages that already reached the consumer queue stay there, so consume those first to keep the partition's order. Keep calling `KafkaConsumer::consume()` to serve rebalance, commit and other callbacks and the partitions that are not split.

The split belongs to librdkafka's state for the partition, not to the `Queue` object. Releasing the queue does not send the partition's messages back to the consumer queue, and splitting it again returns the messages that were waiting. librdkafka keeps the partition's state while the partition is assigned. It discards it only after the topic has been deleted for longer than `topic.metadata.propagation.max.ms` while the partition is no longer assigned; a `Queue` that is still held then refers to the discarded state until it is released, and a recreated topic's partition is forwarded to the consumer queue again until it is split.

Each split partition has its own `queued.max.messages.kbytes` limit, so splitting many partitions allows more prefetched data in memory. See librdkafka's [CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) for this setting and `fetch.queue.backoff.ms`. To be notified about a split partition's messages, enable notifications on its own queue; notifications on the consumer queue only cover the messages and events that reach the consumer queue.

`ConsumerTopic::consumeQueueStart()` only accepts queues created with `Consumer::newQueue()`.

### `RdKafka::setLogger()` and `rd_kafka_errno2err()` are deprecated

Both `RdKafka::setLogger()` and `rd_kafka_errno2err()` are deprecated in librdkafka and will be removed in a future version. Calling them now emits an `E_DEPRECATED` notice.

- Replace `$producer->setLogger(RD_KAFKA_LOG_PRINT)` (and similar) with `$conf->setLogCb(callable $callback)` set before constructing the producer or consumer.
- Replace `rd_kafka_errno2err($errno)` with `rd_kafka_last_error()`, which returns the last error code set by librdkafka directly without requiring an errno argument.

### SASL/SSL OAUTHBEARER support for `KafkaConsumer`

`RdKafka\KafkaConsumer` now fully supports OAUTHBEARER authentication, including over SASL_SSL. Set `setOauthbearerTokenRefreshCb()` on the `Conf` and use the new `oauthbearerSetToken()` / `oauthbearerSetTokenFailure()` methods on the consumer instance to provide tokens.
