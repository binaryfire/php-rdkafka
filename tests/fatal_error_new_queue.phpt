--TEST--
A fatal error while RdKafka\Consumer::newQueue() creates the queue object does not leak the rd_kafka_queue_t handle
--SKIPIF--
<?php
require __DIR__ . '/helpers/memory-limit-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/fatal-error.php';

$conf = new RdKafka\Conf();
$conf->set('log_level', '0');
$consumer = new RdKafka\Consumer($conf);

// The limit is reached while the object is created or while the client
// registers it
callUntilMemoryLimit(fn () => $consumer->newQueue(), 50000);

echo "not reached\n";

?>
--EXPECTF--
Fatal error: Allowed memory size of %d bytes exhausted%s(tried to allocate %d bytes) in %s on line %d
