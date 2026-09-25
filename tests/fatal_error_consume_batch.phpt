--TEST--
A fatal error while ConsumerTopic::consumeBatch() creates a later message does not hang the process
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
require __DIR__ . '/helpers/memory-limit-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
require __DIR__ . '/helpers/fatal-error.php';

$topicName = sprintf('test_rdkafka_%s', uniqid());
produceLargeMessages($topicName);

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$consumer = new RdKafka\Consumer($conf);
$topic = $consumer->newTopic($topicName);
$topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);

// The first messages of the batch fit within the limit
callUntilMemoryLimit(fn () => $topic->consumeBatch(0, 10000, 100));

echo "not reached\n";

?>
--EXPECTF--
Fatal error: Allowed memory size of %d bytes exhausted%s(tried to allocate %d bytes) in %s on line %d
