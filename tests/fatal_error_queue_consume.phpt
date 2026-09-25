--TEST--
A fatal error while Queue::consume() creates the message does not hang the process
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
$queue = $consumer->newQueue();
$topic = $consumer->newTopic($topicName);
$topic->consumeQueueStart(0, RD_KAFKA_OFFSET_BEGINNING, $queue);

callUntilMemoryLimit(fn () => $queue->consume(10000));

echo "not reached\n";

?>
--EXPECTF--
Fatal error: Allowed memory size of %d bytes exhausted%s(tried to allocate %d bytes) in %s on line %d
