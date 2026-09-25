--TEST--
A fatal error while creating the message for a ConsumerTopic::consumeCallback() callback does not hang the process
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

$messages = array_fill(0, 100, null);
$count = 0;
$keep = function (RdKafka\Message $message) use (&$messages, &$count) {
    $messages[$count++] = $message;
};

callUntilMemoryLimit(fn () => $topic->consumeCallback(0, 1000, $keep));

echo "not reached\n";

?>
--EXPECTF--
Fatal error: Allowed memory size of %d bytes exhausted%s(tried to allocate %d bytes) in %s on line %d
