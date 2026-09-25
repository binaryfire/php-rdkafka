--TEST--
A fatal error in a consumer's error callback during ConsumerTopic::consumeBatch() does not hang the process when no message follows the error
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
require __DIR__ . '/helpers/fatal-error.php';

$topicName = sprintf('test_rdkafka_%s', uniqid());
produceMessages($topicName, 1);

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->setErrorCb(function (RdKafka\Consumer $consumer, int $err, string $reason) {
    echo rd_kafka_err2name($err), "\n";
    exceedTimeLimit();
});

$consumer = new RdKafka\Consumer($conf);
$topic = $consumer->newTopic($topicName);
$queue = $consumer->newQueue();

// The topic has no partition 5, so librdkafka queues an error. Partition 0 is
// consumed from its end, so no message follows.
$topic->consumeQueueStart(5, RD_KAFKA_OFFSET_BEGINNING, $queue);
$deadline = microtime(true) + 10;
while ($queue->getLength() === 0) {
    if (microtime(true) > $deadline) {
        throw new RuntimeException('Timed out waiting for the error');
    }

    usleep(10000);
}
$topic->consumeQueueStart(0, RD_KAFKA_OFFSET_END, $queue);

// The error callback does not stop the batch, so it returns at its timeout
$topic->consumeBatch(0, 1000, 10);
echo "not reached\n";

?>
--EXPECTF--
_UNKNOWN_PARTITION

Fatal error: Maximum execution time of 1 second exceeded in %s on line %d
