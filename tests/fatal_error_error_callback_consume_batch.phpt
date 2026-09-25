--TEST--
A fatal error in a consumer's error callback during ConsumerTopic::consumeBatch() does not hang the process when messages follow the error
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
require __DIR__ . '/helpers/fatal-error.php';

$topicName = sprintf('test_rdkafka_%s', uniqid());
produceMessages($topicName, 10);

function waitForQueueLength(RdKafka\Queue $queue, int $length): void
{
    $deadline = microtime(true) + 10;

    while ($queue->getLength() < $length) {
        if (microtime(true) > $deadline) {
            throw new RuntimeException('Timed out waiting for queued events');
        }

        usleep(10000);
    }
}

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->setErrorCb(function (RdKafka\Consumer $consumer, int $err, string $reason) {
    echo rd_kafka_err2name($err), "\n";
    exceedTimeLimit();
});

$consumer = new RdKafka\Consumer($conf);
$topic = $consumer->newTopic($topicName);
$queue = $consumer->newQueue();

// The topic has no partition 5, so librdkafka queues an error, followed by
// the messages of partition 0
$topic->consumeQueueStart(5, RD_KAFKA_OFFSET_BEGINNING, $queue);
waitForQueueLength($queue, 1);
$length = $queue->getLength();
$topic->consumeQueueStart(0, RD_KAFKA_OFFSET_BEGINNING, $queue);
waitForQueueLength($queue, $length + 10);

// The error callback does not stop the batch, so it returns once it is full
$topic->consumeBatch(0, 10000, 10);
echo "not reached\n";

?>
--EXPECTF--
_UNKNOWN_PARTITION

Fatal error: Maximum execution time of 1 second exceeded in %s on line %d
