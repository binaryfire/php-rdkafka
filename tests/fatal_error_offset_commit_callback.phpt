--TEST--
A fatal error in the offset commit callback is raised once KafkaConsumer::consume() returns
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
$conf->set('group.id', sprintf('test_rdkafka_group_%s', uniqid()));
$conf->set('enable.auto.commit', 'false');
$conf->setOffsetCommitCb(function (RdKafka\KafkaConsumer $consumer, int $err, array $partitions) {
    echo "Offsets committed\n";
    exceedTimeLimit();
});

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->assign([new RdKafka\TopicPartition($topicName, 0, RD_KAFKA_OFFSET_BEGINNING)]);

$deadline = microtime(true) + 30;
do {
    if (microtime(true) > $deadline) {
        throw new RuntimeException('Timed out waiting for the message');
    }

    $message = $consumer->consume(100);
} while ($message === null);

$consumer->commitAsync($message);

while (microtime(true) < $deadline) {
    $consumer->consume(100);
}

echo "not reached\n";

?>
--EXPECTF--
Offsets committed

Fatal error: Maximum execution time of 1 second exceeded in %s on line %d
