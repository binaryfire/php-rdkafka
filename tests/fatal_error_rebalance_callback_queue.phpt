--TEST--
A fatal error in a rebalance callback is raised once Queue::consume() returns
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
$conf->setRebalanceCb(function (RdKafka\KafkaConsumer $consumer, int $err, array $partitions) {
    echo "Rebalancing\n";
    exceedTimeLimit();
});

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe([$topicName]);
$queue = $consumer->getConsumerQueue();

$deadline = microtime(true) + 30;
while (microtime(true) < $deadline) {
    $queue->consume(100);
}

echo "not reached\n";

?>
--EXPECTF--
Rebalancing

Fatal error: Maximum execution time of 1 second exceeded in %s on line %d
