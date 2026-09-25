--TEST--
A fatal error in the rebalance callback that KafkaConsumer::close() runs does not hang the process
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
$conf->set('log_level', '0');
$conf->setRebalanceCb(function (RdKafka\KafkaConsumer $consumer, int $err, array $partitions) {
    if ($err === RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        echo "Revoking partitions\n";
        exceedTimeLimit();
    }

    $consumer->assign($partitions);
});

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe([$topicName]);

$deadline = microtime(true) + 30;
while ($consumer->getAssignment() === []) {
    if (microtime(true) > $deadline) {
        throw new RuntimeException('Timed out waiting for the assignment');
    }

    $consumer->consume(100);
}

$consumer->close();
echo "not reached\n";

?>
--EXPECTF--
Revoking partitions

Fatal error: Maximum execution time of 1 second exceeded in %s on line %d
