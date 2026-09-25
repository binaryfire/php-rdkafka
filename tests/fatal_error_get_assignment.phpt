--TEST--
A fatal error while KafkaConsumer::getAssignment() converts the assignment does not hang the process
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
require __DIR__ . '/helpers/memory-limit-check.php';
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
$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->assign([new RdKafka\TopicPartition($topicName, 0, RD_KAFKA_OFFSET_BEGINNING)]);

// The assignment returned by librdkafka keeps its partitions in use
callUntilMemoryLimit(fn () => $consumer->getAssignment(), 20000);

echo "not reached\n";

?>
--EXPECTF--
Fatal error: Allowed memory size of %d bytes exhausted%s(tried to allocate %d bytes) in %s on line %d
