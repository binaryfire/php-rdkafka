--TEST--
A fatal error in a delivery report callback is raised once Producer::commitTransaction() returns
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
$conf->set('transactional.id', sprintf('test_rdkafka_%s', uniqid()));
$conf->setDrMsgCb(function (RdKafka\Producer $producer, RdKafka\Message $message) {
    echo "Delivery report: {$message->payload}\n";
    exceedTimeLimit();
});

$producer = new RdKafka\Producer($conf);
$producer->initTransactions(10000);
$producer->beginTransaction();
$producer->newTopic($topicName)->produce(0, 0, 'message', null, 'opaque');

// Committing flushes the transaction's messages first
$producer->commitTransaction(10000);
echo "not reached\n";

?>
--EXPECTF--
Delivery report: message

Fatal error: Maximum execution time of 1 second exceeded in %s on line %d
