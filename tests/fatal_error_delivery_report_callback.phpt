--TEST--
A fatal error in a nested delivery report callback is raised once librdkafka returns, and shutdown functions can still use the producers
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
require __DIR__ . '/helpers/fatal-error.php';

$topicName = sprintf('test_rdkafka_%s', uniqid());
$fail = true;

function createProducer(callable $deliveryReport): RdKafka\Producer
{
    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
    $conf->setDrMsgCb($deliveryReport);

    return new RdKafka\Producer($conf);
}

$inner = createProducer(function (RdKafka\Producer $producer, RdKafka\Message $message) use (&$fail) {
    echo "Inner delivery report: {$message->payload}\n";

    if ($fail) {
        exceedTimeLimit();
    }
});

// The outer producer's first report produces a second message and polls the
// same producer for its report. That one flushes the inner producer, whose
// report fails.
$outer = createProducer(function (RdKafka\Producer $producer, RdKafka\Message $message) use ($inner, $topicName) {
    echo "Outer delivery report: {$message->payload}\n";

    if ($message->payload === 'first') {
        $producer->newTopic($topicName)->produce(0, 0, 'second', null, 'opaque');
        $producer->poll(10000);
        echo "not reached\n";
    } elseif ($message->payload === 'second') {
        $inner->flush(10000);
        echo "not reached\n";
    }
});

register_shutdown_function(function () use ($inner, $outer, $topicName, &$fail) {
    echo "First shutdown function\n";
    $fail = false;

    foreach ([$inner, $outer] as $producer) {
        $producer->newTopic($topicName)->produce(0, 0, 'after', null, 'opaque');
        var_dump($producer->flush(10000));
    }
});

register_shutdown_function(function () use ($inner, $topicName, &$fail) {
    echo "Second shutdown function\n";
    $fail = true;

    $inner->newTopic($topicName)->produce(0, 0, 'again', null, 'opaque');
    $inner->flush(10000);
    echo "not reached\n";
});

register_shutdown_function(function () {
    echo "not reached\n";
});

$inner->newTopic($topicName)->produce(0, 0, 'inner', null, 'opaque');
$outer->newTopic($topicName)->produce(0, 0, 'first', null, 'opaque');

$outer->flush(10000);
echo "not reached\n";

?>
--EXPECTF--
Outer delivery report: first
Outer delivery report: second
Inner delivery report: inner

Fatal error: Maximum execution time of 1 second exceeded in %s on line %d
First shutdown function
Inner delivery report: after
int(0)
Outer delivery report: after
int(0)
Second shutdown function
Inner delivery report: again

Fatal error: Maximum execution time of 1 second exceeded in %s on line %d
