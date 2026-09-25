--TEST--
A fatal error while KafkaConsumer::subscribe() converts a topic does not leak the topic partition list
--FILE--
<?php
require __DIR__ . '/helpers/fatal-error.php';

$conf = new RdKafka\Conf();
$conf->set('group.id', 'test');
$conf->set('log_level', '0');
$consumer = new RdKafka\KafkaConsumer($conf);

$topic = new class {
    public function __toString(): string
    {
        exceedTimeLimit();
    }
};

$consumer->subscribe(['first', $topic]);

echo "not reached\n";

?>
--EXPECTF--
Fatal error: Maximum execution time of 1 second exceeded in %s on line %d
