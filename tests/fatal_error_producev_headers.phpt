--TEST--
A fatal error while ProducerTopic::producev() converts a header does not leak the rd_kafka_headers_t
--FILE--
<?php
require __DIR__ . '/helpers/fatal-error.php';

$conf = new RdKafka\Conf();
$conf->set('log_level', '0');
$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic('test');

$value = new class {
    public function __toString(): string
    {
        exceedTimeLimit();
    }
};

$topic->producev(RD_KAFKA_PARTITION_UA, 0, 'payload', null, ['first' => 'value', 'second' => $value]);

echo "not reached\n";

?>
--EXPECTF--
Fatal error: Maximum execution time of 1 second exceeded in %s on line %d
