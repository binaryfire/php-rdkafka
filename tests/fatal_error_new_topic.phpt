--TEST--
A fatal error while KafkaConsumer::newTopic() registers the topic releases it before the consumer is closed
--SKIPIF--
<?php
require __DIR__ . '/helpers/memory-limit-check.php';
--FILE--
<?php
$conf = new RdKafka\Conf();
$conf->set('group.id', 'test');
$conf->set('log_level', '0');
$consumer = new RdKafka\KafkaConsumer($conf);

register_shutdown_function(function () use ($consumer) {
    ini_set('memory_limit', '-1');
    $consumer->close();
    echo "Closed the consumer\n";
});

// Fill the consumer's topic registry up to its next resize. Releasing one
// topic leaves room to create the next topic object, but not the registry.
$topics = [];
for ($i = 0; $i < 32768; $i++) {
    $topics[] = $consumer->newTopic('test');
}
unset($topics[0]);

ini_set('memory_limit', (string) (memory_get_usage(true) + 1024 * 1024));
$consumer->newTopic('test');

echo "not reached\n";

?>
--EXPECTF--
Fatal error: Allowed memory size of %d bytes exhausted%s(tried to allocate %d bytes) in %s on line %d
Closed the consumer
