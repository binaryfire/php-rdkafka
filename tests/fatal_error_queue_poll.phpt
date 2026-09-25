--TEST--
A fatal error while RdKafka\Queue::poll() creates the event object does not leak the rd_kafka_event_t
--SKIPIF--
<?php
require __DIR__ . '/helpers/memory-limit-check.php';
--FILE--
<?php
$conf = new RdKafka\Conf();
$conf->set('log_level', '0');
$producer = new RdKafka\Producer($conf);
$queue = $producer->newQueue();

// Without a broker the request times out, which still delivers a result
$options = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_DELETETOPICS);
$options->setRequestTimeout(100);
$producer->deleteTopics([new RdKafka\Admin\DeleteTopic('fatal-error')], $queue, $options);

// Wait without reading, so poll() creates the event's object
$deadline = microtime(true) + 10;
while ($queue->getLength() === 0) {
    if (microtime(true) > $deadline) {
        throw new RuntimeException('Timed out waiting for the admin result');
    }
    usleep(1000);
}

// Fill the object store, so the event's object grows it with an allocation
// that exceeds the memory limit
$objects = [];
do {
    $object = new stdClass();
    $objects[] = $object;
} while (spl_object_id($object) < 262143);
ini_set('memory_limit', (string) (memory_get_usage(true) + 1024 * 1024));

$queue->poll(0);

echo "not reached\n";

?>
--EXPECTF--
Fatal error: Allowed memory size of %d bytes exhausted%s(tried to allocate %d bytes) in %s on line %d
