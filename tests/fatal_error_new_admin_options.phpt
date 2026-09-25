--TEST--
A fatal error while RdKafka::newAdminOptions() creates the object does not leak the rd_kafka_AdminOptions_t
--SKIPIF--
<?php
require __DIR__ . '/helpers/memory-limit-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/fatal-error.php';

$conf = new RdKafka\Conf();
$conf->set('log_level', '0');
$producer = new RdKafka\Producer($conf);

callUntilMemoryLimit(fn () => $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_CREATETOPICS), 50000);

echo "not reached\n";

?>
--EXPECTF--
Fatal error: Allowed memory size of %d bytes exhausted%s(tried to allocate %d bytes) in %s on line %d
