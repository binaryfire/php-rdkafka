--TEST--
A fatal error while Conf::dump() converts the configuration does not leak the native dump
--SKIPIF--
<?php
require __DIR__ . '/helpers/memory-limit-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/fatal-error.php';

$conf = new RdKafka\Conf();

callUntilMemoryLimit(fn () => $conf->dump(), 1000);

echo "not reached\n";

?>
--EXPECTF--
Fatal error: Allowed memory size of %d bytes exhausted%s(tried to allocate %d bytes) in %s on line %d
