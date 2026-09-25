--TEST--
Admin arguments accept references and reject names containing null bytes
--FILE--
<?php

function expectException(callable $callback): void
{
    try {
        $callback();
        echo "No exception\n";
    } catch (Throwable $e) {
        echo get_class($e), ': ', $e->getMessage(), "\n";
    }
}

$conf = new RdKafka\Conf();
$conf->set('log_level', '0');
$producer = new RdKafka\Producer($conf);
$queue = $producer->newQueue();

echo "References\n";
$newTopics = [new RdKafka\Admin\NewTopic('admin-arguments', 1, -1)];
$deleteTopics = [new RdKafka\Admin\DeleteTopic('admin-arguments')];
$newPartitions = [new RdKafka\Admin\NewPartitions('admin-arguments', 2)];
$brokerIds = [1];

// A loop by reference leaves each element a reference, even after unset()
foreach ([&$newTopics, &$deleteTopics, &$newPartitions, &$brokerIds] as &$list) {
    foreach ($list as &$item) {
    }
    unset($item);
}
unset($list);

$newTopics[0]->setReplicaAssignment(0, $brokerIds);
$newPartitions[0]->setReplicaAssignment(0, $brokerIds);

// Without a broker the requests time out, which still delivers their results
foreach ([RD_KAFKA_ADMIN_OP_CREATETOPICS, RD_KAFKA_ADMIN_OP_DELETETOPICS, RD_KAFKA_ADMIN_OP_CREATEPARTITIONS] as $api) {
    $options[$api] = $producer->newAdminOptions($api);
    $options[$api]->setRequestTimeout(100);
}
$producer->createTopics($newTopics, $queue, $options[RD_KAFKA_ADMIN_OP_CREATETOPICS]);
$producer->deleteTopics($deleteTopics, $queue, $options[RD_KAFKA_ADMIN_OP_DELETETOPICS]);
$producer->createPartitions($newPartitions, $queue, $options[RD_KAFKA_ADMIN_OP_CREATEPARTITIONS]);

$names = [];
for ($i = 0; $i < 3; $i++) {
    $names[] = $queue->poll(5000)->getName();
}
sort($names);
echo implode("\n", $names), "\n";

echo "Null bytes\n";
expectException(fn () => new RdKafka\Admin\NewTopic("admin-arguments\0suffix", 1, 1));
expectException(fn () => new RdKafka\Admin\DeleteTopic("admin-arguments\0suffix"));
expectException(fn () => new RdKafka\Admin\NewPartitions("admin-arguments\0suffix", 2));
$newTopic = new RdKafka\Admin\NewTopic('admin-arguments', 1, 1);
expectException(fn () => $newTopic->setConfig("cleanup.policy\0suffix", 'compact'));
expectException(fn () => $newTopic->setConfig('cleanup.policy', "compact\0suffix"));

?>
--EXPECT--
References
CreatePartitionsResult
CreateTopicsResult
DeleteTopicsResult
Null bytes
ValueError: RdKafka\Admin\NewTopic::__construct(): Argument #1 ($topic) must not contain any null bytes
ValueError: RdKafka\Admin\DeleteTopic::__construct(): Argument #1 ($topic) must not contain any null bytes
ValueError: RdKafka\Admin\NewPartitions::__construct(): Argument #1 ($topic) must not contain any null bytes
ValueError: RdKafka\Admin\NewTopic::setConfig(): Argument #1 ($name) must not contain any null bytes
ValueError: RdKafka\Admin\NewTopic::setConfig(): Argument #2 ($value) must not contain any null bytes
