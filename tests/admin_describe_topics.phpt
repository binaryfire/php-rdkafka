--TEST--
Admin - describeTopics integration (queue/event API)
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
if (!method_exists(RdKafka::class, 'describeTopics')) {
    die('skip describeTopics not available (requires librdkafka >= 2.3.0)');
}
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$producer = new RdKafka\Producer($conf);
$queue = $producer->newQueue();
$topicName = sprintf("test_rdkafka_%s", uniqid());

// CREATE topic with 2 partitions
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_CREATETOPICS);
$opts->setRequestTimeout(5000);
$producer->createTopics([new RdKafka\Admin\NewTopic($topicName, 2, 1)], $queue, $opts);
$event = $queue->poll(10000);
$event->getCreateTopicsResult();
unset($event);

sleep(1);

// DESCRIBE
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_DESCRIBETOPICS);
$opts->setRequestTimeout(5000);
$producer->describeTopics([$topicName], $queue, $opts);
$event = $queue->poll(10000);
$descriptions = $event->getDescribeTopicsResult();

printf("result count: %d\n", count($descriptions));

$desc = $descriptions[0];
printf("class: %s\n", get_class($desc));
printf("name matches: %s\n", $desc->name === $topicName ? 'true' : 'false');
printf("is_internal: %s\n", $desc->is_internal ? 'true' : 'false');
printf("error: %d\n", $desc->error);
printf("topic_id is string: %s\n", is_string($desc->topic_id) ? 'true' : 'false');
printf("partition count: %d\n", count($desc->partitions));

$part = $desc->partitions[0];
printf("partition class: %s\n", get_class($part));
printf("partition id: %d\n", $part->partition);

printf("leader class: %s\n", get_class($part->leader));
printf("leader has valid id: %s\n", is_int($part->leader->id) ? 'true' : 'false');
printf("leader has host: %s\n", is_string($part->leader->host) && strlen($part->leader->host) > 0 ? 'true' : 'false');
printf("leader has port: %s\n", is_int($part->leader->port) && $part->leader->port > 0 ? 'true' : 'false');

printf("isr count >= 1: %s\n", count($part->isr) >= 1 ? 'true' : 'false');
printf("isr[0] class: %s\n", get_class($part->isr[0]));
printf("replicas count >= 1: %s\n", count($part->replicas) >= 1 ? 'true' : 'false');
printf("replicas[0] class: %s\n", get_class($part->replicas[0]));
printf("authorized_operations: %s\n", var_export($desc->authorized_operations, true));
unset($event);

// DESCRIBE with authorized operations, from a list whose elements are references
$opts->setIncludeAuthorizedOperations(true);
$topicNames = [$topicName];
foreach ($topicNames as &$name) {
}
unset($name);
$producer->describeTopics($topicNames, $queue, $opts);
$desc = $queue->poll(10000)->getDescribeTopicsResult()[0];
printf("name matches: %s\n", $desc->name === $topicName ? 'true' : 'false');
printf("authorized to describe: %s\n", in_array(RD_KAFKA_ACL_OPERATION_DESCRIBE, $desc->authorized_operations, true) ? 'true' : 'false');

try {
    $producer->describeTopics([$topicName . "\0suffix"], $queue, $opts);
} catch (ValueError $e) {
    echo $e->getMessage(), "\n";
}
var_dump($queue->poll(0));

// CLEANUP
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_DELETETOPICS);
$opts->setRequestTimeout(5000);
$producer->deleteTopics([new RdKafka\Admin\DeleteTopic($topicName)], $queue, $opts);
$queue->poll(10000);

echo "OK\n";
--EXPECT--
result count: 1
class: RdKafka\Admin\TopicDescription
name matches: true
is_internal: false
error: 0
topic_id is string: true
partition count: 2
partition class: RdKafka\Admin\TopicPartitionInfo
partition id: 0
leader class: RdKafka\Admin\Node
leader has valid id: true
leader has host: true
leader has port: true
isr count >= 1: true
isr[0] class: RdKafka\Admin\Node
replicas count >= 1: true
replicas[0] class: RdKafka\Admin\Node
authorized_operations: NULL
name matches: true
authorized to describe: true
RdKafka::describeTopics(): Argument #1 ($topics) must not contain any null bytes
NULL
OK
