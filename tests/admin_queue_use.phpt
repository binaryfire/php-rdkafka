--TEST--
A queue receives either admin results or a legacy topic's messages
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
$consumer = new RdKafka\Consumer($conf);
$topic = $consumer->newTopic('admin-queue-use');

// Without a broker the request times out, which still delivers a result
$options = $consumer->newAdminOptions(RD_KAFKA_ADMIN_OP_DELETETOPICS);
$options->setRequestTimeout(100);
$deleteTopics = [new RdKafka\Admin\DeleteTopic('admin-queue-use')];

echo "Admin results\n";
$adminQueue = $consumer->newQueue();
var_dump($adminQueue->consume(0));
$consumer->deleteTopics($deleteTopics, $adminQueue, $options);

// Wait without reading, so consume() meets the queued result
$deadline = microtime(true) + 10;
while ($adminQueue->getLength() === 0) {
    if (microtime(true) > $deadline) {
        throw new RuntimeException('Timed out waiting for the admin result');
    }
    usleep(1000);
}

$alias = $adminQueue;
expectException(fn () => $alias->consume(0));
expectException(fn () => $topic->consumeQueueStart(0, RD_KAFKA_OFFSET_BEGINNING, $adminQueue));
var_dump($adminQueue->poll(0)->getType() === RD_KAFKA_EVENT_DELETETOPICS_RESULT);
expectException(fn () => $adminQueue->consume(0));

echo "Messages\n";
$messageQueue = $consumer->newQueue();
var_dump($messageQueue->poll(0));
$topic->consumeQueueStart(0, RD_KAFKA_OFFSET_BEGINNING, $messageQueue);
expectException(fn () => $messageQueue->poll(0));
expectException(fn () => $consumer->deleteTopics($deleteTopics, $messageQueue, $options));
var_dump($messageQueue->consume(0));
$topic->consumeStop(0);
expectException(fn () => $messageQueue->poll(0));

echo "Failed attempts do not assign a use\n";
$queue = $consumer->newQueue();
expectException(fn () => $consumer->deleteTopics([], $queue, $options));
expectException(fn () => $topic->consumeQueueStart(RD_KAFKA_PARTITION_UA, RD_KAFKA_OFFSET_BEGINNING, $queue));
$consumer->deleteTopics($deleteTopics, $queue, $options);
var_dump($queue->poll(5000)->getType() === RD_KAFKA_EVENT_DELETETOPICS_RESULT);

?>
--EXPECT--
Admin results
NULL
RdKafka\Exception: RdKafka\Queue::consume() cannot read admin results, use RdKafka\Queue::poll()
InvalidArgumentException: RdKafka\ConsumerTopic::consumeQueueStart() cannot use a queue that receives admin results
bool(true)
RdKafka\Exception: RdKafka\Queue::consume() cannot read admin results, use RdKafka\Queue::poll()
Messages
NULL
RdKafka\Exception: RdKafka\Queue::poll() cannot read messages, use RdKafka\Queue::consume()
RdKafka\Exception: Admin results require a queue that does not receive messages
NULL
RdKafka\Exception: RdKafka\Queue::poll() cannot read messages, use RdKafka\Queue::consume()
Failed attempts do not assign a use
RdKafka\Exception: delete_topics array must not be empty
RdKafka\Exception: Local: Unknown partition
bool(true)
