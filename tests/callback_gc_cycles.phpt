--TEST--
Callback cycles can be collected
--FILE--
<?php

function isCollected(WeakReference $reference): bool
{
    gc_collect_cycles();

    return $reference->get() === null;
}

function confCallbackCycle(): WeakReference
{
    $conf = new RdKafka\Conf();

    foreach ([
        'setErrorCb',
        'setRebalanceCb',
        'setDrMsgCb',
        'setStatsCb',
        'setConsumeCb',
        'setOffsetCommitCb',
        'setLogCb',
        'setOauthbearerTokenRefreshCb',
    ] as $setter) {
        $capture = new stdClass();
        $capture->conf = $conf;
        $conf->{$setter}(function () use ($capture) { });
    }

    return WeakReference::create($conf);
}

function producerCallbackCycle(): WeakReference
{
    $conf = new RdKafka\Conf();

    // Silences librdkafka's missing-broker warning. setLogCb() would do the same,
    // but it adds a callback to the graph under test.
    $conf->set('log_level', '0');

    $capture = new stdClass();
    $conf->setErrorCb(function () use ($capture) { });

    $producer = new RdKafka\Producer($conf);
    $capture->producer = $producer;

    return WeakReference::create($producer);
}

function producerTopicCallbackCycle(): WeakReference
{
    $conf = new RdKafka\Conf();
    $conf->set('log_level', '0');

    $capture = new stdClass();
    $conf->setDrMsgCb(function () use ($capture) { });

    $producer = new RdKafka\Producer($conf);
    $capture->topic = $producer->newTopic('callback-gc');

    return WeakReference::create($producer);
}

function consumerQueueCallbackCycle(): WeakReference
{
    $conf = new RdKafka\Conf();
    $conf->set('log_level', '0');

    $capture = new stdClass();
    $conf->setErrorCb(function () use ($capture) { });

    $consumer = new RdKafka\Consumer($conf);
    $capture->queue = $consumer->newQueue();

    return WeakReference::create($consumer);
}

function producerAdminOptionsCallbackCycle(): WeakReference
{
    $conf = new RdKafka\Conf();
    $conf->set('log_level', '0');

    $capture = new stdClass();
    $conf->setErrorCb(function () use ($capture) { });

    $producer = new RdKafka\Producer($conf);
    $capture->options = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_DELETETOPICS);

    return WeakReference::create($producer);
}

function producerEventCallbackCycle(): WeakReference
{
    $conf = new RdKafka\Conf();
    $conf->set('log_level', '0');

    $capture = new stdClass();
    $conf->setErrorCb(function () use ($capture) { });

    $producer = new RdKafka\Producer($conf);
    $queue = $producer->newQueue();
    $options = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_DELETETOPICS);
    $options->setRequestTimeout(100);

    // Without a broker the request times out, which still delivers a result
    $producer->deleteTopics([new RdKafka\Admin\DeleteTopic('callback-gc')], $queue, $options);
    $capture->event = $queue->poll(5000);
    if ($capture->event === null) {
        throw new RuntimeException('No admin result');
    }

    return WeakReference::create($producer);
}

function kafkaConsumerCallbackCycle(): WeakReference
{
    $conf = new RdKafka\Conf();
    $conf->set('group.id', 'callback-gc');
    $conf->set('log_level', '0');

    $capture = new stdClass();
    $conf->setErrorCb(function () use ($capture) { });

    $consumer = new RdKafka\KafkaConsumer($conf);
    $capture->consumer = $consumer;

    return WeakReference::create($consumer);
}

function kafkaConsumerTopicCallbackCycle(): WeakReference
{
    $conf = new RdKafka\Conf();
    $conf->set('group.id', 'callback-topic-gc');
    $conf->set('log_level', '0');

    $capture = new stdClass();
    $conf->setErrorCb(function () use ($capture) { });

    $consumer = new RdKafka\KafkaConsumer($conf);
    $capture->topic = $consumer->newTopic('callback-gc');

    return WeakReference::create($consumer);
}

class ConfWithProperty extends RdKafka\Conf
{
    public ?object $capture = null;
}

class TopicConfWithProperty extends RdKafka\TopicConf
{
    public ?object $capture = null;
}

class ProducerWithProperty extends RdKafka\Producer
{
    public ?object $capture = null;
}

class KafkaConsumerWithProperty extends RdKafka\KafkaConsumer
{
    public ?object $capture = null;
}

function propertyCycle(object $owner): WeakReference
{
    $capture = new stdClass();
    $capture->owner = $owner;
    $owner->capture = $capture;

    return WeakReference::create($owner);
}

echo "Conf callback cycle\n";
var_dump(isCollected(confCallbackCycle()));

echo "Producer callback cycle\n";
var_dump(isCollected(producerCallbackCycle()));

echo "Producer topic callback cycle\n";
var_dump(isCollected(producerTopicCallbackCycle()));

echo "Consumer queue callback cycle\n";
var_dump(isCollected(consumerQueueCallbackCycle()));

echo "Producer admin options callback cycle\n";
var_dump(isCollected(producerAdminOptionsCallbackCycle()));

echo "Producer event callback cycle\n";
var_dump(isCollected(producerEventCallbackCycle()));

echo "KafkaConsumer callback cycle\n";
var_dump(isCollected(kafkaConsumerCallbackCycle()));

echo "KafkaConsumer topic callback cycle\n";
var_dump(isCollected(kafkaConsumerTopicCallbackCycle()));

$conf = new ConfWithProperty();
$conf->setErrorCb(function () { });
$reference = propertyCycle($conf);
unset($conf);

echo "Conf property cycle\n";
var_dump(isCollected($reference));

echo "TopicConf property cycle\n";
var_dump(isCollected(propertyCycle(new TopicConfWithProperty())));

$conf = new RdKafka\Conf();
$conf->set('log_level', '0');
$conf->setErrorCb(function () { });

echo "Producer property cycle\n";
var_dump(isCollected(propertyCycle(new ProducerWithProperty($conf))));

$conf->set('group.id', 'property-gc');

echo "KafkaConsumer property cycle\n";
var_dump(isCollected(propertyCycle(new KafkaConsumerWithProperty($conf))));

?>
--EXPECT--
Conf callback cycle
bool(true)
Producer callback cycle
bool(true)
Producer topic callback cycle
bool(true)
Consumer queue callback cycle
bool(true)
Producer admin options callback cycle
bool(true)
Producer event callback cycle
bool(true)
KafkaConsumer callback cycle
bool(true)
KafkaConsumer topic callback cycle
bool(true)
Conf property cycle
bool(true)
TopicConf property cycle
bool(true)
Producer property cycle
bool(true)
KafkaConsumer property cycle
bool(true)
