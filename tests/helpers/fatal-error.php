<?php

/*
 * Fixtures for PHP fatal errors raised while the extension converts what
 * librdkafka returned or runs callbacks. Each test ends with a fatal error,
 * after which the extension must still release what librdkafka returned, so
 * destroying the client neither hangs nor leaks.
 */

function produceMessages(string $topicName, int $count, array $headers = []): void
{
    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
    $producer = new RdKafka\Producer($conf);
    $topic = $producer->newTopic($topicName);

    for ($i = 0; $i < $count; $i++) {
        $topic->producev(0, 0, "message $i", null, $headers);
    }

    if ($producer->flush(10000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw new RuntimeException("Timed out producing to $topicName");
    }
}

// Produces messages large enough that keeping a few of them reaches the
// memory limit set by callUntilMemoryLimit()
function produceLargeMessages(string $topicName): void
{
    produceMessages($topicName, 100, ['large' => str_repeat('x', 50000)]);
}

// Keeps what $call returns until the memory limit is reached. The array is
// allocated before the limit is lowered, so the limit is reached while the
// extension converts a result.
function callUntilMemoryLimit(callable $call, int $calls = 100): void
{
    $kept = array_fill(0, $calls, null);
    ini_set('memory_limit', (string) (memory_get_usage(true) + 1024 * 1024));

    for ($i = 0; $i < $calls; $i++) {
        $kept[$i] = $call();
    }
}

function exceedTimeLimit(): void
{
    set_time_limit(1);

    while (true) {
    }
}
