--TEST--
RdKafka\Admin\TopicResult getters read properties provided by hooks
--SKIPIF--
<?php
if (PHP_VERSION_ID < 80400) {
    die('skip property hooks require PHP 8.4');
}
--FILE--
<?php

class HookedTopicResult extends RdKafka\Admin\TopicResult
{
    // Built at runtime, so a copy that is not released is reported
    public int $error {
        get => (int) str_repeat('4', 2);
    }

    public ?string $error_string {
        get => str_repeat('failed', 2);
    }

    public string $name {
        get => str_repeat('topic', 2);
    }
}

$result = new HookedTopicResult();
var_dump($result->getError(), $result->getErrorString(), $result->getName());

?>
--EXPECT--
int(44)
string(12) "failedfailed"
string(10) "topictopic"
