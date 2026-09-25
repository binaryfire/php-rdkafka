--TEST--
RdKafka\Admin\TopicResult getters read uninitialized and magic properties safely
--FILE--
<?php

function dumpResult(callable $callback): void
{
    try {
        var_dump($callback());
    } catch (Throwable $e) {
        echo get_class($e), ': ', $e->getMessage(), "\n";
    }
}

class MagicTopicResult extends RdKafka\Admin\TopicResult
{
    public ?Throwable $exception = null;

    public function __construct()
    {
        unset($this->error, $this->error_string, $this->name);
    }

    public function __get(string $name): mixed
    {
        if ($this->exception !== null) {
            throw $this->exception;
        }

        // Built at runtime, so a copy that is not released is reported
        return match ($name) {
            'error' => (int) str_repeat('4', 2),
            'error_string' => str_repeat('failed', 2),
            'name' => str_repeat('topic', 2),
        };
    }
}

echo "Uninitialized\n";
$result = new RdKafka\Admin\TopicResult();
dumpResult(fn () => $result->getError());
dumpResult(fn () => $result->getErrorString());
dumpResult(fn () => $result->getName());

echo "Unset\n";
$result->error = 0;
$result->error_string = null;
$result->name = 'topic';
var_dump($result->getError(), $result->getErrorString(), $result->getName());
unset($result->name);
dumpResult(fn () => $result->getName());

echo "Magic\n";
$result = new MagicTopicResult();
var_dump($result->getError(), $result->getErrorString(), $result->getName());

echo "Throwing\n";
$result->exception = new RuntimeException('no property');
dumpResult(fn () => $result->getError());
dumpResult(fn () => $result->getErrorString());
dumpResult(fn () => $result->getName());

?>
--EXPECT--
Uninitialized
Error: Typed property RdKafka\Admin\TopicResult::$error must not be accessed before initialization
Error: Typed property RdKafka\Admin\TopicResult::$error_string must not be accessed before initialization
Error: Typed property RdKafka\Admin\TopicResult::$name must not be accessed before initialization
Unset
int(0)
NULL
string(5) "topic"
Error: Typed property RdKafka\Admin\TopicResult::$name must not be accessed before initialization
Magic
int(44)
string(12) "failedfailed"
string(10) "topictopic"
Throwing
RuntimeException: no property
RuntimeException: no property
RuntimeException: no property
