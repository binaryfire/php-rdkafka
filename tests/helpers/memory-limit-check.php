<?php

// Without the Zend memory manager, memory_limit is only enforced when
// allocations are tracked, as with run-tests --asan
if (getenv('USE_ZEND_ALLOC') === '0' && getenv('USE_TRACKED_ALLOC') !== '1') {
    die('skip memory_limit is not enforced without the Zend memory manager');
}
