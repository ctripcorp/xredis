start_server {tags {"ttl compact"}} {
    r config set swap-debug-evict-keys 0
    test {swap out hash} {

        r hset h k v
        r swap.evict h
        wait_key_cold r h
    }

    test {swap in hash} {
        assert_equal [r hget h k] v
        assert ![object_is_cold r h]
    }

    test {ttl compact on expired keys} {

        for {set j 0} { $j < 100} {incr j} {
            
            assert_equal [r swap.evict myhash] 1
        } 

    }
} 