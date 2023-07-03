start_server {tags {persist} overrides {swap-persist-enabled yes swap-dirty-subkeys-enabled yes}} {
    r config set swap-debug-evict-keys 0
    # keep swap-debug-evict-keys config on restart
    r config rewrite

    test {persist client writes} {
        r set mystring1 v0
        r hmset myhash1 a a0 b b0 c c0
        r sadd myset1 a b c
        r rpush mylist1 a b c
        r zadd myzset1 10 a 20 b 30 c

        after 100

        r set mystring1 v1
        r hmset myhash1 b b1 1 10 2 20
        r sadd myset1 b 1 2
        r rpush mylist1 1 2
        r zadd myzset1 21 b 40 1 50 2

        assert_equal [r dbsize] 5
        assert_equal [r get mystring1] v1
        assert_equal [r hmget myhash1 a b c 1 2] {a0 b1 c0 10 20}
        assert_equal [r lrange mylist1 0 -1] {a b c 1 2}
        assert_equal [r ZRANGEBYSCORE myzset1 -inf +inf WITHSCORES] {a 10 b 21 c 30 1 40 2 50}

        restart_server 0 true false

        assert_equal [r dbsize] 5
        assert_equal [r get mystring1] v1
        assert_equal [r hmget myhash1 a b c 1 2] {a0 b1 c0 10 20}
        assert_equal [r lrange mylist1 0 -1] {a b c 1 2}
        assert_equal [r ZRANGEBYSCORE myzset1 -inf +inf WITHSCORES] {a 10 b 21 c 30 1 40 2 50}
    }

    test {persist dirty keys triggered by swap} {
        r hmset myhash2 a a0 b b0 c c0 1 10 2 20
        wait_key_cold r myhash2
        assert_equal [lsort [r hkeys myhash2]] {1 2 a b c}
        r hdel myhash2 a
        wait_key_cold r myhash2

        restart_server 0 true false

        assert_equal [r hmget myhash2 a b c 1 2] {{} b0 c0 10 20}
    }

    test {clean keys stays in memory} {
        r set mystring3 v0
        r hmset myhash3 a a0 b b0 c c0
        r sadd myset3 a b c
        r rpush mylist3 a b c
        r zadd myzset3 10 a 20 b 30 c

        wait_key_cold r mystring3
        wait_key_cold r myhash3
        wait_key_cold r myset3
        wait_key_cold r myzset3

        r get mystring3
        r hgetall myhash3
        r smembers myset3
        r lrange mylist3 0 -1
        r ZRANGEBYSCORE myzset3 -inf +inf

        assert [object_is_hot r mystring3]
        assert [object_is_hot r myhash3]
        assert [object_is_hot r myset3]
        # list is never hot when persist enabled, because list ele resides
        # in either rocksdb or redis.
        assert [object_is_cold r mylist3]
        assert [object_is_hot r myzset3]
    }

    test {persist multiple times untill clean for big keys} {
        set bak_subkeys [lindex [r config get swap-evict-step-max-subkeys] 1]
        r hmset myhash4 a a0 b b0 c c0 1 10 2 20
        wait_key_cold r myhash4

        restart_server 0 true false

        assert_equal [r hmget myhash4 a b c 1 2] {a0 b0 c0 10 20}
        r config set swap-evict-step-max-subkeys $bak_subkeys
    }

    test {persist will merge for writing hot key} {
        set bak_riodelay [lindex [r config get swap-debug-rio-delay-micro] 1]
        r config set swap-debug-rio-delay-micro 100000

        set start [clock milliseconds]
        set num_subkeys 100
        for {set i 0} {$i < $num_subkeys} {incr i} {
            set rd [redis_deferring_client]
            lappend rds $rd
            $rd hmset myhash5 field $i
        }
        foreach rd $rds {
            $rd read
        }
        wait_key_cold r myhash5
        set now [clock milliseconds]
        # wont persist across writes
        assert {[expr $now - $start] < 1000}

        restart_server 0 true false

        assert_equal [r hlen myhash5] 1
        assert_equal [r hget myhash5 field] [expr $num_subkeys-1]

        r config set swap-debug-rio-delay-micro $bak_riodelay
    }

    test {persist will not interfere evict asap} {
        set bak_delay [lindex [r config get swap-debug-rdb-key-save-delay-micro] 1]
        r config set swap-debug-rdb-key-save-delay-micro 100000

        set num_keys 10
        for {set i 0} {$i < $num_keys} {incr i} {
            r set mystring-$i $i
        }

        set num_subkeys 100
        for {set i 0} {$i < $num_subkeys} {incr i} {
            r hmset myhash6 $i $i
        }
        wait_key_cold r myhash6

        r bgsave

        r config set swap-debug-rdb-key-save-delay-micro $bak_delay

        after 100
        assert_equal [status r rdb_bgsave_in_progress] 1

        for {set i $num_subkeys} {$i < 2*$num_subkeys} {incr i} {
            r hmset myhash6 $i $i
        }
        assert_equal [r hlen myhash6] [expr 2*$num_subkeys]

        assert_equal [status r rdb_bgsave_in_progress] 1
        waitForBgsave r
    }

    test {data stay identical across restarts if swap persist enabled without write} {
        r flushdb
        populate 10000 asdf1 256
        populate 10000 asdf2 256
        wait_keyspace_cold r
        restart_server 0 true false
        assert_equal [r dbsize] 20000
    }

    test {data stay similar across restart if swap persist enabled with write} {
        set host [srv 0 host]
        set port [srv 0 port]

        r flushdb
        set load_handle0 [start_bg_complex_data $host $port 0 100000000]
        set load_handle1 [start_bg_complex_data $host $port 0 100000000]
        set load_handle2 [start_bg_complex_data $host $port 0 100000000]
        after 5000
        stop_bg_complex_data $load_handle0
        stop_bg_complex_data $load_handle1
        stop_bg_complex_data $load_handle2

        r set foooooooooooo bar
        wait_key_cold r foooooooooooo

        set dbsize_before [r dbsize]
        restart_server 0 true false
        set dbsize_after [r dbsize]
        assert {[expr $dbsize_before - $dbsize_after] < 1000}
    }
}
