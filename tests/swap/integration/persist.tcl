start_server {tags {persist} overrides {swap-persist-enabled yes swap-dirty-subkeys-enabled yes}} {
    r config set swap-debug-evict-keys 0

    test {persist keep data (string)} {
        r set mystring1 v1
        after 100
        assert [object_is_hot r mystring1]
        assert_equal [r get mystring1] v1
    }

    test {persist keep data (hash)} {
        r hmset myhash0 a a0 b b0 c c0 1 10 2 20
        after 100
        assert [object_is_hot r myhash0]
        assert_equal [r hmget myhash0 a b c 1 2] {a0 b0 c0 10 20}
        assert_equal [r hlen myhash0] 5

        r swap.evict myhash0
        wait_key_cold r myhash0
        # hdel turn hot, mark data dirty, persist keep all subkeys & clear dirty
        assert_equal [r hmget myhash0 a b c 1] {a0 b0 c0 10}
        r hdel myhash0 2
        after 100
        assert [object_is_hot r myhash0]
        assert_equal [r hlen myhash0] 4

        r swap.evict myhash0
        wait_key_cold r myhash0
        set bak_evict_step [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 2
        # hdel turn hot, mark data dirty, delete partial subkeys & clear dirty
        assert_equal [r hmget myhash0 a b c] {a0 b0 c0}
        r hdel myhash0 1
        wait_key_clean r myhash0
        assert [object_is_warm r myhash0]
        assert_equal [r hlen myhash0] 3
        r config set swap-evict-step-max-subkeys $bak_evict_step
    }

    test {persist keep data (set)} {
        r sadd myset0 a b c 1 2
        after 100
        assert [object_is_hot r myset0]
        assert_equal [r smismember myset0 a b c 1 2] {1 1 1 1 1}
        assert_equal [r scard myset0] 5

        r swap.evict myset0
        wait_key_cold r myset0
        # srem turn hot, mark data dirty, persist keep all subkeys & clear dirty
        assert_equal [r smismember myset0 a b c 1] {1 1 1 1}
        r srem myset0 2
        after 100
        assert [object_is_hot r myset0]
        assert_equal [r scard myset0] 4

        r swap.evict myset0
        wait_key_cold r myset0
        set bak_evict_step [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 2
        # srem turn hot, mark data dirty, delete partial subkeys & clear dirty
        assert_equal [r smismember myset0 a b c] {1 1 1}
        r srem myset0 1
        wait_key_clean r myset0
        assert [object_is_warm r myset0]
        assert_equal [r scard myset0] 3
        r config set swap-evict-step-max-subkeys $bak_evict_step
    }

    test {persist keep data (zset)} {
        r zadd myzset0 10 a 20 b 30 c 40 1 50 2
        after 100
        assert [object_is_hot r myzset0]
        assert_equal [r zmscore myzset0 a b c 1 2] {10 20 30 40 50}
        assert_equal [r zcard myzset0] 5

        r swap.evict myzset0
        wait_key_cold r myzset0
        # srem turn hot, mark data dirty, persist keep all subkeys & clear dirty
        assert_equal [r zmscore myzset0 a b c 1] {10 20 30 40}
        r zrem myzset0 2
        after 100
        assert [object_is_hot r myzset0]
        assert_equal [r zcard myzset0] 4

        r swap.evict myzset0
        wait_key_cold r myzset0
        set bak_evict_step [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 2
        # srem turn hot, mark data dirty, delete partial subkeys & clear dirty
        assert_equal [r zmscore myzset0 a b c] {10 20 30}
        r zrem myzset0 1
        wait_key_clean r myzset0
        assert [object_is_warm r myzset0]
        assert_equal [r zcard myzset0] 3
        r config set swap-evict-step-max-subkeys $bak_evict_step
    }
}

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
        wait_key_clean r myhash2
        assert_equal [lsort [r hkeys myhash2]] {1 2 a b c}
        r hdel myhash2 a
        wait_key_clean r myhash2

        restart_server 0 true false

        assert_equal [r hmget myhash2 a b c 1 2] {{} b0 c0 10 20}
    }

    test {clean keys stays in memory} {
        r set mystring3 v0
        r hmset myhash3 a a0 b b0 c c0
        r sadd myset3 a b c
        r rpush mylist3 a b c
        r zadd myzset3 10 a 20 b 30 c

        wait_key_clean r mystring3
        wait_key_clean r myhash3
        wait_key_clean r myset3
        wait_key_clean r myzset3

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
        wait_key_clean r myhash4

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
        wait_key_clean r myhash5
        set now [clock milliseconds]
        # wont persist across writes
        assert {[expr $now - $start] < 2000}

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
        wait_key_clean r myhash6

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

        r set foooooooooooo bar
        wait_key_clean r foooooooooooo

        restart_server 0 true false
        assert_equal [r dbsize] 20001
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
        wait_key_clean r foooooooooooo

        set dbsize_before [r dbsize]
        restart_server 0 true false
        set dbsize_after [r dbsize]
        assert {[expr $dbsize_before - $dbsize_after] < 1000}
    }
}

start_server {tags {persist} overrides {swap-persist-enabled yes swap-dirty-subkeys-enabled yes}} {
    test {swap_version not correctly resumed after load fix} {
        r hmset myhash0 a a b b c c
        r swap.evict myhash0
        wait_key_cold r myhash0
        r del myhash0

        restart_server 0 true false

        r hmset myhash0 e e
        r swap.evict myhash0
        wait_key_cold r myhash0
        r hgetall myhash0
    }
}

start_server {tags {persist} overrides {swap-persist-enabled yes swap-dirty-subkeys-enabled yes}} {
    r config set swap-debug-evict-keys 0
    test {persist keep data did not flag key persisted causing rocksdb data leak} {
        r set mystring val
        r hmset myhash a a b b c c
        r sadd myset a b c
        r zadd myzset 1 a 2 b 3 c
        r expire mystring 1
        r expire myhash 1
        r expire myset 1
        r expire myzset 1
        after 1500
        assert_equal [llength [r swap rio-scan meta {}]] 0
    }
}

