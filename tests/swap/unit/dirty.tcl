start_server {tags {"swap string"}} {
    r config set swap-debug-evict-keys 0
    test {swap out string} {
        r set k v
        for {set j 0} {$j < 100} {incr j} {
            r swap.evict k
            wait_key_cold r k
            assert_equal [r get k] v
        }

        for {set j 0} {$j < 100} {incr j} {
            r set k v$j
            r swap.evict k
            wait_key_cold r k
            assert_equal [r get k] v$j
        }
    }
}

start_server {tags {"swap string"}} {
    r config set swap-debug-evict-keys 0
    test {swap out string} {
        r hset h k v
        for {set j 0} {$j < 100} {incr j} {
            r swap.evict h
            wait_key_cold r h
            assert_equal [r hget h k] v
        }

        for {set j 0} {$j < 100} {incr j} {
            r hset h k v$j
            r swap.evict h
            wait_key_cold r h
            assert_equal [r hget h k] v$j
        }
    }
}

start_server {tags {"dirty subkeys"}} {
    r config set swap-debug-evict-keys 0

    test {enable/disable swap-dirty-subkeys} {
        r hmset myhash a a0 b b0 1 10 2 20
        assert_equal [object_meta_len r myhash] 0
        r swap.evict myhash
        wait_key_cold r myhash
        assert_equal [object_meta_len r myhash] 4

        # dirty_data > dirty_subkeys (when enable dirty subkeys)
        r hgetall myhash
        r hset myhash a a1
        r config set swap-dirty-subkeys-enabled yes
        r hset myhash 1 11
        r swap.evict myhash
        wait_key_cold r myhash

        assert_equal [object_is_cold r myhash] 1
        assert_equal [object_meta_len r myhash] 4
        assert_equal [r hmget myhash a b 1 2] {a1 b0 11 20}

        # evict only dirty_subkeys when dirty subkeys enabled
        r hgetall myhash
        r hset myhash a a1
        r hset myhash 1 11
        r swap.evict myhash
        after 100
        assert_equal [object_hot_meta_len r myhash] 2
        assert_equal [object_cold_meta_len r myhash] 4

        # evict clean subkeys without io
        r swap.evict myhash

        assert_equal [object_is_cold r myhash] 1
        assert_equal [object_meta_len r myhash] 4

        # dirty_data > dirty_subkeys (when disable dirty subkeys)
        r hgetall myhash
        assert_equal [object_is_dirty r myhash] 0
        r hmset myhash a a2 1 12
        assert_equal [object_is_data_dirty r myhash] 0
        r config set swap-dirty-subkeys-enabled no
        r hset myhash b b1
        assert_equal [object_is_data_dirty r myhash] 1
        r swap.evict myhash
        wait_key_cold r myhash

        assert_equal [object_is_cold r myhash] 1
        assert_equal [object_meta_len r myhash] 4
        assert_equal [r hmget myhash a b 1 2] {a2 b1 12 20}

        # evict all subkeys when dirty subkeys disabled
        r hdel myhash 2
        r swap.evict myhash
        wait_key_cold r myhash
        assert_equal [object_is_cold r myhash] 1
        assert_equal [object_meta_len r myhash] 3
        assert_equal [r hmget myhash a b 1 2] {a2 b1 12 {}}
    }
}

start_server {tags {"dirty subkeys"} overrides {swap-dirty-subkeys-enabled yes}} {
    r config set swap-debug-evict-keys 0

    test {expire persists only meta} {
        r hmset hash0 a a0 b b0 1 10 2 20
        r swap.evict hash0
        wait_key_cold r hash0
        assert_equal [r hmget hash0 a b] {a0 b0}
        assert_equal [object_hot_meta_len r hash0] 2
        r expire hash0 3600
        r swap.evict hash0
        after 100
        assert_equal [object_hot_meta_len r hash0] 2
    }

    test {hash: dirty-subkeys feature dont affect not existing key} {
        r hdel not-existing-hash foo bar
        r swap.evict not-existing-hash
        after 100
        assert_equal [r hlen not-existing-hash] 0
    }

    test {hash: hdel flags meta dirty} {
        r hmset hash1 a a0 b b0 c c0 1 10 2 20
        r hdel hash1 a
        r swap.evict hash1
        wait_key_cold r hash1
        assert_equal [object_meta_len r hash1] 4

        r hmget hash1 b c
        r hdel hash1 2
        assert_equal [object_is_meta_dirty r hash1] 1
        assert_equal [object_is_data_dirty r hash1] 0
        r swap.evict hash1
        after 100
        assert_equal [object_is_dirty r hash1] 0
        assert_equal [object_hot_meta_len r hash1] 1
        assert_equal [object_cold_meta_len r hash1] 3

        # hdel triggers persist delete, whole key is dirty
        r hdel hash1 1
        assert_equal [object_is_meta_dirty r hash1] 1
        assert_equal [object_is_data_dirty r hash1] 1
        r swap.evict hash1
        wait_key_cold r hash1
        assert_equal [object_cold_meta_len r hash1] 2
    }

    test {hash: hdel all dirty subkey, key turn hot} {
        r hmset hash2 a a0 1 10
        r swap.evict hash2
        wait_key_cold r hash2
        assert_equal [object_is_cold r hash2] 1

        r hmset hash2 b b0 2 20
        r hdel hash2 a 1
        assert_equal [object_is_hot r hash2] 1
        assert_equal [object_cold_meta_len r hash2] 0

        r swap.evict hash2
        wait_key_cold r hash2
        assert_equal [object_cold_meta_len r hash2] 2
        assert_equal [llength [r hkeys hash2]] 2
    }

    test {hash: evict only dirty subkeys} {
        r hmset hash3 a a0 b b0 1 10 2 20
        r swap.evict hash3
        wait_key_cold r hash3
        assert_equal [object_is_cold r hash3] 1

        r hgetall hash3
        r hmset hash3 a a1 c c0
        assert_equal [object_is_hot r hash3] 1

        r swap.evict hash3
        assert_equal [object_is_warm r hash3] 1

        r swap.evict hash3
        assert_equal [object_is_cold r hash3] 1

        assert_equal [r hmget hash3 a b c 1 2] {a1 b0 c0 10 20}
        assert_equal [object_is_hot r hash3] 1
    }

    test {hash: evict subkeys multiple steps} {
        set bak_evict_step_subkeys [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 1

        r hmset hash4 a a0 b b0 1 10 2 20
        for {set i 0} {$i < 3} {incr i} {
            r swap.evict hash4
            after 100
            assert_equal [object_is_cold r hash4] 0
        }
        r swap.evict hash4
        wait_key_cold r hash4
        assert_equal [r hmget hash4 a b 1 2] {a0 b0 10 20}

        r hmset hash4 a a1 b b1 1 11 2 21
        for {set i 0} {$i < 3} {incr i} {
            r swap.evict hash4
            after 100
            assert_equal [object_is_cold r hash4] 0
        }
        r swap.evict hash4
        wait_key_cold r hash4
        assert_equal [r hmget hash4 a b 1 2] {a1 b1 11 21}

        r config set swap-evict-step-max-subkeys $bak_evict_step_subkeys
    }

    test {set: dirty-subkeys feature dont affect not existing key} {
        r srem not-existing-set foo bar
        r swap.evict not-existing-set
        after 100
        assert_equal [r scard not-existing-set] 0
    }

    test {set: srem flags meta dirty} {
        r sadd set1 a b c 1 2
        r srem set1 a
        r swap.evict set1
        wait_key_cold r set1
        assert_equal [object_meta_len r set1] 4

        r smismember set1 b c
        r srem set1 2
        assert_equal [object_is_meta_dirty r set1] 1
        assert_equal [object_is_data_dirty r set1] 0
        r swap.evict set1
        after 100
        assert_equal [object_is_dirty r set1] 0
        assert_equal [object_hot_meta_len r set1] 1
        assert_equal [object_cold_meta_len r set1] 3

        # srem triggers persist delete, whole key is dirty
        r srem set1 1
        assert_equal [object_is_meta_dirty r set1] 1
        assert_equal [object_is_data_dirty r set1] 1
        r swap.evict set1
        wait_key_cold r set1
        assert_equal [object_cold_meta_len r set1] 2
    }

    test {set: delete all dirty subkey, key turn hot} {
        r sadd set2 a 1
        r swap.evict set2
        wait_key_cold r set2
        assert_equal [object_is_cold r set2] 1

        r sadd set2 b 2
        r srem set2 a 1
        assert_equal [object_is_hot r set2] 1
        assert_equal [object_cold_meta_len r set2] 0

        r swap.evict set2
        wait_key_cold r set2
        assert_equal [object_cold_meta_len r set2] 2
        assert_equal [llength [r smembers set2]] 2
    }

    test {set: evict only dirty subkeys} {
        r sadd set3 a b 1 2
        r swap.evict set3
        wait_key_cold r set3
        assert_equal [object_is_cold r set3] 1

        r smembers set3
        r sadd set3 a c
        assert_equal [object_is_hot r set3] 1

        r swap.evict set3
        assert_equal [object_is_warm r set3] 1

        r swap.evict set3
        assert_equal [object_is_cold r set3] 1

        assert_equal [r smismember set3 a b c 1 2] {1 1 1 1 1}
        assert_equal [object_is_hot r set3] 1
    }

    test {set: evict subkeys multiple steps} {
        set bak_evict_step_subkeys [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 1

        r sadd set4 a b 1 2
        for {set i 0} {$i < 3} {incr i} {
            r swap.evict set4
            after 100
            assert_equal [object_is_cold r set4] 0
        }
        r swap.evict set4
        wait_key_cold r set4
        assert_equal [r smismember set4 a b 1 2] {1 1 1 1}

        r sadd set4 a b 1 2
        for {set i 0} {$i < 3} {incr i} {
            r swap.evict set4
            after 100
            assert_equal [object_is_cold r set4] 0
        }
        r swap.evict set4
        wait_key_cold r set4
        assert_equal [r smismember set4 a b 1 2] {1 1 1 1}

        r config set swap-evict-step-max-subkeys $bak_evict_step_subkeys
    }

    test {zset: dirty-subkeys feature dont affect not existing key} {
        r zrem not-existing-zset foo bar
        r swap.evict not-existing-zset
        after 100
        assert_equal [r zcard not-existing-zset] 0
    }

    r debug set-active-expire 0

    test {zset: zrem flags meta dirty} {
        r zadd zset1 10 a 20 b 30 c 40 1 50 2
        r zrem zset1 a
        r swap.evict zset1
        wait_key_cold r zset1
        assert_equal [object_meta_len r zset1] 4

        r zmscore zset1 b c
        r zrem zset1 2
        assert_equal [object_is_meta_dirty r zset1] 1
        assert_equal [object_is_data_dirty r zset1] 0
        r swap.evict zset1
        after 100
        assert_equal [object_is_dirty r zset1] 0
        assert_equal [object_hot_meta_len r zset1] 1
        assert_equal [object_cold_meta_len r zset1] 3

        # zrem triggers persist delete, whole key is dirty
        r zrem zset1 1
        assert_equal [object_is_meta_dirty r zset1] 1
        assert_equal [object_is_data_dirty r zset1] 1
        r swap.evict zset1
        wait_key_cold r zset1
        assert_equal [object_cold_meta_len r zset1] 2
    }

    test {zset: delete all dirty subkey, key turn hot} {
        r zadd zset2 10 a 30 1
        r swap.evict zset2
        wait_key_cold r zset2
        assert_equal [object_is_cold r zset2] 1

        r zadd zset2 20 b 40 2
        r zrem zset2 a 1
        assert_equal [object_is_hot r zset2] 1
        assert_equal [object_cold_meta_len r zset2] 0

        r swap.evict zset2
        wait_key_cold r zset2
        assert_equal [object_cold_meta_len r zset2] 2
        assert_equal [llength [r zrange zset2 0 -1]] 2
    }

    test {zset: evict only dirty subkeys} {
        r zadd zset3 10 a 20 b 30 1 40 2
        r swap.evict zset3
        wait_key_cold r zset3
        assert_equal [object_is_cold r zset3] 1

        r zrange zset3 0 -1
        r zadd zset3 11 a 50 c
        assert_equal [object_is_hot r zset3] 1

        r swap.evict zset3
        wait_key_cold r zset3
        assert_equal [object_is_cold r zset3] 1

        assert_equal [r zmscore zset3 a b c 1 2] {11 20 50 30 40}
        assert_equal [object_is_hot r zset3] 1
    }

    test {zset: evict subkeys multiple steps} {
        set bak_evict_step_subkeys [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 1

        r zadd zset4 10 a 20 b 30 1 40 2
        for {set i 0} {$i < 3} {incr i} {
            r swap.evict zset4
            after 100
            assert_equal [object_is_cold r zset4] 0
        }
        r swap.evict zset4
        wait_key_cold r zset4
        assert_equal [r zmscore zset4 a b 1 2] {10 20 30 40}

        r zadd zset4 11 a 21 b 31 1 41 2
        for {set i 0} {$i < 3} {incr i} {
            r swap.evict zset4
            after 100
            assert_equal [object_is_cold r zset4] 0
        }
        r swap.evict zset4
        wait_key_cold r zset4
        assert_equal [r zmscore zset4 a b 1 2] {11 21 31 41}

        r config set swap-evict-step-max-subkeys $bak_evict_step_subkeys
    }
}

start_server {tags {"dirty subkeys"} overrides {swap-dirty-subkeys-enabled yes}} {
    r config set swap-debug-evict-keys 0
    test {dirty subkeys encoding convertion} {
        r hmset myhash1 a a b b c c
        r swap.evict myhash1
        wait_key_cold r myhash1

        for {set i 0} {$i < 1024} {incr i} {
            r hset myhash1 field-$i value-$i
        }

        assert ![object_is_data_dirty r myhash1]
        r swap.evict myhash1
        wait_key_cold r myhash1
    }
}
