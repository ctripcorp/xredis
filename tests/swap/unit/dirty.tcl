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
        r hmset hash1 a a0 b b0 1 10 2 20
        r hdel hash1 a
        r swap.evict hash1
        wait_key_cold r hash1
        assert_equal [object_meta_len r hash1] 3

        r hgetall hash1
        assert_equal [object_is_dirty r hash1] 0
        r hdel hash1 1
        assert_equal [object_is_meta_dirty r hash1] 1
        assert_equal [object_is_data_dirty r hash1] 0

        r swap.evict hash1
        after 100
        assert_equal [object_is_hot r hash1] 1
        assert_equal [object_is_dirty r hash1] 0

        assert_equal [object_hot_meta_len r hash1] 0
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
        assert_equal [object_cold_meta_len r hash2] 2
        assert_equal [llength [r hgetall hash2]] 4
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
}

