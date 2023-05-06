start_server {tags {"absent cache"} overrides {swap-cuckoo-filter-enabled no} } {
    test {absent cache hit & miss} {
        r get foo
        assert_equal [status r swap_swapin_not_found_coldfilter_absentcache_filt_count] 0
        assert_equal [status r swap_swapin_not_found_coldfilter_miss] 1

        r get foo
        assert_equal [status r swap_swapin_not_found_coldfilter_absentcache_filt_count] 1

        r del foo
        assert_equal [status r swap_swapin_not_found_coldfilter_absentcache_filt_count] 2

        r set foo bar
        assert_equal [status r swap_swapin_not_found_coldfilter_absentcache_filt_count] 3

        r swap.evict foo
        wait_key_cold r foo

        assert_equal [r get foo] bar
        assert_equal [status r swap_swapin_not_found_coldfilter_absentcache_filt_count] 3
    }

    test {dynamically change absent cache config} {
        for {set i 0} {$i < 10} {incr i} {
            r get $i
        }

        # not-existing-key is the last touched item that will reserved on trim.
        r get not-existing-key

        r config set swap-absent-cache-capacity 1
        set cachehit [status r swap_swapin_not_found_coldfilter_absentcache_filt_count]
        r get not-existing-key
        assert_equal [incr cachehit] [status r swap_swapin_not_found_coldfilter_absentcache_filt_count]

        set cachemiss [status r swap_swapin_not_found_coldfilter_miss]
        for {set i 0} {$i < 10} {incr i} {
            r get $i
            incr cachemiss
        }
        assert_equal $cachemiss [status r swap_swapin_not_found_coldfilter_miss]

        r config set swap-absent-cache-capacity 64000

        # disable absent cache
        r config set swap-absent-cache-enabled no

        r get not-existing-key
        incr cachemiss
        assert_equal $cachemiss [status r swap_swapin_not_found_coldfilter_miss]

        # enable absent cache again
        r config set swap-absent-cache-enabled yes

        r get not-existing-key
        incr cachemiss
        assert_equal $cachemiss [status r swap_swapin_not_found_coldfilter_miss]

        r get not-existing-key
        incr cachehit
        assert_equal $cachemiss [status r swap_swapin_not_found_coldfilter_miss]
        assert_equal $cachehit  [status r swap_swapin_not_found_coldfilter_absentcache_filt_count]
    }

    # flushdb
    test {flushdb resets absent cache} {
        set cachehit [status r swap_swapin_not_found_coldfilter_absentcache_filt_count]
        for {set i 0} {$i < 10} {incr i} {
            r get $i
        }
        r flushdb
        for {set i 0} {$i < 10} {incr i} {
            r get $i
        }
        assert_equal $cachehit [status r swap_swapin_not_found_coldfilter_absentcache_filt_count]
    }
}

start_server {tags {"absent cache (subkey) "}} {
    r config set swap-debug-evict-keys 0

    test {dynamically enable/disable absent subkey cache} {
        r config set swap-absent-cache-include-subkey no

        r hmset myhash1 a a b b 1 1 2 2
        r sadd myset1 a b 1 2
        r zadd myzset1 0 a 0 b 1 1 2 2
        r swap.evict myhash1 myset1 myzset1
        wait_key_cold r myhash1
        wait_key_cold r myset1
        wait_key_cold r myzset1

        set old_dnf [status r swap_swapin_data_not_found_count]
        # absent subkey command triggers io if absent-cache-include-subkey disabled
        r hmget myhash1 c 3
        assert_equal [status r swap_swapin_data_not_found_count] [incr old_dnf 2]

        r sismember myset1 c
        r sismember myset1 3
        assert_equal [status r swap_swapin_data_not_found_count] [incr old_dnf 2]

        r zscore myzset1 c
        r zscore myzset1 3
        assert_equal [status r swap_swapin_data_not_found_count] [incr old_dnf 2]

        # absent subkey get filt if absent-cache-include-subkey enabled
        r config set swap-absent-cache-include-subkey yes
        # 1. populate absent subkey cache
        r hmget myhash1 c 3
        assert_equal [status r swap_swapin_data_not_found_count] [incr old_dnf 2]
        r sismember myset1 c
        r sismember myset1 3
        assert_equal [status r swap_swapin_data_not_found_count] [incr old_dnf 2]
        r zscore myzset1 c
        r zscore myzset1 3
        assert_equal [status r swap_swapin_data_not_found_count] [incr old_dnf 2]
        # 2. filt by absent subkey cache
        r hmget myhash1 c 3
        assert_equal [status r swap_swapin_data_not_found_count] $old_dnf
        r sismember myset1 c
        r sismember myset1 3
        assert_equal [status r swap_swapin_data_not_found_count] $old_dnf
        r zscore myzset1 c
        r zscore myzset1 3
        assert_equal [status r swap_swapin_data_not_found_count] $old_dnf

        # disable subkey cache make subkey cache no effect
        r config set swap-absent-cache-include-subkey no
        r hmget myhash1 c 3
        assert_equal [status r swap_swapin_data_not_found_count] [incr old_dnf 2]
        r sismember myset1 c
        r sismember myset1 3
        assert_equal [status r swap_swapin_data_not_found_count] [incr old_dnf 2]
        r zscore myzset1 c
        r zscore myzset1 3
        assert_equal [status r swap_swapin_data_not_found_count] [incr old_dnf 2]

        r config set swap-absent-cache-include-subkey yes
    }

    test {swapout invalidates subkey cache} {
        r hmset myhash2 a a b b 1 1 2 2
        r swap.evict myhash2
        wait_key_cold r myhash2

        r hmset myhash2 x x y y; # make myhash2 dirty(will be persist to rocksdb again)
        r hmget myhash2 c 3; # pop subkey absent cache
        set old_dnf [status r swap_swapin_data_not_found_count]
        r hmget myhash2 c 3
        assert_equal [status r swap_swapin_data_not_found_count] $old_dnf

        set old_swap_max_subkeys [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 1
        r swap.evict myhash2
        after 200
        assert {[object_is_warm r myhash2]}
        r hmget myhash2 c 3
        assert_equal [status r swap_swapin_data_not_found_count] [incr old_dnf 2]
        r hmget myhash2 c 3
        assert_equal [status r swap_swapin_data_not_found_count] $old_dnf

        r swap.evict myhash2
        after 200
        assert {[object_is_cold r myhash2]}
        r hmget myhash2 c 3
        assert_equal [status r swap_swapin_data_not_found_count] [incr old_dnf 2]
        r hmget myhash2 c 3
        assert_equal [status r swap_swapin_data_not_found_count] $old_dnf

        r config set swap-evict-step-max-subkeys $old_swap_max_subkeys
    }

    # following case disables cuckoo filter to avoid key filter interference
    r config set swap-cuckoo-filter-enabled no

    test {in_del does not affect subkey absent cache} {
        r hmset myhash3 a a b b 1 1 2 2
        r swap.evict myhash3
        wait_key_cold r myhash3

        assert_equal [r hmget myhash3 a 1] {a 1}

        r hmget myhash3 c 3
        set old_filt [status r swap_absent_subkey_filt_count]

        r hmget myhash3 c 3
        assert_equal [status r swap_absent_subkey_filt_count] [incr old_filt 2]

        r hdel myhash3 a

        r hmget myhash3 c 3
        assert_equal [status r swap_absent_subkey_filt_count] [incr old_filt 2]
    }

    test {overwrite/expire makes subkey absent cache unaccesable} {
        r hmset myhash4 a a b b 1 1 2 2
        r swap.evict myhash4
        wait_key_cold r myhash4

        r hmget myhash4 c 3
        set old_filt [status r swap_absent_subkey_filt_count]

        r set myhash4 foo
        r del myhash4

        r hmget myhash4 c 3
        # key not exists: absent subkey cache not accessed in swap thread
        assert_equal [status r swap_absent_subkey_filt_count] $old_filt

        r hmset myhash4 a a b b 1 1 2 2
        r hmget myhash4 c 3
        # key pure hot: absent subkey cache not accessed
        assert_equal [status r swap_absent_subkey_filt_count] $old_filt

        r swap.evict myhash4
        wait_key_cold r myhash4

        r pexpire myhash3 100
        after 200

        # if myhash3 expired, swapAna will be executed in swap thread, absent
        # subkey cache will not be used.
        r hmget myhash3 c 3
        assert_equal [status r swap_absent_subkey_filt_count] $old_filt

        r hmset myhash3 x x
        r hmget myhash3 c 3
        assert_equal [status r swap_absent_subkey_filt_count] $old_filt
    }

    test {flushdb resets absent subkey cache} {
        r hmset myhash5 a a b b 1 1 2 2
        r swap.evict myhash5
        wait_key_cold r myhash5

        r hmget myhash5 c 3
        set old_filt [status r swap_absent_subkey_filt_count]

        r hmget myhash5 c 3
        assert_equal [status r swap_absent_subkey_filt_count] [incr old_filt 2]

        r flushdb

        r hmget myhash5 c 3
        assert_equal [status r swap_absent_subkey_filt_count] $old_filt

        r hmget myhash5 c 3
        assert_equal [status r swap_absent_subkey_filt_count] $old_filt
    }
}
