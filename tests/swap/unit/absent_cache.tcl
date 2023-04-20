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
