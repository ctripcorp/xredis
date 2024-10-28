start_server {tags {"ttl compact 0"} 
    overrides {swap-debug-evict-keys {0} 
               swap-ttl-compact-period {1} 
               swap-sst-age-limit-refresh-period {1} 
               swap-swap-info-slave-period {1}}}  {

    # ttl compact will not work on L0 sst 

    test {ttl compact on empty dataset} {

        # 1.2s, bigger than swap-sst-age-limit-refresh-period
        after 1200

        set request_sst_count [get_info_property r Swap swap_ttl_compact request_sst_count]
        assert_equal $request_sst_count 0

        set compact_times [get_info_property r Swap swap_ttl_compact times]
        assert_equal $compact_times 0

        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 0
    }
}

start_server {tags {"ttl compact 1"} 
    overrides {swap-debug-evict-keys {0} 
               swap-ttl-compact-period {1} 
               swap-sst-age-limit-refresh-period {1} 
               swap-swap-info-slave-period {1}}}  {

    test {ttl compact on keys without expire} {
        for {set j 0} { $j < 100} {incr j} {
            set mybitmap "mybitmap-$j"

            r setbit $mybitmap 32768 1

            r swap.evict $mybitmap
            wait_key_cold r $mybitmap
        }

        # sst in L0 is forced to be compacted to L1
        r swap compact 

        # make sure info property updated
        after 1200

        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_morethan $sst_age_limit 9000000000000000000

        set request_sst_count [get_info_property r Swap swap_ttl_compact request_sst_count]
        assert_equal $request_sst_count 0

        set compact_times [get_info_property r Swap swap_ttl_compact times]
        assert_equal $compact_times 0
    }
}

start_server {tags {"ttl compact 2"} 
    overrides {swap-debug-evict-keys {0} 
               swap-ttl-compact-period {1} 
               swap-sst-age-limit-refresh-period {1} 
               swap-swap-info-slave-period {1}}}  {
    test {ttl compact on non-expired keys} {

        for {set j 0} { $j < 100} {incr j} {
            set mybitmap "mybitmap-$j"

            r setbit $mybitmap 32768 1
            r pexpire $mybitmap 1000000

            r swap.evict $mybitmap
            wait_key_cold r $mybitmap
        }

        # sst in L0 is forced to be compacted to L1
        r swap compact 

        # make sure info property updated
        after 1200

        set request_sst_count [get_info_property r Swap swap_ttl_compact request_sst_count]
        assert_equal $request_sst_count 0

        set compact_times [get_info_property r Swap swap_ttl_compact times]
        assert_equal $compact_times 0

        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_range $sst_age_limit 900000 1000000
    }
}

start_server {tags {"ttl compact 3"} 
    overrides {swap-debug-evict-keys {0} 
               swap-ttl-compact-period {1} 
               swap-sst-age-limit-refresh-period {1} 
               swap-swap-info-slave-period {1}}}  {

    test {ttl compact on expired keys} {

        for {set j 0} { $j < 100} {incr j} {
            set mybitmap "mybitmap-$j"

            r setbit $mybitmap 32768 1
            r pexpire $mybitmap 500

            r swap.evict $mybitmap
            wait_key_cold r $mybitmap
        } 

        # sst in L0 is forced to be compacted to L1
        r swap compact 

        # 1.2s, bigger than swap-ttl-compact-period
        after 1200

        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_lessthan $sst_age_limit 500

        set compact_times [get_info_property r Swap swap_ttl_compact times]
        assert_range $compact_times 0 1

        set request_sst_count [get_info_property r Swap swap_ttl_compact request_sst_count]
        assert_range $request_sst_count 0 2

        # set new keys again, check info
        for {set j 100} { $j < 200} {incr j} {
            set mybitmap "mybitmap-$j"

            r setbit $mybitmap 32768 1
            r pexpire $mybitmap 1000

            r swap.evict $mybitmap
            wait_key_cold r $mybitmap
        } 

        # sst in L0 is forced to be compacted to L1
        r swap compact 

        # 1.6s, much bigger than ttl compact period
        after 1600

        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_range $sst_age_limit 500 1000

        set request_sst_count [get_info_property r Swap swap_ttl_compact request_sst_count]
        assert_range $request_sst_count 1 3

        set compact_times [get_info_property r Swap swap_ttl_compact times]
        assert_range $compact_times 0 3
    }
}

start_server {tags {"ttl compact 4"} 
    overrides {swap-debug-evict-keys {0} 
               swap-ttl-compact-period {1} 
               swap-sst-age-limit-refresh-period {1} 
               swap-swap-info-slave-period {1}}}  {
    test {ttl compact after flushdb} {

        for {set j 0} { $j < 100} {incr j} {
            set mybitmap "mybitmap-$j"

            # bitmap is spilt as subkey of 4KB by default
            r setbit $mybitmap 32768 1

            r swap.evict $mybitmap
            wait_key_cold r $mybitmap
        }

        # 1.2s, bigger than swap-sst-age-limit-refresh-period
        after 1200

        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_morethan $sst_age_limit 9000000000000000000

        # sst_age_limit will be gradually 0 after flushdb if no new keys, just mock that  
        # stop sst-age-limit refresh
        r config set swap-sst-age-limit-refresh-period 3600

        r flushdb
        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_equal $sst_age_limit 0
    }
}

start_server {tags {"ttl compact 5"} 
    overrides {swap-debug-evict-keys {0} 
               swap-ttl-compact-period {1} 
               swap-sst-age-limit-refresh-period {1} 
               swap-swap-info-slave-period {1}}}  {
    test {ttl compact on expire key and no expire key} {

        for {set j 0} { $j < 100} {incr j} {
            set mybitmap "mybitmap-$j"

            # bitmap is spilt as subkey of 4KB by default
            r setbit $mybitmap 32768 1
            r pexpire $mybitmap 1000

            r swap.evict $mybitmap
            wait_key_cold r $mybitmap
        }

        for {set j 100} { $j < 200} {incr j} {
            set mybitmap "mybitmap-$j"

            # bitmap is spilt as subkey of 4KB by default
            r setbit $mybitmap 32768 1

            r swap.evict $mybitmap
            wait_key_cold r $mybitmap
        }

        # 1.2s, bigger than swap-sst-age-limit-refresh-period
        after 1200

        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]
        assert_morethan $sst_age_limit 9000000000000000000

        set request_sst_count [get_info_property r Swap swap_ttl_compact request_sst_count]
        assert_equal $request_sst_count 0

        set compact_times [get_info_property r Swap swap_ttl_compact times]
        assert_equal $compact_times 0
    }
}

start_server {tags {"master propagate expire test"} overrides {save ""}} {

    start_server {overrides {swap-repl-rordb-sync {no} 
                             swap-debug-evict-keys {0}
                             swap-swap-info-slave-period {1} 
                             swap-sst-age-limit-refresh-period {1000}}} {

        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set master [srv 0 client]
        set slave_host [srv -1 host]
        set slave_port [srv -1 port]
        set slave [srv -1 client]
        $slave slaveof $master_host $master_port
        wait_for_sync $slave
        test {ttl compact master slave propagate check} {

            $master swap.info SST-AGE-LIMIT 1111

            # more than swap-swap-info-slave-period
            after 1100
            # wait_for_ofs_sync $master $slave

            set sst_age_limit1 [get_info_property $master Swap swap_ttl_compact sst_age_limit]
            set sst_age_limit2 [get_info_property $slave Swap swap_ttl_compact sst_age_limit]

            assert_equal $sst_age_limit1 1111
            assert_equal $sst_age_limit1 $sst_age_limit2
        }
    }
}
