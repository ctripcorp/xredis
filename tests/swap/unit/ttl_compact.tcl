start_server {tags {"ttl compact"}} {
    r config set swap-debug-evict-keys 0
    r config set swap-ttl-compact-interval-seconds 120

    test {ttl compact on expired keys} {

        for {set j 0} { $j < 100} {incr j} {
            set mybitmap "mybitmap-$j"

            # bitmap is spilt as subkey of 4KB by default
            r setbit $mybitmap 32768 1
            r expire $mybitmap 10

            r swap.evict $mybitmap
            wait_key_cold r $mybitmap
        } 

        # sst in L0 is forced to be compacted to L1
        r swap compact 

        # 180s, bigger than default ttl compact period, ensure that ttl compact happen
        after 180000

        set expire_of_quantile [get_info_property r Swap swap_ttl_compact expire_of_quantile]

        # expire seconds -> milliseconds
        assert_lessthan $expire_of_quantile 10000

        set request_sst_count [get_info_property r Swap swap_ttl_compact request_sst_count]
        assert_equal {1} $request_sst_count

        set compact_times [get_info_property r Swap swap_ttl_compact times]
        assert_equal {1} $compact_times

        # set keys again
        for {set j 100} { $j < 200} {incr j} {
            set mybitmap "mybitmap-$j"

            r setbit $mybitmap 32768 1
            r expire $mybitmap 20

            r swap.evict $mybitmap
            wait_key_cold r $mybitmap
        } 

        # sst in L0 is forced to be compacted to L1
        r swap compact 

        # 180s, bigger than default ttl compact period, ensure that ttl compact happen
        after 180000

        set expire_of_quantile [get_info_property r Swap swap_ttl_compact expire_of_quantile]

        # expire seconds -> milliseconds
        assert_lessthan $expire_of_quantile 20000

        set request_sst_count [get_info_property r Swap swap_ttl_compact request_sst_count]
        assert_equal {2} $request_sst_count

        set compact_times [get_info_property r Swap swap_ttl_compact times]
        assert_equal {2} $compact_times
    }

    r flushdb
}

start_server {tags {"master propagate expire test"} overrides {save ""}} {

    start_server {overrides {swap-repl-rordb-sync {no} swap-debug-evict-keys {0} swap-swap-info-slave-period {60}}} {

        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set master [srv 0 client]
        set slave_host [srv -1 host]
        set slave_port [srv -1 port]
        set slave [srv -1 client]
        $slave slaveof $master_host $master_port
        wait_for_sync $slave
        test {ttl compact master slave propagate check} {

            for {set j 0} { $j < 100} {incr j} {
                set mybitmap "mybitmap-$j"

                # bitmap is spilt as subkey of 4KB by default
                $master setbit $mybitmap 32768 1
                $master expire $mybitmap 10

                $master swap.evict $mybitmap
                wait_key_cold $master $mybitmap
            } 

            # more than 60s
            after 100000
            wait_for_ofs_sync $master $slave

            set expire_of_quantile1 [get_info_property $master Swap swap_ttl_compact expire_of_quantile]
            set expire_of_quantile2 [get_info_property $slave Swap swap_ttl_compact expire_of_quantile]

            assert_lessthan $expire_of_quantile1 10000
            assert_equal $expire_of_quantile1 $expire_of_quantile2

        }
    }
}
