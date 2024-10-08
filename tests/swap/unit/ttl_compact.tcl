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

        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]

        # expire seconds -> milliseconds
        assert_lessthan $sst_age_limit 10000

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

        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]

        # expire seconds -> milliseconds
        assert_lessthan $sst_age_limit 20000

        set request_sst_count [get_info_property r Swap swap_ttl_compact request_sst_count]
        assert_equal {2} $request_sst_count

        set compact_times [get_info_property r Swap swap_ttl_compact times]
        assert_equal {2} $compact_times
    }

    r flushdb
} 