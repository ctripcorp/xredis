start_server {tags "multi"} {
    r config set swap-debug-evict-keys 0; # evict manually

    test {transaction reserves cmd intention flag} {
        r set key val
        r swap.evict key
        wait_key_cold r key
        assert {[rio_get_meta r key] != ""}
        r multi
        r del key
        r exec
        assert {[rio_get_meta r key] == ""}
    }

    proc initList {r list cnt} {
        set i 0
        $r del $list
        while {$i < $cnt} {
            $r rpush $list $i
            incr i 1
        }
    }

    test {multi args rewrite} {
        # list len should gt SEGMENT_MAX_PADDING which defined in ctrip_swap_list.c
        initList r mlist1 64
        initList r mlist2 64
        initList r mlist3 64
        initList r mlist4 64
        r swap.evict mlist1 mlist2 mlist3 mlist4
        wait_key_cold r mlist1
        wait_key_cold r mlist2
        wait_key_cold r mlist3
        wait_key_cold r mlist4

        # query index should gt SEGMENT_MAX_PADDING and make args rewrite
        r multi
        r lindex mlist1 41
        r lindex mlist2 42
        r lindex mlist3 43
        r lindex mlist4 44
        assert_equal [r exec] {41 42 43 44}
    }

    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    start_server {tags "multi-slave"} {
        set slave [srv 0 client]
        set slave_host [srv 0 host]
        set slave_port [srv 0 port]

        $slave slaveof $master_host $master_port

        test {multi arg rewrite and propagate correctly} {
            initList $master mlist1 64
            initList $master mlist2 64
            $master swap.evict mlist1 mlist2
            wait_key_cold $master mlist1
            wait_key_cold $master mlist2

            $master multi
            $master LSET mlist1 41 1
            $master LSET mlist2 42 2
            $master exec
            wait_for_ofs_sync $master $slave

            assert_equal [$slave lindex mlist1 41] {1}
            assert_equal [$slave lindex mlist2 42] {2}
        }
    }

}
