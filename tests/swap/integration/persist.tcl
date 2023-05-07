start_server {tags {persist} overrides {swap-persist-enabled yes}} {
    r config set swap-debug-evict-keys 0

    test {data stay identical across restarts if swap persist enabled without write} {
        populate 100000 asdf1 256
        populate 100000 asdf2 256
        restart_server 0 true false
        assert_equal [r dbsize] 200000
    }

    test {data stay similar across restart if swap persist enabled with write} {
        set host [srv 0 host]
        set port [srv 0 port]

        set load_handle0 [start_bg_complex_data $host $port 0 100000000]
        set load_handle1 [start_bg_complex_data $host $port 0 100000000]
        set load_handle2 [start_bg_complex_data $host $port 0 100000000]
        after 5000
        stop_bg_complex_data $load_handle0
        stop_bg_complex_data $load_handle1
        stop_bg_complex_data $load_handle2

        set dbsize_before [r dbsize]
        restart_server 0 true false
        set dbsize_after [r dbsize]
        # puts "dbsize_before:$dbsize_before, dbsize_after:$dbsize_after"
        assert {[expr $dbsize_before - $dbsize_after] < 1000}
    }
}

