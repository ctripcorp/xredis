proc start_hash_hset {host port seconds start end} {
    set tclsh [info nameofexecutable]
    exec $tclsh tests/swap/helpers/client_rate_limit_bug_hash_hset.tcl $host $port $seconds $::tls $::target_db $start $end &
}

proc start_hash_hget {host port seconds start end} {
    set tclsh [info nameofexecutable]
    exec $tclsh tests/swap/helpers/client_rate_limit_bug_hash_hget.tcl $host $port $seconds $::tls $::target_db $start $end &
}

proc format_command {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args {
        append cmd "$[string length $a]\r\n$a\r\n"
    }
    set _ $cmd
}
start_server [list overrides [list save ""] tags {"nosanitizer"}] {
    set master [srv 0 client]
    $master config set swap-ratelimit-maxmemory-percentage 100
    $master config set maxmemory-policy allkeys-lru
    $master config set maxmemory 20MB
    $master config set swap-debug-evict-keys 0
    $master config set hz 100
    # puts [randstring 100000 100000 binary]
    set host [srv 0 host]
    set port [srv 0 port]
    set load_handles []
    set j 0
    for {set j 0} {$j < 20} {incr j} {
        set load_handle [start_hash_hset $host $port 0 [expr {$j * 100}] [expr {($j +1)*100 }] ]
        lappend load_handles $load_handle
    }
    after 1000
    wait_for_condition 1000 500 {
        [$master dbsize] == 2000
    } else {
        fail "Fail to full sync"
    }

    for {set j 0} {$j < 20} {incr j} {
       set ele [lindex $load_handles $j]
       stop_bg_complex_data $ele
    }
    
    # hget 
    for {set j 0} {$j < 100} {incr j} {
        set load_handle [start_hash_hget $host $port 0 [expr {$j * 20}] [expr {($j +1)*20 }] ]
        lappend load_handles $load_handle
    }
    after 10000
    for {set j 0} {$j < 100} {incr j} {
       set ele [lindex $load_handles $j]
       stop_bg_complex_data $ele
    }

}