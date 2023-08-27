source tests/support/redis.tcl
source tests/support/util.tcl
set ::tlsdir "tests/tls"
proc format_command {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args {
        append cmd "$[string length $a]\r\n$a\r\n"
    }
    set _ $cmd
}

proc gen_write_hset_load {host port seconds tls db start end} {
    set start_time [clock seconds]
    set r [redis $host $port 1 $tls]
    $r client setname LOAD_HANDLER
    $r select $db
    set i $start
    set val [randstring 100000 100000 binary]
    while {$i < $end} {
        for {set j 0} {$j < 10} {incr j} {
            $r write [format_command hset  $i  $j $val ]
        }
        $r flush 
        for {set j 0} {$j < 10} {incr j} {
            $r read
        }
        incr i 
    }
    
}

gen_write_hset_load [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3] [lindex $argv 4] [lindex $argv 5] [lindex $argv 6]
