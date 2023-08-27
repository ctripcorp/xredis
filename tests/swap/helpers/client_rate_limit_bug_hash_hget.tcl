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
    set i $start
    while {$i < $end} {
        set r [redis $host $port 0 $tls]
        if {$db != 0} {
            $r select $db
        }
        for {set j 0} {$j < 10} {incr j} {
            $r write [format_command hget  $i  $j]
        }
        $r flush 
        catch {close [$r channel]}
        incr i 
    }
    
}

gen_write_hset_load [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3] [lindex $argv 4] [lindex $argv 5] [lindex $argv 6]
