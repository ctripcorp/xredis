proc log_file_matches {log pattern} {
    set fp [open $log r]
    set content [read $fp]
    close $fp
    string match $pattern $content
}

start_server {tags {"swap replication"} overrides {}} {
    start_server {} {
    start_server {} {
        set master2 [srv -2 client]
        set master2_host [srv -2 host]
        set master2_port [srv -2 port]
        set master2_log [srv -2 stdout]

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set master_log [srv -1 stdout]

        set slave [srv 0 client]
        set slave_host [srv 0 host]
        set slave_port [srv 0 port]
        set slave_log [srv 0 stdout]

        set keycount 250

        test {Replication setup and init cold key} {
            $slave slaveof $master_host $master_port
            after 500
            assert_equal [s 0 role] slave

            for {set i 0} {$i < $keycount} {incr i} {
                $master set key_$i val_$i
            }
            wait_keyspace_cold $master
            wait_keyspace_cold $slave
        }

        test {slave reconnect master will defer without discarding replicated commands} {
            $slave config set swap-debug-rio-delay-micro 1000

            for {set i 0} {$i < $keycount} {incr i} {
                $master set key_$i val_$i
            }

            $master client kill type replica

            set master_repl_offset [status $master master_repl_offset]

            assert_equal [status $slave master_link_status] {down}

            assert {[status $master master_repl_offset] > [status $slave master_repl_offset]}

            wait_for_sync $slave
            assert {[log_file_matches $master_log "*Sending 0 bytes of backlog starting from offset [expr $master_repl_offset+1]*"]}
        }

        test {shift replid will be defered untill previous master client drain} {
            $slave config set swap-debug-rio-delay-micro 10000

            set master_rd [redis_deferring_client -1]
            set slave_rd [redis_deferring_client 0]

            for {set i 0} {$i < $keycount} {incr i} {
                $master set key_$i val_$i
            }

            set master_repl_offset [status $master master_repl_offset]

            $slave_rd slaveof no one
            $master_rd slaveof $slave_host $slave_port

            $slave_rd read
            $master_rd read

            wait_for_condition 50 1000 {
                [log_file_matches $slave_log "*Sending 0 bytes of backlog starting from offset [expr $master_repl_offset+1]*"]
            } else {
                fail "Drainging slaveof no one results in fullresync! "
            }

            assert {[log_file_matches $slave_log "*Replication id shift defer done*master_repl_offset=$master_repl_offset*"]}
            assert {[log_file_matches $master_log "*NOMASTERLINK Can't SYNC while replid shift in progress*"]}

            # restore master slave replication
            $slave_rd slaveof $master_host $master_port
            $master_rd slaveof no one
            $slave_rd read
            $master_rd read
            wait_for_sync $slave
        }

        test {slave => master(defering) => slave: fullresync because replid not match} {
            $slave config set swap-debug-rio-delay-micro 1000

            set master_rd [redis_deferring_client -1]
            set slave_rd [redis_deferring_client 0]

            for {set i 0} {$i < $keycount} {incr i} {
                $master set key_$i val_$i
            }

            set master_replid [status $master master_replid]
            set master_repl_offset [status $master master_repl_offset]
            set slave_repl_offset [status $slave master_repl_offset]

            $slave_rd slaveof no one
            $slave_rd slaveof $master_host $master_port

            $slave_rd read
            $slave_rd read

            wait_for_sync $slave

            # psync with different repid
            assert {[log_file_matches $slave_log "*Setting secondary replication ID to $master_replid, valid up to offset: [expr $master_repl_offset+1]*"]}
            assert {[log_file_matches $slave_log "*Full resync from master: $master_replid:$master_repl_offset*"]}
        }

        test {slave slaveof another master will defer without descarding replicated commands} {
            $slave config set swap-debug-rio-delay-micro 1000

            set slave_rd [redis_deferring_client 0]

            for {set i 0} {$i < $keycount} {incr i} {
                $master set key_$i val_$i
            }

            $slave_rd slaveof $master2_host $master2_port

            set master_repl_offset [status $master master_repl_offset]

            assert {[status $master master_repl_offset] > [status $slave master_repl_offset]}

            $slave_rd read
            wait_for_sync $slave

            assert {[log_file_matches $slave_log "*Trying a partial resynchronization *:[expr $master_repl_offset+1]*"]}
        }

    }
    }
}

