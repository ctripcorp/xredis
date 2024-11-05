start_server {tags {"repl"}} {
    start_server {} {
        test {First server should have role slave after SLAVEOF} {
            r -1 slaveof [srv 0 host] [srv 0 port]
            wait_for_condition 50 100 {
                [s -1 master_link_status] eq {up}
            } else {
                fail "Replication not started."
            }
        }
        # Fix parameters for the next test to work
        r config set min-slaves-to-write 0
        r -1 config set min-slaves-to-write 0
        r flushall

        test {MASTER and SLAVE dataset should be identical after complex ops} {
            createComplexDataset r 10000
            after 500

            if {[r dbsize] ne [r -1 dbsize]} {
                set csv1 [csvdump r]
                set csv2 [csvdump {r -1}]
                set fd [open /tmp/repldump1.txt w]
                puts -nonewline $fd $csv1
                close $fd
                set fd [open /tmp/repldump2.txt w]
                puts -nonewline $fd $csv2
                close $fd
                puts "Master - Replica inconsistency"
                puts "Run diff -u against /tmp/repldump*.txt for more info"
            }
            assert_equal [r dbsize] [r -1 dbsize]
            swap_data_comp [srv 0 client] [srv -1 client]
        }
    }
}
