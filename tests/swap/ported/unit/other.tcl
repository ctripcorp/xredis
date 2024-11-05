## case from unit/other (redis 6.2.6)


start_server {overrides {save ""} tags {"other"}} {


    tags {consistency} {
        if {true} {
            if {$::accurate} {set numops 10000} else {set numops 1000}
            test {Check consistency of different data types after a reload} {
                r flushdb
                createComplexDataset r $numops
                set dump [csvdump r]
                set dbsize [r dbsize]
                r debug reload
                set dbsize_after [r dbsize]
                # debug digest not supported in disk swap mode
                if {$dbsize eq $dbsize_after} {
                    set _ 1
                } else {
                    set newdump [csvdump r]
                    puts "Consistency test failed!"
                    puts "You can inspect the two dumps in /tmp/repldump*.txt"

                    set fd [open /tmp/repldump1.txt w]
                    puts $fd $dump
                    close $fd
                    set fd [open /tmp/repldump2.txt w]
                    puts $fd $newdump
                    close $fd

                    set _ 0
                }
            } {1}

        }
    }



}


