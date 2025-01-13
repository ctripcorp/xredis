proc log_file_matches {log pattern} {
    set fp [open $log r]
    set content [read $fp]
    close $fp
    string match $pattern $content
}

start_server {tags {"repl" "nosanitizer"}} {
    set A [srv 0 client]
    set A_host [srv 0 host]
    set A_port [srv 0 port]
    start_server {} {
        set B [srv 0 client]
        set B_host [srv 0 host]
        set B_port [srv 0 port]

        test {Set instance A as slave of B} {
            $A slaveof $B_host $B_port
            wait_for_condition 50 100 {
                [lindex [$A role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$A info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }
        }

        test {INCRBYFLOAT replication, should not remove expire} {
            r set test 1 EX 100
            r incrbyfloat test 0.1
            after 1000
            assert_equal [$A ttl test] [$B ttl test]
        }

        test {BRPOPLPUSH replication, list exists} {
            $A config resetstat
            set rd [redis_deferring_client]
            r lpush c 1
            r lpush c 2
            r lpush c 3
            $rd brpoplpush c d 5
            after 1000
            assert_equal [$A llen c] [$B llen c]
            assert_equal [$A llen d] [$B llen d]
            assert_match {*calls=1,*} [cmdrstat rpoplpush $A]
            assert_match {} [cmdrstat lmove $A]
        }

		foreach wherefrom {left right} {
            foreach whereto {left right} {
                test "BLMOVE ($wherefrom, $whereto) replication, when blocking against empty list" {
                    $A config resetstat
                    set rd [redis_deferring_client]
                    $rd blmove a b $wherefrom $whereto 5
                    r lpush a foo
                    wait_for_condition 50 100 {
                        [$A debug digest-value a] eq [$B debug digest-value a] && [$A debug digest-value b] eq [$B debug digest-value b]
                    } else {
                        puts "A.a:[$A debug digest-value a], B.a:[$B debug digest-value a], A.b:[$A debug digest-value b] B.b:[$B debug digest-value b]"
                        fail "Master and replica have different digest: [$A debug digest] VS [$B debug digest]"
                    }
                    assert_match {*calls=1,*} [cmdrstat lmove $A]
                    assert_match {} [cmdrstat rpoplpush $A]
                }

                test "BLMOVE ($wherefrom, $whereto) replication, list exists" {
                    $A config resetstat
                    set rd [redis_deferring_client]
                    r lpush c 1
                    r lpush c 2
                    r lpush c 3
                    $rd blmove c d $wherefrom $whereto 5
                    after 1000
                    assert_equal [$A llen c] [$B llen c]
                    assert_equal [$A llen d] [$B llen d]
                    assert_match {*calls=1,*} [cmdrstat lmove $A]
                    assert_match {} [cmdrstat rpoplpush $A]
                }
            }
        }
    }
}

foreach mdl {no yes} {
    foreach sdl {disabled swapdb} {
        start_server {tags {"repl" "nosanitizer"}} {
            set master [srv 0 client]
            $master config set repl-diskless-sync $mdl
            $master config set repl-diskless-sync-delay 1
            set master_host [srv 0 host]
            set master_port [srv 0 port]
            set slaves {}
            start_server {} {
                lappend slaves [srv 0 client]
                start_server {} {
                    lappend slaves [srv 0 client]
                    start_server {} {
                        lappend slaves [srv 0 client]
                        test "Connect multiple replicas at the same time (issue #141), master diskless=$mdl, replica diskless=$sdl" {
                            # start load handles only inside the test, so that the test can be skipped
                            set load_handle0 [start_bg_complex_data $master_host $master_port 0 100000000]
                            set load_handle1 [start_bg_complex_data $master_host $master_port 0 100000000]
                            set load_handle2 [start_bg_complex_data $master_host $master_port 0 100000000]
                            set load_handle3 [start_write_load $master_host $master_port 8]
                            set load_handle4 [start_write_load $master_host $master_port 4]
                            after 5000 ;# wait for some data to accumulate so that we have RDB part for the fork

                            # Send SLAVEOF commands to slaves
                            [lindex $slaves 0] config set repl-diskless-load $sdl
                            [lindex $slaves 1] config set repl-diskless-load $sdl
                            [lindex $slaves 2] config set repl-diskless-load $sdl
                            [lindex $slaves 0] slaveof $master_host $master_port
                            [lindex $slaves 1] slaveof $master_host $master_port
                            [lindex $slaves 2] slaveof $master_host $master_port

                            # Wait for all the three slaves to reach the "online"
                            # state from the POV of the master.
                            set retry 500
                            while {$retry} {
                                set info [r -3 info]
                                if {[string match {*slave0:*state=online*slave1:*state=online*slave2:*state=online*} $info]} {
                                    break
                                } else {
                                    incr retry -1
                                    after 100
                                }
                            }
                            if {$retry == 0} {
                                error "assertion:Slaves not correctly synchronized"
                            }

                            # Wait that slaves acknowledge they are online so
                            # we are sure that DBSIZE and DEBUG DIGEST will not
                            # fail because of timing issues.
                            wait_for_condition 500 100 {
                                [lindex [[lindex $slaves 0] role] 3] eq {connected} &&
                                [lindex [[lindex $slaves 1] role] 3] eq {connected} &&
                                [lindex [[lindex $slaves 2] role] 3] eq {connected}
                            } else {
                                fail "Slaves still not connected after some time"
                            }

                            # Stop the write load
                            stop_bg_complex_data $load_handle0
                            stop_bg_complex_data $load_handle1
                            stop_bg_complex_data $load_handle2
                            stop_write_load $load_handle3
                            stop_write_load $load_handle4

                            # Make sure no more commands processed
                            wait_load_handlers_disconnected

                            wait_for_ofs_sync $master [lindex $slaves 0]
                            wait_for_ofs_sync $master [lindex $slaves 1]
                            wait_for_ofs_sync $master [lindex $slaves 2]

                            set dbsize [$master dbsize]
                            set dbsize0 [[lindex $slaves 0] dbsize]
                            set dbsize1 [[lindex $slaves 1] dbsize]
                            set dbsize2 [[lindex $slaves 2] dbsize]
                            assert {$dbsize>0}
                            assert {$dbsize==$dbsize0}
                            assert {$dbsize==$dbsize1}
                            assert {$dbsize==$dbsize2}
                            swap_data_comp $master [lindex $slaves 0]
                            swap_data_comp $master [lindex $slaves 1]
                            swap_data_comp $master [lindex $slaves 2]
                        }
                   }
                }
            }
        }
    }
}

start_server {tags {"repl"} overrides {repl-backlog-size 10mb}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    start_server {} {
        test "Master stream is correctly processed while the replica has a script in -BUSY state" {
            set load_handle0 [start_write_load $master_host $master_port 3]
            set slave [srv 0 client]
            $slave config set lua-time-limit 500
            $slave slaveof $master_host $master_port

            # Wait for the slave to be online
            wait_for_condition 500 100 {
                [lindex [$slave role] 3] eq {connected}
            } else {
                fail "Replica still not connected after some time"
            }

            # Wait some time to make sure the master is sending data
            # to the slave.
            after 5000

            # Stop the ability of the slave to process data by sendig
            # a script that will put it in BUSY state.
            $slave eval {for i=1,3000000000 do end} 0

            # Wait some time again so that more master stream will
            # be processed.
            after 2000

            # Stop the write load
            stop_write_load $load_handle0

            # number of keys
			wait_for_condition 500 100 {
				[$master dbsize] eq [$slave dbsize] && [$master dbsize] > 0
			} else {
				fail "Different datasets between replica and master"
			}
			swap_data_comp $master $slave
        }
    }
}

test {slave fails full sync and diskless load swapdb recovers it} {
    start_server {tags {"repl"}} {
        set slave [srv 0 client]
        set slave_host [srv 0 host]
        set slave_port [srv 0 port]
        set slave_log [srv 0 stdout]
        start_server {overrides {swap-repl-rordb-sync no}} {
            set master [srv 0 client]
            set master_host [srv 0 host]
            set master_port [srv 0 port]

            # Put different data sets on the master and slave
            # we need to put large keys on the master since the slave replies to info only once in 2mb
            $slave debug populate 2000 slave 10
            $master config set rdbcompression no
            $master debug populate 200 master 100000

            # Set master and slave to use diskless replication
            $master config set repl-diskless-sync yes
            $master config set repl-diskless-sync-delay 0
            $slave config set repl-diskless-load swapdb

            # Set master with a slow rdb generation, so that we can easily disconnect it mid sync
            # 10ms per key, with 200 keys is 2 seconds
            $master config set rdb-key-save-delay 10000

            # Start the replication process...
            $slave slaveof $master_host $master_port

            # wait for the slave to start reading the rdb
            wait_for_condition 500 100 {
                [s -1 loading] eq 1
            } else {
                fail "Replica didn't get into loading mode"
            }

            # make sure that next sync will not start immediately so that we can catch the slave in betweeen syncs
            $master config set repl-diskless-sync-delay 5
            # for faster server shutdown, make rdb saving fast again (the fork is already uses the slow one)
            $master config set rdb-key-save-delay 0

            # waiting slave to do flushdb (key count drop)
            wait_for_condition 50 100 {
                2000 != [scan [regexp -inline {keys\=([\d]*)} [$slave info keyspace]] keys=%d]
            } else {
                fail "Replica didn't flush"
            }

            # make sure we're still loading
            assert_equal [s -1 loading] 1

            # kill the slave connection on the master
            set killed [$master client kill type slave]

            # wait for loading to stop (fail)
            wait_for_condition 50 100 {
                [s -1 loading] eq 0
            } else {
                fail "Replica didn't disconnect"
            }

            # make sure the original keys were restored
            assert_equal [$slave dbsize] 2000
        }
    }
}

# get current stime and utime metrics for a thread (since it's creation)
proc get_cpu_metrics { statfile } {
    if { [ catch {
        set fid   [ open $statfile r ]
        set data  [ read $fid 1024 ]
        ::close $fid
        set data  [ split $data ]

        ;## number of jiffies it has been scheduled...
        set utime [ lindex $data 13 ]
        set stime [ lindex $data 14 ]
    } err ] } {
        error "assertion:can't parse /proc: $err"
    }
    set mstime [clock milliseconds]
    return [ list $mstime $utime $stime ]
}

# compute %utime and %stime of a thread between two measurements
proc compute_cpu_usage {start end} {
    set clock_ticks [exec getconf CLK_TCK]
    # convert ms time to jiffies and calc delta
    set dtime [ expr { ([lindex $end 0] - [lindex $start 0]) * double($clock_ticks) / 1000 } ]
    set utime [ expr { [lindex $end 1] - [lindex $start 1] } ]
    set stime [ expr { [lindex $end 2] - [lindex $start 2] } ]
    set pucpu  [ expr { ($utime / $dtime) * 100 } ]
    set pscpu  [ expr { ($stime / $dtime) * 100 } ]
    return [ list $pucpu $pscpu ]
}


# test diskless rdb pipe with multiple replicas, which may drop half way
start_server {tags {"repl" "nosanitizer"} overrides {swap-repl-rordb-sync no}} {
    set master [srv 0 client]
    $master config set repl-diskless-sync yes
    $master config set repl-diskless-sync-delay 1
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set master_pid [srv 0 pid]
    # put enough data in the db that the rdb file will be bigger than the socket buffers
    # and since we'll have key-load-delay of 100, 20000 keys will take at least 2 seconds
    # we also need the replica to process requests during transfer (which it does only once in 2mb)
    $master config set rdbcompression no
    $master debug populate 20000 test 10000
    # If running on Linux, we also measure utime/stime to detect possible I/O handling issues
    set os [catch {exec uname}]
    set measure_time [expr {$os == "Linux"} ? 1 : 0]
    foreach all_drop {no slow fast all timeout} {
        test "diskless $all_drop replicas drop during rdb pipe" {
            set replicas {}
            set replicas_alive {}
            # start one replica that will read the rdb fast, and one that will be slow
            start_server {} {
                lappend replicas [srv 0 client]
                lappend replicas_alive [srv 0 client]
                start_server {} {
                    lappend replicas [srv 0 client]
                    lappend replicas_alive [srv 0 client]

                    # start replication
                    # it's enough for just one replica to be slow, and have it's write handler enabled
                    # so that the whole rdb generation process is bound to that
                    set loglines [count_log_lines -1]
                    [lindex $replicas 0] config set repl-diskless-load swapdb
                    [lindex $replicas 0] config set key-load-delay 100 ;# 20k keys and 100 microseconds sleep means at least 2 seconds
                    [lindex $replicas 0] replicaof $master_host $master_port
                    [lindex $replicas 1] replicaof $master_host $master_port

                    # wait for the replicas to start reading the rdb
                    # using the log file since the replica only responds to INFO once in 2mb
                    wait_for_log_messages -1 {"*Loading DB in memory*"} $loglines 800 10

                    if {$measure_time} {
                        set master_statfile "/proc/$master_pid/stat"
                        set master_start_metrics [get_cpu_metrics $master_statfile]
                        set start_time [clock seconds]
                    }

                    # wait a while so that the pipe socket writer will be
                    # blocked on write (since replica 0 is slow to read from the socket)
                    after 500

                    # add some command to be present in the command stream after the rdb.
                    $master incr $all_drop

                    # disconnect replicas depending on the current test
                    set loglines [count_log_lines -2]
                    if {$all_drop == "all" || $all_drop == "fast"} {
                        exec kill [srv 0 pid]
                        set replicas_alive [lreplace $replicas_alive 1 1]
                    }
                    if {$all_drop == "all" || $all_drop == "slow"} {
                        exec kill [srv -1 pid]
                        set replicas_alive [lreplace $replicas_alive 0 0]
                    }
                    if {$all_drop == "timeout"} {
                        $master config set repl-timeout 2
                        # we want the slow replica to hang on a key for very long so it'll reach repl-timeout
                        exec kill -SIGSTOP [srv -1 pid]
                        after 2000
                    }

                    # wait for rdb child to exit
                    wait_for_condition 500 100 {
                        [s -2 rdb_bgsave_in_progress] == 0
                    } else {
                        fail "rdb child didn't terminate"
                    }

                    # make sure we got what we were aiming for, by looking for the message in the log file
                    if {$all_drop == "all"} {
                        wait_for_log_messages -2 {"*Diskless rdb transfer, last replica dropped, killing fork child*"} $loglines 1 1
                    }
                    if {$all_drop == "no"} {
                        wait_for_log_messages -2 {"*Diskless rdb transfer, done reading from pipe, 2 replicas still up*"} $loglines 1 1
                    }
                    if {$all_drop == "slow" || $all_drop == "fast"} {
                        wait_for_log_messages -2 {"*Diskless rdb transfer, done reading from pipe, 1 replicas still up*"} $loglines 1 1
                    }
                    if {$all_drop == "timeout"} {
                        wait_for_log_messages -2 {"*Disconnecting timedout replica (full sync)*"} $loglines 1 1
                        wait_for_log_messages -2 {"*Diskless rdb transfer, done reading from pipe, 1 replicas still up*"} $loglines 1 1
                        # master disconnected the slow replica, remove from array
                        set replicas_alive [lreplace $replicas_alive 0 0]
                        # release it
                        exec kill -SIGCONT [srv -1 pid]
                    }

                    # make sure we don't have a busy loop going thought epoll_wait
                    if {$measure_time} {
                        set master_end_metrics [get_cpu_metrics $master_statfile]
                        set time_elapsed [expr {[clock seconds]-$start_time}]
                        set master_cpu [compute_cpu_usage $master_start_metrics $master_end_metrics]
                        set master_utime [lindex $master_cpu 0]
                        set master_stime [lindex $master_cpu 1]
                        if {$::verbose} {
                            puts "elapsed: $time_elapsed"
                            puts "master utime: $master_utime"
                            puts "master stime: $master_stime"
                        }
                        if {!$::no_latency && ($all_drop == "all" || $all_drop == "slow" || $all_drop == "timeout")} {
                            assert {$master_utime < 70}
                            assert {$master_stime < 70}
                        }
                        if {!$::no_latency && ($all_drop == "none" || $all_drop == "fast")} {
                            assert {$master_utime < 15}
                            assert {$master_stime < 15}
                        }
                    }

                    # verify the data integrity
                    foreach replica $replicas_alive {
                        # Wait that replicas acknowledge they are online so
                        # we are sure that DBSIZE and DEBUG DIGEST will not
                        # fail because of timing issues.
                        wait_for_condition 150 100 {
                            [lindex [$replica role] 3] eq {connected}
                        } else {
                            fail "replicas still not connected after some time"
                        }

                        # Make sure that replicas and master have same
                        # number of keys
                        wait_for_condition 50 100 {
                            [$master dbsize] == [$replica dbsize]
                        } else {
                            fail "Different number of keys between master and replicas after too long time."
                        }

                        set dbsize [$master dbsize]
                        set dbsize0 [$replica dbsize]
                        assert {$dbsize > 0}
                        assert {$dbsize eq $dbsize0}
                    }
                }
            }
        }
    }
}

test "diskless replication child being killed is collected" {
    # when diskless master is waiting for the replica to become writable
    # it removes the read event from the rdb pipe so if the child gets killed
    # the replica will hung. and the master may not collect the pid with waitpid
    start_server {tags {"repl"} overrides {swap-repl-rordb-sync no}} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set master_pid [srv 0 pid]
        $master config set repl-diskless-sync yes
        $master config set repl-diskless-sync-delay 0
        # put enough data in the db that the rdb file will be bigger than the socket buffers
        $master debug populate 20000 test 10000
        $master config set rdbcompression no
        start_server {} {
            set replica [srv 0 client]
            set loglines [count_log_lines 0]
            $replica config set repl-diskless-load swapdb
            $replica config set key-load-delay 1000000
            $replica replicaof $master_host $master_port

            # wait for the replicas to start reading the rdb
            wait_for_log_messages 0 {"*Loading DB in memory*"} $loglines 800 10

            # wait to be sure the eplica is hung and the master is blocked on write
            after 500

            # simulate the OOM killer or anyone else kills the child
            set fork_child_pid [get_child_pid -1]
            exec kill -9 $fork_child_pid

            # wait for the parent to notice the child have exited
            wait_for_condition 50 100 {
                [s -1 rdb_bgsave_in_progress] == 0
            } else {
                fail "rdb child didn't terminate"
            }
        }
    }
}

test "diskless replication read pipe cleanup" {
    # In diskless replication, we create a read pipe for the RDB, between the child and the parent.
    # When we close this pipe (fd), the read handler also needs to be removed from the event loop (if it still registered).
    # Otherwise, next time we will use the same fd, the registration will be fail (panic), because
    # we will use EPOLL_CTL_MOD (the fd still register in the event loop), on fd that already removed from epoll_ctl
    start_server {tags {"repl" "nosanitizer"} overrides {swap-repl-rordb-sync no}} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set master_pid [srv 0 pid]
        $master config set repl-diskless-sync yes
        $master config set repl-diskless-sync-delay 0

        # put enough data in the db, and slowdown the save, to keep the parent busy at the read process
        $master config set rdb-key-save-delay 100000
        $master config set rdbcompression no
        $master debug populate 20000 test 10000
        start_server {} {
            set replica [srv 0 client]
            set loglines [count_log_lines 0]
            $replica config set repl-diskless-load swapdb
            $replica replicaof $master_host $master_port

            # wait for the replicas to start reading the rdb
            wait_for_log_messages 0 {"*Loading DB in memory*"} $loglines 800 10

            set loglines [count_log_lines 0]
            # send FLUSHALL so the RDB child will be killed
            $master flushall

            # wait for another RDB child process to be started
            wait_for_log_messages -1 {"*Background RDB transfer started by pid*"} $loglines 800 10

            # make sure master is alive
            $master ping
        }
    }
}

test {Kill rdb child process if its dumping RDB is not useful} {
    start_server {tags {"repl" "nosanitizer"}} {
        set slave1 [srv 0 client]
        start_server {} {
            set slave2 [srv 0 client]
			# disable rordb to test rdb related feature.
            start_server {overrides {swap-repl-rordb-sync no}} {
                set master [srv 0 client]
                set master_host [srv 0 host]
                set master_port [srv 0 port]
                for {set i 0} {$i < 10} {incr i} {
                    $master set $i $i
                }
                # Generating RDB will cost 10s(10 * 1s)
                $master config set rdb-key-save-delay 1000000
                $master config set repl-diskless-sync no
                $master config set save ""

                $slave1 slaveof $master_host $master_port
                $slave2 slaveof $master_host $master_port

                # Wait for starting child
                wait_for_condition 50 100 {
                    ([s 0 rdb_bgsave_in_progress] == 1) &&
                    ([string match "*wait_bgsave*" [s 0 slave0]]) &&
                    ([string match "*wait_bgsave*" [s 0 slave1]])
                } else {
                    fail "rdb child didn't start"
                }

                # Slave1 disconnect with master
                $slave1 slaveof no one
                # Shouldn't kill child since another slave wait for rdb
                after 100
                assert {[s 0 rdb_bgsave_in_progress] == 1}

                # Slave2 disconnect with master
                $slave2 slaveof no one
                # Should kill child
                wait_for_condition 100 10 {
                    [s 0 rdb_bgsave_in_progress] eq 0
                } else {
                    fail "can't kill rdb child"
                }

                # If have save parameters, won't kill child
                $master config set save "900 1"
                $slave1 slaveof $master_host $master_port
                $slave2 slaveof $master_host $master_port
                wait_for_condition 50 100 {
                    ([s 0 rdb_bgsave_in_progress] == 1) &&
                    ([string match "*wait_bgsave*" [s 0 slave0]]) &&
                    ([string match "*wait_bgsave*" [s 0 slave1]])
                } else {
                    fail "rdb child didn't start"
                }
                $slave1 slaveof no one
                $slave2 slaveof no one
                after 200
                assert {[s 0 rdb_bgsave_in_progress] == 1}
                catch {$master shutdown nosave}
            }
        }
    }
}

test {swap on rocksdb flush and crash} {
    start_server {tags "repl"} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        start_server {} {
            r config set swap-debug-before-exec-swap-delay-micro 2000000
            r config set swap-debug-init-rocksdb-delay-micro 2000000
            r config set swap-rocksdb-stats-collect-interval-ms 1000
            after 1000; # wait for rocksdb-stats-fresh job starting
            r slaveof $master_host $master_port
            wait_for_condition 100 5000 {
                [lindex [r role] 0] eq {slave} && [string match {*master_link_status:up*} [r info replication]]
            } else {
                fail "repl fail"
            }
            r ping
        }
    }
}
