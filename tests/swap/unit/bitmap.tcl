proc set_data r {
	# 335872 bit = 41 kb
    r setbit mybitmap1 32767 1
    r setbit mybitmap1 65535 1
    r setbit mybitmap1 98303 1
    r setbit mybitmap1 131071 1
    r setbit mybitmap1 163839 1
    r setbit mybitmap1 196607 1
    r setbit mybitmap1 229375 1
    r setbit mybitmap1 262143 1
    r setbit mybitmap1 294911 1
    r setbit mybitmap1 327679 1
    r setbit mybitmap1 335871 1

    r setbit mybitmap2 32767 1
    r setbit mybitmap2 65535 1
    r setbit mybitmap2 98303 1
    r setbit mybitmap2 131071 1
    r setbit mybitmap2 163839 1
    r setbit mybitmap2 196607 1
    r setbit mybitmap2 229375 1
    r setbit mybitmap2 262143 1
    r setbit mybitmap2 294911 1
    r setbit mybitmap2 327679 1
    r setbit mybitmap2 335871 1
}

proc build_pure_hot_data r {
    # each fragment need to set 1 bit, for bitcount test 
    r flushdb
    set_data r
}

proc build_hot_data r {
    # build hot data
    build_pure_hot_data r
	r swap.evict mybitmap1
    assert_equal {11} [r bitcount mybitmap1]
}

proc build_extend_hot_data r {
    # build hot data
    build_pure_hot_data r
	r swap.evict mybitmap1
    assert_equal {11} [r bitcount mybitmap1]
    r setbit mybitmap1 368639 1
}

proc build_cold_data r {
    # build cold data
	build_hot_data r
    r swap.evict mybitmap1
}

proc build_warm_data r {
    build_cold_data r
    r getbit mybitmap1 32767
}

proc check_data r {
    assert_equal {1} [r getbit mybitmap1 98303]
    assert_equal {1} [r getbit mybitmap1 131071]
    assert_equal {1} [r getbit mybitmap1 163839]
    assert_equal {1} [r getbit mybitmap1 196607]
    assert_equal {1} [r getbit mybitmap1 229375]
    assert_equal {1} [r getbit mybitmap1 262143]
    assert_equal {1} [r getbit mybitmap1 294911]
    assert_equal {1} [r getbit mybitmap1 327679]
    assert_equal {1} [r getbit mybitmap1 335871]
    
    assert_equal {2} [r bitcount mybitmap1 0 9216]
    assert_equal {3} [r bitcount mybitmap1 9216 20480]
    assert_equal {6} [r bitcount mybitmap1 20480 43008]

    assert_equal {8}  [r bitfield mybitmap1 get u4 32767]
    assert_equal {8}  [r bitfield_ro mybitmap1 get u4 65535]

    assert_equal {32767} [r bitpos mybitmap1 1 0]
    assert_equal {98303} [r bitpos mybitmap1 1 9216]
    assert_equal {196607} [r bitpos mybitmap1 1 20480]
    assert_equal {196607} [r bitpos mybitmap1 1 20480 43008]
    assert_equal {98303} [r bitpos mybitmap1 1 10000 2000000]
    assert_equal {98303} [r bitpos mybitmap1 1 10000 -10000]
    assert_equal {98303} [r bitpos mybitmap1 1 10000 2147483647]
    assert_equal {32767} [r bitpos mybitmap1 1 -2147483648 2147483647]

    assert_equal {-1} [r bitpos mybitmap1 1 2000000 10000]
    assert_equal {-1} [r bitpos mybitmap1 1 -10000 10000]

    assert_equal {1} [r getbit mybitmap2 32767]
    assert_equal {1} [r getbit mybitmap2 65535]
    assert_equal {1} [r getbit mybitmap2 98303]
    assert_equal {1} [r getbit mybitmap2 131071]
    assert_equal {1} [r getbit mybitmap2 163839]
    assert_equal {1} [r getbit mybitmap2 196607]
    assert_equal {1} [r getbit mybitmap2 229375]
    assert_equal {1} [r getbit mybitmap2 262143]
    assert_equal {1} [r getbit mybitmap2 294911]
    assert_equal {1} [r getbit mybitmap2 327679]
    assert_equal {1} [r getbit mybitmap2 335871]
    assert_equal {0} [r getbit mybitmap2 2147483647]
    
    assert_equal {11} [r bitcount mybitmap2]
    assert_equal {8}  [r bitfield mybitmap2 get u4 32767]
    assert_equal {8}  [r bitfield_ro mybitmap2 get u4 65535]
    assert_equal {41984} [r bitop not dest mybitmap2]
    assert_equal {65535} [r bitpos mybitmap2 1 8191]

}

start_server {
    tags {"bitmap swap"}
}   {
    test {pure hot full and non full swap out} {
        build_pure_hot_data r
        r swap.evict mybitmap1

        check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]

        set bak_evict_step [lindex [r config get swap-evict-step-max-memory] 1]
        # swap-evict-step-max-memory 5kb
        r config set swap-evict-step-max-memory 5120
        r swap.evict mybitmap1
        
        check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
        r del mybitmap1
        r del mybitmap2

        r config set swap-evict-step-max-memory $bak_evict_step
    }

    test {hot full swap out} {
        # build hot data
		build_hot_data r
        assert [object_is_hot r mybitmap1]

        r swap.evict mybitmap1

		check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
        r del mybitmap1
        r del mybitmap2
        assert_equal {0} [r bitcount mybitmap1 0]
    }

    test {hot non full swap out} {
        # build hot data
        set bak_evict_step [lindex [r config get swap-evict-step-max-memory] 1]
        # swap fragment 5kb
        r config set swap-evict-step-max-memory 5120
        # build hot data
		build_hot_data r
        assert [object_is_hot r mybitmap1]

        r swap.evict mybitmap1

		check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
        r del mybitmap1
        r del mybitmap2
        assert_equal {0} [r bitcount mybitmap1 0]

        r config set swap-evict-step-max-memory $bak_evict_step
    }

    test {warm non full swap out} {
        build_warm_data r
        assert_equal {1} [r getbit mybitmap1 163839]

        # swap in fragment {0, 3}
        assert_equal {0} [r getbit mybitmap1 0]
        assert [object_is_warm r mybitmap1]
        # fragment {0, 3} is warm
        r swap.evict mybitmap1

		check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
        r del mybitmap1
        r del mybitmap2
        assert_equal {0} [r getbit mybitmap1 0]

        r config set swap-evict-step-max-memory $bak_evict_step
    }

    test {warm full swap out} {
        build_warm_data r
        r config set swap-evict-step-max-memory $bak_evict_step
        assert_equal {1} [r getbit mybitmap1 163839]
        assert [object_is_warm r mybitmap1]

        # swap in fragment {0, 3}
        assert_equal {0} [r getbit mybitmap1 0]
        assert [object_is_warm r mybitmap1]
        # fragment {0, 3} is warm
        r swap.evict mybitmap1

        r del mybitmap1
        r del mybitmap2
        assert_equal {0} [r bitcount mybitmap1]
    }

    test {warm non full swap in} {
        build_warm_data r
        assert_equal {1} [r getbit mybitmap1 163839]
        # fragment {0, 1, 2} is cold
        assert_equal {11} [r bitcount mybitmap1]
		check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
        r del mybitmap1
        r del mybitmap2
        assert_equal {0} [r bitcount mybitmap1]
        r config set swap-evict-step-max-memory $bak_evict_step
    }

    test {warm full swap in} {
        # build mybitmap1 20kb size, count by bit
        # 5 fragment
        build_warm_data r
        r setbit mybitmap1 163839 1
        r setbit mybitmap1 0 1
        r swap.evict mybitmap1
        assert_equal {1} [r getbit mybitmap1 163839]
        # fragment {0, 1, 2, 3} is cold
        assert_equal {12} [r bitcount mybitmap1]
        r del mybitmap1
        r del mybitmap2
        assert_equal {0} [r bitcount mybitmap1]
    }

    test {cold full swap in} {
        build_cold_data r
        assert_equal {11} [r bitcount mybitmap1]
		check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
        r del mybitmap1
        r del mybitmap2
        assert_equal {0} [r bitcount mybitmap1]
    }

    test {cold non full swap in} {
        build_cold_data r

        assert_equal {0} [r getbit mybitmap1 0]
        assert [object_is_warm r mybitmap1]
		check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
        r del mybitmap1
        r del mybitmap2
        assert_equal {0} [r bitcount mybitmap1]
    }
}

start_server  {
    tags {"bitmap to string"}
}  {
    test "GETBIT against string-encoded key" {
        r setbit mykey 81919 1

        r setbit mykey 1 1
        r setbit mykey 3 1
        r setbit mykey 5 1
        r setbit mykey 9 1
        r setbit mykey 11 1

        r swap.evict mykey
        assert_equal "TP" [r getrange mykey 0 1]
        assert_equal "\x01" [r getrange mykey 10239 10239]
        r swap.evict mykey
        assert_equal 6 [r bitcount mykey]
    }

    test "SETBIT against non-existing key" {
        r del mykey
        assert_equal 0 [r setbit mykey 1 1]
        r swap.evict mykey
        assert_equal [binary format B* 01000000] [r get mykey]
    }

    test "SETBIT against string-encoded key" {
        # Ascii "@" is integer 64 = 01 00 00 00
        r set mykey "@"
        r swap.evict mykey

        assert_equal 0 [r setbit mykey 2 1]
        assert_equal [binary format B* 01100000] [r get mykey]
        assert_equal 1 [r setbit mykey 1 0]
        assert_equal [binary format B* 00100000] [r get mykey]
    }

    test "SETBIT against integer-encoded key" {
        # Ascii "1" is integer 49 = 00 11 00 01
        r set mykey 1
        r swap.evict mykey
        assert_encoding int mykey

        assert_equal 0 [r setbit mykey 6 1]
        assert_equal [binary format B* 00110011] [r get mykey]
        assert_equal 1 [r setbit mykey 2 0]
        assert_equal [binary format B* 00010011] [r get mykey]
    }

    test "SETBIT against key with wrong type" {
        r del mykey
        r lpush mykey "foo"
        r swap.evict mykey
        assert_error "WRONGTYPE*" {r setbit mykey 0 1}
    }
}

proc build_pure_hot_rdb r {
    r flushdb
    r setbit mybitmap1 0 1
    r setbit mybitmap1 335871 1
    r setbit mybitmap2 0 1
    r setbit mybitmap2 335871 1
}

proc build_extend_hot_rdb r {
    r flushdb
    r setbit mybitmap1 0 1
    r setbit mybitmap1 335871 1
    r swap.evict mybitmap1
    r bitcount mybitmap1
    r setbit mybitmap1 368639 1

    r setbit mybitmap2 0 1
    r setbit mybitmap2 335871 1
    r swap.evict mybitmap2
    r bitcount mybitmap2
    r setbit mybitmap2 368639 1
}

proc build_warm_rdb r {
    r flushdb
    # mybitmap 41kb
    r setbit mybitmap1 0 1
    r setbit mybitmap1 335871 1
    r swap.evict mybitmap1
    r bitcount mybitmap1
    r setbit mybitmap1 368639 1

    r setbit mybitmap2 0 1
    r setbit mybitmap2 335871 1
    r swap.evict mybitmap2
    r bitcount mybitmap2
    r setbit mybitmap2 368639 1
}

proc build_cold_rdb r {
    r flushdb
    r setbit mybitmap1 0 1
    r setbit mybitmap1 335871 1
    r swap.evict mybitmap1
    r bitcount mybitmap1
    r setbit mybitmap1 368639 1

    r setbit mybitmap2 0 1
    r setbit mybitmap2 335871 1
    r swap.evict mybitmap2
    r bitcount mybitmap2
    r setbit mybitmap2 368639 1
    r debug reload
}

start_server {
    tags {"bitmap rdb"}
}   {
    test {pure hot rdbsave and rdbload} {
        # mybitmap 41kb
        build_pure_hot_data r
        r debug reload
        check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
        r del mybitmap1
        r del mybitmap2
        assert_equal {0} [r bitcount mybitmap1]
    }

    test {extend hot rdbsave and rdbload} {
        # mybitmap 41kb
        build_extend_hot_data r

        r debug reload
        check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {46080} [r bitop not dest mybitmap1]
        assert_equal {12} [r bitcount mybitmap1]
        r del mybitmap1
        r del mybitmap2
        assert_equal {0} [r bitcount mybitmap1]
    }

    test {cold rdbsave and rdbload} {
        set bak_evict_step [lindex [r config get swap-evict-step-max-memory] 1]
        # swap-evict-step-max-memory 1kb
        r config set swap-evict-step-max-memory 1024
        # mybitmap 41kb
        build_cold_data r

        check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
        r debug reload
        check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]

        r del mybitmap1
        r del mybitmap2
    }

    test {cold rdbsave and rdbload with bitmap-subkey-size exchange 4096 to 2048} {
        # mybitmap 41kb
        build_cold_data r

        r SAVE
        r CONFIG SET bitmap-subkey-size 2048
        r DEBUG RELOAD NOSAVE
        check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
        r CONFIG SET bitmap-subkey-size 4096
    }

    test {cold rdbsave and rdbload with bitmap-subkey-size exchange 2048 to 4096} {
        # mybitmap 41kb
        build_cold_data r

        r CONFIG SET bitmap-subkey-size 2048
        r SAVE
        r CONFIG SET bitmap-subkey-size 4096
        r DEBUG RELOAD NOSAVE
        check_data r
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
    }
}

start_server {
    tags {"bitmap chaos test"}
}  {
    start_server {overrides {save ""}} {
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set master [srv 0 client]
        set slave_host [srv -1 host]
        set slave_port [srv -1 port]
        set slave [srv -1 client]

        $slave slaveof $master_host $master_port
        wait_for_sync $slave

        test {swap-bitmap chaos} {
            set rounds 5
            set loaders 5
            set duration 30
            set bitmaps 4; # NOTE: keep it equal to bitmaps in run load below

            for {set round 0} {$round < $rounds} {incr round} {
                puts "chaos load $bitmaps bitmaps with $loaders loaders in $duration seconds ($round/$rounds)"

                # load with chaos bitmap operations
                for {set loader 0} {$loader < $loaders} {incr loader} {
                    lappend load_handles [start_run_load $master_host $master_port $duration 0 {
                        set bitmaps 4
                        # in bit
                        set bitmap_max_length 335872
                        set block_timeout 0.1
                        set trim_shink_max 4

                        set count 0
                        set mybitmap "mybitmap-[randomInt $bitmaps]"
                        # set mybitmap_len [$r1 llen $mybitmap]
                        set mybitmap_len [expr {[$r1 strlen $mybitmap] * 8}]

                        set mybitmap_start [randomInt $trim_shink] 
                        set mybitmap_stop [expr $mybitmap_len - [randomInt $trim_shink]]
                        set otherbitmap "mybitmap-[randomInt $bitmaps]"
                        set src_direction [randpath {return LEFT} {return RIGHT}]
                        set dst_direction [randpath {return LEFT} {return RIGHT}]

                        randpath {
                            set randIdx1 [randomInt $bitmap_max_length]
                            set randIdx2 [randomInt $bitmap_max_length]
                            $r1 BITCOUNT $mybitmap $randIdx1 $randIdx2
                        } {
                            set randIdx [randomInt $bitmap_max_length]
                            $r1 BITFIELD $mybitmap get u4 $randIdx
                        } {
                            set randIdx [randomInt $bitmap_max_length]
                            set randVal [randomInt 2]
                            $r1 BITFIELD_RO $mybitmap set u4 $randIdx $randVal
                        } {
                            $r1 BITOP NOT dest $mybitmap
                        } {
                            $r1 BITPOS $mybitmap
                        } {
                            set randIdx [randomInt $bitmap_max_length]
                            $r1 BITPOS $mybitmap $randIdx
                        } {
                            set randIdx1 [randomInt $bitmap_max_length]
                            set randIdx2 [randomInt $bitmap_max_length]
                            $r1 BITPOS $mybitmap $randIdx1 $randIdx2
                        } {
                            set randIdx [randomInt $bitmap_max_length]
                            set randVal [randomInt 2]
                            $r1 SETBIT $mybitmap $randIdx $randVal
                        } {
                            set randIdx [randomInt $bitmap_max_length]
                            $r1 GETBIT $mybitmap $randIdx
                        } {
                            $r1 r swap.evict $mybitmap
                        } {
                            $r1 GET $mybitmap
                        } {
                            $r1 DEL $mybitmap
                        } {
                            $r1 UNLINK $mybitmap
                        }
                    }]

                }

                after [expr $duration*1000]
                wait_load_handlers_disconnected

                wait_for_ofs_sync $master $slave

                # save to check bitmap meta consistency
                $master save
                $slave save
                verify_log_message 0 "*DB saved on disk*" 0
                verify_log_message -1 "*DB saved on disk*" 0

                # digest to check master slave consistency
                for {set keyidx 0} {$keyidx < $bitmaps} {incr keyidx} {
                    set master_digest [$master debug digest-value mybitmap-$keyidx]
                    set slave_digest [$slave debug digest-value mybitmap-$keyidx]
                    assert_equal $master_digest $slave_digest
                }
            }
        }
    }
}