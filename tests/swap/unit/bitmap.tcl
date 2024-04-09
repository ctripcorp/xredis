proc set_small_bitmap {}  {
    # only one subkey
    r config set swap-debug-evict-keys 0
    r setbit small_bitmap 0 1
}

proc build_pure_hot_small_bitmap {}  {
    # each fragment need to set 1 bit, for bitcount test 
    r flushdb
    set_small_bitmap
    assert [bitmap_object_is_pure_hot r mybitmap1]
}

proc build_hot_small_bitmap {}  {
    # build hot data
    build_pure_hot_small_bitmap
	r swap.evict small_bitmap
    wait_key_cold r mybitmap1
    assert_equal {1} [r bitcount small_bitmap]
    assert [object_is_hot r mybitmap1]
}

proc build_cold_small_bitmap {}  {
    # build cold data
	build_pure_hot_small_bitmap
    r swap.evict small_bitmap
    wait_key_cold r mybitmap1
    assert [object_is_cold r mybitmap1]
}

proc check_small_bitmap_getbit0 {}  {
    # normal getbit 
    assert_equal {1} [r getbit small_bitmap 0]
}

proc check_small_bitmap_getbit1 {}  {
    # abnormal getbit 
    assert_equal {ERR bit offset is not an integer or out of range} [r getbit small_bitmap -1]
}

proc check_small_bitmap_bitcount0 {}  {
    # normal bitcount
    assert_equal {1} [r bitcount small_bitmap 0 0]
}

proc check_small_bitmap_bitcount1 {}  {
    # normal bitcount
    assert_equal {1} [r bitcount small_bitmap]
}

proc check_small_bitmap_bitcount2 {}  {
    # abnormal bitcount
    assert_equal {1} [r bitcount small_bitmap 0 -2]
}

proc check_small_bitmap_bitcount3 {}  {
    # abnormal bitcount
    assert_equal {1} [r bitcount small_bitmap -1 2]
}

proc check_small_bitmap_bitpos0 {}  {
    assert_equal {0} [r bitpos small_bitmap 1 0]
}

proc check_small_bitmap_bitpos1 {}  {
    assert_equal {-1} [r bitpos small_bitmap 1 1]
}

proc check_small_bitmap_bitpos2 {}  {
    assert_equal {0} [r bitpos small_bitmap 1 -1]
}

proc check_small_bitmap_bitpos3 {}  {
    assert_equal {1} [r bitpos small_bitmap 0 0]
}

proc check_small_bitmap_bitpos4 {}  {
    assert_equal {-1} [r bitpos small_bitmap 0 1]
}

proc check_small_bitmap_bitpos5 {}  {
    assert_equal {1} [r bitpos small_bitmap 0 -1]
}

proc check_small_bitmap_bitpos6 {}  {
    assert_equal {0} [r bitpos small_bitmap 1 0 2]
}

proc check_small_bitmap_bitpos7 {}  {
    assert_equal {0} [r bitpos small_bitmap 1 -1 1]
}

proc check_small_bitmap_bitpos8 {}  {
    assert_equal {1} [r bitpos small_bitmap 0 0 2]
}

proc check_small_bitmap_bitpos9 {}  {
    assert_equal {1} [r bitpos small_bitmap 0 -1 1]
}



proc set_data0 {} {
    r config set swap-debug-evict-keys 0
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
}

proc set_data1 {} {
	# 335872 bit = 41 kb
    r config set swap-debug-evict-keys 0
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

proc build_pure_hot_data {}  {
    # each fragment need to set 1 bit, for bitcount test 
    r flushdb
    set_data0
    assert [bitmap_object_is_pure_hot r mybitmap1]
}

proc build_cold_data {}  {
    # build cold data
	build_pure_hot_data
    r swap.evict mybitmap1
    wait_key_cold r mybitmap1
    assert [object_is_cold r mybitmap1]
}

proc build_hot_data {}  {
    # build hot data
    build_cold_data
    assert_equal {11} [r bitcount mybitmap1]
    assert [object_is_hot r mybitmap1]
}

#   condition:
#  hot  ############
#  cold ###########
proc build_extend_hot_data {}  {
    # build hot data
    build_hot_data
    r setbit mybitmap1 368639 1
    assert_equal {12} [r bitcount mybitmap1]
    assert [object_is_hot r mybitmap1]
}

proc build_warm_data {}  {
    build_cold_data
    r getbit mybitmap1 32767
    assert [object_is_warm r mybitmap1]
}

#   condition:
#  hot  #_#__#_#__#
#  cold ###########
proc build_warm_with_hole0 {} {
    build_cold_data

   # press_enter_to_continue
    r getbit mybitmap1 32767
    r getbit mybitmap1 98303
    r getbit mybitmap1 196607
    r getbit mybitmap1 262143

   # press_enter_to_continue
    r getbit mybitmap1 335871
    assert [object_is_warm r mybitmap1]
}

#   condition:
#   hot  ____##_____
#   cold ###########
proc build_warm_with_hole1 {}  {
    build_cold_data
    r getbit mybitmap1 163839
    r getbit mybitmap1 196607
    assert [object_is_warm r mybitmap1]
}

#   condition:
#   hot  ____#___#_#
#   cold ###########
proc build_warm_with_hole2 {}  {
    build_cold_data
    r getbit mybitmap1 163839
    r getbit mybitmap1 294911
    r getbit mybitmap1 335871
    assert [object_is_warm r mybitmap1]
}

#   condition:
#   hot  #_#_##_____
#   cold ########### 
proc build_warm_with_hole3 {}  {
    build_cold_data
    r getbit mybitmap1 32767
    r getbit mybitmap1 98303 
    r getbit mybitmap1 163839
    r getbit mybitmap1 196607 
    assert [object_is_warm r mybitmap1]
}

#   condition:
#   hot  ________###
#   cold ########### 
proc build_warm_with_hole4 {}  {
    build_cold_data
    r getbit mybitmap1 294911
    r getbit mybitmap1 327679
    r getbit mybitmap1 335871
    assert [object_is_warm r mybitmap1]
}

#   condition:
#   hot  ##_________
#   cold ########### 
proc build_warm_with_hole5 {}  {
    build_cold_data
    r getbit mybitmap1 1
    r getbit mybitmap1 65535 
    assert [object_is_warm r mybitmap1]
}

#   condition:
#   hot  ##________#
#   cold ########### 
proc build_warm_with_hole6 {}  {
    build_cold_data
    r getbit mybitmap1 1
    r getbit mybitmap1 65535
    r getbit mybitmap1 335871
    assert [object_is_warm r mybitmap1]
}

proc check_mybitmap1_getbit0 {}  {
    # normal getbit 
    assert_equal {1} [r getbit mybitmap1 32767]
}

proc check_mybitmap1_getbit1 {}  {
    # normal getbit 
    assert_equal {1} [r getbit mybitmap1 65535]
}

proc check_mybitmap1_getbit2 {}  {
    # normal getbit 
    assert_equal {1} [r getbit mybitmap1 98303]
}

proc check_mybitmap1_getbit3 {}  {
    # normal getbit 
    assert_equal {1} [r getbit mybitmap1 131071]
}

proc check_mybitmap1_getbit4 {}  {
    # normal getbit 
    assert_equal {1} [r getbit mybitmap1 163839]
}

proc check_mybitmap1_getbit5 {}  {
    # normal getbit 
    assert_equal {1} [r getbit mybitmap1 196607]
}

proc check_mybitmap1_getbit6 {}  {
    # normal getbit 
    assert_equal {1} [r getbit mybitmap1 229375]
}

proc check_mybitmap1_getbit7 {}  {
    # normal getbit 
    assert_equal {1} [r getbit mybitmap1 262143]
}

proc check_mybitmap1_getbit8 {}  {
    # normal getbit 
    assert_equal {1} [r getbit mybitmap1 294911]
}

proc check_mybitmap1_getbit9 {}  {
    # normal getbit 
    assert_equal {1} [r getbit mybitmap1 327679]
}

proc check_mybitmap1_getbit10 {}  {
    # normal getbit 
    assert_equal {1} [r getbit mybitmap1 335871]
}

proc check_mybitmap1_getbit11 {}  {
    # normal getbit 
    assert_equal {0} [r getbit mybitmap1 335872]
}

proc check_mybitmap1_getbit12 {}  {
    # Abnormal getbit
    assert_equal {ERR bit offset is not an integer or out of range} [r getbit mybitmap1 -1]
}

proc check_mybitmap1_bitcount0 {}  {
    # normal bitcount
    assert_equal {11} [r bitcount mybitmap1]
}

proc check_mybitmap1_bitcount1 {}  {
    # normal bitcount
    assert_equal {2} [r bitcount mybitmap1 0 9216]
}

proc check_mybitmap1_bitcount2 {}  {
    # normal bitcount
    assert_equal {2} [r bitcount mybitmap1 5000 15000]
}

proc check_mybitmap1_bitcount3 {}  {
    # normal bitcount
    assert_equal {3} [r bitcount mybitmap1 9216 20480]
}

proc check_mybitmap1_bitcount4 {}  {
    # normal bitcount
    assert_equal {3} [r bitcount mybitmap1 15000 25000]
}

proc check_mybitmap1_bitcount5 {}  {
    # normal bitcount
    assert_equal {6} [r bitcount mybitmap1 20480 43008]
}

proc check_mybitmap1_bitcount6  {}  {
    # Abnormal bitcount
    assert_equal {9} [r bitcount mybitmap1 10000 2000000]
}

proc check_mybitmap1_bitcount7  {}  {
    # Abnormal bitcount
    assert_equal {9} [r bitcount mybitmap1 10000 2147483647]
}

proc check_mybitmap1_bitcount8  {}  {
    # Abnormal bitcount
    assert_equal {11} [r bitcount mybitmap1 -2147483648 2147483647]
}

proc check_mybitmap1_bitcount9  {}  {
    # Abnormal bitcount
    assert_equal {0} [r bitcount mybitmap1 2000000 10000]
}

proc check_mybitmap1_bitcount10  {}  {
    # Abnormal bitcount
    assert_equal {5} [r bitcount mybitmap1 10000 -10000]
}

proc check_mybitmap1_bitcount11  {}  {
    # Abnormal bitcount
    assert_equal {0} [r bitcount mybitmap1 -10000 10000]
}

proc check_mybitmap1_bitcount12  {}  {
    # Abnormal bitcount
    assert_equal {7} [r bitcount mybitmap1 -41984 -11984]
}

proc check_mybitmap1_bitcount13  {}  {
    # Abnormal bitcount
    assert_equal {0} [r bitcount mybitmap1 -11984 -41984]
}

proc check_mybitmap1_bitcount14  {}  {
    # Abnormal bitcount
    assert_equal {3} [r bitcount mybitmap1 -21984 -11984]
}

proc check_mybitmap1_bitpos0  {}  {
    # normal bitpos
    assert_equal {32767} [r bitpos mybitmap1 1]
    assert_equal {32767} [r bitpos mybitmap1 1 0]
    assert_equal {98303} [r bitpos mybitmap1 1 9216]
    assert_equal {196607} [r bitpos mybitmap1 1 20480]
    assert_equal {335871} [r bitpos mybitmap1 1 41983]

    assert_equal {32767} [r bitpos mybitmap1 1 0 9216]
    assert_equal {65535} [r bitpos mybitmap1 1 5000 15000]
    assert_equal {98303} [r bitpos mybitmap1 1 9216 20480]
    assert_equal {131071} [r bitpos mybitmap1 1 15000 25000]
    assert_equal {196607} [r bitpos mybitmap1 1 20480 43008]
}

proc check_mybitmap1_bitpos1  {}  {
    # normal bitpos
    assert_equal {0} [r bitpos mybitmap1 0]
    assert_equal {0} [r bitpos mybitmap1 0 0]
    assert_equal {73728} [r bitpos mybitmap1 0 9216]
    assert_equal {163840} [r bitpos mybitmap1 0 20480]
    assert_equal {335864} [r bitpos mybitmap1 0 41983]

    assert_equal {0} [r bitpos mybitmap1 0 0 9216]
    assert_equal {40000} [r bitpos mybitmap1 0 5000 15000]
    assert_equal {73728} [r bitpos mybitmap1 0 9216 20480]
    assert_equal {120000} [r bitpos mybitmap1 0 15000 25000]
    assert_equal {163840} [r bitpos mybitmap1 0 20480 43008]
}

proc check_mybitmap1_bitpos2  {}  {
    # Abnormal bitpos

    assert_equal {-1} [r bitpos mybitmap1 1 41984]
    assert_equal {-1} [r bitpos mybitmap1 1 2147483647]

    assert_equal {327679} [r bitpos mybitmap1 1 -1984]
    assert_equal {262143} [r bitpos mybitmap1 1 -11984]
    assert_equal {98303} [r bitpos mybitmap1 1 -31984]
    assert_equal {32767} [r bitpos mybitmap1 1 -41984]
    assert_equal {32767} [r bitpos mybitmap1 1 -2147483648]

    assert_equal {98303} [r bitpos mybitmap1 1 10000 2000000]
    assert_equal {98303} [r bitpos mybitmap1 1 10000 2147483647]
    assert_equal {-1} [r bitpos mybitmap1 1 2000000 10000]
    assert_equal {-1} [r bitpos mybitmap1 1 20000 10000]

    assert_equal {163839} [r bitpos mybitmap1 1 -21984 -11984]
    assert_equal {32767} [r bitpos mybitmap1 1 -41984 -11984]
    assert_equal {-1} [r bitpos mybitmap1 1 -11984 -41984]

    assert_equal {98303} [r bitpos mybitmap1 1 10000 -10000]
    assert_equal {32767} [r bitpos mybitmap1 1 -2147483648 2147483647]
    assert_equal {-1} [r bitpos mybitmap1 1 -10000 10000]
}

proc check_mybitmap1_bitpos3  {}  {
    # Abnormal bitpos

    assert_equal {-1} [r bitpos mybitmap1 0 41984]
    assert_equal {-1} [r bitpos mybitmap1 0 2147483647]

    assert_equal {320000} [r bitpos mybitmap1 0 -1984]
    assert_equal {240000} [r bitpos mybitmap1 0 -11984]
    assert_equal {80000} [r bitpos mybitmap1 0 -31984]
    assert_equal {0} [r bitpos mybitmap1 0 -41984]
    assert_equal {0} [r bitpos mybitmap1 0 -2147483648]
        
    assert_equal {80000} [r bitpos mybitmap1 0 10000 2000000]
    assert_equal {80000} [r bitpos mybitmap1 0 10000 2147483647]
    assert_equal {-1} [r bitpos mybitmap1 0 2000000 10000]
    assert_equal {-1} [r bitpos mybitmap1 0 20000 10000]

    assert_equal {160000} [r bitpos mybitmap1 0 -21984 -11984]
    assert_equal {0} [r bitpos mybitmap1 0 -41984 -11984]
    assert_equal {-1} [r bitpos mybitmap1 0 -11984 -41984]

    assert_equal {80000} [r bitpos mybitmap1 0 10000 -10000]
    assert_equal {0} [r bitpos mybitmap1 0 -2147483648 2147483647]

    assert_equal {-1} [r bitpos mybitmap1 0 -10000 10000]
}

proc check_mybitmap1_bitpos4  {}  {
    # find clear bit from out of range(when all bit 1 in range)
    assert_equal {0} [r setbit mybitmap1 335870 1]
    assert_equal {0} [r setbit mybitmap1 335869 1]
    assert_equal {0} [r setbit mybitmap1 335868 1]
    assert_equal {0} [r setbit mybitmap1 335867 1]
    assert_equal {0} [r setbit mybitmap1 335866 1]
    assert_equal {0} [r setbit mybitmap1 335865 1]
    assert_equal {0} [r setbit mybitmap1 335864 1]

    r swap.evict mybitmap1
    wait_key_cold r mybitmap1

    assert_equal {335872} [r bitpos mybitmap1 0 41983]
}

proc check_mybitmap1_bitfield  {}  {
    # normal bitfield
    assert_equal {8}  [r bitfield mybitmap1 get u4 32767]
    assert_equal {8}  [r bitfield_ro mybitmap1 get u4 65535]
}

proc check_mybitmap1_bitop  {}  {

    r setbit src1 32767 1
    r setbit src2 335871 1
    r swap.evict src1
    wait_key_cold r src1
    r swap.evict src2
    wait_key_cold r src2
    assert_equal {41984} [r bitop XOR dest mybitmap1 src1 src2]
    assert_equal {9} [r bitcount dest]
}

proc check_mybitmap2  {}  {
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

proc check_mybitmap1_is_right {} {
    set_data1
    assert_equal {41984} [r bitop XOR dest mybitmap1 mybitmap2]

   # press_enter_to_continue

    assert_equal {11} [r bitcount mybitmap1 0 41983]
    assert_equal {11} [r bitcount mybitmap2]
    assert_equal {0} [r bitcount dest]
}

start_server {
    tags {"bitmap swap"}
}   {
    r config set swap-debug-evict-keys 0

    test {pure hot full swap out} {
        r flushdb

        build_pure_hot_data
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1

        check_mybitmap1_is_right

        r flushdb
    }

    test {pure hot non full swap out} {
        r flushdb

        set bak_evict_step [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 5

        build_pure_hot_data

        r swap.evict mybitmap1
        r swap.evict mybitmap1
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1
        
        check_mybitmap1_is_right

        r flushdb

        r config set swap-evict-step-max-subkeys $bak_evict_step
    }

    test {hot full swap out} {
        r flushdb

		build_hot_data
        assert [object_is_hot r mybitmap1]

        r swap.evict mybitmap1
        wait_key_cold r mybitmap1

        check_mybitmap1_is_right

        r flushdb
    }

    test {hot non full swap out} {
        r flushdb

        set bak_evict_step [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 5

		build_hot_data
        # assert [object_is_hot r mybitmap1]

        r swap.evict mybitmap1
        r swap.evict mybitmap1
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1

        check_mybitmap1_is_right

        r flushdb

        r config set swap-evict-step-max-subkeys $bak_evict_step
    }

#  hot  swap in  ??

    test {warm full swap out} {
        r flushdb

        for {set i 0} {$i <= 6} {incr i} {
            set data_str "build_warm_with_hole"

            append data_str $i
            
            eval $data_str

            r swap.evict mybitmap1
            wait_key_cold r mybitmap1

            check_mybitmap1_is_right

            r flushdb
        }

    }

    test {warm non full swap out} {
        r flushdb
        set bak_evict_step [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 5

        for {set i 0} {$i <= 6} {incr i} {
            r flushdb

            set data_str "build_warm_with_hole"

            append data_str $i
            
            eval $data_str

            r swap.evict mybitmap1
            r swap.evict mybitmap1
            r swap.evict mybitmap1
            wait_key_cold r mybitmap1

            check_mybitmap1_is_right

            r flushdb
        }

        r config set swap-evict-step-max-subkeys $bak_evict_step
    }

    test {warm getbit} {
        r flushdb

        for {set i 0} {$i <= 6} {incr i} {

            set data_str "build_warm_with_hole"

            append data_str $i
            
            for {set j 0} {$j <= 11} {incr j} {

                eval $data_str

                set cmd_str "check_mybitmap1_getbit"

                append cmd_str $j

                eval $cmd_str

                check_mybitmap1_is_right

                r flushdb
            }
        }
    }

    test {warm bitcount} {
        r flushdb

        for {set i 0} {$i <= 6} {incr i} {

            set data_str "build_warm_with_hole"

            append data_str $i
            
            for {set j 0} {$j <= 14} {incr j} {

                eval $data_str

                set cmd_str "check_mybitmap1_bitcount"

                append cmd_str $j

                eval $cmd_str

                check_mybitmap1_is_right

                r flushdb
            }
        }
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
        wait_key_cold r mykey
        assert_equal "TP" [r getrange mykey 0 1]
        assert_equal "\x01" [r getrange mykey 10239 10239]
        r swap.evict mykey
        wait_key_cold r mykey
        assert_equal 6 [r bitcount mykey]
    }

    test "SETBIT against non-existing key" {
        r del mykey
        assert_equal 0 [r setbit mykey 1 1]
        r swap.evict mykey
        wait_key_cold r mykey
        assert_equal [binary format B* 01000000] [r get mykey]
    }

    test "SETBIT against string-encoded key" {
        # Ascii "@" is integer 64 = 01 00 00 00
        r set mykey "@"
        r swap.evict mykey
        wait_key_cold r mykey

        assert_equal 0 [r setbit mykey 2 1]
        assert_equal [binary format B* 01100000] [r get mykey]
        assert_equal 1 [r setbit mykey 1 0]
        assert_equal [binary format B* 00100000] [r get mykey]
    }

    test "SETBIT against integer-encoded key" {
        # Ascii "1" is integer 49 = 00 11 00 01
        r set mykey 1
        r swap.evict mykey
        wait_key_cold r mykey
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
        wait_key_cold r mykey
        assert_error "WRONGTYPE*" {r setbit mykey 0 1}
    }
}

proc build_pure_hot_rdb  {}  {
    r config set swap-debug-evict-keys 0
    r flushdb
    r setbit mybitmap1 0 1
    r setbit mybitmap1 335871 1
    r setbit mybitmap2 0 1
    r setbit mybitmap2 335871 1
    assert_equal [bitmap_object_is_pure_hot r mybitmap1]
}

proc build_extend_hot_rdb  {}  {
    r config set swap-debug-evict-keys 0
    r flushdb
    r setbit mybitmap1 0 1
    r setbit mybitmap1 335871 1
    r swap.evict mybitmap1
    wait_key_cold r mybitmap1
    r bitcount mybitmap1
    r setbit mybitmap1 368639 1

    r setbit mybitmap2 0 1
    r setbit mybitmap2 335871 1
    r swap.evict mybitmap2
    wait_key_cold r mybitmap2
    r bitcount mybitmap2
    r setbit mybitmap2 368639 1
    assert_equal [object_is_hot r mybitmap1]
}

proc build_warm_rdb  {}  {
    r config set swap-debug-evict-keys 0
    r flushdb
    # mybitmap 41kb
    r setbit mybitmap1 0 1
    r setbit mybitmap1 335871 1
    r swap.evict mybitmap1
    wait_key_cold r mybitmap1
    r bitcount mybitmap1
    r setbit mybitmap1 368639 1

    r setbit mybitmap2 0 1
    r setbit mybitmap2 335871 1
    r swap.evict mybitmap2
    wait_key_cold r mybitmap2
    r bitcount mybitmap2
    r setbit mybitmap2 368639 1
    assert_equal [object_is_warm r mybitmap1]
}

proc build_cold_rdb  {}  {
    r config set swap-debug-evict-keys 0
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
    assert_equal [object_is_cold r mybitmap1]
}

start_server {
    tags {"bitmap rdb"}
}   {
    test {pure hot rdbsave and rdbload} {
        # mybitmap 41kb
        build_pure_hot_data
        r debug reload
        # check_data
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
        build_extend_hot_data

        r debug reload
        # check_data
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {46080} [r bitop not dest mybitmap1]
        assert_equal {12} [r bitcount mybitmap1]
        r del mybitmap1
        r del mybitmap2
        assert_equal {0} [r bitcount mybitmap1]
    }

    test {cold rdbsave and rdbload} {

        # mybitmap 41kb
        build_cold_data

        # check_data
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
        r debug reload
        # check_data
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]

        r del mybitmap1
        r del mybitmap2
    }

    test {cold rdbsave and rdbload with bitmap-subkey-size exchange 4096 to 2048} {
        # mybitmap 41kb
        build_cold_data

        r SAVE
        r CONFIG SET bitmap-subkey-size 2048
        r DEBUG RELOAD NOSAVE
        # check_data
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
        r CONFIG SET bitmap-subkey-size 4096
    }

    test {cold rdbsave and rdbload with bitmap-subkey-size exchange 2048 to 4096} {
        # mybitmap 41kb
        build_cold_data

        r CONFIG SET bitmap-subkey-size 2048
        r SAVE
        r CONFIG SET bitmap-subkey-size 4096
        r DEBUG RELOAD NOSAVE
        # check_data
        assert_equal {1} [r getbit mybitmap1 32767]
        assert_equal {1} [r getbit mybitmap1 65535]
        assert_equal {41984} [r bitop not dest mybitmap1]
        assert_equal {11} [r bitcount mybitmap1]
    }
}

start_server {tags {"bitmap chaos test"} overrides {save ""}} {
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

                        set count 0
                        set mybitmap "mybitmap-[randomInt $bitmaps]"
                        # set mybitmap_len [$r1 llen $mybitmap]
                        set mybitmap_len [expr {[$r1 strlen $mybitmap] * 8}]

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
                            $r1 BITFIELD_RO $mybitmap get u4 $randIdx
                        } {
                            $r1 BITOP NOT dest $mybitmap
                        } {
                            set randVal [randomInt 2]
                            $r1 BITPOS $mybitmap $randVal
                        } {
                            set randVal [randomInt 2]
                            set randIdx [randomInt $bitmap_max_length]
                            $r1 BITPOS $mybitmap $randVal $randIdx
                        } {
                            set randVal [randomInt 2]
                            set randIdx1 [randomInt $bitmap_max_length]
                            set randIdx2 [randomInt $bitmap_max_length]
                            $r1 BITPOS $mybitmap $randVal $randIdx1 $randIdx2
                        } {
                            set randIdx [randomInt $bitmap_max_length]
                            set randVal [randomInt 2]
                            $r1 SETBIT $mybitmap $randIdx $randVal
                        } {
                            set randIdx [randomInt $bitmap_max_length]
                            $r1 GETBIT $mybitmap $randIdx
                        } {
                            $r1 swap.evict $mybitmap
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