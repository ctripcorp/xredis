proc build_pure_hot_small_bitmap {small_bitmap}  {
    # each fragment need to set 1 bit, for bitcount test 
    r setbit $small_bitmap 0 1
    assert [bitmap_object_is_pure_hot r $small_bitmap]
}

proc build_hot_small_bitmap {small_bitmap}  {
    # build hot data
    build_pure_hot_small_bitmap $small_bitmap
	r swap.evict $small_bitmap
    wait_key_cold r $small_bitmap
    assert_equal {1} [r bitcount $small_bitmap]
    assert [object_is_hot r $small_bitmap]
}

proc build_extend_hot_small_bitmap {small_bitmap}  {
    # build hot data
    build_pure_hot_small_bitmap $small_bitmap
	r swap.evict $small_bitmap
    wait_key_cold r $small_bitmap
    assert_equal {1} [r bitcount $small_bitmap]
    r setbit $small_bitmap 15 1
    assert [object_is_hot r $small_bitmap]
}

proc build_cold_small_bitmap {small_bitmap}  {
    # build cold data
	build_pure_hot_small_bitmap $small_bitmap
    r swap.evict $small_bitmap
    wait_key_cold r $small_bitmap
    assert [object_is_cold r $small_bitmap]
}

array set small_bitmap1_getbit {
    {0} {assert_equal {1} [r getbit small_bitmap1 0]}
    {1} {assert_equal {0} [r getbit small_bitmap1 10]}
    {2} {assert_error "ERR*" {r getbit small_bitmap1 -1}}
}

array set small_bitmap1_bitcount {
    {0} {assert_equal {1} [r bitcount small_bitmap1 0 0]}
    {1} {assert_equal {1} [r bitcount small_bitmap1]}
    {2} {assert_equal {1} [r bitcount small_bitmap1 0 -2]}
    {3} {assert_equal {1} [r bitcount small_bitmap1 -1 2]}
}

array set small_bitmap1_bitpos {
    {0} {assert_equal {0} [r bitpos small_bitmap1 1 0]}
    {1} {assert_equal {-1} [r bitpos small_bitmap1 1 1]}
    {2} {assert_equal {0} [r bitpos small_bitmap1 1 -1]}
    {3} {assert_equal {1} [r bitpos small_bitmap1 0 0]}
    {4} {assert_equal {-1} [r bitpos small_bitmap1 0 1]}
    {5} {assert_equal {1} [r bitpos small_bitmap1 0 -1]}
    {6} {assert_equal {0} [r bitpos small_bitmap1 1 0 2]}
    {7} {assert_equal {0} [r bitpos small_bitmap1 1 -1 1]}
    {8} {assert_equal {1} [r bitpos small_bitmap1 0 0 2]}
    {9} {assert_equal {1} [r bitpos small_bitmap1 0 -1 1]}
}

proc check_small_bitmap_bitop {small_bitmap}  {
    r setbit src1 0 1
    r setbit src2 2 1
    after 100
    r swap.evict src1
    wait_key_cold r src1
    after 100
    r swap.evict src2
    wait_key_cold r src2
    assert_equal {1} [r bitop XOR $small_bitmap $small_bitmap src1 src2]
    assert_equal {1} [r bitcount $small_bitmap]
}

proc check_extend_small_bitmap1_bitop  {}  {
    r setbit src1 0 1
    r setbit src2 2 1
    after 100
    r swap.evict src1
    wait_key_cold r src1
    after 100
    r swap.evict src2
    wait_key_cold r src2
    assert_equal {1} [r bitop XOR small_bitmap1 src1 src2]
    assert_equal {2} [r bitcount small_bitmap1]
}

proc check_small_bitmap1_is_right {} {
    r setbit small_bitmap0 0 1
    assert_equal {1} [r bitop XOR dest small_bitmap1 small_bitmap0]
    assert_equal {1} [r bitcount small_bitmap1 0 0]
    assert_equal {1} [r bitcount small_bitmap0]
    assert_equal {0} [r bitcount dest]

}

proc check_extend_small_bitmap1_is_right {} {
    r setbit small_bitmap0 0 1
    assert_equal {2} [r bitop XOR dest small_bitmap1 small_bitmap0]
    assert_equal {2} [r bitcount small_bitmap1]
    assert_equal {1} [r bitcount small_bitmap0]
    assert_equal {1} [r bitcount dest]
}

proc set_data {bitmap} {
	# 335872 bit = 41 kb
    r setbit $bitmap 32767 1
    r setbit $bitmap 65535 1
    r setbit $bitmap 98303 1
    r setbit $bitmap 131071 1
    r setbit $bitmap 163839 1
    r setbit $bitmap 196607 1
    r setbit $bitmap 229375 1
    r setbit $bitmap 262143 1
    r setbit $bitmap 294911 1
    r setbit $bitmap 327679 1
    r setbit $bitmap 335871 1
}

proc check_mybitmap_is_right {mybitmap} {
    set_data mybitmap0
    assert_equal {41984} [r bitop XOR dest $mybitmap mybitmap0]
    assert_equal {11} [r bitcount $mybitmap 0 41983]
    assert_equal {11} [r bitcount mybitmap0]
    assert_equal {0} [r bitcount dest]
}


proc check_extend_mybitmap1_is_right {} {
    set_data mybitmap0
    assert_equal {46080} [r bitop XOR dest mybitmap1 mybitmap0]
    assert_equal {12} [r bitcount mybitmap1]
    assert_equal {11} [r bitcount mybitmap0]
    assert_equal {1} [r bitcount dest]
}

proc build_pure_hot_data {bitmap}  {
    # each fragment need to set 1 bit, for bitcount test 
    set_data $bitmap
    assert [bitmap_object_is_pure_hot r $bitmap]
}

proc build_cold_data {mybitmap}  {
    # build cold data
	build_pure_hot_data $mybitmap
    r swap.evict $mybitmap
    wait_key_cold r $mybitmap
    assert [object_is_cold r $mybitmap]
}

proc build_hot_data {mybitmap}  {
    # build hot data
    build_cold_data $mybitmap
    assert_equal {11} [r bitcount $mybitmap]
    assert [object_is_hot r $mybitmap]
}

#   condition:
#  hot  ############
#  cold ###########
proc build_extend_hot_data {mybitmap}  {
    # build hot data
    build_hot_data $mybitmap
    r setbit $mybitmap 368639 1
    assert_equal {12} [r bitcount $mybitmap]
    assert [object_is_hot r $mybitmap]
}

proc build_warm_data {mybitmap}  {
    build_cold_data $mybitmap
    r getbit $mybitmap 32767
    assert [object_is_warm r $mybitmap]
}

set build_warm_with_hole {
    #   condition:
    #  hot  #_#__#_#__#
    #  cold ###########
    {1} {
        build_cold_data mybitmap1
        r getbit mybitmap1 32767
        r getbit mybitmap1 98303
        r getbit mybitmap1 196607
        r getbit mybitmap1 262143
        r getbit mybitmap1 335871
        assert [object_is_warm r mybitmap1]
    }

    #   condition:
    #   hot  ____##_____
    #   cold ###########
    {2}  {
        build_cold_data mybitmap1
        r getbit mybitmap1 163839
        r getbit mybitmap1 196607
        assert [object_is_warm r mybitmap1]
    }

    #   condition:
    #   hot  ____#___#_#
    #   cold ###########
    {3}  {
        build_cold_data mybitmap1
        r getbit mybitmap1 163839
        r getbit mybitmap1 294911
        r getbit mybitmap1 335871
        assert [object_is_warm r mybitmap1]
    }

    #   condition:
    #   hot  #_#_##_____
    #   cold ########### 
    {4}  {
        build_cold_data mybitmap1
        r getbit mybitmap1 32767
        r getbit mybitmap1 98303 
        r getbit mybitmap1 163839
        r getbit mybitmap1 196607 
        assert [object_is_warm r mybitmap1]
    }

    #   condition:
    #   hot  ________###
    #   cold ########### 
    {5}  {
        build_cold_data mybitmap1
        r getbit mybitmap1 294911
        r getbit mybitmap1 327679
        r getbit mybitmap1 335871
        assert [object_is_warm r mybitmap1]
    }

    #   condition:
    #   hot  ##_________
    #   cold ########### 
    {6}  {
        build_cold_data mybitmap1
        r getbit mybitmap1 1
        r getbit mybitmap1 65535 
        assert [object_is_warm r mybitmap1]
    }

    #   condition:
    #   hot  ##________#
    #   cold ########### 
    {7}  {
        build_cold_data mybitmap1
        r getbit mybitmap1 1
        r getbit mybitmap1 65535
        r getbit mybitmap1 335871
        assert [object_is_warm r mybitmap1]
    }
}

set check_mybitmap1_setbit {
    {0}  {
        # normal setbit 
        assert_equal {1} [r setbit mybitmap1 98303 0]
        set_data mybitmap0
        assert_equal {41984} [r bitop XOR dest mybitmap1 mybitmap0]
        assert_equal {10} [r bitcount mybitmap1]
        assert_equal {11} [r bitcount mybitmap0]
        assert_equal {1} [r bitcount dest]
    }

    {1}  {
        # normal setbit 
        assert_equal {0} [r setbit mybitmap1 344063 1]
        set_data mybitmap0
        assert_equal {43008} [r bitop XOR dest mybitmap1 mybitmap0]
        assert_equal {12} [r bitcount mybitmap1]
        assert_equal {11} [r bitcount mybitmap0]
        assert_equal {1} [r bitcount dest]
    }

    {2}  {
        # normal setbit 
        assert_equal {0} [r setbit mybitmap1 368639 1]
        set_data mybitmap0
        assert_equal {46080} [r bitop XOR dest mybitmap1 mybitmap0]
        assert_equal {12} [r bitcount mybitmap1]
        assert_equal {11} [r bitcount mybitmap0]
        assert_equal {1} [r bitcount dest]
    }

    {3}  {
        # normal setbit 
        assert_equal {1} [r setbit mybitmap1 335871 0]
        set_data mybitmap0
        assert_equal {41984} [r bitop XOR dest mybitmap1 mybitmap0]
        assert_equal {10} [r bitcount mybitmap1]
        assert_equal {11} [r bitcount mybitmap0]
        assert_equal {1} [r bitcount dest]
    }

    {4}  {
        # Abnormal setbit
        assert_error "ERR*" {r setbit mybitmap1 -1 1}
        check_mybitmap_is_right mybitmap1
    }
}

array set check_mybitmap1_getbit {
    {1} {assert_equal {1} [r getbit mybitmap1 32767]}
    {2} {assert_equal {1} [r getbit mybitmap1 65535]}
    {3} {assert_equal {1} [r getbit mybitmap1 98303]}
    {4} {assert_equal {1} [r getbit mybitmap1 131071]}
    {5} {assert_equal {1} [r getbit mybitmap1 163839]}
    {6} {assert_equal {1} [r getbit mybitmap1 196607]}
    {7} {assert_equal {1} [r getbit mybitmap1 229375]}
    {8} {assert_equal {1} [r getbit mybitmap1 262143]}
    {9} {assert_equal {1} [r getbit mybitmap1 294911]}
    {10} {assert_equal {1} [r getbit mybitmap1 327679]}
    {11} {assert_equal {1} [r getbit mybitmap1 335871]}
    {12} {assert_equal {0} [r getbit mybitmap1 335872]}
    {13} {assert_error "ERR*" {r getbit mybitmap1 -1}}
    {14} {assert_equal {0} [r getbit mybitmap1 2147483647]}
}

array set check_mybitmap1_bitcount {
    {1} {assert_equal {11} [r bitcount mybitmap1]}
    {2} {assert_equal {2} [r bitcount mybitmap1 0 9216]}
    {3} {assert_equal {2} [r bitcount mybitmap1 5000 15000]}
    {4} {assert_equal {3} [r bitcount mybitmap1 9216 20480]}
    {5} {assert_equal {3} [r bitcount mybitmap1 15000 25000]}
    {6} {assert_equal {6} [r bitcount mybitmap1 20480 43008]}
    {7} {assert_equal {9} [r bitcount mybitmap1 10000 2000000]}
    {8} {assert_equal {9} [r bitcount mybitmap1 10000 2147483647]}
    {9} {assert_equal {11} [r bitcount mybitmap1 -2147483648 2147483647]}
    {10} {assert_equal {0} [r bitcount mybitmap1 2000000 10000]}
    {11} {assert_equal {5} [r bitcount mybitmap1 10000 -10000]}
    {12} {assert_equal {0} [r bitcount mybitmap1 -10000 10000]}
    {13} {assert_equal {7} [r bitcount mybitmap1 -41984 -11984]}
    {14} {assert_equal {0} [r bitcount mybitmap1 -11984 -41984]}
    {15} {assert_equal {3} [r bitcount mybitmap1 -21984 -11984]}
}

array set check_mybitmap1_bitpos {
    {1} {assert_equal {32767} [r bitpos mybitmap1 1]}
    {2} {assert_equal {32767} [r bitpos mybitmap1 1 0]}
    {3} {assert_equal {98303} [r bitpos mybitmap1 1 9216]}
    {4} {assert_equal {196607} [r bitpos mybitmap1 1 20480]}
    {5} {assert_equal {335871} [r bitpos mybitmap1 1 41983]}
    {6} {assert_equal {32767} [r bitpos mybitmap1 1 0 9216]}
    {7} {assert_equal {65535} [r bitpos mybitmap1 1 5000 15000]}
    {8} {assert_equal {98303} [r bitpos mybitmap1 1 9216 20480]}
    {9} {assert_equal {131071} [r bitpos mybitmap1 1 15000 25000]}
    {10} {assert_equal {196607} [r bitpos mybitmap1 1 20480 43008]}
    {11} {assert_equal {0} [r bitpos mybitmap1 0]}
    {12} {assert_equal {0} [r bitpos mybitmap1 0 0]}
    {13} {assert_equal {73728} [r bitpos mybitmap1 0 9216]}
    {14} {assert_equal {163840} [r bitpos mybitmap1 0 20480]}
    {15} {assert_equal {335864} [r bitpos mybitmap1 0 41983]}
    {16} {assert_equal {0} [r bitpos mybitmap1 0 0 9216]}
    {17} {assert_equal {40000} [r bitpos mybitmap1 0 5000 15000]}
    {18} {assert_equal {73728} [r bitpos mybitmap1 0 9216 20480]}
    {19} {assert_equal {120000} [r bitpos mybitmap1 0 15000 25000]}
    {20} {assert_equal {163840} [r bitpos mybitmap1 0 20480 43008]}
    {21} {assert_equal {-1} [r bitpos mybitmap1 1 41984]}
    {22} {assert_equal {-1} [r bitpos mybitmap1 1 2147483647]}
    {23} {assert_equal {327679} [r bitpos mybitmap1 1 -1984]}
    {24} {assert_equal {262143} [r bitpos mybitmap1 1 -11984]}
    {25} {assert_equal {98303} [r bitpos mybitmap1 1 -31984]}
    {26} {assert_equal {32767} [r bitpos mybitmap1 1 -41984]}
    {27} {assert_equal {32767} [r bitpos mybitmap1 1 -2147483648]}
    {28} {assert_equal {98303} [r bitpos mybitmap1 1 10000 2000000]}
    {29} {assert_equal {98303} [r bitpos mybitmap1 1 10000 2147483647]}
    {30} {assert_equal {-1} [r bitpos mybitmap1 1 2000000 10000]}
    {31} {assert_equal {-1} [r bitpos mybitmap1 1 20000 10000]}
    {32} {assert_equal {163839} [r bitpos mybitmap1 1 -21984 -11984]}
    {33} {assert_equal {32767} [r bitpos mybitmap1 1 -41984 -11984]}
    {34} {assert_equal {-1} [r bitpos mybitmap1 1 -11984 -41984]}
    {35} {assert_equal {98303} [r bitpos mybitmap1 1 10000 -10000]}
    {36} {assert_equal {32767} [r bitpos mybitmap1 1 -2147483648 2147483647]}
    {37} {assert_equal {-1} [r bitpos mybitmap1 1 -10000 10000]}
    {38} {assert_equal {-1} [r bitpos mybitmap1 0 41984]}
    {39} {assert_equal {-1} [r bitpos mybitmap1 0 2147483647]}
    {40} {assert_equal {320000} [r bitpos mybitmap1 0 -1984]}
    {41} {assert_equal {240000} [r bitpos mybitmap1 0 -11984]}
    {42} {assert_equal {80000} [r bitpos mybitmap1 0 -31984]}
    {43} {assert_equal {0} [r bitpos mybitmap1 0 -41984]}
    {44} {assert_equal {0} [r bitpos mybitmap1 0 -2147483648]}
    {45} {assert_equal {80000} [r bitpos mybitmap1 0 10000 2000000]}
    {46} {assert_equal {80000} [r bitpos mybitmap1 0 10000 2147483647]}
    {47} {assert_equal {-1} [r bitpos mybitmap1 0 2000000 10000]}
    {48} {assert_equal {-1} [r bitpos mybitmap1 0 20000 10000]}
    {49} {assert_equal {160000} [r bitpos mybitmap1 0 -21984 -11984]}
    {50} {assert_equal {0} [r bitpos mybitmap1 0 -41984 -11984]}
    {51} {assert_equal {-1} [r bitpos mybitmap1 0 -11984 -41984]}
    {52} {assert_equal {80000} [r bitpos mybitmap1 0 10000 -10000]}
    {53} {assert_equal {0} [r bitpos mybitmap1 0 -2147483648 2147483647]}
    {54} {assert_equal {-1} [r bitpos mybitmap1 0 -10000 10000]}
}

proc check_mybitmap1_bitpos  {}  {
    # find clear bit from out of range(when all bit 1 in range)
    assert_equal {0} [r setbit mybitmap1 335870 1]
    assert_equal {0} [r setbit mybitmap1 335869 1]
    assert_equal {0} [r setbit mybitmap1 335868 1]
    assert_equal {0} [r setbit mybitmap1 335867 1]
    assert_equal {0} [r setbit mybitmap1 335866 1]
    assert_equal {0} [r setbit mybitmap1 335865 1]
    assert_equal {0} [r setbit mybitmap1 335864 1]
    assert_equal {335872} [r bitpos mybitmap1 0 41983]
}

proc build_bitmap_args {} {
    r setbit src1 32767 1
    r setbit src2 335871 1
    after 100
    r swap.evict src1
    wait_key_cold r src1
    after 100
    r swap.evict src2
    wait_key_cold r src2
}

proc check_mybitmap1_bitop_xor_3_bitmap  {}  {
    build_bitmap_args
    assert_equal {41984} [r bitop XOR mybitmap1 mybitmap1 src1 src2]
    assert_equal {9} [r bitcount mybitmap1]
}

proc check_mybitmap1_bitop_xor_2_bitmap  {}  {
    build_bitmap_args
    assert_equal {41984} [r bitop XOR mybitmap1 src1 src2]
    assert_equal {2} [r bitcount mybitmap1]
}

proc check_extend_mybitmap1_xor_3_bitmap  {}  {
    build_bitmap_args
    assert_equal {46080} [r bitop XOR mybitmap1 mybitmap1 src1 src2]
    assert_equal {10} [r bitcount mybitmap1]
}

start_server  {
    tags {"bitmap string switch"}
}  {
    r config set swap-debug-evict-keys 0

    test "cold bitmap to string checking character" { 
        r setbit mykey 81919 1
        r setbit mykey 1 1
        r setbit mykey 3 1
        r setbit mykey 5 1
        r setbit mykey 9 1
        r setbit mykey 11 1
        r swap.evict mykey
        wait_key_cold r mykey
        assert_equal "TP" [r getrange mykey 0 1]
        assert [object_is_string r mykey] 
        assert_equal "\x01" [r getrange mykey 10239 10239]
        r swap.evict mykey
        wait_key_cold r mykey
        assert_equal {6} [r bitcount mykey]
        set bitmap_to_string_count0 [get_info_property r Swap bitmap_string_switched_count bitmap_to_string_count]
        assert_equal {1} $bitmap_to_string_count0
        assert_equal $bitmap_to_string_count0 [get_info_property r Swap bitmap_string_switched_count string_to_bitmap_count]
        r flushdb
    }

    test "warm bitmap to string checking character" {
        r setbit mykey 81919 1
        r setbit mykey 1 1
        r setbit mykey 3 1
        r setbit mykey 5 1
        r setbit mykey 9 1
        r setbit mykey 11 1

        r swap.evict mykey
        wait_key_cold r mykey
        r getbit mykey 40000
        assert_equal "TP" [r getrange mykey 0 1]
        assert [object_is_string r mykey] 
        assert_equal "\x01" [r getrange mykey 10239 10239]
        r swap.evict mykey
        wait_key_cold r mykey
        assert_equal {6} [r bitcount mykey]
        r flushdb
    }

    test "hot bitmap to string checking character" {
        r setbit mykey 81919 1
        r setbit mykey 1 1
        r setbit mykey 3 1
        r setbit mykey 5 1
        r setbit mykey 9 1
        r setbit mykey 11 1
        assert_equal "TP" [r getrange mykey 0 1]
        assert [object_is_string r mykey] 
        assert_equal "\x01" [r getrange mykey 10239 10239]
        r swap.evict mykey
        wait_key_cold r mykey
        assert_equal {6} [r bitcount mykey]
        r flushdb
    }

    test "cold bitmap to string checking binary" {
        assert_equal {0} [r setbit mykey 1 1]
        r swap.evict mykey
        wait_key_cold r mykey
        assert_equal [binary format B* 01000000] [r get mykey]
        assert [object_is_string r mykey] 
        assert_equal {1} [r bitcount mykey]
        r flushdb
    }

    test "hot bitmap to string checking binary" {
        assert_equal {0} [r setbit mykey 1 1]
        assert_equal [binary format B* 01000000] [r get mykey]
        assert [object_is_string r mykey] 
        assert_equal {1} [r bitcount mykey]
        r flushdb
    }

    test "cold string to bitmap checking character" {
        # cold string to bitmap
        # Ascii "@" is integer 64 = 01 00 00 00
        r set mykey "@"
        r swap.evict mykey
        wait_key_cold r mykey
        assert_equal {0} [r setbit mykey 2 1]
        assert [object_is_bitmap r mykey]
        assert [bitmap_object_is_pure_hot r mykey]
        assert_equal [binary format B* 01100000] [r get mykey]
        assert_equal {1} [r setbit mykey 1 0]
        assert_equal [binary format B* 00100000] [r get mykey]
        assert_equal {1} [r bitcount mykey]
        r flushdb
    }

    test "cold string to bitmap checking binary" {
        # Ascii "1" is integer 49 = 00 11 00 01
        r set mykey 1
        r swap.evict mykey
        wait_key_cold r mykey
        assert_encoding int mykey
        assert_equal {0} [r setbit mykey 6 1]
        assert [object_is_bitmap r mykey]
        assert [bitmap_object_is_pure_hot r mykey]
        assert_equal [binary format B* 00110011] [r get mykey]
        assert_equal {1} [r setbit mykey 2 0]
        assert_equal [binary format B* 00010011] [r get mykey]
        assert_equal {3} [r bitcount mykey]
        r flushdb
    }

    test "hot string to bitmap checking binary" {
        # Ascii "1" is integer 49 = 00 11 00 01
        r set mykey 1
        assert_encoding int mykey
        assert_equal {0} [r setbit mykey 6 1]
        assert [object_is_bitmap r mykey]
        assert [bitmap_object_is_pure_hot r mykey]
        assert_equal [binary format B* 00110011] [r get mykey]
        assert_equal {1} [r setbit mykey 2 0]
        assert_equal [binary format B* 00010011] [r get mykey]
        assert_equal {3} [r bitcount mykey]
        r flushdb
    }

    test "SETBIT operate wrong type" {
        r lpush mykey "foo"
        r swap.evict mykey
        wait_key_cold r mykey
        assert_error "WRONGTYPE*" {r setbit mykey 0 1}
        r flushdb
    }

    test "wrong cmd operate bitmap" {
        r setbit mykey 0 1
        r swap.evict mykey
        assert_error "WRONGTYPE*" {r lpush mykey "foo"}
        r flushdb
    }
}

start_server {
    tags {"bitmap generic operate test"}
}   {
    r config set swap-debug-evict-keys 0
    test {bitmap del} {
        r flushdb
        build_pure_hot_data mybitmap1
        r del mybitmap1
        assert_equal [r exists mybitmap1] 0
        build_cold_data mybitmap1
        r del mybitmap1
        assert_equal [r exists mybitmap1] 0
        r flushdb
    }

    test {bitmap active expire cold} {
        r flushdb
        build_cold_data mybitmap1
        r pexpire mybitmap1 10
        after 100
        assert_equal [r ttl mybitmap1] -2
        r flushdb
    }

    test {bitmap active expire hot} {
        r flushdb
        build_hot_data mybitmap1
        r pexpire mybitmap1 10
        after 100
        assert_equal [r ttl mybitmap1] -2
        r flushdb
    }

    test {bitmap lazy expire cold} {
        r flushdb
        r debug set-active-expire 0
        build_cold_data mybitmap1
        r pexpire mybitmap1 10
        after 100
        assert_equal [r ttl mybitmap1] -2
        r flushdb
    }

    test {bitmap lazy expire hot} {
        r flushdb
        build_hot_data mybitmap1
        r pexpire mybitmap1 10
        after 100
        assert_equal [r ttl mybitmap1] -2
        assert_equal [r del mybitmap1] 0
        r flushdb
        r debug set-active-expire 1
    }

    test {bitmap lazy del obsoletes rocksdb data} {
        r flushdb
        build_cold_data mybitmap1
        set meta_raw [r swap rio-get meta [r swap encode-meta-key mybitmap1]]
        assert {$meta_raw != {{}}}
        r del mybitmap1
        assert_equal [r exists mybitmap1] 0
        assert_equal [r bitcount mybitmap1] 0
        set meta_raw [r swap rio-get meta [r swap encode-meta-key mybitmap1]]
        assert {$meta_raw == {{}}}
        r flushdb
    }

    test {bitmap evict clean triggers no swap} {
        r flushdb
        build_hot_data mybitmap1
        set old_swap_out_count [get_info_property r Swap swap_OUT count]
        r swap.evict mybitmap1
        assert ![object_is_dirty r mybitmap1]
        # evict clean hash triggers no swapout
        assert_equal $old_swap_out_count [get_info_property r Swap swap_OUT count]
        r flushdb
    }

    test {bitmap dirty & meta} {
        r flushdb
        set old_swap_max_subkeys [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 6
        # initialized as dirty
        build_pure_hot_data mybitmap1
        assert [object_is_dirty r mybitmap1]
        # partial evict remains dirty
        after 100
        r swap.evict mybitmap1
        wait_key_warm r mybitmap1
        assert [object_is_dirty r mybitmap1]
        assert_equal [object_meta_pure_cold_subkeys_num r mybitmap1] 6
        # dirty bighash all evict
        after 100
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1
        assert ![object_is_dirty r mybitmap1]
        assert_equal [object_meta_pure_cold_subkeys_num r mybitmap1] 11
        # cold data turns clean when swapin
        assert_equal {2} [r bitcount mybitmap1 0 9216]
        assert ![object_is_dirty r mybitmap1]
        assert_equal [object_meta_pure_cold_subkeys_num r mybitmap1] 8
        # clean bighash all swapin remains clean
        assert_equal {11} [r bitcount mybitmap1]
        assert ![object_is_dirty r mybitmap1]
        # all-swapin meta remains
        assert_equal [object_meta_pure_cold_subkeys_num r mybitmap1] 0
        # clean bighash swapout does not triggers swap
        set orig_swap_out_count [get_info_property r Swap swap_OUT count]
        after 100
        r swap.evict mybitmap1
        assert ![object_is_dirty r mybitmap1]
        assert_equal $orig_swap_out_count [get_info_property r Swap swap_OUT count]
        assert_equal [object_meta_pure_cold_subkeys_num r mybitmap1] 6
        assert_equal [r getbit mybitmap1 0] {0}
        # modify bitmap makes it dirty
        assert_equal [r setbit mybitmap1 0 1] {0}
        assert [object_is_dirty r mybitmap1]
        # dirty bitmap evict triggers swapout
        after 100
        assert_equal [r swap.evict mybitmap1] 1
        after 100
        assert ![object_is_dirty r mybitmap1]
        assert_equal [r bitcount mybitmap1] 12
        assert {[get_info_property r Swap swap_OUT count] > $orig_swap_out_count}
        r config set swap-evict-step-max-subkeys $old_swap_max_subkeys
        r flushdb
    }

}


start_server {
    tags {"small bitmap swap"}
}   {
    r config set swap-debug-evict-keys 0

    test {small_bitmap pure hot full swap out} {
        r flushdb
        build_pure_hot_small_bitmap small_bitmap1
        r swap.evict small_bitmap1
        wait_key_cold r small_bitmap1
        check_small_bitmap1_is_right
        r flushdb
    }

    test {small_bitmap hot full swap out} {
        r flushdb
        build_hot_small_bitmap small_bitmap1
        r swap.evict small_bitmap1
        wait_key_cold r small_bitmap1
        check_small_bitmap1_is_right
        r flushdb
    }

    test {small_bitmap extend hot full swap out} {
        r flushdb
        build_extend_hot_small_bitmap small_bitmap1
        assert [object_is_hot r small_bitmap1]
        r swap.evict small_bitmap1
        wait_key_cold r small_bitmap1
        check_extend_small_bitmap1_is_right
        r flushdb
    }

    test {small_bitmap pure hot getbit} {
        foreach {key value} [array get small_bitmap1_getbit] {
            build_pure_hot_small_bitmap small_bitmap1
            eval $value
            check_small_bitmap1_is_right
            r flushdb
        }
    }

    test {small_bitmap pure hot bitcount} {
        foreach {key value} [array get small_bitmap1_bitcount] {
            build_pure_hot_small_bitmap small_bitmap1
            eval $value
            check_small_bitmap1_is_right
            r flushdb
        }
    }

    test {small_bitmap pure hot bitpos} {
        foreach {key value} [array get small_bitmap1_bitpos] {
            build_pure_hot_small_bitmap small_bitmap1
            eval $value
            check_small_bitmap1_is_right
            r flushdb
        }
    }

    test {small_bitmap pure hot bitfield} {
        build_pure_hot_small_bitmap small_bitmap1
        assert_equal {3}  [r BITFIELD small_bitmap1 INCRBY u2 0 1]
        assert_equal {2} [r bitcount small_bitmap1]
        r flushdb
    }

    test {small_bitmap pure hot bitop overwrite} {
        build_pure_hot_small_bitmap small_bitmap1
        check_small_bitmap_bitop small_bitmap1
        r flushdb
    }

    test {small_bitmap pure hot bitop} {
            
        build_pure_hot_small_bitmap small_bitmap1

        r setbit src1 0 1
        r setbit src2 2 1

        after 100
        r swap.evict src1
        wait_key_cold r src1

        after 100
        r swap.evict src2
        wait_key_cold r src2
        assert_equal {1} [r bitop XOR small_bitmap1 src1 src2]
        assert_equal {2} [r bitcount small_bitmap1]

        r flushdb
    }

    test {small_bitmap hot getbit} {
        foreach {key value} [array get small_bitmap1_getbit] {
            build_hot_small_bitmap small_bitmap1
            eval $value
            check_small_bitmap1_is_right
            r flushdb
        }
    }

    test {small_bitmap hot bitcount} {
        foreach {key value} [array get small_bitmap1_bitcount] {
            build_hot_small_bitmap small_bitmap1
            eval $value
            check_small_bitmap1_is_right
            r flushdb
        }
    }

    test {small_bitmap hot bitpos} {
        foreach {key value} [array get small_bitmap1_bitpos] {
            build_hot_small_bitmap small_bitmap1
            eval $value
            check_small_bitmap1_is_right
            r flushdb
        }
    }

    test {small_bitmap hot bitfield} {
        build_hot_small_bitmap small_bitmap1
        eval $value
        assert_equal {3}  [r BITFIELD small_bitmap1 INCRBY u2 0 1]
        assert_equal {2} [r bitcount small_bitmap1]
        r flushdb
    }

    test {small_bitmap hot bitop overwrite} {
        build_hot_small_bitmap small_bitmap1
        check_small_bitmap_bitop small_bitmap1
        r flushdb
    }

    test {small_bitmap hot bitop} {
            
        build_hot_small_bitmap small_bitmap1

        r setbit src1 0 1
        r setbit src2 2 1

        after 100
        r swap.evict src1
        wait_key_cold r src1

        after 100
        r swap.evict src2
        wait_key_cold r src2
        assert_equal {1} [r bitop XOR small_bitmap1 src1 src2]
        assert_equal {2} [r bitcount small_bitmap1]

        r flushdb
    }

    test {small_bitmap extend hot getbit} {
        build_extend_hot_small_bitmap small_bitmap1
        assert_equal {1} [r getbit small_bitmap1 15]
        check_extend_small_bitmap1_is_right
        r flushdb
    }

    test {small_bitmap extend hot bitcount} {
        build_extend_hot_small_bitmap small_bitmap1
        assert_equal {2} [r bitcount small_bitmap1]
        check_extend_small_bitmap1_is_right
        r flushdb
    }

    test {small_bitmap extend hot bitpos} {
        build_extend_hot_small_bitmap small_bitmap1
        assert_equal {15} [r bitpos small_bitmap1 1 1]
        check_extend_small_bitmap1_is_right
        r flushdb

    }

    test {small_bitmap extend hot bitfield} {
        build_extend_hot_small_bitmap small_bitmap1
        assert_equal {3}  [r BITFIELD small_bitmap1 INCRBY u2 0 1]
        assert_equal {3} [r bitcount small_bitmap1]
        r flushdb
    }

    test {small_bitmap extend hot bitop overwrite} {
        build_extend_hot_small_bitmap small_bitmap1
        r setbit src1 0 1
        r setbit src2 2 1
        after 100
        r swap.evict src1
        wait_key_cold r src1
        after 100
        r swap.evict src2
        wait_key_cold r src2
        assert_equal {2} [r bitop XOR small_bitmap1 small_bitmap1 src1 src2]
        assert_equal {2} [r bitcount small_bitmap1]
        r flushdb
    }

    test {small_bitmap extend hot bitop} {
        build_extend_hot_small_bitmap small_bitmap1
        r setbit src1 0 1
        r setbit src2 2 1
        after 100
        r swap.evict src1
        wait_key_cold r src1
        after 100
        r swap.evict src2
        wait_key_cold r src2
        assert_equal {1} [r bitop XOR small_bitmap1 src1 src2]
        assert_equal {2} [r bitcount small_bitmap1]
        r flushdb
    }

    test {small_bitmap cold getbit} {
        foreach {key value} [array get small_bitmap1_getbit] {
            build_cold_small_bitmap small_bitmap1
            eval $value
            check_small_bitmap1_is_right
            r flushdb
        }
    }

    test {small_bitmap cold bitcount} {
        foreach {key value} [array get small_bitmap1_bitcount] {
            build_cold_small_bitmap small_bitmap1
            eval $value
            check_small_bitmap1_is_right
            r flushdb
        }
    }

    test {small_bitmap cold bitpos} {
        foreach {key value} [array get small_bitmap1_bitpos] {
            build_cold_small_bitmap small_bitmap1
            eval $value
            check_small_bitmap1_is_right
            r flushdb
        }
    }

    test {small_bitmap cold bitfield} {
        build_cold_small_bitmap small_bitmap1
        assert_equal {3}  [r BITFIELD small_bitmap1 INCRBY u2 0 1]
        assert_equal {2} [r bitcount small_bitmap1]
        r flushdb
    }

    test {small_bitmap cold bitop overwrite} {
        build_cold_small_bitmap small_bitmap1
        check_small_bitmap_bitop small_bitmap1
        r flushdb
    }

    test {small_bitmap cold bitop} {
        build_cold_small_bitmap small_bitmap1
        r setbit src1 0 1
        r setbit src2 2 1

        after 100
        r swap.evict src1
        wait_key_cold r src1

        after 100
        r swap.evict src2
        wait_key_cold r src2
        assert_equal {1} [r bitop XOR small_bitmap1 src1 src2]
        assert_equal {2} [r bitcount small_bitmap1]
        r flushdb
    }

}

start_server {
    tags {"small bitmap rdb"}
}   {
    r config set swap-debug-evict-keys 0

    test {small_bitmap pure hot rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        build_pure_hot_small_bitmap small_bitmap1
        r debug reload
        # check_data
        check_small_bitmap1_is_right
        r flushdb
    }

    test {small_bitmap extend hot rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        build_extend_hot_small_bitmap small_bitmap1
        r debug reload
        # check_data
        check_extend_small_bitmap1_is_right
        r flushdb
    }

    test {small_bitmap hot rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        build_hot_small_bitmap small_bitmap1
        r debug reload
        # check_data
        check_small_bitmap1_is_right
        r flushdb
    }

    test {small_bitmap cold rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        build_cold_small_bitmap small_bitmap1
        r debug reload
        # check_data
        check_small_bitmap1_is_right
        r flushdb
    }

    test {small_bitmap cold rdbsave and rdbload with bitmap-subkey-size exchange 2048 to 4096} {
        r flushdb
        set bak_bitmap_subkey_size [lindex [r config get bitmap-subkey-size] 1]
        r CONFIG SET bitmap-subkey-size 2048
        build_cold_small_bitmap small_bitmap1
        r SAVE
        # check if this bit will exist after loading
        r setbit small_bitmap1 16 1
        r CONFIG SET bitmap-subkey-size 4096
        r DEBUG RELOAD NOSAVE
        # check_data
        check_small_bitmap1_is_right
        r config set bitmap-subkey-size $bak_bitmap_subkey_size
        r flushdb
    }

    test {small_bitmap cold rdbsave and rdbload with bitmap-subkey-size exchange 4096 to 2048} {
        r flushdb
        set bak_bitmap_subkey_size [lindex [r config get bitmap-subkey-size] 1]
        build_cold_small_bitmap small_bitmap1
        r SAVE
        # check if this bit will exist after loading
        r setbit small_bitmap1 16 1
        r CONFIG SET bitmap-subkey-size 2048
        r DEBUG RELOAD NOSAVE
        # check_data
        check_small_bitmap1_is_right
        r flushdb
        # reset default config
        r config set bitmap-subkey-size $bak_bitmap_subkey_size
    }

    set bak_rdb_bitmap_enable [lindex [r config get swap-rdb-bitmap-encode-enabled] 1]
    r CONFIG SET swap-rdb-bitmap-encode-enabled no

    test {small_bitmap pure hot rdbsave and rdbload for RDB_TYPE_STRING} {
        r flushdb
        build_pure_hot_small_bitmap small_bitmap1
        r debug reload
        # check it is string 
        assert [object_is_string r small_bitmap1]
        assert [object_is_cold r small_bitmap1]
        check_small_bitmap1_is_right
        r flushdb
    }

    test {small_bitmap extend hot rdbsave and rdbload for RDB_TYPE_STRING} {
        r flushdb
        build_extend_hot_small_bitmap small_bitmap1
        r debug reload
        # check it is string
        assert [object_is_string r small_bitmap1]
        assert [object_is_cold r small_bitmap1]
        check_extend_small_bitmap1_is_right
        r flushdb
    }

    test {small_bitmap hot rdbsave and rdbload for RDB_TYPE_STRING} {
        r flushdb
        build_hot_small_bitmap small_bitmap1
        r debug reload
        # check it is string
        assert [object_is_string r small_bitmap1]
        assert [object_is_cold r small_bitmap1]
        check_small_bitmap1_is_right
        r flushdb
    }

    test {small_bitmap cold rdbsave and rdbload for RDB_TYPE_STRING} {
        r flushdb
        build_cold_small_bitmap small_bitmap1
        r debug reload
        # check it is string
        assert [object_is_string r small_bitmap1]
        assert [object_is_cold r small_bitmap1]
        check_small_bitmap1_is_right
        r flushdb
    }

    r config set swap-rdb-bitmap-encode-enabled $bak_rdb_bitmap_enable

}

start_server {
    tags {"bitmap swap"}
}   {
    r config set swap-debug-evict-keys 0

    test {pure hot full swap out} {
        r flushdb
        build_pure_hot_data mybitmap1
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1
        check_mybitmap_is_right mybitmap1
        r flushdb
    }

    test {pure hot non full swap out} {
        r flushdb
        build_pure_hot_data mybitmap1
        set bak_evict_step [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 5
        after 100
        r swap.evict mybitmap1
        after 100
        r swap.evict mybitmap1
        after 100
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1
        check_mybitmap_is_right mybitmap1
        r flushdb
        r config set swap-evict-step-max-subkeys $bak_evict_step
    }

    test {hot full swap out} {
        r flushdb
        build_hot_data mybitmap1
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1
        check_mybitmap_is_right mybitmap1
        r flushdb
    }

    test {hot non full swap out} {
        r flushdb
        build_hot_data mybitmap1
        set bak_evict_step [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 5
        after 100
        r swap.evict mybitmap1
        after 100
        r swap.evict mybitmap1
        after 100
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1
        check_mybitmap_is_right mybitmap1
        r flushdb
        r config set swap-evict-step-max-subkeys $bak_evict_step
    }

    test {extend hot full swap out} {
        r flushdb
        build_extend_hot_data mybitmap1
        assert [object_is_hot r mybitmap1]
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1
        check_extend_mybitmap1_is_right
        r flushdb
    }

    test {extend hot non full swap out} {
        r flushdb
        build_extend_hot_data mybitmap1
        set bak_evict_step [lindex [r config get swap-evict-step-max-subkeys] 1]
        r config set swap-evict-step-max-subkeys 5
        after 100
        r swap.evict mybitmap1
        after 100
        r swap.evict mybitmap1
        after 100
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1
        check_extend_mybitmap1_is_right
        r flushdb
        r config set swap-evict-step-max-subkeys $bak_evict_step
    }

    test {warm full swap out} {
        r flushdb
        foreach {key value} [array get build_warm_with_hole] {
            eval $value
            r swap.evict mybitmap1
            wait_key_cold r mybitmap1
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }

    test {warm non full swap out} {
        r flushdb
        set bak_evict_step [lindex [r config get swap-evict-step-max-subkeys] 1]
        foreach {key value} [array get build_warm_with_hole] {
            eval $value
            r config set swap-evict-step-max-subkeys 5
            after 100
            r swap.evict mybitmap1
            after 100
            r swap.evict mybitmap1
            wait_key_cold r mybitmap1
            check_mybitmap_is_right mybitmap1
            r config set swap-evict-step-max-subkeys $bak_evict_step
            r flushdb
        }
    }

    test {pure hot getbit} {
        foreach {key value} [array get check_mybitmap1_getbit] {
            build_pure_hot_data mybitmap1
            eval $value
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }

    test {pure hot bitcount} {
        foreach {key value} [array get check_mybitmap1_getbit] {
            build_pure_hot_data mybitmap1
            eval $value
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }

    test {pure hot bitpos} {
        foreach {key value} [array get check_mybitmap1_bitpos] {
            build_pure_hot_data mybitmap1
            eval $value
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }

    test {pure hot bitpos corner case} {
        build_pure_hot_data mybitmap1
        check_mybitmap1_bitpos
        set_data mybitmap0
        #press_enter_to_continue
        assert_equal {41984} [r bitop XOR dest mybitmap1 mybitmap0]
        # press_enter_to_continue
        assert_equal {18} [r bitcount mybitmap1 0 41983]
        assert_equal {11} [r bitcount mybitmap0]
        assert_equal {7} [r bitcount dest]
        r flushdb
    }

    test {pure hot bitfield modify} {
        build_pure_hot_data mybitmap1
        assert_equal {-15}  [r BITFIELD mybitmap1 INCRBY i5 335871 1]
        assert_equal {12} [r bitcount mybitmap1]
        r flushdb
    }

    test {pure hot bitfield raw} {
        build_pure_hot_data mybitmap1
        assert_equal {8}  [r bitfield_ro mybitmap1 get u4 65535]
        check_mybitmap_is_right mybitmap1
        r flushdb
    }

    test {pure hot bitop} {
        build_pure_hot_data mybitmap1
        check_mybitmap1_bitop_xor_3_bitmap
        r flushdb
        build_pure_hot_data mybitmap1
        check_mybitmap1_bitop_xor_2_bitmap
        r flushdb
    }

    test {hot getbit} {
        foreach {key value} [array get check_mybitmap1_getbit] {
            build_hot_data mybitmap1
            eval $value
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }

    test {hot bitcount} {
        foreach {key value} [array get check_mybitmap1_getbit] {
            build_hot_data mybitmap1
            eval $value
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }

    test {hot bitpos} {
        foreach {key value} [array get check_mybitmap1_bitpos] {
            build_hot_data mybitmap1
            eval $value
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }

    test {hot bitpos corner case} {
        build_hot_data mybitmap1
        check_mybitmap1_bitpos
        set_data mybitmap0
        #press_enter_to_continue
        assert_equal {41984} [r bitop XOR dest mybitmap1 mybitmap0]
        # press_enter_to_continue
        assert_equal {18} [r bitcount mybitmap1 0 41983]
        assert_equal {11} [r bitcount mybitmap0]
        assert_equal {7} [r bitcount dest]
        r flushdb
    }

    test {hot bitfield} {
        build_hot_data mybitmap1
        assert_equal {-15}  [r BITFIELD mybitmap1 INCRBY i5 335871 1]
        assert_equal {12} [r bitcount mybitmap1]
        r flushdb
        build_hot_data mybitmap1
        assert_equal {8}  [r bitfield_ro mybitmap1 get u4 65535]
        check_mybitmap_is_right mybitmap1
        r flushdb
    }

    test {hot bitop} {
        build_hot_data mybitmap1
        check_mybitmap1_bitop_xor_3_bitmap
        r flushdb
        build_hot_data mybitmap1
        check_mybitmap1_bitop_xor_2_bitmap
        r flushdb
    }

    test {extend hot getbit} {
        build_extend_hot_data mybitmap1
        assert_equal {1} [r getbit mybitmap1 368639]
        check_extend_mybitmap1_is_right
        r flushdb
    }

    test {extend hot bitcount} {
        build_extend_hot_data mybitmap1
        assert_equal {12} [r bitcount mybitmap1]
        check_extend_mybitmap1_is_right
        r flushdb
    }

    test {extend hot bitpos} {
        build_extend_hot_data mybitmap1
        assert_equal {368639} [r bitpos mybitmap1 1 41984]
        check_extend_mybitmap1_is_right
        r flushdb

    }

    test {extend hot bitfield} {
        build_extend_hot_data mybitmap1
        assert_equal {-15}  [r BITFIELD mybitmap1 INCRBY i5 335871 1]
        assert_equal {13} [r bitcount mybitmap1]
        r flushdb
        build_extend_hot_data mybitmap1
        assert_equal {8}  [r bitfield_ro mybitmap1 get u4 65535]
        check_extend_mybitmap1_is_right
        r flushdb
    }

    test {extend hot bitop} {
        build_extend_hot_data mybitmap1
        check_extend_mybitmap1_xor_3_bitmap
        r flushdb
        build_extend_hot_data mybitmap1
        check_mybitmap1_bitop_xor_2_bitmap
        r flushdb
    }

    test {warm getbit} {
        r flushdb
        foreach {key outvalue} [array get build_warm_with_hole] {
            foreach {key innvalue} [array get check_mybitmap1_getbit] {
                eval $outvalue
                eval $innvalue
                check_mybitmap_is_right mybitmap1
                r flushdb
            }
        }
    }

    test {warm bitcount} {
        r flushdb
        foreach {key outvalue} [array get build_warm_with_hole] {
            foreach {key innvalue} [array get check_mybitmap1_bitcount] {
                eval $outvalue
                eval $innvalue
                check_mybitmap_is_right mybitmap1
                r flushdb
            }
        }
    }

    test {warm bitpos} {
        foreach {key outvalue} [array get build_warm_with_hole] {
            foreach {key innvalue} [array get check_mybitmap1_bitpos] {
                eval $outvalue
                eval $innvalue
                check_mybitmap_is_right mybitmap1
                r flushdb
            }
        }
    }

    test {warm bitpos corner case} {
        r flushdb
        foreach {key value} [array get build_warm_with_hole] {
            eval $value
            check_mybitmap1_bitpos
            set_data mybitmap0
            assert_equal {41984} [r bitop XOR dest mybitmap1 mybitmap0]
            assert_equal {18} [r bitcount mybitmap1 0 41983]
            assert_equal {11} [r bitcount mybitmap0]
            assert_equal {7} [r bitcount dest]
            r flushdb
        }
    }

    test {warm bitfield} {
        foreach {key value} [array get build_warm_with_hole] {
            eval $value
            append data_str $i
            eval $data_str
            assert_equal {-15}  [r BITFIELD mybitmap1 INCRBY i5 335871 1]
            r flushdb
            eval $data_str
            assert_equal {8}  [r bitfield_ro mybitmap1 get u4 65535]
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }

    test {warm bitop} {
        foreach {key value} [array get build_warm_with_hole] {
            eval $value
            append data_str $i
            eval $data_str
            check_mybitmap1_bitop_xor_3_bitmap
            r flushdb
            eval $data_str
            check_mybitmap1_bitop_xor_2_bitmap
            r flushdb
        }
    }

    test {cold getbit} {
        foreach {key value} [array get check_mybitmap1_getbit] {
            build_cold_data mybitmap1
            eval $value
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }

    test {cold bitcount} {
        foreach {key value} [array get check_mybitmap1_getbit] {
            build_cold_data mybitmap1
            eval $value
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }

    test {cold bitpos} {
        foreach {key value} [array get check_mybitmap1_bitpos] {
            build_cold_data mybitmap1
            eval $value
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }

    test {cold bitpos corner case} {
        build_cold_data mybitmap1
        check_mybitmap1_bitpos
        set_data mybitmap0
        assert_equal {41984} [r bitop XOR dest mybitmap1 mybitmap0]
        assert_equal {18} [r bitcount mybitmap1 0 41983]
        assert_equal {11} [r bitcount mybitmap0]
        assert_equal {7} [r bitcount dest]
        r flushdb
    }

    test {cold bitfield} {
        build_cold_data mybitmap1
        assert_equal {-15}  [r BITFIELD mybitmap1 INCRBY i5 335871 1]
        assert_equal {12} [r bitcount mybitmap1]
        r flushdb
        build_cold_data mybitmap1
        assert_equal {8}  [r bitfield_ro mybitmap1 get u4 65535]
        check_mybitmap_is_right mybitmap1
        r flushdb
    }

    test {cold bitop} {
        build_cold_data mybitmap1
        check_mybitmap1_bitop_xor_3_bitmap
        r flushdb
        build_cold_data mybitmap1
        check_mybitmap1_bitop_xor_2_bitmap
        r flushdb
    }

    test {pure hot setbit} {
        foreach {key value} [array get check_mybitmap1_setbit] {
            r flushdb
            build_pure_hot_data mybitmap1
            eval $value
            r flushdb
        }
    }

    test {hot setbit} {
        foreach {key value} [array get check_mybitmap1_setbit] {
            r flushdb
            build_hot_data mybitmap1
            eval $value
            r flushdb
        }
    }

    test {warm setbit} {
        r flushdb
        foreach {key outvalue} [array get build_warm_with_hole] {
            foreach {key innvalue} [array get check_mybitmap1_setbit] {
                r flushdb
                eval $outvalue
                eval $innvalue
            }
        }
    }

    test {cold setbit} {
        foreach {key value} [array get check_mybitmap1_setbit] {
            r flushdb
            build_cold_data mybitmap1
            eval $value
        }
    }
}

start_server {
    tags {"bitmap rdb"}
}   {
    r config set swap-debug-evict-keys 0

    test {pure hot rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        # mybitmap 41kb
        build_pure_hot_data mybitmap1
        build_pure_hot_data mybitmap2
        r debug reload
        # check_data
        check_mybitmap_is_right mybitmap1
        check_mybitmap_is_right mybitmap2
        r flushdb
    }

    test {extend hot rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        # mybitmap 41kb
        build_extend_hot_data mybitmap1
        r debug reload
        # check_data
        check_extend_mybitmap1_is_right
        r flushdb
    }

    test {hot rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        # mybitmap 41kb
        build_hot_data mybitmap1
        r debug reload
        # check_data
        check_mybitmap_is_right mybitmap1
        r flushdb
    }

    test {cold rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        # mybitmap 41kb
        build_cold_data mybitmap1
        build_pure_hot_data mybitmap2
        r swap.evict mybitmap2
        wait_key_cold r mybitmap2
        r debug reload
        # check_data
        check_mybitmap_is_right mybitmap1
        check_mybitmap_is_right mybitmap2
        r flushdb
    }

    test {warm rdbsave and rdbload for RDB_TYPE_BITMAP} {
        # mybitmap 41kb
        foreach {key value} [array get build_warm_with_hole] {
            r flushdb
            build_cold_data mybitmap1
            eval $value
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }


    test {cold rdbsave and rdbload with bitmap-subkey-size exchange 2048 to 4096} {
        r flushdb
        set bak_bitmap_subkey_size [lindex [r config get bitmap-subkey-size] 1]
        # mybitmap 41kb
        r CONFIG SET bitmap-subkey-size 2048
        build_cold_data mybitmap1
        r SAVE
        # check if this bit will exist after loading
        r setbit mybitmap1 335872 1
        r CONFIG SET bitmap-subkey-size 4096
        r DEBUG RELOAD NOSAVE
        # check_data
        check_mybitmap_is_right mybitmap1
        r config set bitmap-subkey-size $bak_bitmap_subkey_size
        r flushdb
    }

    test {cold rdbsave and rdbload with bitmap-subkey-size exchange 4096 to 2048} {
        r flushdb
        set bak_bitmap_subkey_size [lindex [r config get bitmap-subkey-size] 1]
        # mybitmap 41kb
        build_cold_data mybitmap1
        r SAVE
        # check if this bit will exist after loading
        r setbit mybitmap1 335872 1
        r CONFIG SET bitmap-subkey-size 2048
        r DEBUG RELOAD NOSAVE
        # check_data
        check_mybitmap_is_right mybitmap1
        r flushdb
        # reset default config
        r config set bitmap-subkey-size $bak_bitmap_subkey_size
    }

    set bak_rdb_bitmap_enable [lindex [r config get swap-rdb-bitmap-encode-enabled] 1]
    r CONFIG SET swap-rdb-bitmap-encode-enabled no

    test {pure hot rdbsave and rdbload for RDB_TYPE_STRING} {
        r flushdb
        # mybitmap 41kb
        build_pure_hot_data mybitmap1
        build_pure_hot_data mybitmap2
        r debug reload
        # check it is string 
        assert [object_is_string r mybitmap1]
        assert [object_is_string r mybitmap2]
        assert [object_is_cold r mybitmap1]
        assert [object_is_cold r mybitmap2]
        check_mybitmap_is_right mybitmap1
        r flushdb
    }

    test {extend hot rdbsave and rdbload for RDB_TYPE_STRING} {
        r flushdb
        # mybitmap 41kb
        build_extend_hot_data mybitmap1
        r debug reload
        # check it is string
        assert [object_is_cold r mybitmap1]
        assert [object_is_string r mybitmap1]
        check_extend_mybitmap1_is_right
        r flushdb
    }

    test {hot rdbsave and rdbload for RDB_TYPE_STRING} {
        r flushdb
        # mybitmap 41kb
        build_hot_data mybitmap1
        r debug reload
        # check it is string
        assert [object_is_cold r mybitmap1]
        assert [object_is_string r mybitmap1]
        check_mybitmap_is_right mybitmap1
        r flushdb
    }

    test {cold rdbsave and rdbload for RDB_TYPE_STRING} {
        r flushdb
        # mybitmap 41kb
        build_cold_data mybitmap1
        r debug reload
        # check it is string
        assert [object_is_cold r mybitmap1]
        assert [object_is_string r mybitmap1]
        check_mybitmap_is_right mybitmap1
        r flushdb
    }

    test {warm rdbsave and rdbload for RDB_TYPE_STRING} {
        # mybitmap 41kb
        foreach {key value} [array get build_warm_with_hole] {
            r flushdb
            eval $value
            r debug reload
            # check it is string 
            assert [object_is_cold r mybitmap1]
            assert [object_is_string r mybitmap1]
            check_mybitmap_is_right mybitmap1
            r flushdb
        }
    }

    r config set swap-rdb-bitmap-encode-enabled $bak_rdb_bitmap_enable
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
                            set randIdx [randomInt $bitmap_max_length]
                            set randVal [randomInt 2]
                            $r1 SETBIT $mybitmap $randIdx $randVal
                        } {
                            set randIdx1 [randomInt $bitmap_max_length]
                            $r1 SETRANGE $mybitmap $randIdx1 "redis"
                        } {
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