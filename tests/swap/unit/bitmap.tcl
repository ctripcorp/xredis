# util
proc replace_delimiter {str target} {
    set delimiter "%"
    set target $target
    set splitted [split $str $delimiter]
    set joined [join $splitted $target]
    return $joined
}

set extend 1
set notextend 0

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

set small_bitmap_getbit {
    {assert_equal {1} [r getbit % 0]}
    {assert_equal {0} [r getbit % 10]}
    {assert_error "ERR*" {r getbit % -1}}
}

set small_bitmap_bitcount {
    {assert_equal {1} [r bitcount % 0 0]}
    {assert_equal {1} [r bitcount %]}
    {assert_equal {1} [r bitcount % 0 -2]}
    {assert_equal {1} [r bitcount % -1 2]}
}

set small_bitmap_bitpos {
    {assert_equal {0} [r bitpos % 1 0]}
    {assert_equal {-1} [r bitpos % 1 1]}
    {assert_equal {0} [r bitpos % 1 -1]}
    {assert_equal {1} [r bitpos % 0 0]}
    {assert_equal {-1} [r bitpos % 0 1]}
    {assert_equal {1} [r bitpos % 0 -1]}
    {assert_equal {0} [r bitpos % 1 0 2]}
    {assert_equal {0} [r bitpos % 1 -1 1]}
    {assert_equal {1} [r bitpos % 0 0 2]}
    {assert_equal {1} [r bitpos % 0 -1 1]}
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

proc check_extend_small_bitmap_bitop  {small_bitmap}  {
    r setbit src1 0 1
    r setbit src2 2 1
    after 100
    r swap.evict src1
    wait_key_cold r src1
    after 100
    r swap.evict src2
    wait_key_cold r src2
    assert_equal {1} [r bitop XOR $small_bitmap src1 src2]
    assert_equal {2} [r bitcount $small_bitmap]
}

proc check_small_bitmap_is_right {small_bitmap0 small_bitmap1 is_extend} {
    r setbit $small_bitmap0 0 1
    if {$is_extend == 0} {
        assert_equal {1} [r bitop XOR dest $small_bitmap1 $small_bitmap0]
        assert_equal {1} [r bitcount $small_bitmap1 0 0]
        assert_equal {1} [r bitcount $small_bitmap0]
        assert_equal {0} [r bitcount dest]
    } else {
        assert_equal {2} [r bitop XOR dest $small_bitmap1 $small_bitmap0]
        assert_equal {2} [r bitcount $small_bitmap1]
        assert_equal {1} [r bitcount $small_bitmap0]
        assert_equal {1} [r bitcount dest]
    }

}

proc check_extend_small_bitmap_is_right {small_bitmap0 small_bitmap1} {
    r setbit $small_bitmap0 0 1
    assert_equal {2} [r bitop XOR dest $small_bitmap1 $small_bitmap0]
    assert_equal {2} [r bitcount $small_bitmap1]
    assert_equal {1} [r bitcount $small_bitmap0]
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

proc check_mybitmap_is_right {mybitmap is_extend} {
    if {$is_extend == 0} {
        set_data mybitmap0
        assert_equal {41984} [r bitop XOR dest $mybitmap mybitmap0]
        assert_equal {11} [r bitcount $mybitmap 0 41983]
        assert_equal {11} [r bitcount mybitmap0]
        assert_equal {0} [r bitcount dest]
    } else {
        set_data mybitmap0
        assert_equal {46080} [r bitop XOR dest $mybitmap mybitmap0]
        assert_equal {12} [r bitcount $mybitmap]
        assert_equal {11} [r bitcount mybitmap0]
        assert_equal {1} [r bitcount dest]
    }
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

#   condition:
#  hot  #_#__#_#__#
#  cold ###########

#   condition:
#   hot  ____##_____
#   cold ###########

#   condition:
#   hot  ____#___#_#
#   cold ###########

#   condition:
#   hot  #_#_##_____
#   cold ########### 

#   condition:
#   hot  ________###
#   cold ########### 

#   condition:
#   hot  ##_________
#   cold ########### 

#   condition:
#   hot  ##________#
#   cold ########### 

set build_warm_with_hole {

    {1} {
        { build_cold_data mybitmap1 }
        { r getbit mybitmap1 32767 }
        { r getbit mybitmap1 98303 }
        { r getbit mybitmap1 196607 }
        { r getbit mybitmap1 262143 }
        { r getbit mybitmap1 335871 }
        { assert [object_is_warm r mybitmap1] }
    }

    {2}  {
        { build_cold_data mybitmap1 }
        { r getbit mybitmap1 163839 }
        { r getbit mybitmap1 196607 }
        { assert [object_is_warm r mybitmap1] }
    }

    {3}  {
        { build_cold_data mybitmap1 }
        { r getbit mybitmap1 163839 }
        { r getbit mybitmap1 294911 }
        { r getbit mybitmap1 335871 }
        { assert [object_is_warm r mybitmap1] }
    }

    {4}  {
        { build_cold_data mybitmap1 }
        { r getbit mybitmap1 32767 }
        { r getbit mybitmap1 98303  }
        { r getbit mybitmap1 163839 }
        { r getbit mybitmap1 196607  }
        { assert [object_is_warm r mybitmap1] }
    }

    {5}  {
        { build_cold_data mybitmap1 }
        { r getbit mybitmap1 294911 }
        { r getbit mybitmap1 327679 }
        { r getbit mybitmap1 335871 }
        { assert [object_is_warm r mybitmap1] }
    }

    {6}  {
        { build_cold_data mybitmap1 }
        { r getbit mybitmap1 1 }
        { r getbit mybitmap1 65535  }
        { assert [object_is_warm r mybitmap1] }
    }

    {7}  {
        { build_cold_data mybitmap1 }
        { r getbit mybitmap1 1 }
        { r getbit mybitmap1 65535 }
        { r getbit mybitmap1 335871 }
        { assert [object_is_warm r mybitmap1] }
    }
}

set check_mybitmap1_setbit {
    {0} {
        { assert_equal {1} [r setbit mybitmap1 98303 0] }
        { set_data mybitmap0 }
        {r setbit mybitmap0 98303 0}
        { assert_equal {41984} [r bitop XOR dest mybitmap1 mybitmap0] }
        { assert_equal {10} [r bitcount mybitmap1] }
        { assert_equal {10} [r bitcount mybitmap0] }
        { assert_equal {0} [r bitcount dest] }
    }

    {1} {
        { assert_equal {0} [r setbit mybitmap1 344063 1] }
        { set_data mybitmap0 }
        {r setbit mybitmap0 344063 1}
        { assert_equal {43008} [r bitop XOR dest mybitmap1 mybitmap0] }
        { assert_equal {12} [r bitcount mybitmap1] }
        { assert_equal {12} [r bitcount mybitmap0] }
        { assert_equal {0} [r bitcount dest] }
    }

    {2} { 
        { assert_equal {0} [r setbit mybitmap1 368639 1] }
        { set_data mybitmap0 }
        {r setbit mybitmap0 368639 1}
        { assert_equal {46080} [r bitop XOR dest mybitmap1 mybitmap0] }
        { assert_equal {12} [r bitcount mybitmap1] }
        { assert_equal {12} [r bitcount mybitmap0] }
        { assert_equal {0} [r bitcount dest] }
    }

    {3} {
        { assert_equal {1} [r setbit mybitmap1 335871 0] }
        { set_data mybitmap0 }
        {r setbit mybitmap0 335871 0}
        { assert_equal {41984} [r bitop XOR dest mybitmap1 mybitmap0] }
        { assert_equal {10} [r bitcount mybitmap1] }
        { assert_equal {10} [r bitcount mybitmap0] }
        { assert_equal {0} [r bitcount dest] }
    }

    {4}  {
        { assert_equal {0} [r setbit mybitmap1 468639 1]}
        { set_data mybitmap0 }
        {r setbit mybitmap0 468639 1}
        { assert_equal {58580} [r bitop XOR dest mybitmap1 mybitmap0] }
        { assert_equal {12} [r bitcount mybitmap1] }
        { assert_equal {12} [r bitcount mybitmap0] }
        { assert_equal {0} [r bitcount dest] }
    }

    {5}  {
        { assert_error "ERR*" {r setbit mybitmap1 -1 1} }
        { check_mybitmap_is_right mybitmap1 $notextend }
    }

    {6}  {
        { assert_error "ERR*" {r setbit mybitmap1 4294967296 1} }
        { check_mybitmap_is_right mybitmap1 $notextend }
    }

    {7}  {
        { assert_equal {0} [r setbit mybitmap1 4294967295 1]}
        { set_data mybitmap0 }
        { assert_equal {536870912} [r bitop XOR dest mybitmap1 mybitmap0] }
        { assert_equal {12} [r bitcount mybitmap1] }
        { assert_equal {11} [r bitcount mybitmap0] }
        { assert_equal {1} [r bitcount dest] }
        { assert_equal {1} [r getbit dest 4294967295] }
    }
}

set check_mybitmap_getbit {
    {assert_equal {1} [r getbit % 32767]}
    {assert_equal {1} [r getbit % 65535]}
    {assert_equal {1} [r getbit % 98303]}
    {assert_equal {1} [r getbit % 131071]}
    {assert_equal {1} [r getbit % 163839]}
    {assert_equal {1} [r getbit % 196607]}
    {assert_equal {1} [r getbit % 229375]}
    {assert_equal {1} [r getbit % 262143]}
    {assert_equal {1} [r getbit % 294911]}
    {assert_equal {1} [r getbit % 327679]}
    {assert_equal {1} [r getbit % 335871]}
    {assert_equal {0} [r getbit % 335872]}
    {assert_error "ERR*" {r getbit % -1}}
    {assert_equal {0} [r getbit % 2147483647]}
    {assert_equal {0} [r getbit % 4294967295]}
    {assert_error "ERR*" {r getbit % 4294967296}}
}

set check_mybitmap_bitcount {
    {assert_equal {11} [r bitcount %]}
    {assert_equal {2} [r bitcount % 0 9216]}
    {assert_equal {2} [r bitcount % 5000 15000]}
    {assert_equal {3} [r bitcount % 9216 20480]}
    {assert_equal {3} [r bitcount % 15000 25000]}
    {assert_equal {6} [r bitcount % 20480 43008]}
    {assert_equal {9} [r bitcount % 10000 2000000]}
    {assert_equal {9} [r bitcount % 10000 2147483647]}
    {assert_equal {11} [r bitcount % -2147483648 2147483647]}
    {assert_equal {0} [r bitcount % 2000000 10000]}
    {assert_equal {5} [r bitcount % 10000 -10000]}
    {assert_equal {0} [r bitcount % -10000 10000]}
    {assert_equal {7} [r bitcount % -41984 -11984]}
    {assert_equal {0} [r bitcount % -11984 -41984]}
    {assert_equal {3} [r bitcount % -21984 -11984]}
}

set check_mybitmap_bitpos {
    {assert_equal {32767} [r bitpos % 1]}
    {assert_equal {32767} [r bitpos % 1 0]}
    {assert_equal {98303} [r bitpos % 1 9216]}
    {assert_equal {196607} [r bitpos % 1 20480]}
    {assert_equal {335871} [r bitpos % 1 41983]}
    {assert_equal {32767} [r bitpos % 1 0 9216]}
    {assert_equal {65535} [r bitpos % 1 5000 15000]}
    {assert_equal {98303} [r bitpos % 1 9216 20480]}
    {assert_equal {131071} [r bitpos % 1 15000 25000]}
    {assert_equal {196607} [r bitpos % 1 20480 43008]}
    {assert_equal {0} [r bitpos % 0]}
    {assert_equal {0} [r bitpos % 0 0]}
    {assert_equal {73728} [r bitpos % 0 9216]}
    {assert_equal {163840} [r bitpos % 0 20480]}
    {assert_equal {335864} [r bitpos % 0 41983]}
    {assert_equal {0} [r bitpos % 0 0 9216]}
    {assert_equal {40000} [r bitpos % 0 5000 15000]}
    {assert_equal {73728} [r bitpos % 0 9216 20480]}
    {assert_equal {120000} [r bitpos % 0 15000 25000]}
    {assert_equal {163840} [r bitpos % 0 20480 43008]}
    {assert_equal {-1} [r bitpos % 1 41984]}
    {assert_equal {-1} [r bitpos % 1 2147483647]}
    {assert_equal {327679} [r bitpos % 1 -1984]}
    {assert_equal {262143} [r bitpos % 1 -11984]}
    {assert_equal {98303} [r bitpos % 1 -31984]}
    {assert_equal {32767} [r bitpos % 1 -41984]}
    {assert_equal {32767} [r bitpos % 1 -2147483648]}
    {assert_equal {98303} [r bitpos % 1 10000 2000000]}
    {assert_equal {98303} [r bitpos % 1 10000 2147483647]}
    {assert_equal {-1} [r bitpos % 1 2000000 10000]}
    {assert_equal {-1} [r bitpos % 1 20000 10000]}
    {assert_equal {163839} [r bitpos % 1 -21984 -11984]}
    {assert_equal {32767} [r bitpos % 1 -41984 -11984]}
    {assert_equal {-1} [r bitpos % 1 -11984 -41984]}
    {assert_equal {98303} [r bitpos % 1 10000 -10000]}
    {assert_equal {32767} [r bitpos % 1 -2147483648 2147483647]}
    {assert_equal {-1} [r bitpos % 1 -10000 10000]}
    {assert_equal {-1} [r bitpos % 0 41984]}
    {assert_equal {-1} [r bitpos % 0 2147483647]}
    {assert_equal {320000} [r bitpos % 0 -1984]}
    {assert_equal {240000} [r bitpos % 0 -11984]}
    {assert_equal {80000} [r bitpos % 0 -31984]}
    {assert_equal {0} [r bitpos % 0 -41984]}
    {assert_equal {0} [r bitpos % 0 -2147483648]}
    {assert_equal {80000} [r bitpos % 0 10000 2000000]}
    {assert_equal {80000} [r bitpos % 0 10000 2147483647]}
    {assert_equal {-1} [r bitpos % 0 2000000 10000]}
    {assert_equal {-1} [r bitpos % 0 20000 10000]}
    {assert_equal {160000} [r bitpos % 0 -21984 -11984]}
    {assert_equal {0} [r bitpos % 0 -41984 -11984]}
    {assert_equal {-1} [r bitpos % 0 -11984 -41984]}
    {assert_equal {80000} [r bitpos % 0 10000 -10000]}
    {assert_equal {0} [r bitpos % 0 -2147483648 2147483647]}
    {assert_equal {-1} [r bitpos % 0 -10000 10000]}
}

proc check_bitpos_mybitmap {mybitmap}  {
    # find clear bit from out of range(when all bit 1 in range)
    assert_equal {0} [r setbit $mybitmap 335870 1]
    assert_equal {0} [r setbit $mybitmap 335869 1]
    assert_equal {0} [r setbit $mybitmap 335868 1]
    assert_equal {0} [r setbit $mybitmap 335867 1]
    assert_equal {0} [r setbit $mybitmap 335866 1]
    assert_equal {0} [r setbit $mybitmap 335865 1]
    assert_equal {0} [r setbit $mybitmap 335864 1]
    assert_equal {335872} [r bitpos $mybitmap 0 41983]
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

proc check_mybitmap_bitop_xor_3_bitmap {mybitmap}  {
    build_bitmap_args
    assert_equal {41984} [r bitop XOR $mybitmap $mybitmap src1 src2]
    assert_equal {9} [r bitcount $mybitmap]
}

proc check_mybitmap_bitop_xor_2_bitmap {mybitmap}  {
    build_bitmap_args
    assert_equal {41984} [r bitop XOR $mybitmap src1 src2]
    assert_equal {2} [r bitcount $mybitmap]
}

proc check_extend_mybitmap_xor_3_bitmap {mybitmap}  {
    build_bitmap_args
    assert_equal {46080} [r bitop XOR $mybitmap $mybitmap src1 src2]
    assert_equal {10} [r bitcount $mybitmap]
}

proc init_bitmap_string_data {mykey} {
    r setbit $mykey 81919 1
    r setbit $mykey 1 1
    r setbit $mykey 3 1
    r setbit $mykey 5 1
    r setbit $mykey 9 1
    r setbit $mykey 11 1
}

start_server  {
    tags {"bitmap string switch"}
}  {
    r config set swap-debug-evict-keys 0

    test "cold bitmap to string checking character" { 
        init_bitmap_string_data mykey

        r swap.evict mykey
        wait_key_cold r mykey
        assert_equal "TP" [r getrange mykey 0 1]
        assert [object_is_string r mykey] 
        assert_equal "\x01" [r getrange mykey 10239 10239]
        r swap.evict mykey
        wait_key_cold r mykey
        assert_equal {6} [r bitcount mykey]
        set bitmap_to_string_count0 [get_info_property r Swap swap_type_switch_count bitmap_to_string]
        assert_equal {1} $bitmap_to_string_count0
        assert_equal $bitmap_to_string_count0 [get_info_property r Swap swap_type_switch_count string_to_bitmap]
        r flushdb
    }

    test "warm bitmap to string checking character" {
        init_bitmap_string_data mykey

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
        init_bitmap_string_data mykey
        
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
        # dirty bitmap all evict
        after 100
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1
        assert ![object_is_dirty r mybitmap1]
        assert_equal [object_meta_pure_cold_subkeys_num r mybitmap1] 11
        # cold data turns clean when swapin
        assert_equal {2} [r bitcount mybitmap1 0 9216]
        assert ![object_is_dirty r mybitmap1]
        assert_equal [object_meta_pure_cold_subkeys_num r mybitmap1] 8
        # clean bitmap all swapin remains clean
        assert_equal {11} [r bitcount mybitmap1]
        assert ![object_is_dirty r mybitmap1]
        # all-swapin meta remains
        assert_equal [object_meta_pure_cold_subkeys_num r mybitmap1] 0
        # clean bitmap swapout does not triggers swap
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

    test {bitmap always processed as whole string} {
        set bitmap_enabled [lindex [r config get swap-bitmap-subkeys-enabled] 1]
        r config set swap-bitmap-subkeys-enabled no

        r flushdb
        
        # check setbit

        set_data mybitmap1
        assert [object_is_string r mybitmap1]
    
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1

        assert_equal [r getbit mybitmap1 0] 0

        check_mybitmap_is_right mybitmap1 $notextend
        assert [object_is_string r mybitmap1]

        # check bitop

        r setbit src1 32767 1
        r setbit src2 335871 1

        assert_equal {41984} [r bitop XOR mybitmap1 src1 src2]
        assert_equal {2} [r bitcount mybitmap1]
        assert [object_is_string r mybitmap1]

        # check bitfield

        set_data mybitmap2

        assert_equal {-15}  [r BITFIELD mybitmap2 INCRBY i5 335871 1]
        assert [object_is_string r mybitmap2]

        # check string to bitmap

        r set mykey "@"
        r swap.evict mykey
        wait_key_cold r mykey
        assert_equal {0} [r setbit mykey 2 1]
        assert [object_is_string r mykey]
        assert_equal [binary format B* 01100000] [r get mykey]

        r config set swap-bitmap-subkeys-enabled $bitmap_enabled
        r flushdb
    }

    test {swap-bitmap-subkeys-enabled during server running} {
        set bitmap_enabled [lindex [r config get swap-bitmap-subkeys-enabled] 1]
        r config set swap-bitmap-subkeys-enabled no
        r flushdb

        r set mykey1 "@"
        r swap.evict mykey1
        wait_key_cold r mykey1
        assert_equal {0} [r setbit mykey1 2 1]
        assert [object_is_string r mykey1]

        r config set swap-bitmap-subkeys-enabled yes

        # mykey1 could be transfered to bitmap subkeys
        assert_equal {1} [r setbit mykey1 2 1]
        assert [object_is_bitmap r mykey1]

        r config set swap-bitmap-subkeys-enabled no

        # mykey1 could be continued treated as bitmap subkeys
        assert_equal {1} [r setbit mykey1 2 1]
        assert [object_is_bitmap r mykey1]

        # new string object will not be treated as bitmap subkeys
        r set mykey2 "@"
        r swap.evict mykey2
        wait_key_cold r mykey2
        assert_equal {0} [r setbit mykey2 2 1]
        assert [object_is_string r mykey2]

        r config set swap-bitmap-subkeys-enabled $bitmap_enabled
        r flushdb
    }

    test {bug: assert fail when execute GET on expired bitmap key} { 

        r setbit mybit 81919 1
        r expire mybit 1
        r swap.evict mybit
        wait_key_cold r mybit
        after 1100

        r get mybit
        assert_equal {0} [r bitcount mybit]

        r flushdb
    }

}


start_server {
    tags {"small bitmap swap"}
}   {
    r config set swap-debug-evict-keys 0

    test {small_bitmap full swap out} {
        set builds {
            build_pure_hot_small_bitmap
            build_hot_small_bitmap
        }
        foreach build $builds {
            $build small_bitmap1
            r swap.evict small_bitmap1
            wait_key_cold r small_bitmap1
            check_small_bitmap_is_right small_bitmap0 small_bitmap1 $notextend
            r flushdb
        }
    }

    test {small_bitmap extend hot full swap out} {
        r flushdb
        build_extend_hot_small_bitmap small_bitmap1
        assert [object_is_hot r small_bitmap1]
        r swap.evict small_bitmap1
        wait_key_cold r small_bitmap1
        check_small_bitmap_is_right small_bitmap0 small_bitmap1 $extend
        r flushdb
    }

    test {small_bitmap swaps} {
        set builds {
            build_pure_hot_small_bitmap
            build_hot_small_bitmap
            build_cold_small_bitmap
        }
        set cmds {
            small_bitmap_getbit
            small_bitmap_bitcount
            small_bitmap_bitpos
        }
        foreach build $builds {
            foreach cmd $cmds {
                foreach {key value} $cmd {
                    set mybitmap "small_bitmap1"
                    $build $mybitmap
                    eval [replace_delimiter $value $mybitmap]
                    check_small_bitmap_is_right small_bitmap0 $mybitmap $notextend
                    r flushdb
                }
            }
        }
    }

    test {small_bitmap bitfield/bitop overwrite} {
        set builds {
            build_pure_hot_small_bitmap
            build_hot_small_bitmap
            build_cold_small_bitmap
        }
        foreach build $builds {
            $build small_bitmap1
            assert_equal {3}  [r BITFIELD small_bitmap1 INCRBY u2 0 1]
            assert_equal {2} [r bitcount small_bitmap1]
            r flushdb
            $build small_bitmap1
            check_small_bitmap_bitop small_bitmap1
            r flushdb
        }
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
        check_small_bitmap_is_right small_bitmap0 small_bitmap1 $extend
        r flushdb
    }

    test {small_bitmap extend hot bitcount} {
        build_extend_hot_small_bitmap small_bitmap1
        assert_equal {2} [r bitcount small_bitmap1]
        check_small_bitmap_is_right small_bitmap0 small_bitmap1 $extend
        r flushdb
    }

    test {small_bitmap extend hot bitpos} {
        build_extend_hot_small_bitmap small_bitmap1
        assert_equal {15} [r bitpos small_bitmap1 1 1]
        check_small_bitmap_is_right small_bitmap0 small_bitmap1 $extend
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
        check_small_bitmap_is_right small_bitmap0 small_bitmap1 $notextend
        r flushdb
    }

    test {small_bitmap extend hot rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        build_extend_hot_small_bitmap small_bitmap1
        r debug reload
        # check_data
        check_small_bitmap_is_right small_bitmap0 small_bitmap1 $extend
        r flushdb
    }

    test {small_bitmap hot rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        build_hot_small_bitmap small_bitmap1
        r debug reload
        # check_data
        check_small_bitmap_is_right small_bitmap0 small_bitmap1 $notextend
        r flushdb
    }

    test {small_bitmap cold rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        build_cold_small_bitmap small_bitmap1
        r debug reload
        # check_data
        check_small_bitmap_is_right small_bitmap0 small_bitmap1 $notextend
        r flushdb
    }

    test {small_bitmap cold rdbsave and rdbload with swap-bitmap-subkey-size exchange 2048 to 4096} {
        r flushdb
        set bak_bitmap_subkey_size [lindex [r config get swap-bitmap-subkey-size] 1]
        r CONFIG SET swap-bitmap-subkey-size 2048
        build_cold_small_bitmap small_bitmap1
        r SAVE
        # check if this bit will exist after loading
        r setbit small_bitmap1 16 1
        r CONFIG SET swap-bitmap-subkey-size 4096
        r DEBUG RELOAD NOSAVE
        # check_data
        check_small_bitmap_is_right small_bitmap0 small_bitmap1 $notextend
        r config set swap-bitmap-subkey-size $bak_bitmap_subkey_size
        r flushdb
    }

    test {small_bitmap cold rdbsave and rdbload with swap-bitmap-subkey-size exchange 4096 to 2048} {
        r flushdb
        set bak_bitmap_subkey_size [lindex [r config get swap-bitmap-subkey-size] 1]
        build_cold_small_bitmap small_bitmap1
        r SAVE
        # check if this bit will exist after loading
        r setbit small_bitmap1 16 1
        r CONFIG SET swap-bitmap-subkey-size 2048
        r DEBUG RELOAD NOSAVE
        # check_data
        check_small_bitmap_is_right small_bitmap0 small_bitmap1 $notextend
        r flushdb
        # reset default config
        r config set swap-bitmap-subkey-size $bak_bitmap_subkey_size
    }

    set bak_rdb_bitmap_enable [lindex [r config get swap-rdb-bitmap-encode-enabled] 1]
    r CONFIG SET swap-rdb-bitmap-encode-enabled no

    test {small_bitmap pur_hot/hot/cold rdbsave and rdbload for RDB_TYPE_STRING} {
        set builds {
            build_pure_hot_small_bitmap
            build_hot_small_bitmap
            build_cold_small_bitmap
        }
        foreach build $builds {
            $build small_bitmap1
            r debug reload
            # check it is string
            assert [object_is_string r small_bitmap1]
            assert [object_is_cold r small_bitmap1]
            check_small_bitmap_is_right small_bitmap0 small_bitmap1 $notextend
            r flushdb
        }
    }

    test {small_bitmap extend hot rdbsave and rdbload for RDB_TYPE_STRING} {
        r flushdb
        build_extend_hot_small_bitmap small_bitmap1
        r debug reload
        # check it is string
        assert [object_is_string r small_bitmap1]
        assert [object_is_cold r small_bitmap1]
        check_small_bitmap_is_right small_bitmap0 small_bitmap1 $extend
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
        check_mybitmap_is_right mybitmap1 $notextend
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
        check_mybitmap_is_right mybitmap1 $notextend
        r flushdb
        r config set swap-evict-step-max-subkeys $bak_evict_step
    }

    test {hot full swap out} {
        r flushdb
        build_hot_data mybitmap1
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1
        check_mybitmap_is_right mybitmap1 $notextend
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
        check_mybitmap_is_right mybitmap1 $notextend
        r flushdb
        r config set swap-evict-step-max-subkeys $bak_evict_step
    }

    test {extend hot full swap out} {
        r flushdb
        build_extend_hot_data mybitmap1
        assert [object_is_hot r mybitmap1]
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1
        check_mybitmap_is_right mybitmap1 $extend
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
        check_mybitmap_is_right mybitmap1 $extend
        r flushdb
        r config set swap-evict-step-max-subkeys $bak_evict_step
    }

    test {warm full swap out} {
        r flushdb
        foreach {key value} $build_warm_with_hole {
            foreach cmd $value {
                eval $cmd
            }
            r swap.evict mybitmap1
            wait_key_cold r mybitmap1
            check_mybitmap_is_right mybitmap1 $notextend
            r flushdb
        }
    }

    test {warm non full swap out} {
        r flushdb
        set bak_evict_step [lindex [r config get swap-evict-step-max-subkeys] 1]
        foreach {key value} $build_warm_with_hole {
            foreach cmd $value {
                eval $cmd
            }
            r config set swap-evict-step-max-subkeys 5
            after 100
            r swap.evict mybitmap1
            after 100
            r swap.evict mybitmap1
            wait_key_cold r mybitmap1
            check_mybitmap_is_right mybitmap1 $notextend
            r config set swap-evict-step-max-subkeys $bak_evict_step
            r flushdb
        }
    }

    test {bitmap swaps} {
        set builds {
            build_pure_hot_data
            build_hot_data
            build_cold_data
        }
        set cmds {
            check_mybitmap_setbit
            check_mybitmap_getbit
            check_mybitmap_bitcount
            check_mybitmap_bitpos
        }
        foreach build $builds {
            foreach cmd $cmds {
                foreach {key value} $cmd {
                    set mybitmap "mybitmap1"
                    $build $mybitmap
                    eval [replace_delimiter $value $mybitmap]
                    check_mybitmap_is_right $mybitmap $notextend
                    r flushdb
                }
            }
        }
    }

    test {pure hot bitpos corner case} {
        build_pure_hot_data mybitmap1
        check_bitpos_mybitmap mybitmap1
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
        check_mybitmap_is_right mybitmap1 $notextend
        r flushdb
    }

    test {pure hot bitop} {
        build_pure_hot_data mybitmap1
        check_mybitmap_bitop_xor_3_bitmap mybitmap1
        r flushdb
        build_pure_hot_data mybitmap1
        check_mybitmap_bitop_xor_2_bitmap mybitmap1
        r flushdb
    }

    test {hot bitpos corner case} {
        build_hot_data mybitmap1
        check_bitpos_mybitmap mybitmap1
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
        check_mybitmap_is_right mybitmap1 $notextend
        r flushdb
    }

    test {hot bitop} {
        build_hot_data mybitmap1
        check_mybitmap_bitop_xor_3_bitmap mybitmap1
        r flushdb
        build_hot_data mybitmap1
        check_mybitmap_bitop_xor_2_bitmap mybitmap1
        r flushdb
    }

    test {extend hot getbit} {
        build_extend_hot_data mybitmap1
        assert_equal {1} [r getbit mybitmap1 368639]
        check_mybitmap_is_right mybitmap1 $extend
        r flushdb
    }

    test {extend hot bitcount} {
        build_extend_hot_data mybitmap1
        assert_equal {12} [r bitcount mybitmap1]
        check_mybitmap_is_right mybitmap1 $extend
        r flushdb
    }

    test {extend hot bitpos} {
        build_extend_hot_data mybitmap1
        assert_equal {368639} [r bitpos mybitmap1 1 41984]
        check_mybitmap_is_right mybitmap1 $extend
        r flushdb

    }

    test {extend hot bitfield} {
        build_extend_hot_data mybitmap1
        assert_equal {-15}  [r BITFIELD mybitmap1 INCRBY i5 335871 1]
        assert_equal {13} [r bitcount mybitmap1]
        r flushdb
        build_extend_hot_data mybitmap1
        assert_equal {8}  [r bitfield_ro mybitmap1 get u4 65535]
        check_mybitmap_is_right mybitmap1 $extend
        r flushdb
    }

    test {extend hot bitop} {
        build_extend_hot_data mybitmap1
        check_extend_mybitmap_xor_3_bitmap mybitmap1
        r flushdb
        build_extend_hot_data mybitmap1
        check_mybitmap_bitop_xor_2_bitmap mybitmap1
        r flushdb
    }

    test {extend hot setbit} {
        r flushdb
        build_extend_hot_data mybitmap1

        assert_equal {0} [r setbit mybitmap1 4294967295 1]
        set_data mybitmap0 
        assert_equal {536870912} [r bitop XOR dest mybitmap1 mybitmap0]
        assert_equal {13} [r bitcount mybitmap1] 
        assert_equal {11} [r bitcount mybitmap0]
        assert_equal {2} [r bitcount dest] 
        assert_equal {4294967295} [r bitpos dest 1 -1] 

        r flushdb
    }

    test {warm getbit} {
        r flushdb
        # puts "r flushdb"
        foreach {outkey outvalue} $build_warm_with_hole {
            foreach {innkey innvalue} $check_mybitmap_getbit {
                foreach cmd $outvalue {
                    eval $cmd
                    # puts $cmd
                }
                set mybitmap "mybitmap1"
                # puts [replace_delimiter $innvalue $mybitmap]
                eval [replace_delimiter $innvalue $mybitmap]
                check_mybitmap_is_right $mybitmap $notextend
                r flushdb
                # puts "r flushdb"
            }
        }
    }

    test {warm bitcount} {
        r flushdb
        # puts "r flushdb"
        foreach {outkey outvalue} $build_warm_with_hole {
            foreach {innkey innvalue} $check_mybitmap_bitcount {
                foreach cmd $outvalue {
                    eval $cmd
                    # puts $cmd
                }
                set mybitmap "mybitmap1"
                # puts [replace_delimiter $innvalue $mybitmap]
                eval [replace_delimiter $innvalue $mybitmap]
                check_mybitmap_is_right $mybitmap $notextend
                r flushdb
                # puts "r flushdb"
            }
        }
    }

    test {warm bitpos} {
        foreach {key value} $build_warm_with_hole {
            foreach {key innvalue} $check_mybitmap_bitpos {
                r flushdb
                foreach cmd $value {
                    eval $cmd
                }
                set mybitmap "mybitmap1"
                # puts [replace_delimiter $innvalue $mybitmap]
                eval [replace_delimiter $innvalue $mybitmap]
                check_mybitmap_is_right $mybitmap $notextend
                r flushdb
            }
        }
    }

    test {warm bitpos corner case} {
        r flushdb
        foreach {key value} $build_warm_with_hole {
            foreach cmd $value {
                eval $cmd
            }
            check_bitpos_mybitmap mybitmap1
            set_data mybitmap0
            assert_equal {41984} [r bitop XOR dest mybitmap1 mybitmap0]
            assert_equal {18} [r bitcount mybitmap1 0 41983]
            assert_equal {11} [r bitcount mybitmap0]
            assert_equal {7} [r bitcount dest]
            r flushdb
        }
    }

    test {warm bitfield} {
        foreach {key value} $build_warm_with_hole {
            foreach cmd $value {
                eval $cmd
            }
            assert_equal {-15}  [r BITFIELD mybitmap1 INCRBY i5 335871 1]
            r flushdb
            foreach cmd $value {
                eval $cmd
            }
            assert_equal {8}  [r bitfield_ro mybitmap1 get u4 65535]
            check_mybitmap_is_right mybitmap1 $notextend
            r flushdb
        }
    }

    test {warm bitop} {
        foreach {key value} $build_warm_with_hole {
            foreach cmd $value {
                eval $cmd
            }
            check_mybitmap_bitop_xor_3_bitmap mybitmap1
            r flushdb
            foreach cmd $value {
                eval $cmd
            }
            check_mybitmap_bitop_xor_2_bitmap mybitmap1
            r flushdb
        }
    }

    test {cold bitpos corner case} {
        build_cold_data mybitmap1
        check_bitpos_mybitmap mybitmap1
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
        check_mybitmap_is_right mybitmap1 $notextend
        r flushdb
    }

    test {cold bitop} {
        build_cold_data mybitmap1
        check_mybitmap_bitop_xor_3_bitmap mybitmap1
        r flushdb
        build_cold_data mybitmap1
        check_mybitmap_bitop_xor_2_bitmap mybitmap1
        r flushdb
    }

    test {warm setbit} {
        r flushdb
        foreach {key outvalue} $build_warm_with_hole {
            foreach {key innvalue} $check_mybitmap1_setbit {
                r flushdb
                foreach cmd $outvalue {
                    eval $cmd
                }
                foreach cmd $innvalue {
                    eval $cmd
                }
                r flushdb
            }
        }
    }

    test {modify swap-bitmap-subkey-size during swap} {
        r flushdb
        set bak_bitmap_subkey_size [lindex [r config get swap-bitmap-subkey-size] 1]

        r CONFIG SET swap-bitmap-subkey-size 4096

        build_cold_data mybitmap1
        build_pure_hot_data mybitmap2

        r CONFIG SET swap-bitmap-subkey-size 2048

        r swap.evict mybitmap2
        wait_key_cold r mybitmap2

        # once bitmap become persisted, bitmap meta existed, subkey size is determined by configuration

        assert_equal [object_meta_subkey_size r mybitmap1] 4096
        assert_equal [object_meta_subkey_size r mybitmap2] 2048

        # mybitmap1 turn pure hot, then turn cold
        r getrange mybitmap1 0 1
        r getbit mybitmap1 0
        r swap.evict mybitmap1
        wait_key_cold r mybitmap1

        assert_equal [object_meta_subkey_size r mybitmap1] 2048

        check_mybitmap_is_right mybitmap1 $notextend
        check_mybitmap_is_right mybitmap2 $notextend

        r flushdb
        r config set swap-bitmap-subkey-size $bak_bitmap_subkey_size
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
        check_mybitmap_is_right mybitmap1 $notextend
        check_mybitmap_is_right mybitmap2 $notextend
        r flushdb
    }

    test {extend hot rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        # mybitmap 41kb
        build_extend_hot_data mybitmap1
        r debug reload
        # check_data
        check_mybitmap_is_right mybitmap1 $extend
        r flushdb
    }

    test {hot rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        # mybitmap 41kb
        build_hot_data mybitmap1
        r debug reload
        # check_data
        check_mybitmap_is_right mybitmap1 $notextend
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
        check_mybitmap_is_right mybitmap1 $notextend
        check_mybitmap_is_right mybitmap2 $notextend
        r flushdb
    }

    test {warm rdbsave and rdbload for RDB_TYPE_BITMAP} {
        # mybitmap 41kb
        foreach {key outvalue} $build_warm_with_hole {
            foreach cmd $outvalue {
                eval $cmd
            }
            r debug reload
            check_mybitmap_is_right mybitmap1 $notextend
            r flushdb
        }
    }

    test {cold rdbsave and rdbload with swap-bitmap-subkey-size exchange 2048 to 4096} {
        r flushdb
        set bak_bitmap_subkey_size [lindex [r config get swap-bitmap-subkey-size] 1]
        # mybitmap 41kb
        r CONFIG SET swap-bitmap-subkey-size 2048
        build_cold_data mybitmap1
        r SAVE
        # check if this bit will exist after loading
        r setbit mybitmap1 335872 1
        r CONFIG SET swap-bitmap-subkey-size 4096
        r DEBUG RELOAD NOSAVE
        # check_data
        check_mybitmap_is_right mybitmap1 $notextend
        r config set swap-bitmap-subkey-size $bak_bitmap_subkey_size
        r flushdb
    }

    test {cold rdbsave and rdbload with swap-bitmap-subkey-size exchange 4096 to 2048} {
        r flushdb
        set bak_bitmap_subkey_size [lindex [r config get swap-bitmap-subkey-size] 1]
        # mybitmap 41kb
        build_cold_data mybitmap1
        r SAVE
        # check if this bit will exist after loading
        r setbit mybitmap1 335872 1
        r CONFIG SET swap-bitmap-subkey-size 2048
        r DEBUG RELOAD NOSAVE
        # check_data
        check_mybitmap_is_right mybitmap1 $notextend
        r flushdb
        # reset default config
        r config set swap-bitmap-subkey-size $bak_bitmap_subkey_size
    }

    test {modify swap-bitmap-subkey-size during rdbsave and load} {
        r flushdb
        set bak_bitmap_subkey_size [lindex [r config get swap-bitmap-subkey-size] 1]

        r CONFIG SET swap-bitmap-subkey-size 4096

        build_cold_data mybitmap1
        build_pure_hot_data mybitmap2

        r CONFIG SET swap-bitmap-subkey-size 2048

        # once bitmap become persisted, bitmap meta existed, subkey size is determined by configuration

        assert_equal [object_meta_subkey_size r mybitmap1] 4096
        assert_equal [object_meta_subkey_size r mybitmap2] 0

        r SAVE
        r CONFIG SET swap-bitmap-subkey-size 1024
        r DEBUG RELOAD NOSAVE

        assert_equal [object_meta_subkey_size r mybitmap1] 1024
        assert_equal [object_meta_subkey_size r mybitmap1] 1024

        check_mybitmap_is_right mybitmap1 $notextend
        check_mybitmap_is_right mybitmap2 $notextend

        r flushdb
        r config set swap-bitmap-subkey-size $bak_bitmap_subkey_size
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
        check_mybitmap_is_right mybitmap1 $notextend
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
        check_mybitmap_is_right mybitmap1 $extend
        r flushdb
    }

    test {rdbsave and rdbload for RDB_TYPE_STRING} {
        set builds {
            build_hot_data
            build_cold_data
        }
        foreach build $builds {
            $build mybitmap1
            r debug reload
            # check it is string
            assert [object_is_cold r mybitmap1]
            assert [object_is_string r mybitmap1]
            check_mybitmap_is_right mybitmap1 $notextend
            r flushdb
        }
    }

    test {warm rdbsave and rdbload for RDB_TYPE_STRING} {
        # mybitmap 41kb
        foreach {key outvalue} $build_warm_with_hole {
            foreach cmd $outvalue {
                eval $cmd
            }
            r debug reload
            # check it is string 
            assert [object_is_cold r mybitmap1]
            assert [object_is_string r mybitmap1]
            check_mybitmap_is_right mybitmap1 $notextend
            r flushdb
        }
    }

    r config set swap-rdb-bitmap-encode-enabled $bak_rdb_bitmap_enable
}

start_server {
    tags {"empty bitmap test"}
}   {
    r config set swap-debug-evict-keys 0

    # check string bitmap switch
    test "empty pure hot string to bitmap" {
        r flushdb
        r set mykey ""

        assert_equal {0} [r bitcount mykey]
        assert [object_is_bitmap r mykey]
        assert [bitmap_object_is_pure_hot r mykey]

        assert_equal [r dbsize] 1 
    }

    test "empty cold string to bitmap" {
        r flushdb
        r set mykey ""
        r swap.evict mykey
        wait_key_cold r mykey

        assert_equal {0} [r bitcount mykey]
        assert [object_is_bitmap r mykey]
        assert [bitmap_object_is_pure_hot r mykey]

        assert_equal [r dbsize] 1 
    }

    test "empty hot string to bitmap" {
        r flushdb
        r set mykey ""
        r swap.evict mykey
        wait_key_cold r mykey
        r get mykey

        assert_equal {0} [r bitcount mykey]
        assert [object_is_bitmap r mykey]
        assert [bitmap_object_is_pure_hot r mykey]
    
        assert_equal [r dbsize] 1 
    }

    test "empty pure hot bitmap to string" {
        r flushdb
        r set mykey ""
        assert_equal {0} [r bitcount mykey]

        assert_equal {} [r get mykey]
        assert [object_is_string r mykey]

        assert_equal [r dbsize] 1 
    }

    test "empty cold bitmap to string" {
        r flushdb
        r set mykey ""
        assert_equal {0} [r bitcount mykey]
        r swap.evict mykey
        wait_key_cold r mykey

        assert_equal {} [r get mykey]
        assert [object_is_string r mykey]

        assert_equal [r dbsize] 1 
    }

    test "empty hot bitmap to string" {
        r flushdb
        r set mykey ""
        assert_equal {0} [r bitcount mykey]
        r swap.evict mykey
        wait_key_cold r mykey
        assert_equal {0} [r bitcount mykey]

        assert_equal {} [r get mykey]
        assert [object_is_string r mykey]

        assert_equal [r dbsize] 1 
    }

    # check empty bitmap swap out swap in
    test "pure hot swap out" {
        r flushdb
        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap 0 1]

        r swap.evict mybitmap
        wait_key_cold r mybitmap

        assert_equal [r dbsize] 1 
    }

    test "hot full swap out" {
        r flushdb
        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap 0 1]

        r swap.evict mybitmap
        wait_key_cold r mybitmap
        assert_equal {0} [r bitcount mybitmap 0 1]

        r swap.evict mybitmap
        wait_key_cold r mybitmap

        assert_equal [r dbsize] 1 
    }

    test "extend hot" {
        r flushdb
        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap 0 1]

        r swap.evict mybitmap
        wait_key_cold r mybitmap
        assert_equal {0} [r bitcount mybitmap 0 1]

        r setbit mybitmap 4096 1
        assert_equal {1} [r bitcount mybitmap]

        r swap.evict mybitmap
        wait_key_cold r mybitmap

        assert_equal {1} [r bitcount mybitmap]

        assert_equal [r dbsize] 1 
    }

    test "extend cold" {
        r flushdb
        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap 0 1]

        r swap.evict mybitmap
        wait_key_cold r mybitmap

        r setbit mybitmap 4096 1
        assert_equal {1} [r bitcount mybitmap]

        r swap.evict mybitmap
        wait_key_cold r mybitmap

        assert_equal {1} [r bitcount mybitmap]

        assert_equal [r dbsize] 1 
    }

    test "pure hot bitcount" {
        r flushdb
        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap]

        assert_equal {0} [r bitcount mybitmap 0 1]

        assert_equal [r dbsize] 1 
    }

    test "cold bitcount" {
        r flushdb
        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap]
        r swap.evict mybitmap
        wait_key_cold r mybitmap

        assert_equal {0} [r bitcount mybitmap 0 1]

        assert_equal [r dbsize] 1 
    }

    test "hot bitcount" {
        r flushdb
        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap]
        r swap.evict mybitmap
        wait_key_cold r mybitmap
        assert_equal {0} [r bitcount mybitmap]

        assert_equal {0} [r bitcount mybitmap 0 1]
        
        assert_equal [r dbsize] 1 
    }

    test {pure hot rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap 0 1]

        r debug reload
        # check_data
        assert_equal {0} [r bitcount mybitmap]

        assert_equal [r dbsize] 1 
        r flushdb
    }

    test {cold rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap 0 1]
        r swap.evict mybitmap
        wait_key_cold r mybitmap

        r debug reload
        # check_data
        assert_equal {0} [r bitcount mybitmap]

        assert_equal [r dbsize] 1 
        r flushdb
    }

    test {hot rdbsave and rdbload for RDB_TYPE_BITMAP} {
        r flushdb
        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap 0 1]

        r swap.evict mybitmap
        wait_key_cold r mybitmap
        assert_equal {0} [r bitcount mybitmap 0 1]

        r debug reload
        # check_data
        assert_equal {0} [r bitcount mybitmap]

        assert_equal [r dbsize] 1 
        r flushdb
    }

    test {cold rdbsave and rdbload with swap-bitmap-subkey-size exchange 2048 to 4096} {
        r flushdb
        set bak_bitmap_subkey_size [lindex [r config get swap-bitmap-subkey-size] 1]
        r CONFIG SET swap-bitmap-subkey-size 2048

        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap]

        r swap.evict mybitmap
        wait_key_cold r mybitmap

        r SAVE
        # check if this bit will exist after loading, expect it not exist
        r setbit mybitmap 16 1
        r CONFIG SET swap-bitmap-subkey-size 4096
        r DEBUG RELOAD NOSAVE
        # check_data
        assert_equal {0} [r bitcount mybitmap]

        assert_equal [r dbsize] 1 
        r config set swap-bitmap-subkey-size $bak_bitmap_subkey_size

        r flushdb
    }

    test {cold rdbsave and rdbload with swap-bitmap-subkey-size exchange 4096 to 2048} {
        r flushdb
        set bak_bitmap_subkey_size [lindex [r config get swap-bitmap-subkey-size] 1]

        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap]

        r swap.evict mybitmap
        wait_key_cold r mybitmap

        r SAVE
        # check if this bit will exist after loading, expect it not exist
        r setbit small_bitmap1 16 1
        r CONFIG SET swap-bitmap-subkey-size 2048
        r DEBUG RELOAD NOSAVE
        # check_data
        assert_equal {0} [r bitcount mybitmap]
        assert_equal [r dbsize] 1 

        r flushdb
        # reset default config
        r config set swap-bitmap-subkey-size $bak_bitmap_subkey_size
    }

    set bak_rdb_bitmap_enable [lindex [r config get swap-rdb-bitmap-encode-enabled] 1]
    r CONFIG SET swap-rdb-bitmap-encode-enabled no

    test {pure hot rdbsave and rdbload for RDB_TYPE_STRING} {
        r flushdb
        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap 0 1]
        r debug reload
        # check it is string
        assert [object_is_string r mybitmap]
        assert [object_is_cold r mybitmap]
        assert_equal {0} [r bitcount mybitmap]

        assert_equal [r dbsize] 1 
        r flushdb
    }

    test {cold rdbsave and rdbload for RDB_TYPE_STRING} {
        r flushdb
        r set mybitmap ""
        assert_equal {0} [r bitcount mybitmap]

        r swap.evict mybitmap
        wait_key_cold r mybitmap
        r debug reload
        # check it is string
        assert [object_is_string r mybitmap]
        assert [object_is_cold r mybitmap]
        assert_equal {0} [r bitcount mybitmap]
    
        assert_equal [r dbsize] 1 
        r flushdb
    }

    r config set swap-rdb-bitmap-encode-enabled $bak_rdb_bitmap_enable
} 

start_server {tags {"bitmap chaos test"} overrides {save ""}} {
    start_server {overrides {swap-repl-rordb-sync no}} {
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

                set min_subkey_size 2048
                set max_subkey_size 4096

                for {set loader 0} {$loader < $loaders} {incr loader} {
                    set subkey_size_slave [expr { int(rand() * ($max_subkey_size - $min_subkey_size + 1)) + $min_subkey_size }]
                    $slave config set swap-bitmap-subkey-size $subkey_size_slave

                    lappend load_handles [start_run_load $master_host $master_port $duration 0 {
                        set bitmaps 4
                        # in bit
                        set bitmap_max_length 335872
                        set block_timeout 0.1
                        set count 0
                        set mybitmap "mybitmap-[randomInt $bitmaps]"

                        set min_subkey_size 2048
                        set max_subkey_size 4096

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
                            set subkey_size_master [expr { int(rand() * ($max_subkey_size - $min_subkey_size + 1)) + $min_subkey_size }]
                            $r1 config set swap-bitmap-subkey-size $subkey_size_master
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