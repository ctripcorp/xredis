start_server {
    tags {"bitmap"}
} {
    test {pure hot full swap out} {
        setbit mybitmap1 40960 1
        swap.evict mybitmap1
        assert_equal {1} [r getbit mybitmap1 40960]
        setbit mybitmap1 40960 0
        swap.evict mybitmap1
        assert_equal {0} [r getbit mybitmap1 40960]
        del mybitmap1
        assert_equal {0} [r getbit mybitmap1 40960]
    }

    test {pure hot non full swap out} {
        set bak_evict_step [lindex [r config get swap-evict-step-max-memory] 1]
        r config set swap-evict-step-max-memory 5120
        # build mybitmap1 10kb size
        setbit mybitmap1 81919 1
        setbit mybitmap1 0 1
        swap.evict mybitmap1
        assert_equal {1} [r getbit mybitmap1 81919]
        assert [object_is_warm r mybitmap1]
        del mybitmap1
        assert_equal {0} [r getbit mybitmap1 81919]

        r config set swap-evict-step-max-memory $bak_evict_step
    }

    test {warm non full swap out} {
        # build warm data
        set bak_evict_step [lindex [r config get swap-evict-step-max-memory] 1]
        # swap fragment 5kb
        r config set swap-evict-step-max-memory 5120
        # build mybitmap1 20kb size
        setbit mybitmap1 163839 1
        setbit mybitmap1 0 1
        swap.evict mybitmap1
        assert_equal {1} [r getbit mybitmap1 163839]
        assert [object_is_warm r mybitmap1]

        # swap out fragment {0, 3}
        assert_equal {1} [r getbit mybitmap1 0]
        assert [object_is_warm r mybitmap1]
        # fragment {0, 3} is warm
        swap.evict mybitmap1

        del mybitmap1
        assert_equal {0} [r getbit mybitmap1 40960]

        r config set swap-evict-step-max-memory $bak_evict_step
    }

    test {warm full swap out} {
        # build warm data
        # build mybitmap1 20kb size
        # 5 fragment
        setbit mybitmap1 163839 1
        setbit mybitmap1 0 1
        swap.evict mybitmap1
        assert_equal {1} [r getbit mybitmap1 163839]
        assert [object_is_warm r mybitmap1]

        # swap out fragment {0, 3}
        assert_equal {1} [r getbit mybitmap1 0]
        assert [object_is_warm r mybitmap1]
        # fragment {0, 3} is warm
        swap.evict mybitmap1

        del mybitmap1
        assert_equal {0} [r getbit mybitmap1 40960]
    }

    test {warm non full swap in} {
        # build warm data
        set bak_evict_step [lindex [r config get swap-evict-step-max-memory] 1]
        # swap fragment 5kb, count by byte
        r config set swap-evict-step-max-memory 5120
        # build mybitmap1 20kb size, count by bit
        # 4 fragment
        setbit mybitmap1 163839 1
        setbit mybitmap1 0 1
        swap.evict mybitmap1
        assert_equal {1} [r getbit mybitmap1 163839]
        # fragment {0, 1, 2} is cold
        assert_equal {2} [r bitcount mybitmap1]
        del mybitmap1
        assert_equal {0} [r getbit mybitmap1 40960]
    }

    test {warm full swap in} {
        # build warm data
        # build mybitmap1 20kb size, count by bit
        # 5 fragment
        setbit mybitmap1 163839 1
        setbit mybitmap1 0 1
        swap.evict mybitmap1
        assert_equal {1} [r getbit mybitmap1 163839]
        # fragment {0, 1, 2, 3} is cold
        assert_equal {2} [r bitcount mybitmap1]
        del mybitmap1
        assert_equal {0} [r getbit mybitmap1 40960]
    }

    test {cold full swap in} {
        # build cold data
        # build mybitmap1 20kb size, count by bit
        setbit mybitmap1 163839 1
        setbit mybitmap1 0 1
        swap.evict mybitmap1

        assert_equal {2} [r bitcount mybitmap1]
        del mybitmap1
        assert_equal {0} [r getbit mybitmap1 40960]
    }

    test {cold non full swap in} {
        # build cold data
        # build mybitmap1 20kb size, count by bit
        setbit mybitmap1 163839 1
        setbit mybitmap1 0 1
        swap.evict mybitmap1

        assert_equal {1} [r getbit mybitmap1 0]
        assert [object_is_warm r mybitmap1]
        del mybitmap1
        assert_equal {0} [r getbit mybitmap1 40960]
    }
}